"""ADR 0100 (#2877): the worker is the sole issuer of a turn's channel read capability.

These tests drive the real ``Kernel.process_event`` against real Valkey and the
scriptable HTTP runner in ``conftest.py``. The binding double plays the API's
context route and writes the ledger through the API's own ``ChannelReadLedger``
over the same Valkey; revocation is asserted on those keys, never on the double.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import io
import json
import re
import secrets
import socket
import sys
import time
import uuid
import zipfile
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import Final, QueuedTurn, SessionStatus, TextDelta
from curie_internal import channel_read_ledger as _ledger
from curie_worker.binding import BindingResolver
from curie_worker.config import WorkerConfig
from curie_worker.kernel import channel_read as kernel_channel_read
from curie_worker.kernel.constants import RETRYABLE_CLASSIFICATIONS
from curie_worker.kernel.routing import _thread_key_for as _thread_key
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from queue_fixtures import wait_until as _wait_until  # noqa: E402
from test_memory_token_event import (  # noqa: E402
    AGENT_ID,
    CHANNEL,
    DEPLOYMENT_ID,
    _MemoryBinding,
    _qevent,
)

_RUNNER_FACING = "http://curie-api.test:8000/"
_CAPABILITY_URL = "http://curie-api.test:8000/channel-read"
_REFUSAL = (
    "This agent cannot start: its runner cannot enforce channel read for this turn, "
    "so the turn was not run."
)
_OWNER = re.compile(r"^[A-Za-z0-9_-]{16,64}$")
_REDEPLOYED = uuid.UUID("44444444-4444-4444-8444-444444444444")


async def _until_gone(
    get: Callable[[], Awaitable[object]], what: str, timeout: float = 5.0
) -> None:
    """Poll ``get`` until it answers None; fail naming ``what`` after ``timeout``."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await get() is None:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"timed out waiting for {what}")


class _ChannelReadApi:
    """Plays ``POST /v1/internal/channel-read/context`` over the real shared Valkey.

    An open mint runs the API ledger's open; a steer mint bumps the generation
    only while the active key exists, else answers None as the real binding does
    on a 409 ``channel_read.turn_inactive``.
    """

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.mints: list[Any] = []
        self.down = False  # every later mint answers as with the API unreachable
        self.steer_gate: asyncio.Event | None = None
        self.steer_entered = asyncio.Event()
        # An open commits, then loses its response: "returns-none", "raises-timeout", "hangs".
        self.lose_open_response: str | None = None
        self.open_committed = asyncio.Event()
        self.times_out = False  # an open times out without committing
        # An open commits only once this gate opens, after the worker gave up.
        self.late_gate: asyncio.Event | None = None
        self.open_requested = asyncio.Event()
        self.late_commit: asyncio.Future[Any] | None = None
        self._redis: Any = None
        self._prefix: str | None = None

    def attach(self, h: Any) -> None:
        self._redis = h.async_redis
        self._prefix = h.config.key_prefix

    def keys(self, agent_id: uuid.UUID, turn_key: str) -> tuple[str, str, str, str]:
        assert self._prefix is not None, "attach the harness first"
        return _ledger.ledger_keys(self._prefix, agent_id, turn_key)

    async def active(self, agent_id: uuid.UUID, turn_key: str) -> str | None:
        value = await self._redis.get(self.keys(agent_id, turn_key)[1])
        return None if value is None else str(value)

    def _api_ledger(self) -> Any:
        # The API's own ledger, so the double writes exactly what the API writes.
        from curie_api.channel_read.ledger import ChannelReadLedger

        assert self._prefix is not None, "attach the harness first"
        return ChannelReadLedger(self._redis, self._prefix)

    async def open_ledger(self, agent_id: uuid.UUID, turn: str, owner: str, ttl_s: int) -> int:
        gen = await self._api_ledger().open(agent_id, turn, owner, ttl_s, resume=False)
        assert isinstance(gen, int), gen
        return gen

    async def _commit_late(self, agent_id: uuid.UUID, turn: str, owner: str, ttl_s: int) -> Any:
        assert self.late_gate is not None
        await self.late_gate.wait()
        return await self._api_ledger().open(agent_id, turn, owner, ttl_s, resume=False)

    async def context(self, **kwargs: Any) -> Any:
        from curie_worker.binding import ChannelReadMint

        self.calls.append(dict(kwargs))
        mode = kwargs["mode"]
        if mode == "steer" and self.steer_gate is not None:
            self.steer_entered.set()
            await self.steer_gate.wait()
        if self.down:
            return None
        agent_id, event_id = kwargs["agent_id"], kwargs["event_id"]
        turn_key = _ledger.turn_key(event_id)
        ttl_s = int(kwargs["ttl_s"])
        now = int(time.time())
        if mode == "open" and self.late_gate is not None:
            self.late_commit = asyncio.ensure_future(
                self._commit_late(agent_id, event_id, kwargs["owner"], ttl_s)
            )
            self.open_requested.set()
            await asyncio.Event().wait()
        if mode == "open" and self.times_out:
            raise TimeoutError("the context request timed out")
        if mode == "open":
            gen = await self.open_ledger(agent_id, event_id, kwargs["owner"], ttl_s)
            expires_at = now + ttl_s
            if self.lose_open_response == "returns-none":
                return None  # the real binding answers a client timeout with None
            if self.lose_open_response == "raises-timeout":
                raise TimeoutError("the context response timed out")
            if self.lose_open_response == "hangs":
                self.open_committed.set()
                await asyncio.Event().wait()
        else:
            bumped = await self._api_ledger().steer(agent_id, event_id, ttl_s)
            if bumped is None:
                return None
            gen, pttl_ms = bumped
            expires_at = now + min(ttl_s, max(1, pttl_ms // 1000))
        claims = json.dumps({"turn": turn_key, "gen": gen}).encode()
        body = base64.urlsafe_b64encode(claims).decode().rstrip("=")
        token = f"chr.{body}.{secrets.token_urlsafe(32)}"
        mint = ChannelReadMint(
            token=token, generation=gen, expires_at=expires_at, turn_key=turn_key
        )
        self.mints.append(mint)
        return mint

    def of_mode(self, mode: str) -> list[dict[str, Any]]:
        return [call for call in self.calls if call["mode"] == mode]


def _zip(manifest: str | None) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("README.md", "no plugin here")
        if manifest is not None:
            archive.writestr(".claude-plugin/plugin.json", manifest)
    return buffer.getvalue()


def _bundle_zip(*, granted: bool) -> bytes:
    return _zip(json.dumps({"name": "acme-bot", **({"channelRead": True} if granted else {})}))


class _Bundles:
    """A ``BundleReader`` serving granted bundles from memory, recording reads.

    ``ungranted`` keys serve no grant, ``fail`` raises as an object store outage
    does, ``raw`` serves bytes as is, and ``scripted`` serves (delay, granted)
    per read in order."""

    def __init__(self) -> None:
        self.reads: list[str] = []
        self.ungranted: set[str] = set()
        self.fail = False
        self.raw: dict[str, bytes] = {}
        self.scripted: list[tuple[float, bool]] = []

    def get(self, key: str) -> bytes:
        self.reads.append(key)
        if self.fail:
            raise OSError("the object store is unavailable")
        if key in self.raw:
            return self.raw[key]
        if self.scripted:
            delay_s, granted = self.scripted.pop(0)
            time.sleep(delay_s)
            return _bundle_zip(granted=granted)
        return _bundle_zip(granted=key not in self.ungranted)


_BUNDLE_V1 = "bundles/acme-bot/v1.zip"
_BUNDLE_V2 = "bundles/acme-bot/v2.zip"


class _ChannelReadBinding(_MemoryBinding):
    """The memory test's binding (real ``ResolvedDeployment``) plus the API seam."""

    def __init__(self) -> None:
        super().__init__(memory_writes=False)
        self.api = _ChannelReadApi()
        self.bundles = _Bundles()
        # What ``resolve`` answers; a test redeploys by changing these.
        self.deployment_id = DEPLOYMENT_ID
        self.bundle_ref = _BUNDLE_V1

    def attach(self, h: Any) -> None:
        self.api.attach(h)
        h.kernel._bundles = self.bundles  # the kernel reads the grant from the manifest

    async def resolve(self, kind: str, adapter: str | None, channel: str) -> object:
        resolved = await super().resolve(kind, adapter, channel)
        return resolved.model_copy(  # type: ignore[attr-defined]
            update={"deployment_id": self.deployment_id, "bundle_ref": self.bundle_ref}
        )

    async def channel_read_context(self, **kwargs: Any) -> Any:
        return await self.api.context(**kwargs)


class _UnreachableApiBinding(_ChannelReadBinding):
    """Mints through the REAL resolver method, aimed at a port nothing serves."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[dict[str, Any]] = []
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            port = int(sock.getsockname()[1])
        self._real._config = WorkerConfig(  # type: ignore[attr-defined]
            api_base_url=f"http://127.0.0.1:{port}",
            internal_worker_token="test-worker-token-2877",
        )

    async def channel_read_context(self, **kwargs: Any) -> Any:
        self.calls.append(dict(kwargs))
        return await self._real.channel_read_context(**kwargs)  # type: ignore[attr-defined]


def _run(
    make_harness: Any,
    body: Callable[[Any, _ChannelReadBinding], Awaitable[None]],
    *,
    enforced: object = True,
    binding: _ChannelReadBinding | None = None,
    **harness_kw: Any,
) -> None:
    """Run ``body(h, binding)`` on a harness wired to a channel read binding."""

    async def go() -> None:
        b = binding or _ChannelReadBinding()
        async with make_harness(binding=b, **harness_kw) as h:
            b.attach(h)
            h.runner.channel_read_enforced = enforced
            await body(h, b)

    asyncio.run(go())


async def _live_record(h: Any, turn: QueuedTurn) -> str | None:
    value = await h.async_redis.get(h.config.channel_read_turn_key(_thread_key(turn)))
    return None if value is None else str(value)


@contextlib.asynccontextmanager
async def _live(h: Any, turn: QueuedTurn) -> AsyncIterator[asyncio.Task[Any]]:
    """Hold ``turn`` live on the runner for the block, then let it finish."""
    h.runner.hold = asyncio.Event()
    h.runner.default_script = [TextDelta(text="working")]
    h.runner.tail = [Final(text="done", status=SessionStatus.DONE)]
    task = asyncio.create_task(h.kernel.process_event(turn))
    try:
        await _wait_until(lambda: h.runner.turn_active, "the turn to be live")
        yield task
    finally:
        h.runner.hold.set()
        await asyncio.gather(task, return_exceptions=True)


async def _until_revoked(
    b: _ChannelReadBinding, turn_key: str, what: str, timeout: float = 5.0
) -> None:
    await _until_gone(lambda: b.api.active(AGENT_ID, turn_key), what, timeout)


def _classification(h: Any) -> str:
    text = h.sink.last_text
    assert text is not None and text.startswith("curie-turn-failure: "), text
    return str(text.split("\n", 1)[0].removeprefix("curie-turn-failure: "))


def _assert_refused_unenforced(h: Any, opened: list[str]) -> None:
    assert h.runner.opened == opened, "a runner that cannot enforce was sent the turn"
    text = h.sink.last_text
    assert text is not None and text.startswith("curie-turn-failure: channel-read-unenforced\n\n")
    assert _REFUSAL in text


def _fail_marker(h: Any, mp: pytest.MonkeyPatch, name: str, exc: Exception | None = None) -> None:
    async def failing(*args: Any, **kwargs: Any) -> Any:
        raise exc or RedisConnectionError("Valkey is unavailable")

    mp.setattr(h.kernel._markers, name, failing)


_PIN = {"read": "read_channel_read_pin", "write": "pin_channel_read_deployment"}
_LIVE_RECORD = "record_channel_read_turn"


def test_a_granted_turn_carries_the_capability(make_harness: Any) -> None:
    async def body(h: Any, b: _ChannelReadBinding) -> None:
        turn = _qevent("what did we decide yesterday", thread="th-cr-1")
        await h.kernel.process_event(turn)

        assert h.runner.opened == [turn.text]
        assert h.sink.last_text == "ok"
        (call,) = b.api.calls
        assert call["mode"] == "open"
        assert call["agent_id"] == AGENT_ID
        assert call["deployment_id"] == DEPLOYMENT_ID
        assert call["event_id"] == turn.event_id
        assert tuple(call["default"]) == ("slack", CHANNEL)
        assert isinstance(call["owner"], str) and _OWNER.fullmatch(call["owner"])
        assert isinstance(call["ttl_s"], int) and 1 <= call["ttl_s"] <= 86400
        assert h.runner.event_bodies[0]["channel_read"] == {
            "url": _CAPABILITY_URL,
            "token": b.api.mints[0].token,
        }

    _run(make_harness, body, runner_api_base_url=_RUNNER_FACING)


def test_a_failed_mint_runs_the_turn_without_capability(make_harness: Any) -> None:
    # Liveness: the real resolver method cannot reach the API; the turn runs uncovered.
    binding = _UnreachableApiBinding()

    async def body(h: Any, b: _ChannelReadBinding) -> None:
        await h.kernel.process_event(_qevent("hello", thread="th-cr-3"))

        assert len(binding.calls) == 1, binding.calls
        assert h.runner.opened == ["hello"]
        assert h.runner.event_bodies[0]["channel_read"] is None
        assert h.sink.last_text == "ok"

    _run(make_harness, body, binding=binding)


@pytest.mark.parametrize(
    ("fault", "advertised"),
    [
        ("none", None),
        ("none", False),
        ("none", "true"),
        ("known-grant-api-down", None),
        ("cold-mint-times-out", None),
        ("live-record-fails", None),
        ("pin-read", None),
        ("pin-write", None),
    ],
)
def test_a_granted_turn_on_a_runner_not_advertising_true_is_refused(
    make_harness: Any, monkeypatch: pytest.MonkeyPatch, fault: str, advertised: object
) -> None:
    # Only the literal true passes, and no mint, pin or record fault lets the turn through.
    async def body(h: Any, b: _ChannelReadBinding) -> None:
        if fault == "known-grant-api-down":
            await h.kernel.process_event(_qevent("first", thread="th-cr-r4a"))
            assert h.runner.event_bodies[0]["channel_read"] is not None
            b.api.down = True
        elif fault == "cold-mint-times-out":
            b.api.times_out = True
        elif fault == "live-record-fails":
            _fail_marker(h, monkeypatch, _LIVE_RECORD, RedisError("live record write failed"))
        elif fault != "none":
            _fail_marker(h, monkeypatch, _PIN[fault.removeprefix("pin-")])
        warmed = list(h.runner.opened)
        h.runner.channel_read_enforced = advertised
        turn = _qevent("summarize", thread=f"th-cr-f-{fault}")

        await h.kernel.process_event(turn)

        assert b.bundles.reads, "the grant was not read from the bundle"
        _assert_refused_unenforced(h, warmed)
        if fault == "none":
            assert [c.outcome for c in h.sink.completions][-1] == "escalated"
            assert len(b.api.calls) == 1, "the unenforced refusal was retried"
            await _until_revoked(b, b.api.mints[0].turn_key, "the refused turn's revoke")
            await _until_gone(lambda: _live_record(h, turn), "the refused turn's record deletion")

    _run(make_harness, body, max_attempts=3)


def test_a_steer_renews_the_same_logical_turn_and_never_revokes(
    make_harness: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def body(h: Any, b: _ChannelReadBinding) -> None:
        revokes: list[tuple[uuid.UUID, str, str]] = []
        real_revoke = h.kernel._markers.revoke_channel_read_owner

        async def spy(agent_id: uuid.UUID, turn_key: str, owner: str) -> bool:
            revokes.append((agent_id, turn_key, owner))
            return bool(await real_revoke(agent_id, turn_key, owner))

        monkeypatch.setattr(h.kernel._markers, "revoke_channel_read_owner", spy)
        first = _qevent("first", thread="th-cr-6", placeholder="ph-1")
        async with _live(h, first):
            record_before = await _live_record(h, first)
            assert record_before is not None, "the opener recorded no live turn"
            await h.kernel.process_event(_qevent("and this", thread="th-cr-6", placeholder="ph-2"))

            assert h.runner.steers == ["and this"]
            (opened,) = b.api.of_mode("open")
            (steered,) = b.api.of_mode("steer")
            # The opener's logical turn and default, a new generation, no owner.
            assert steered["event_id"] == first.event_id
            assert tuple(steered["default"]) == tuple(opened["default"]) == ("slack", CHANNEL)
            assert steered["agent_id"] == AGENT_ID
            assert steered["deployment_id"] == DEPLOYMENT_ID
            assert steered.get("owner") is None
            open_mint, steer_mint = b.api.mints
            assert steer_mint.turn_key == open_mint.turn_key
            assert steer_mint.generation == open_mint.generation + 1
            assert h.runner.steer_bodies[-1]["channel_read"] == {
                "url": _CAPABILITY_URL,
                "token": steer_mint.token,
            }
            await asyncio.sleep(0.2)  # give a wrongly eager revoke the chance to land
            assert revokes == [], "the steer attempt revoked"
            assert await b.api.active(AGENT_ID, open_mint.turn_key) == (
                f"{opened['owner']}:{steer_mint.generation}"
            )
            assert await _live_record(h, first) == record_before, "the steer wrote the record"

        # Only the opener's owner revokes, and that kills the steer generation too.
        await _until_revoked(b, open_mint.turn_key, "the opener's end to revoke the steer gen")
        assert set(revokes) == {(AGENT_ID, open_mint.turn_key, opened["owner"])}

    _run(make_harness, body, runner_api_base_url=_RUNNER_FACING)


def test_terminal_then_steer_gets_no_capability(make_harness: Any) -> None:
    # The opener's revoke lands while the steer mint is in flight: the steer gets null.
    async def body(h: Any, b: _ChannelReadBinding) -> None:
        thread = "th-cr-11a"
        steer_task: asyncio.Task[Any] | None = None
        b.api.steer_gate = asyncio.Event()
        try:
            async with _live(h, _qevent("first", thread=thread, placeholder="ph-1")):
                owner = b.api.of_mode("open")[0]["owner"]
                turn_key = b.api.mints[0].turn_key
                steer_task = asyncio.create_task(
                    h.kernel.process_event(_qevent("and this", thread=thread, placeholder="ph-2"))
                )
                await asyncio.wait_for(b.api.steer_entered.wait(), 5)
                revoke = h.kernel._markers.revoke_channel_read_owner
                assert await revoke(AGENT_ID, turn_key, owner) is True
                b.api.steer_gate.set()
                await asyncio.wait_for(steer_task, 10)

                assert h.runner.steers == ["and this"]
                assert h.runner.steer_bodies[-1]["channel_read"] is None
                assert len(b.api.mints) == 1, "the steer got a capability"
                assert await b.api.active(AGENT_ID, turn_key) is None
        finally:
            b.api.steer_gate.set()
            if steer_task is not None:
                await asyncio.gather(steer_task, return_exceptions=True)

    _run(make_harness, body)


def test_the_openers_end_revokes_through_the_bound_close_with_the_api_down(
    make_harness: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Revocation is the owner checked delete in Valkey, so it lands with the API down.
    from curie_worker.kernel.core import Kernel

    assert Kernel._close_channel_read is kernel_channel_read._close_channel_read
    assert Kernel._settle_channel_read is kernel_channel_read._settle_channel_read

    async def body(h: Any, b: _ChannelReadBinding) -> None:
        closes: list[Any] = []
        real_close = h.kernel._close_channel_read

        async def spy(record: Any) -> None:
            closes.append(record)
            await real_close(record)

        monkeypatch.setattr(h.kernel, "_close_channel_read", spy)
        turn = _qevent("long one", thread="th-cr-8")
        async with _live(h, turn):
            b.api.down = True
            mint = b.api.mints[0]
            assert await b.api.active(AGENT_ID, mint.turn_key) is not None

        await _until_revoked(b, mint.turn_key, "the revoke to land with the API down")
        assert mint.expires_at > time.time(), "the key may only have expired"
        assert await _live_record(h, turn) is None
        (record,) = closes
        assert record.owner == b.api.of_mode("open")[0]["owner"]
        assert record.opened == [(AGENT_ID, mint.turn_key, _thread_key(turn))]

    _run(make_harness, body)


@pytest.mark.parametrize("ending", ["success", "runner-error-then-retry", "cancelled"])
def test_every_attempt_end_revokes_and_deletes_the_record(make_harness: Any, ending: str) -> None:
    async def body(h: Any, b: _ChannelReadBinding) -> None:
        turn = _qevent("one turn", thread=f"th-cr-8-{ending}")
        if ending == "cancelled":
            async with _live(h, turn) as task:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        else:
            if ending == "runner-error-then-retry":
                h.runner.event_fail_times = 1
            await h.kernel.process_event(turn)
        if ending == "runner-error-then-retry":
            owners = [c["owner"] for c in b.api.of_mode("open")]
            assert len(owners) == 2 and owners[0] != owners[1], owners

        assert b.api.mints, "nothing was minted"
        await _until_revoked(b, b.api.mints[0].turn_key, f"the {ending} attempt end to revoke")
        await _until_gone(lambda: _live_record(h, turn), f"the {ending} end to delete the record")

    _run(make_harness, body, max_attempts=3)


def test_two_attempt_records_have_distinct_owners(make_harness: Any) -> None:
    # An approval resume opens the same logical turn under a new owner; the
    # original attempt's late settlement must not revoke it.
    from curie_worker.kernel.channel_read import _AttemptChannelRead

    first, resumed = _AttemptChannelRead(), _AttemptChannelRead()
    assert first.owner != resumed.owner
    assert _OWNER.fullmatch(first.owner) and _OWNER.fullmatch(resumed.owner)
    assert first.opened is not resumed.opened
    assert not first.pending() and not resumed.pending()

    async def go() -> None:
        api = _ChannelReadApi()
        async with make_harness() as h:
            api.attach(h)
            event = _qevent("needs approval", thread="th-cr-9")
            opened = (AGENT_ID, _ledger.turn_key(event.event_id), _thread_key(event))
            await api.open_ledger(AGENT_ID, event.event_id, first.owner, 600)
            first.opened.append(opened)
            resumed_gen = await api.open_ledger(AGENT_ID, event.event_id, resumed.owner, 600)
            resumed.opened.append(opened)
            assert first.pending() and resumed.pending()
            assert resumed.opened == [opened]

            revoke = h.kernel._markers.revoke_channel_read_owner
            assert await revoke(AGENT_ID, opened[1], first.owner) is False
            await h.kernel._close_channel_read(first)
            assert await api.active(AGENT_ID, opened[1]) == f"{resumed.owner}:{resumed_gen}"
            await h.kernel._close_channel_read(resumed)
            assert await api.active(AGENT_ID, opened[1]) is None

    asyncio.run(go())


@pytest.mark.parametrize(
    ("fault", "granted"),
    [
        ("live-record", True),
        ("pin-read", True),
        ("pin-write", True),
        ("none", False),
        ("pin-read", False),
        ("pin-write", False),
    ],
)
def test_an_able_runner_runs_without_capability_when_ungranted_or_faulted(
    make_harness: Any, monkeypatch: pytest.MonkeyPatch, fault: str, granted: bool
) -> None:
    # Granted on an enforcing runner with a fault, or ungranted with nothing to enforce.
    # A failed live record write revokes before the runner sees the event.
    async def body(h: Any, b: _ChannelReadBinding) -> None:
        if not granted:
            b.bundles.ungranted.add(_BUNDLE_V1)
        if fault == "live-record":
            _fail_marker(h, monkeypatch, _LIVE_RECORD, RedisError("live record write failed"))
        elif fault != "none":
            _fail_marker(h, monkeypatch, _PIN[fault.removeprefix("pin-")])
        active_at_start: list[str | None] = []
        real_start = h.kernel._runner.start_turn

        async def spy(base_url: str, event: Any, **kwargs: Any) -> Any:
            for mint in b.api.mints:
                active_at_start.append(await b.api.active(AGENT_ID, mint.turn_key))
            return await real_start(base_url, event, **kwargs)

        monkeypatch.setattr(h.kernel._runner, "start_turn", spy)
        await h.kernel.process_event(_qevent("summarize", thread=f"th-cr-p-{fault}-{granted}"))

        assert b.bundles.reads, "the grant was not read from the bundle"
        assert h.runner.opened == ["summarize"]
        assert h.runner.event_bodies[0]["channel_read"] is None
        assert h.sink.last_text == "ok"
        assert all(a is None for a in active_at_start), "the runner saw a live capability"
        if fault == "live-record":
            assert len(b.api.mints) == 1 and active_at_start == [None], b.api.calls
        if not granted:
            assert b.api.calls == [], b.api.calls

    _run(make_harness, body, enforced=True if granted else None)


@pytest.mark.parametrize("lost", ["returns-none", "raises-timeout", "hangs"])
def test_a_lost_mint_response_is_still_revoked_by_owner(make_harness: Any, lost: str) -> None:
    # The API committed the open but the worker never saw the response.
    async def body(h: Any, b: _ChannelReadBinding) -> None:
        b.api.lose_open_response = lost
        turn = _qevent("hello", thread=f"th-cr-r5-{lost}")
        turn_key = _ledger.turn_key(turn.event_id)
        if lost == "hangs":
            task = asyncio.create_task(h.kernel.process_event(turn))
            try:
                await asyncio.wait_for(b.api.open_committed.wait(), 5)
                assert await b.api.active(AGENT_ID, turn_key) is not None
                task.cancel()
            finally:
                await asyncio.gather(task, return_exceptions=True)
        else:
            await h.kernel.process_event(turn)
            assert h.runner.event_bodies[0]["channel_read"] is None
        owner = b.api.of_mode("open")[0]["owner"]
        await _until_revoked(b, turn_key, f"the {lost} open of owner {owner} to be revoked")

    _run(make_harness, body)


class _TargetlessResolver(BindingResolver):
    """The real resolver over real Postgres, with the API seam played."""

    def __init__(self, engine: Any, config: WorkerConfig) -> None:
        super().__init__(engine, config)
        self.api = _ChannelReadApi()

    async def channel_read_context(self, **kwargs: Any) -> Any:  # type: ignore[override]
        return await self.api.context(**kwargs)


def test_a_targetless_hook_turn_mints_with_a_null_default(
    make_harness: Any, make_hook_run: Any
) -> None:
    from test_targetless_cron import PROMPT, _seed_deployments, _targetless

    async def go() -> None:
        resolvers: list[_TargetlessResolver] = []

        def factory(config: WorkerConfig) -> _TargetlessResolver:
            resolvers.append(_TargetlessResolver(run.engine, config))
            return resolvers[-1]

        async with (
            make_hook_run() as run,
            _seed_deployments(run.engine, run.agent_id, with_decoy=False),
            make_harness(hook_runs=run.recorder(), binding_factory=factory) as h,
        ):
            (resolver,) = resolvers
            resolver.api.attach(h)
            h.kernel._bundles = _Bundles()
            h.runner.channel_read_enforced = True
            h.runner.default_script = [Final(text="report done", status=SessionStatus.DONE)]
            event = _targetless(run.ref)

            await h.kernel.process_event(event)

            assert h.runner.opened == [PROMPT]
            (call,) = resolver.api.calls
            assert call["mode"] == "open"
            assert call["agent_id"] == run.agent_id
            assert call["event_id"] == event.event_id
            assert call["default"] is None
            channel_read = h.runner.event_bodies[0]["channel_read"]
            assert channel_read is not None
            assert channel_read["token"] == resolver.api.mints[0].token

    asyncio.run(go())


_LEDGER_KEY_MARK = ":channel-read:"
_SHORT_LEASE_S = 2
_SHORT_RENEW_S = 0.1


def _short_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 2 s lease renewed every 0.1 s, in the shared ledger and the kernel."""
    import curie_api.channel_read.ledger as api_ledger

    monkeypatch.setattr(_ledger, "LEASE_TTL_S", _SHORT_LEASE_S, raising=False)
    monkeypatch.setattr(api_ledger, "LEASE_TTL_S", _SHORT_LEASE_S, raising=False)
    monkeypatch.setattr(kernel_channel_read, "LEASE_TTL_S", _SHORT_LEASE_S, raising=False)
    monkeypatch.setattr(kernel_channel_read, "_LEASE_RENEW_S", _SHORT_RENEW_S, raising=False)
    monkeypatch.setattr(kernel_channel_read, "_CLOSE_TIMEOUT_S", 0.2, raising=False)


def _fault_ledger_writes(h: Any, monkeypatch: pytest.MonkeyPatch, mode: str) -> dict[str, Any]:
    """While ``state["down"]``, scripts on ledger keys raise or hang; the live record
    (not a ledger key) still deletes, so a failed revoke cannot look settled."""

    real_eval = h.async_redis.eval
    state: dict[str, Any] = {"down": False, "faulted": 0}

    async def eval_(script: str, numkeys: int, *keys_and_args: Any) -> Any:
        keys = [str(k) for k in keys_and_args[:numkeys]]
        if state["down"] and any(_LEDGER_KEY_MARK in k for k in keys):
            state["faulted"] += 1
            if mode == "timeout":
                await asyncio.sleep(3600)
            raise RedisConnectionError("Valkey is unavailable")
        return await real_eval(script, numkeys, *keys_and_args)

    monkeypatch.setattr(h.async_redis, "eval", eval_)
    return state


@pytest.mark.parametrize("stop", ["crash", "revoke-unavailable", "revoke-times-out"])
def test_a_stopped_heartbeat_lets_the_lease_expire(
    make_harness: Any, monkeypatch: pytest.MonkeyPatch, stop: str
) -> None:
    # Nothing revokes, so the active entry dies within one lease of the last renewal.
    _short_lease(monkeypatch)

    async def body(h: Any, b: _ChannelReadBinding) -> None:
        state: dict[str, Any] = {"down": False, "faulted": 1}
        if stop == "crash":
            monkeypatch.setattr(h.kernel, "_settle_channel_read", lambda _record: None)
        else:
            mode = "unavailable" if stop == "revoke-unavailable" else "timeout"
            state = _fault_ledger_writes(h, monkeypatch, mode)
        async with _live(h, _qevent("long one", thread=f"th-cr-l2-{stop}")):
            mint = b.api.mints[0]
            state["down"] = True
        await _wait_until(lambda: state["faulted"] >= 1, "the first revoke to fail")
        ended = time.monotonic()

        await _until_revoked(b, mint.turn_key, "the lease to expire", _SHORT_LEASE_S + 1.5)
        assert time.monotonic() - ended <= _SHORT_LEASE_S + 1.5
        assert mint.expires_at > time.time(), "only the lease may have ended it"

    _run(make_harness, body)


def test_the_heartbeat_renews_while_live_and_stops_when_the_attempt_ends(
    make_harness: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _short_lease(monkeypatch)
    revoke_lua = _ledger.REVOKE_LUA

    async def body(h: Any, b: _ChannelReadBinding) -> None:
        renewals: list[float] = []
        active_key: list[str] = []
        real_eval = h.async_redis.eval

        async def eval_(script: str, numkeys: int, *keys_and_args: Any) -> Any:
            keys = [str(k) for k in keys_and_args[:numkeys]]
            if active_key and active_key[0] in keys and script != revoke_lua and numkeys == 1:
                renewals.append(time.monotonic())
            return await real_eval(script, numkeys, *keys_and_args)

        monkeypatch.setattr(h.async_redis, "eval", eval_)
        async with _live(h, _qevent("long one", thread="th-cr-l3")):
            mint = b.api.mints[0]
            active_key.append(b.api.keys(AGENT_ID, mint.turn_key)[1])
            await asyncio.sleep(_SHORT_LEASE_S + 0.5)  # longer than one lease
            assert await b.api.active(AGENT_ID, mint.turn_key) is not None, (
                "the lease lapsed while the attempt was live"
            )
            assert len(renewals) >= 2, "the heartbeat never renewed the lease"

        await asyncio.sleep(0.3)
        settled = len(renewals)
        await asyncio.sleep(10 * _SHORT_RENEW_S)
        assert len(renewals) == settled, "the heartbeat outlived its attempt"

    _run(make_harness, body)


def test_a_late_open_after_settlement_is_refused(make_harness: Any) -> None:
    # The settled owner is tombstoned, so its late open is refused; a live owner's is not.
    async def body(h: Any, b: _ChannelReadBinding) -> None:
        b.api.late_gate = asyncio.Event()
        turn = _qevent("hello", thread="th-cr-l4")
        task = asyncio.create_task(h.kernel.process_event(turn))
        try:
            await asyncio.wait_for(b.api.open_requested.wait(), 5)
            task.cancel()
        finally:
            await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0.5)  # let the cancelled attempt settle
        b.api.late_gate.set()
        assert b.api.late_commit is not None
        late = await asyncio.wait_for(b.api.late_commit, 5)

        assert not (isinstance(late, int) and late > 0), f"the late open committed: {late!r}"
        assert await b.api.active(AGENT_ID, _ledger.turn_key(turn.event_id)) is None

        other = _qevent("another", thread="th-cr-l4b")
        assert await b.api.open_ledger(AGENT_ID, other.event_id, uuid.uuid4().hex, 60) >= 1
        assert await b.api.active(AGENT_ID, _ledger.turn_key(other.event_id))

    _run(make_harness, body)


@pytest.mark.parametrize(
    "bundle",
    [
        None,
        _zip(None),
        _zip("{not json"),
        _zip(json.dumps({"name": "acme-bot", "channelRead": "yes"})),
    ],
    ids=["store-unavailable", "manifest-missing", "manifest-not-json", "channel-read-not-a-bool"],
)
def test_an_unreadable_or_malformed_bundle_is_an_unknown_grant(
    make_harness: Any, bundle: bytes | None
) -> None:
    async def body(h: Any, b: _ChannelReadBinding) -> None:
        if bundle is None:
            b.bundles.fail = True
        else:
            b.bundles.raw[_BUNDLE_V1] = bundle
        b.api.times_out = True

        await h.kernel.process_event(_qevent("summarize", thread="th-cr-m1"))

        assert b.bundles.reads, "the bundle was never read"
        assert h.runner.opened == [], "an unknown grant ran the turn"
        assert _classification(h) in RETRYABLE_CLASSIFICATIONS
        assert len(b.api.of_mode("open")) >= 2, "the refusal was not retried"

        # Not cached: once the bundle reads cleanly, the next turn mints.
        b.bundles.fail = False
        b.bundles.raw.clear()
        b.api.times_out = False
        await h.kernel.process_event(_qevent("again", thread="th-cr-m2"))

        assert h.runner.opened == ["again"]
        assert b.api.mints, "the unknown read was cached as no grant"
        assert h.runner.event_bodies[-1]["channel_read"] is not None

    _run(make_harness, body, max_attempts=3)


def test_a_slow_bundle_lookup_times_out_as_an_unknown_grant(
    make_harness: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(kernel_channel_read, "_BUNDLE_GRANT_TIMEOUT_S", 0.2, raising=False)
    slow_s = 1.0

    async def body(h: Any, b: _ChannelReadBinding) -> None:
        b.bundles.scripted = [(slow_s, True)] * 3  # every first-turn lookup is slow and granted
        b.api.times_out = True

        started = time.monotonic()
        await h.kernel.process_event(_qevent("summarize", thread="th-cr-t1"))
        elapsed = time.monotonic() - started

        assert h.runner.opened == [], "the turn waited out the slow lookup"
        assert _classification(h) in RETRYABLE_CLASSIFICATIONS
        assert elapsed < slow_s, f"the lookup was not bounded ({elapsed:.2f}s)"

        # Not cached after the abandoned lookups finish: the next turn reads "no grant".
        await asyncio.sleep(slow_s + 0.3)
        b.bundles.scripted = [(0.0, False)]
        b.api.times_out = False
        opens_before = len(b.api.of_mode("open"))
        await h.kernel.process_event(_qevent("again", thread="th-cr-t2"))

        assert h.runner.opened == ["again"]
        assert len(b.api.of_mode("open")) == opens_before, (
            "a timed out lookup's late answer was cached as granted"
        )
        assert not b.api.mints
        assert h.runner.event_bodies[-1]["channel_read"] is None

    _run(make_harness, body, max_attempts=3)


def _surface_claim_bundle_refs(h: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the fake control plane report each claim's ``CURIE_BUNDLE_REF`` from its env."""
    from curie_worker.binding import BUNDLE_REF_ENV

    fake = h.fake_k8s
    envs: dict[str, dict[str, str]] = {}
    real_create, real_get = fake.create_claim, fake.get_claim

    def create_claim(name: str, **kwargs: Any) -> None:
        envs[name] = dict(kwargs.get("env") or {})
        real_create(name, **kwargs)

    def get_claim(name: str, **kwargs: Any) -> Any:
        view = real_get(name, **kwargs)
        return view and replace(view, bundle_ref=envs.get(name, {}).get(BUNDLE_REF_ENV))

    monkeypatch.setattr(fake, "create_claim", create_claim)
    monkeypatch.setattr(fake, "get_claim", get_claim)


@pytest.mark.parametrize("bundle", ["pinned", "matching", "mismatched"])
def test_a_retained_sandbox_mints_against_its_pinned_or_recovered_deployment(
    make_harness: Any, monkeypatch: pytest.MonkeyPatch, bundle: str
) -> None:
    # A redeploy between two turns on one retained sandbox. "pinned" mints the
    # booted deployment; without a pin the claim's bundle ref decides: equal pins
    # the resolved deployment, different gives no capability and no pin.
    async def body(h: Any, b: _ChannelReadBinding) -> None:
        _surface_claim_bundle_refs(h, monkeypatch)
        first = _qevent("before", thread="th-cr-l8")
        await h.kernel.process_event(first)
        thread_key = _thread_key(first)
        if bundle != "pinned":
            await h.async_redis.delete(h.config.channel_read_pin_key(thread_key))
            b.bundle_ref = _BUNDLE_V1 if bundle == "matching" else _BUNDLE_V2
        b.deployment_id = _REDEPLOYED
        await h.kernel.process_event(_qevent("after", thread="th-cr-l8"))

        assert h.runner.opened == ["before", "after"]
        assert len(h.fake_k8s.claim_envs) == 1, "the second turn did not retain the sandbox"
        deployments = [c["deployment_id"] for c in b.api.of_mode("open")]
        pin = await h.kernel._markers.read_channel_read_pin(thread_key)
        if bundle == "pinned":
            assert deployments == [DEPLOYMENT_ID, DEPLOYMENT_ID]
        elif bundle == "matching":
            assert deployments == [DEPLOYMENT_ID, _REDEPLOYED]
            assert h.runner.event_bodies[1]["channel_read"] is not None
            assert pin is not None and pin.deployment_id == _REDEPLOYED
        else:
            assert deployments == [DEPLOYMENT_ID]
            assert h.runner.event_bodies[1]["channel_read"] is None
            assert pin is None

    _run(make_harness, body)


@pytest.mark.parametrize("boot", ["granted", "ungranted", "unknown"])
def test_with_the_pin_unreadable_the_grant_is_the_retained_sandboxs_booted_bundle(
    make_harness: Any, monkeypatch: pytest.MonkeyPatch, boot: str
) -> None:
    # Boot on V1, redeploy to V2 with the opposite grant, fail the pin read, then send
    # a second turn to the retained sandbox. "unknown": nothing names the booted bundle.
    async def body(h: Any, b: _ChannelReadBinding) -> None:
        if boot == "unknown":
            _fail_marker(h, monkeypatch, _PIN["read"])

            def unreadable(_claim_name: str) -> str | None:
                raise OSError("the control plane is unavailable")

            monkeypatch.setattr(h.kernel._substrate, "claim_bundle_ref", unreadable, raising=False)
            real = h.kernel._with_channel_read

            async def without_route_bundle(event: Any, handle: Any, *args: Any, **kw: Any) -> Any:
                # A route written by a worker that did not record the bundle.
                return await real(event, replace(handle, bundle_ref=None), *args, **kw)

            monkeypatch.setattr(h.kernel, "_with_channel_read", without_route_bundle)
            await h.kernel.process_event(_qevent("summarize", thread="th-cr-r4-unknown"))
            assert h.runner.opened == [], "a turn with an unknown booted bundle ran"
            assert _classification(h) in RETRYABLE_CLASSIFICATIONS
            return

        thread = f"th-cr-r4-{boot}-boot"
        b.bundles.ungranted.add(_BUNDLE_V2 if boot == "granted" else _BUNDLE_V1)
        await h.kernel.process_event(_qevent("before", thread=thread))
        assert h.runner.opened == ["before"]

        b.deployment_id = _REDEPLOYED
        b.bundle_ref = _BUNDLE_V2
        _fail_marker(h, monkeypatch, _PIN["read"])
        h.runner.channel_read_enforced = None
        await h.kernel.process_event(_qevent("after", thread=thread))
        assert len(h.fake_k8s.claim_envs) == 1, "the second turn did not retain the sandbox"

        if boot == "granted":
            assert h.runner.opened == ["before"], "the retained granted turn reached the runner"
            assert _classification(h) == "channel-read-unenforced"
        else:
            assert h.runner.opened == ["before", "after"], h.sink.last_text
            assert h.runner.event_bodies[-1]["channel_read"] is None
            assert h.sink.last_text == "ok"

    _run(make_harness, body, max_attempts=3)
