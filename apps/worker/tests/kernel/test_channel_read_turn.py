"""ADR 0100 (#2877): the worker is the sole issuer of a turn's channel read capability.

For a deployment whose bundle grants channel read, the kernel mints a ``chr``
capability for one logical turn through the API's internal context route
(``BindingResolver.channel_read_context``), sends it as ``Event.channel_read``,
records the live logical turn in Valkey so a steer can renew it, refuses a
granted turn on a runner that does not advertise ``channel_read: true``, and
revokes on every attempt end by running the owner checked delete directly in
the shared Valkey, so revocation holds while the API is down.

These tests drive the real ``Kernel.process_event`` against real Valkey and the
scriptable HTTP runner in ``conftest.py``. The API is reached only through the
binding, exactly as ``test_memory_turn_close.py`` reaches it: the binding double
below plays the API's context route and writes the ledger through the API's
own ``ChannelReadLedger`` over the same Valkey.
Revocation is asserted on those keys in real Valkey, never on the double.
"""

from __future__ import annotations

import asyncio
import base64
import io
import json
import re
import secrets
import socket
import sys
import time
import uuid
import zipfile
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import Final, QueuedTurn, SessionStatus, TextDelta
from curie_worker.binding import BindingResolver
from curie_worker.config import WorkerConfig
from curie_worker.kernel import routing as kernel_routing
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


def _ledger() -> Any:
    from curie_internal import channel_read_ledger

    return channel_read_ledger


def _closed_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def _until(check: Callable[[], Awaitable[bool]], what: str, timeout: float = 5.0) -> None:
    """Poll an async ``check`` until true; fail naming ``what`` after ``timeout``."""

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await check():
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"timed out waiting for {what}")


class _ChannelReadApi:
    """Plays ``POST /v1/internal/channel-read/context`` over the real shared Valkey.

    An open mint writes the marker and page keys, bumps the generation and sets
    the active value ``<owner>:<gen>``; a steer mint bumps the generation only
    while the active key exists and keeps its owner, else answers as a 409
    ``channel_read.turn_inactive`` does through the real binding (None). The
    logical turn of a plain event is its own event id.
    """

    def __init__(self, *, grant: bool = True) -> None:
        self.grant = grant
        self.calls: list[dict[str, Any]] = []
        self.mints: list[Any] = []
        # When set, every later mint answers as the real binding does with the
        # API unreachable.
        self.down = False
        self.steer_gate: asyncio.Event | None = None
        self.steer_entered = asyncio.Event()
        # When set, an open mint commits in the ledger and then loses its
        # response: "returns-none", "raises-timeout" or "hangs".
        self.lose_open_response: str | None = None
        self.open_committed = asyncio.Event()
        # When set, an open mint times out without committing.
        self.times_out = False
        # When set, an open mint commits only after this gate opens, long
        # after the worker stopped waiting for it.
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
        return _ledger().ledger_keys(self._prefix, agent_id, turn_key)

    async def active(self, agent_id: uuid.UUID, turn_key: str) -> str | None:
        _gen, active, _pages, _marker = self.keys(agent_id, turn_key)
        value = await self._redis.get(active)
        return None if value is None else str(value)

    def _api_ledger(self) -> Any:
        # The API's own ledger over the same Valkey, so the double writes
        # exactly what the API writes, owner index included.
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

    async def steer_ledger(
        self, agent_id: uuid.UUID, turn: str, ttl_s: int
    ) -> tuple[int, int] | None:
        return await self._api_ledger().steer(agent_id, turn, ttl_s)

    async def context(self, **kwargs: Any) -> Any:
        from curie_worker.binding import ChannelReadGrantAbsent, ChannelReadMint

        self.calls.append(dict(kwargs))
        mode = kwargs["mode"]
        if mode == "steer" and self.steer_gate is not None:
            self.steer_entered.set()
            await self.steer_gate.wait()
        if self.down:
            return None
        if not self.grant:
            raise ChannelReadGrantAbsent("channel_read.grant_absent")
        agent_id = kwargs["agent_id"]
        turn_key = _ledger().turn_key(kwargs["event_id"])
        ttl_s = int(kwargs["ttl_s"])
        now = int(time.time())
        if mode == "open" and self.late_gate is not None:
            # The request is in flight at the API, which commits the open only
            # after the worker has given up and settled.
            self.late_commit = asyncio.ensure_future(
                self._commit_late(agent_id, kwargs["event_id"], kwargs["owner"], ttl_s)
            )
            self.open_requested.set()
            await asyncio.Event().wait()
        if mode == "open" and self.times_out:
            raise TimeoutError("the context request timed out")
        if mode == "open":
            gen = await self.open_ledger(agent_id, kwargs["event_id"], kwargs["owner"], ttl_s)
            expires_at = now + ttl_s
            if self.lose_open_response == "returns-none":
                # The API committed the open; the response never arrived and
                # the real binding answers a client timeout with None.
                return None
            if self.lose_open_response == "raises-timeout":
                raise TimeoutError("the context response timed out")
            if self.lose_open_response == "hangs":
                self.open_committed.set()
                await asyncio.Event().wait()
        else:
            bumped = await self.steer_ledger(agent_id, kwargs["event_id"], ttl_s)
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


def _bundle_zip(*, granted: bool) -> bytes:
    """A bundle whose ``.claude-plugin/plugin.json`` grants channel read or not."""

    manifest: dict[str, Any] = {"name": "acme-bot"}
    if granted:
        manifest["channelRead"] = True
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(".claude-plugin/plugin.json", json.dumps(manifest))
    return buffer.getvalue()


class _Bundles:
    """A ``BundleReader`` serving bundles by key from memory, recording reads.

    Every key serves a granted bundle unless ``ungranted`` names it; ``fail``
    makes every read raise, as an object store outage does."""

    def __init__(self) -> None:
        self.reads: list[str] = []
        self.ungranted: set[str] = set()
        self.fail = False
        # Raw bytes served for a key instead of a well formed bundle.
        self.raw: dict[str, bytes] = {}
        # Per read, in order: (seconds to block, granted). Empty serves the
        # defaults above at once.
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


def _serve_bundles(h: Any, bundles: _Bundles) -> None:
    # The kernel reads the grant from the bundle manifest (``Kernel(bundles=)``).
    h.kernel._bundles = bundles


class _ChannelReadBinding(_MemoryBinding):
    """The memory test's binding (real ``ResolvedDeployment``) plus the API seam."""

    def __init__(self, *, grant: bool = True) -> None:
        super().__init__(memory_writes=False)
        self.api = _ChannelReadApi(grant=grant)
        self.bundles = _Bundles()
        # What ``resolve`` answers; a test redeploys by changing these.
        self.deployment_id = DEPLOYMENT_ID
        self.bundle_ref = _BUNDLE_V1

    def attach(self, h: Any) -> None:
        self.api.attach(h)
        _serve_bundles(h, self.bundles)

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
        self._real._config = WorkerConfig(  # type: ignore[attr-defined]
            api_base_url=f"http://127.0.0.1:{_closed_port()}",
            internal_worker_token="test-worker-token-2877",
        )

    async def channel_read_context(self, **kwargs: Any) -> Any:
        self.calls.append(dict(kwargs))
        return await self._real.channel_read_context(**kwargs)  # type: ignore[attr-defined]


def _thread_key(turn: QueuedTurn) -> str:
    return kernel_routing._thread_key_for(turn)


async def _live_record(h: Any, turn: QueuedTurn) -> str | None:
    value = await h.async_redis.get(h.config.channel_read_turn_key(_thread_key(turn)))
    return None if value is None else str(value)


def _live_turn(h: Any) -> None:
    h.runner.hold = asyncio.Event()
    h.runner.default_script = [TextDelta(text="working")]
    h.runner.tail = [Final(text="done", status=SessionStatus.DONE)]


# --------------------------------------------------------------------------- #
# Open
# --------------------------------------------------------------------------- #


def test_a_granted_turn_carries_the_capability(make_harness) -> None:
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding, runner_api_base_url=_RUNNER_FACING) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = True
            turn = _qevent("what did we decide yesterday", thread="th-cr-1")

            await h.kernel.process_event(turn)

            assert h.sink.last_text == "ok"
            assert len(binding.api.calls) == 1, binding.api.calls
            call = binding.api.calls[0]
            assert call["mode"] == "open"
            assert call["agent_id"] == AGENT_ID
            assert call["deployment_id"] == DEPLOYMENT_ID
            assert call["event_id"] == turn.event_id
            assert tuple(call["default"]) == ("slack", CHANNEL)
            assert isinstance(call["owner"], str) and _OWNER.fullmatch(call["owner"])
            assert isinstance(call["ttl_s"], int) and 1 <= call["ttl_s"] <= 86400
            body = h.runner.event_bodies[0]
            assert body["channel_read"] == {
                "url": _CAPABILITY_URL,
                "token": binding.api.mints[0].token,
            }

    asyncio.run(go())


def test_a_failed_mint_runs_the_turn_without_capability(make_harness) -> None:
    # Liveness: the API is down, the real resolver method cannot mint, and the
    # turn still runs, without a capability (its tools refuse).
    async def go() -> None:
        binding = _UnreachableApiBinding()
        async with make_harness(binding=binding) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = True

            await h.kernel.process_event(_qevent("hello", thread="th-cr-3"))

            assert len(binding.calls) == 1, binding.calls
            assert h.runner.opened == ["hello"]
            assert h.runner.event_bodies[0]["channel_read"] is None
            assert h.sink.last_text == "ok"

    asyncio.run(go())


@pytest.mark.parametrize(
    "advertised", [None, False, "true"], ids=["key-missing", "false", "string-true"]
)
def test_a_runner_not_advertising_refuses_the_turn_and_revokes(
    make_harness, advertised: object
) -> None:
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding, max_attempts=3) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = advertised
            turn = _qevent("summarize the channel", thread="th-cr-4")

            await h.kernel.process_event(turn)

            assert h.runner.opened == [], "a runner that cannot enforce was sent the turn"
            assert h.sink.last_text is not None
            assert h.sink.last_text.startswith("curie-turn-failure: channel-read-unenforced\n\n")
            assert _REFUSAL in h.sink.last_text
            assert [c.outcome for c in h.sink.completions][-1] == "escalated"
            # Not retried: the class is not retryable.
            assert len(binding.api.calls) == 1, binding.api.calls
            turn_key = binding.api.mints[0].turn_key

            async def revoked() -> bool:
                return await binding.api.active(AGENT_ID, turn_key) is None

            await _until(revoked, "the refused turn's capability to be revoked")

            async def record_gone() -> bool:
                return await _live_record(h, turn) is None

            await _until(record_gone, "the refused turn's live record to be deleted")

    asyncio.run(go())


def test_a_runner_advertising_true_runs_the_granted_turn(make_harness) -> None:
    # Liveness for the refusal above: only the literal true passes.
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = True
            turn = _qevent("summarize the channel", thread="th-cr-5")

            await h.kernel.process_event(turn)

            assert h.runner.opened == [turn.text]
            assert h.runner.event_bodies[0]["channel_read"] is not None
            assert h.sink.last_text == "ok"

    asyncio.run(go())


# --------------------------------------------------------------------------- #
# Steer
# --------------------------------------------------------------------------- #


def test_a_steer_renews_the_same_logical_turn(make_harness) -> None:
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding, runner_api_base_url=_RUNNER_FACING) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = True
            _live_turn(h)
            thread = "th-cr-6"
            first = _qevent("first", thread=thread, placeholder="ph-1")
            task = asyncio.create_task(h.kernel.process_event(first))
            try:
                await _wait_until(lambda: h.runner.turn_active, "the first turn to be live")
                record_before = await _live_record(h, first)
                assert record_before is not None, "the opener recorded no live turn"
                second = _qevent("and this", thread=thread, placeholder="ph-2")
                await h.kernel.process_event(second)

                assert h.runner.steers == ["and this"]
                (opened,) = binding.api.of_mode("open")
                (steered,) = binding.api.of_mode("steer")
                # The opener's logical turn and default, a new generation, no owner.
                assert steered["event_id"] == first.event_id
                assert tuple(steered["default"]) == tuple(opened["default"]) == ("slack", CHANNEL)
                assert steered["agent_id"] == AGENT_ID
                assert steered["deployment_id"] == DEPLOYMENT_ID
                assert steered.get("owner") is None
                open_mint, steer_mint = binding.api.mints
                assert steer_mint.turn_key == open_mint.turn_key
                assert steer_mint.generation == open_mint.generation + 1
                assert await binding.api.active(AGENT_ID, open_mint.turn_key) == (
                    f"{opened['owner']}:{steer_mint.generation}"
                )
                body = h.runner.steer_bodies[-1]
                assert body["channel_read"] == {
                    "url": _CAPABILITY_URL,
                    "token": steer_mint.token,
                }
                # No Valkey write on the steer side.
                assert await _live_record(h, first) == record_before
            finally:
                h.runner.hold.set()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(go())


def test_a_steer_attempt_never_revokes_the_live_turn(make_harness) -> None:
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = True
            _live_turn(h)
            thread = "th-cr-7"
            first = _qevent("first", thread=thread, placeholder="ph-1")
            task = asyncio.create_task(h.kernel.process_event(first))
            try:
                await _wait_until(lambda: h.runner.turn_active, "the first turn to be live")
                await h.kernel.process_event(_qevent("and this", thread=thread, placeholder="ph-2"))
                assert h.runner.steers == ["and this"]
                owner = binding.api.of_mode("open")[0]["owner"]
                turn_key = binding.api.mints[0].turn_key
                # Give a wrongly eager revoke the chance to land.
                await asyncio.sleep(0.2)
                assert await binding.api.active(AGENT_ID, turn_key) == f"{owner}:2"
                assert await _live_record(h, first) is not None
            finally:
                h.runner.hold.set()
                await asyncio.gather(task, return_exceptions=True)

            async def revoked() -> bool:
                return await binding.api.active(AGENT_ID, turn_key) is None

            # The opener's end revokes, and the steer generation dies with it.
            await _until(revoked, "the opener's end to revoke the live turn")

    asyncio.run(go())


def test_terminal_then_steer_gets_no_capability(make_harness) -> None:
    # Interleaving (a): the opener's revoke lands while the steer mint is in
    # flight, so the steer finds no active key and the runner is sent null.
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = True
            _live_turn(h)
            thread = "th-cr-11a"
            first = _qevent("first", thread=thread, placeholder="ph-1")
            task = asyncio.create_task(h.kernel.process_event(first))
            steer_task: asyncio.Task[Any] | None = None
            try:
                await _wait_until(lambda: h.runner.turn_active, "the first turn to be live")
                owner = binding.api.of_mode("open")[0]["owner"]
                turn_key = binding.api.mints[0].turn_key
                binding.api.steer_gate = asyncio.Event()
                steer_task = asyncio.create_task(
                    h.kernel.process_event(_qevent("and this", thread=thread, placeholder="ph-2"))
                )
                await asyncio.wait_for(binding.api.steer_entered.wait(), 5)
                # The opener's terminal revoke: the exact worker call.
                assert (
                    await h.kernel._markers.revoke_channel_read_owner(AGENT_ID, turn_key, owner)
                    is True
                )
                binding.api.steer_gate.set()
                await asyncio.wait_for(steer_task, 10)

                assert h.runner.steers == ["and this"]
                assert h.runner.steer_bodies[-1]["channel_read"] is None
                assert len(binding.api.mints) == 1, "the steer got a capability"
                assert await binding.api.active(AGENT_ID, turn_key) is None
            finally:
                if binding.api.steer_gate is not None:
                    binding.api.steer_gate.set()
                h.runner.hold.set()
                await asyncio.gather(task, return_exceptions=True)
                if steer_task is not None:
                    await asyncio.gather(steer_task, return_exceptions=True)

    asyncio.run(go())


def test_steer_then_terminal_revokes_only_as_the_opener(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Interleaving (b): the steer mint lands first, then the opener ends. The
    # steer attempt revokes nothing; the opener's revoke, under its own owner,
    # kills the steer generation too.
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = True
            revokes: list[tuple[uuid.UUID, str, str]] = []
            real_revoke = h.kernel._markers.revoke_channel_read_owner

            async def spy(agent_id: uuid.UUID, turn_key: str, owner: str) -> bool:
                revokes.append((agent_id, turn_key, owner))
                return await real_revoke(agent_id, turn_key, owner)

            monkeypatch.setattr(h.kernel._markers, "revoke_channel_read_owner", spy)
            _live_turn(h)
            thread = "th-cr-11b"
            first = _qevent("first", thread=thread, placeholder="ph-1")
            task = asyncio.create_task(h.kernel.process_event(first))
            try:
                await _wait_until(lambda: h.runner.turn_active, "the first turn to be live")
                await h.kernel.process_event(_qevent("and this", thread=thread, placeholder="ph-2"))
                assert h.runner.steers == ["and this"]
                assert len(binding.api.mints) == 2, binding.api.calls
                await asyncio.sleep(0.2)
                assert revokes == [], "the steer attempt revoked"
            finally:
                h.runner.hold.set()
                await asyncio.gather(task, return_exceptions=True)

            owner = binding.api.of_mode("open")[0]["owner"]
            turn_key = binding.api.mints[0].turn_key

            async def revoked() -> bool:
                return await binding.api.active(AGENT_ID, turn_key) is None

            await _until(revoked, "the opener's end to revoke the steer generation")
            assert revokes, "nothing was revoked"
            assert set(revokes) == {(AGENT_ID, turn_key, owner)}

    asyncio.run(go())


# --------------------------------------------------------------------------- #
# Revocation
# --------------------------------------------------------------------------- #


def test_the_openers_end_revokes_in_valkey_with_the_api_down(make_harness) -> None:
    # The API goes down right after the mint. Revocation is the owner checked
    # delete in the shared Valkey, so it still lands, well before expiry.
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = True
            _live_turn(h)
            turn = _qevent("long one", thread="th-cr-8")
            task = asyncio.create_task(h.kernel.process_event(turn))
            try:
                await _wait_until(lambda: h.runner.turn_active, "the turn to be live")
                binding.api.down = True
                mint = binding.api.mints[0]
                assert await binding.api.active(AGENT_ID, mint.turn_key) is not None
            finally:
                h.runner.hold.set()
                await asyncio.gather(task, return_exceptions=True)

            async def revoked() -> bool:
                return await binding.api.active(AGENT_ID, mint.turn_key) is None

            await _until(revoked, "the revoke to land with the API down")
            assert mint.expires_at > time.time(), "the key may only have expired"
            assert await _live_record(h, turn) is None

    asyncio.run(go())


@pytest.mark.parametrize("ending", ["success", "runner-error-then-retry", "cancelled"])
def test_every_attempt_end_revokes_and_deletes_the_record(make_harness, ending: str) -> None:
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding, max_attempts=3) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = True
            turn = _qevent("one turn", thread=f"th-cr-8-{ending}")
            if ending == "success":
                await h.kernel.process_event(turn)
            elif ending == "runner-error-then-retry":
                h.runner.event_fail_times = 1
                await h.kernel.process_event(turn)
                owners = [c["owner"] for c in binding.api.of_mode("open")]
                assert len(owners) == 2 and owners[0] != owners[1], owners
            else:
                _live_turn(h)
                task = asyncio.create_task(h.kernel.process_event(turn))
                try:
                    await _wait_until(lambda: h.runner.turn_active, "the turn to be live")
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                finally:
                    h.runner.hold.set()
                    await asyncio.gather(task, return_exceptions=True)

            assert binding.api.mints, "nothing was minted"
            turn_key = binding.api.mints[0].turn_key

            async def revoked() -> bool:
                return await binding.api.active(AGENT_ID, turn_key) is None

            await _until(revoked, f"the {ending} attempt end to revoke")

            async def record_gone() -> bool:
                return await _live_record(h, turn) is None

            await _until(record_gone, f"the {ending} attempt end to delete the live record")

    asyncio.run(go())


def test_two_attempt_records_have_distinct_owners(make_harness) -> None:
    # An approval resume opens the same logical turn under a new owner. The
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
            turn_key = _ledger().turn_key(event.event_id)
            thread_key = _thread_key(event)
            await api.open_ledger(AGENT_ID, event.event_id, first.owner, 600)
            first.opened.append((AGENT_ID, turn_key, thread_key))
            resumed_gen = await api.open_ledger(AGENT_ID, event.event_id, resumed.owner, 600)
            resumed.opened.append((AGENT_ID, turn_key, thread_key))
            assert first.pending() and resumed.pending()
            assert resumed.opened == [(AGENT_ID, turn_key, thread_key)]

            late = await h.kernel._markers.revoke_channel_read_owner(
                AGENT_ID, turn_key, first.owner
            )
            assert late is False
            await h.kernel._close_channel_read(first)
            assert await api.active(AGENT_ID, turn_key) == f"{resumed.owner}:{resumed_gen}"

            await h.kernel._close_channel_read(resumed)
            assert await api.active(AGENT_ID, turn_key) is None

    asyncio.run(go())


def test_a_failed_live_record_write_revokes_at_once(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Fault at our own seam; Valkey stays real. The capability is revoked
    # before the runner sees the event, which carries none, and the turn runs.
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = True

            async def failing(*args: Any, **kwargs: Any) -> Any:
                raise RedisError("live record write failed")

            monkeypatch.setattr(h.kernel._markers, "record_channel_read_turn", failing)
            active_at_start: list[str | None] = []
            real_start = h.kernel._runner.start_turn

            async def spy(base_url: str, event: Any, **kwargs: Any) -> Any:
                mint = binding.api.mints[0]
                active_at_start.append(await binding.api.active(AGENT_ID, mint.turn_key))
                return await real_start(base_url, event, **kwargs)

            monkeypatch.setattr(h.kernel._runner, "start_turn", spy)
            turn = _qevent("hello", thread="th-cr-10")

            await h.kernel.process_event(turn)

            assert len(binding.api.mints) == 1, binding.api.calls
            assert active_at_start == [None], "the runner saw a live capability"
            assert h.runner.event_bodies[0]["channel_read"] is None
            assert h.runner.opened == ["hello"]
            assert h.sink.last_text == "ok"

    asyncio.run(go())


def test_settlement_runs_through_the_bound_close_on_a_real_kernel(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_worker.kernel import channel_read as kernel_channel_read
    from curie_worker.kernel.core import Kernel

    assert Kernel._close_channel_read is kernel_channel_read._close_channel_read
    assert Kernel._settle_channel_read is kernel_channel_read._settle_channel_read

    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = True
            closes: list[Any] = []
            real_close = h.kernel._close_channel_read

            async def spy(record: Any) -> None:
                closes.append(record)
                await real_close(record)

            monkeypatch.setattr(h.kernel, "_close_channel_read", spy)
            turn = _qevent("hello", thread="th-cr-12")

            await h.kernel.process_event(turn)
            binding.api.down = True

            await _wait_until(lambda: bool(closes), "the attempt's settlement to close")
            (record,) = closes
            owner = binding.api.of_mode("open")[0]["owner"]
            mint = binding.api.mints[0]
            assert record.owner == owner
            assert record.opened == [(AGENT_ID, mint.turn_key, _thread_key(turn))]

            async def revoked() -> bool:
                return await binding.api.active(AGENT_ID, mint.turn_key) is None

            await _until(revoked, "the bound close to revoke in Valkey")

    asyncio.run(go())


# --------------------------------------------------------------------------- #
# Targetless hook turns
# --------------------------------------------------------------------------- #


class _TargetlessResolver(BindingResolver):
    """The real resolver over real Postgres, with the API seam played."""

    def __init__(self, engine: Any, config: WorkerConfig) -> None:
        super().__init__(engine, config)
        self.api = _ChannelReadApi()

    async def channel_read_context(self, **kwargs: Any) -> Any:  # type: ignore[override]
        return await self.api.context(**kwargs)


def test_a_targetless_hook_turn_mints_with_a_null_default(make_harness, make_hook_run) -> None:
    from test_targetless_cron import PROMPT, _seed_deployments, _targetless

    async def go() -> None:
        resolvers: list[_TargetlessResolver] = []

        def factory(config: WorkerConfig) -> _TargetlessResolver:
            resolver = _TargetlessResolver(run.engine, config)
            resolvers.append(resolver)
            return resolver

        async with (
            make_hook_run() as run,
            _seed_deployments(run.engine, run.agent_id, with_decoy=False),
            make_harness(hook_runs=run.recorder(), binding_factory=factory) as h,
        ):
            (resolver,) = resolvers
            resolver.api.attach(h)
            _serve_bundles(h, _Bundles())
            h.runner.channel_read_enforced = True
            h.runner.default_script = [Final(text="report done", status=SessionStatus.DONE)]
            event = _targetless(run.ref)

            await h.kernel.process_event(event)

            assert h.runner.opened == [PROMPT]
            assert len(resolver.api.calls) == 1, resolver.api.calls
            call = resolver.api.calls[0]
            assert call["mode"] == "open"
            assert call["agent_id"] == run.agent_id
            assert call["event_id"] == event.event_id
            assert call["default"] is None
            body = h.runner.event_bodies[0]
            assert body["channel_read"] is not None
            assert body["channel_read"]["token"] == resolver.api.mints[0].token

    asyncio.run(go())


# --------------------------------------------------------------------------- #
# Review round 1 regressions
# --------------------------------------------------------------------------- #

_LEDGER_KEY_MARK = ":channel-read:"


def _fault_ledger_writes(h: Any, monkeypatch: pytest.MonkeyPatch, mode: str) -> dict[str, Any]:
    """Make every script on a channel read ledger key fail while ``state["down"]``.

    The live record (``...:channel-read-turn:...``) is not a ledger key, so its
    owner checked delete still succeeds: a failed revoke must not look settled
    because the record went. ``mode`` is "unavailable" (the client raises) or
    "timeout" (the call hangs past the close bound).
    """

    client = h.async_redis
    real_eval = client.eval
    state: dict[str, Any] = {"down": False, "faulted": 0}

    async def eval_(script: str, numkeys: int, *keys_and_args: Any) -> Any:
        keys = [str(k) for k in keys_and_args[:numkeys]]
        if state["down"] and any(_LEDGER_KEY_MARK in k for k in keys):
            state["faulted"] += 1
            if mode == "timeout":
                await asyncio.sleep(3600)
            raise RedisConnectionError("Valkey is unavailable")
        return await real_eval(script, numkeys, *keys_and_args)

    monkeypatch.setattr(client, "eval", eval_)
    return state


async def _end_a_live_turn_with_valkey_down(
    h: Any, binding: _ChannelReadBinding, state: dict[str, Any], turn: QueuedTurn
) -> Any:
    _live_turn(h)
    task = asyncio.create_task(h.kernel.process_event(turn))
    try:
        await _wait_until(lambda: h.runner.turn_active, "the turn to be live")
        mint = binding.api.mints[0]
        state["down"] = True
    finally:
        h.runner.hold.set()
        await asyncio.gather(task, return_exceptions=True)
    await _wait_until(lambda: state["faulted"] >= 1, "the first revoke to fail")
    return mint


def test_a_retained_sandbox_mints_against_its_pinned_deployment(make_harness) -> None:
    # A redeploy between two turns on one thread: the second turn runs on the
    # sandbox the first booted (no new claim), so its capability names the
    # deployment that sandbox runs, not the newly resolved one.
    redeployed = uuid.UUID("44444444-4444-4444-8444-444444444444")

    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = True
            thread = "th-cr-r3"

            await h.kernel.process_event(_qevent("before the redeploy", thread=thread))
            binding.deployment_id = redeployed
            await h.kernel.process_event(_qevent("after the redeploy", thread=thread))

            assert h.runner.opened == ["before the redeploy", "after the redeploy"]
            assert len(h.fake_k8s.claim_envs) == 1, "the second turn did not retain the sandbox"
            opens = binding.api.of_mode("open")
            assert [c["deployment_id"] for c in opens] == [DEPLOYMENT_ID, DEPLOYMENT_ID]

    asyncio.run(go())


def test_a_known_grant_with_a_failed_mint_still_refuses_an_unenforcing_runner(
    make_harness,
) -> None:
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding, max_attempts=3) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = True
            await h.kernel.process_event(_qevent("first", thread="th-cr-r4a"))
            assert h.runner.event_bodies[0]["channel_read"] is not None

            # The grant is now known true. The API goes down, and the next
            # runner cannot enforce: the turn is refused, not run uncovered.
            binding.api.down = True
            h.runner.channel_read_enforced = None
            await h.kernel.process_event(_qevent("second", thread="th-cr-r4b"))

            assert h.runner.opened == ["first"], "a runner that cannot enforce was sent the turn"
            assert h.sink.last_text is not None
            assert h.sink.last_text.startswith("curie-turn-failure: channel-read-unenforced\n\n")
            assert _REFUSAL in h.sink.last_text

    asyncio.run(go())


@pytest.mark.parametrize("lost", ["returns-none", "raises-timeout", "hangs"])
def test_a_lost_mint_response_is_still_revoked_by_owner(make_harness, lost: str) -> None:
    # The API committed the open but the worker never saw the response. The
    # attempt end still revokes, through the ledger's owner index.
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = True
            binding.api.lose_open_response = lost
            turn = _qevent("hello", thread=f"th-cr-r5-{lost}")
            turn_key = _ledger().turn_key(turn.event_id)
            if lost == "hangs":
                task = asyncio.create_task(h.kernel.process_event(turn))
                try:
                    await asyncio.wait_for(binding.api.open_committed.wait(), 5)
                    assert await binding.api.active(AGENT_ID, turn_key) is not None
                    task.cancel()
                finally:
                    await asyncio.gather(task, return_exceptions=True)
            else:
                await h.kernel.process_event(turn)
                assert h.runner.event_bodies[0]["channel_read"] is None
            owner = binding.api.of_mode("open")[0]["owner"]

            async def revoked() -> bool:
                return await binding.api.active(AGENT_ID, turn_key) is None

            await _until(revoked, f"the {lost} open of owner {owner} to be revoked")

    asyncio.run(go())


# --------------------------------------------------------------------------- #
# Review round 2: the lease, the owner tombstone, the local grant, pin recovery
# --------------------------------------------------------------------------- #

_SHORT_LEASE_S = 2
_SHORT_RENEW_S = 0.1


def _short_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 2 s lease renewed every 0.1 s, in the shared ledger and the kernel."""

    import curie_api.channel_read.ledger as api_ledger
    from curie_worker.kernel import channel_read as kernel_channel_read

    monkeypatch.setattr(_ledger(), "LEASE_TTL_S", _SHORT_LEASE_S, raising=False)
    monkeypatch.setattr(api_ledger, "LEASE_TTL_S", _SHORT_LEASE_S, raising=False)
    monkeypatch.setattr(kernel_channel_read, "LEASE_TTL_S", _SHORT_LEASE_S, raising=False)
    monkeypatch.setattr(kernel_channel_read, "_LEASE_RENEW_S", _SHORT_RENEW_S, raising=False)
    monkeypatch.setattr(kernel_channel_read, "_CLOSE_TIMEOUT_S", 0.2, raising=False)


@pytest.mark.parametrize("stop", ["crash", "revoke-unavailable", "revoke-times-out"])
def test_a_stopped_heartbeat_lets_the_lease_expire(
    make_harness, monkeypatch: pytest.MonkeyPatch, stop: str
) -> None:
    # Nothing revokes (the worker died, or Valkey refused the revoke), so the
    # active entry must die on its own within one lease of the last renewal.
    _short_lease(monkeypatch)

    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = True
            turn = _qevent("long one", thread=f"th-cr-l2-{stop}")
            if stop == "crash":
                monkeypatch.setattr(h.kernel, "_settle_channel_read", lambda _record: None)
                _live_turn(h)
                task = asyncio.create_task(h.kernel.process_event(turn))
                try:
                    await _wait_until(lambda: h.runner.turn_active, "the turn to be live")
                    mint = binding.api.mints[0]
                finally:
                    h.runner.hold.set()
                    await asyncio.gather(task, return_exceptions=True)
            else:
                mode = "unavailable" if stop == "revoke-unavailable" else "timeout"
                state = _fault_ledger_writes(h, monkeypatch, mode)
                mint = await _end_a_live_turn_with_valkey_down(h, binding, state, turn)
            ended = time.monotonic()

            async def gone() -> bool:
                return await binding.api.active(AGENT_ID, mint.turn_key) is None

            await _until(gone, "the lease to expire", timeout=_SHORT_LEASE_S + 1.5)
            assert time.monotonic() - ended <= _SHORT_LEASE_S + 1.5
            assert mint.expires_at > time.time(), "only the lease may have ended it"

    asyncio.run(go())


def test_the_heartbeat_renews_while_live_and_stops_when_the_attempt_ends(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    _short_lease(monkeypatch)
    revoke_lua = _ledger().REVOKE_LUA

    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = True
            renewals: list[float] = []
            active_key: list[str] = []
            real_eval = h.async_redis.eval

            async def eval_(script: str, numkeys: int, *keys_and_args: Any) -> Any:
                keys = [str(k) for k in keys_and_args[:numkeys]]
                if active_key and active_key[0] in keys and script != revoke_lua and numkeys == 1:
                    renewals.append(time.monotonic())
                return await real_eval(script, numkeys, *keys_and_args)

            monkeypatch.setattr(h.async_redis, "eval", eval_)
            _live_turn(h)
            turn = _qevent("long one", thread="th-cr-l3")
            task = asyncio.create_task(h.kernel.process_event(turn))
            try:
                await _wait_until(lambda: h.runner.turn_active, "the turn to be live")
                mint = binding.api.mints[0]
                active_key.append(binding.api.keys(AGENT_ID, mint.turn_key)[1])
                # Longer than one lease: only renewals keep it alive.
                await asyncio.sleep(_SHORT_LEASE_S + 0.5)
                assert await binding.api.active(AGENT_ID, mint.turn_key) is not None, (
                    "the lease lapsed while the attempt was live"
                )
                assert len(renewals) >= 2, "the heartbeat never renewed the lease"
            finally:
                h.runner.hold.set()
                await asyncio.gather(task, return_exceptions=True)

            await asyncio.sleep(0.3)
            settled = len(renewals)
            await asyncio.sleep(10 * _SHORT_RENEW_S)
            assert len(renewals) == settled, "the heartbeat outlived its attempt"

    asyncio.run(go())


def test_a_late_open_after_settlement_is_refused(make_harness) -> None:
    # The API commits the open only after the worker gave up and settled: the
    # settled owner is tombstoned, so the open is refused. A live owner's
    # open still succeeds.
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = True
            binding.api.late_gate = asyncio.Event()
            turn = _qevent("hello", thread="th-cr-l4")
            task = asyncio.create_task(h.kernel.process_event(turn))
            try:
                await asyncio.wait_for(binding.api.open_requested.wait(), 5)
                task.cancel()
            finally:
                await asyncio.gather(task, return_exceptions=True)
            # Let the cancelled attempt settle.
            await asyncio.sleep(0.5)
            binding.api.late_gate.set()
            assert binding.api.late_commit is not None
            late = await asyncio.wait_for(binding.api.late_commit, 5)

            turn_key = _ledger().turn_key(turn.event_id)
            assert not (isinstance(late, int) and late > 0), f"the late open committed: {late!r}"
            assert await binding.api.active(AGENT_ID, turn_key) is None

            # Positive control: a live owner opens the same way.
            other = _qevent("another", thread="th-cr-l4b")
            gen = await binding.api.open_ledger(AGENT_ID, other.event_id, uuid.uuid4().hex, 60)
            assert gen >= 1
            assert await binding.api.active(AGENT_ID, _ledger().turn_key(other.event_id))

    asyncio.run(go())


def test_a_cold_grant_with_a_timed_out_mint_refuses_an_unenforcing_runner(make_harness) -> None:
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding, max_attempts=3) as h:
            binding.attach(h)
            binding.api.times_out = True
            assert h.runner.channel_read_enforced is None
            turn = _qevent("summarize", thread="th-cr-l5")

            await h.kernel.process_event(turn)

            assert binding.bundles.reads, "the grant was not read from the bundle"
            assert h.runner.opened == [], "a runner that cannot enforce was sent the turn"
            assert h.sink.last_text is not None
            assert h.sink.last_text.startswith("curie-turn-failure: channel-read-unenforced\n\n")

    asyncio.run(go())


def test_a_granted_turn_whose_live_record_fails_still_refuses_an_unenforcing_runner(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding, max_attempts=3) as h:
            binding.attach(h)

            async def failing(*args: Any, **kwargs: Any) -> Any:
                raise RedisError("live record write failed")

            monkeypatch.setattr(h.kernel._markers, "record_channel_read_turn", failing)
            assert h.runner.channel_read_enforced is None

            await h.kernel.process_event(_qevent("summarize", thread="th-cr-l5b"))

            assert h.runner.opened == [], "a runner that cannot enforce was sent the turn"
            assert h.sink.last_text is not None
            assert h.sink.last_text.startswith("curie-turn-failure: channel-read-unenforced\n\n")

    asyncio.run(go())


def test_an_ungranted_bundle_makes_no_mint_call(make_harness) -> None:
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding) as h:
            binding.attach(h)
            binding.bundles.ungranted.add(_BUNDLE_V1)
            assert h.runner.channel_read_enforced is None

            await h.kernel.process_event(_qevent("first", thread="th-cr-l6a"))
            await h.kernel.process_event(_qevent("second", thread="th-cr-l6b"))

            assert binding.bundles.reads, "the grant was not read from the bundle"
            assert binding.api.calls == [], binding.api.calls
            assert h.runner.opened == ["first", "second"]
            assert [b["channel_read"] for b in h.runner.event_bodies] == [None, None]
            assert h.sink.last_text == "ok"

    asyncio.run(go())


def test_an_unreadable_bundle_and_a_failed_mint_refuse_retryably(make_harness) -> None:
    from curie_worker.kernel.constants import RETRYABLE_CLASSIFICATIONS

    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding, max_attempts=3) as h:
            binding.attach(h)
            binding.bundles.fail = True
            binding.api.times_out = True
            h.runner.channel_read_enforced = True

            await h.kernel.process_event(_qevent("summarize", thread="th-cr-l7"))

            assert binding.bundles.reads, "the bundle was never read"
            assert h.runner.opened == [], "an ambiguous grant ran the turn"
            text = h.sink.last_text
            assert text is not None and text.startswith("curie-turn-failure: "), text
            classification = text.split("\n", 1)[0].removeprefix("curie-turn-failure: ")
            assert classification in RETRYABLE_CLASSIFICATIONS, classification
            assert len(binding.api.of_mode("open")) >= 2, "the refusal was not retried"

    asyncio.run(go())


def _surface_claim_bundle_refs(h: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the fake control plane report each claim's ``CURIE_BUNDLE_REF``.

    A real claim carries it in its spec env (``sandbox/k8s.py``); the kernel
    reads it back through ``get_claim`` as ``ClaimView.bundle_ref``."""

    from dataclasses import replace

    from curie_worker.binding import BUNDLE_REF_ENV

    fake = h.fake_k8s
    envs: dict[str, dict[str, str]] = {}
    real_create = fake.create_claim
    real_get = fake.get_claim

    def create_claim(name: str, **kwargs: Any) -> None:
        envs[name] = dict(kwargs.get("env") or {})
        real_create(name, **kwargs)

    def get_claim(name: str, **kwargs: Any) -> Any:
        view = real_get(name, **kwargs)
        if view is None:
            return None
        return replace(view, bundle_ref=envs.get(name, {}).get(BUNDLE_REF_ENV))

    monkeypatch.setattr(fake, "create_claim", create_claim)
    monkeypatch.setattr(fake, "get_claim", get_claim)


@pytest.mark.parametrize("bundle", ["matching", "mismatched"])
def test_a_retained_sandbox_without_a_pin_recovers_it_from_the_claim(
    make_harness, monkeypatch: pytest.MonkeyPatch, bundle: str
) -> None:
    # The pin is gone (an older worker opened the sandbox, or it expired). The
    # claim's bundle ref decides: equal to the resolved row's pins the resolved
    # deployment; different gives no capability and no pin.
    redeployed = uuid.UUID("44444444-4444-4444-8444-444444444444")

    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding) as h:
            binding.attach(h)
            _surface_claim_bundle_refs(h, monkeypatch)
            h.runner.channel_read_enforced = True
            first = _qevent("before", thread="th-cr-l8")
            await h.kernel.process_event(first)
            thread_key = _thread_key(first)
            await h.async_redis.delete(h.config.channel_read_pin_key(thread_key))

            binding.deployment_id = redeployed
            binding.bundle_ref = _BUNDLE_V1 if bundle == "matching" else _BUNDLE_V2
            await h.kernel.process_event(_qevent("after", thread="th-cr-l8"))

            assert h.runner.opened == ["before", "after"]
            assert len(h.fake_k8s.claim_envs) == 1, "the second turn did not retain the sandbox"
            opens = binding.api.of_mode("open")
            pin = await h.kernel._markers.read_channel_read_pin(thread_key)
            if bundle == "matching":
                assert [c["deployment_id"] for c in opens] == [DEPLOYMENT_ID, redeployed]
                assert h.runner.event_bodies[1]["channel_read"] is not None
                assert pin is not None and pin.deployment_id == redeployed
            else:
                assert len(opens) == 1, opens
                assert h.runner.event_bodies[1]["channel_read"] is None
                assert pin is None

    asyncio.run(go())


# --------------------------------------------------------------------------- #
# Review round 3: the grant and the enforcement check survive pin and bundle
# failures
# --------------------------------------------------------------------------- #


def _fail_pin(h: Any, monkeypatch: pytest.MonkeyPatch, which: str) -> None:
    async def failing(*args: Any, **kwargs: Any) -> Any:
        raise RedisConnectionError("Valkey is unavailable")

    name = "read_channel_read_pin" if which == "read" else "pin_channel_read_deployment"
    monkeypatch.setattr(h.kernel._markers, name, failing)


def _classification(h: Any) -> str:
    text = h.sink.last_text
    assert text is not None and text.startswith("curie-turn-failure: "), text
    return str(text.split("\n", 1)[0].removeprefix("curie-turn-failure: "))


@pytest.mark.parametrize("which", ["read", "write"])
def test_a_pin_failure_still_refuses_a_granted_turn_on_an_unenforcing_runner(
    make_harness, monkeypatch: pytest.MonkeyPatch, which: str
) -> None:
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding, max_attempts=3) as h:
            binding.attach(h)
            _fail_pin(h, monkeypatch, which)
            assert h.runner.channel_read_enforced is None

            await h.kernel.process_event(_qevent("summarize", thread=f"th-cr-p1-{which}"))

            assert h.runner.opened == [], "a granted turn reached a runner that cannot enforce"
            assert _classification(h) == "channel-read-unenforced"

    asyncio.run(go())


@pytest.mark.parametrize("which", ["read", "write"])
def test_a_pin_failure_runs_a_granted_turn_on_an_enforcing_runner_without_capability(
    make_harness, monkeypatch: pytest.MonkeyPatch, which: str
) -> None:
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding) as h:
            binding.attach(h)
            _fail_pin(h, monkeypatch, which)
            h.runner.channel_read_enforced = True

            await h.kernel.process_event(_qevent("summarize", thread=f"th-cr-p2-{which}"))

            assert h.runner.opened == ["summarize"]
            assert h.runner.event_bodies[0]["channel_read"] is None
            assert h.sink.last_text == "ok"

    asyncio.run(go())


@pytest.mark.parametrize("which", ["read", "write"])
def test_a_pin_failure_with_an_ungranted_bundle_runs_normally(
    make_harness, monkeypatch: pytest.MonkeyPatch, which: str
) -> None:
    # Liveness control: nothing to enforce, so a pin failure changes nothing.
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding) as h:
            binding.attach(h)
            binding.bundles.ungranted.add(_BUNDLE_V1)
            _fail_pin(h, monkeypatch, which)
            assert h.runner.channel_read_enforced is None

            await h.kernel.process_event(_qevent("summarize", thread=f"th-cr-p3-{which}"))

            assert h.runner.opened == ["summarize"]
            assert h.runner.event_bodies[0]["channel_read"] is None
            assert binding.api.calls == []
            assert h.sink.last_text == "ok"

    asyncio.run(go())


def _manifest_only(text: str | None) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("README.md", "no plugin here")
        if text is not None:
            archive.writestr(".claude-plugin/plugin.json", text)
    return buffer.getvalue()


@pytest.mark.parametrize(
    "bundle",
    [
        _manifest_only(None),
        _manifest_only("{not json"),
        _manifest_only(json.dumps({"name": "acme-bot", "channelRead": "yes"})),
    ],
    ids=["manifest-missing", "manifest-not-json", "channel-read-not-a-bool"],
)
def test_a_missing_or_malformed_manifest_is_an_unknown_grant(make_harness, bundle: bytes) -> None:
    from curie_worker.kernel.constants import RETRYABLE_CLASSIFICATIONS

    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding, max_attempts=3) as h:
            binding.attach(h)
            binding.bundles.raw[_BUNDLE_V1] = bundle
            binding.api.times_out = True
            h.runner.channel_read_enforced = True

            await h.kernel.process_event(_qevent("summarize", thread="th-cr-m1"))

            assert h.runner.opened == [], "a damaged manifest was read as no grant"
            assert _classification(h) in RETRYABLE_CLASSIFICATIONS

            # Not cached: once the bundle reads cleanly, the next turn mints.
            del binding.bundles.raw[_BUNDLE_V1]
            binding.api.times_out = False
            await h.kernel.process_event(_qevent("again", thread="th-cr-m2"))

            assert h.runner.opened == ["again"]
            assert binding.api.mints, "the damaged read was cached as no grant"
            assert h.runner.event_bodies[-1]["channel_read"] is not None

    asyncio.run(go())


def test_a_slow_bundle_lookup_times_out_as_an_unknown_grant(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_worker.kernel import channel_read as kernel_channel_read
    from curie_worker.kernel.constants import RETRYABLE_CLASSIFICATIONS

    monkeypatch.setattr(kernel_channel_read, "_BUNDLE_GRANT_TIMEOUT_S", 0.2, raising=False)
    slow_s = 1.0

    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding, max_attempts=3) as h:
            binding.attach(h)
            # Every lookup in the first turn is slow and would say granted.
            binding.bundles.scripted = [(slow_s, True)] * 3
            binding.api.times_out = True
            h.runner.channel_read_enforced = True

            started = time.monotonic()
            await h.kernel.process_event(_qevent("summarize", thread="th-cr-t1"))
            elapsed = time.monotonic() - started

            assert h.runner.opened == [], "the turn waited out the slow lookup"
            assert _classification(h) in RETRYABLE_CLASSIFICATIONS
            assert elapsed < slow_s, f"the lookup was not bounded ({elapsed:.2f}s)"

            # Not cached, even after the abandoned lookups finish: the next
            # turn reads again, and this time the bundle says no grant.
            await asyncio.sleep(slow_s + 0.3)
            binding.bundles.scripted = [(0.0, False)]
            binding.api.times_out = False
            opens_before = len(binding.api.of_mode("open"))
            await h.kernel.process_event(_qevent("again", thread="th-cr-t2"))

            assert h.runner.opened == ["again"]
            assert len(binding.api.of_mode("open")) == opens_before, (
                "a timed out lookup's late answer was cached as granted"
            )
            assert not binding.api.mints
            assert h.runner.event_bodies[-1]["channel_read"] is None

    asyncio.run(go())


# --------------------------------------------------------------------------- #
# Review round 4: with the pin unreadable, the grant is the sandbox's booted
# bundle, never the newly resolved one
# --------------------------------------------------------------------------- #


async def _redeploy_then_fail_the_pin(
    h: Any,
    binding: _ChannelReadBinding,
    monkeypatch: pytest.MonkeyPatch,
    *,
    booted_granted: bool,
) -> None:
    """Boot a sandbox on V1, redeploy to V2 with the opposite grant, then
    make the pin unreadable and send a second turn to the retained sandbox."""

    thread = f"th-cr-r4-{'granted' if booted_granted else 'ungranted'}-boot"
    if booted_granted:
        binding.bundles.ungranted.add(_BUNDLE_V2)
    else:
        binding.bundles.ungranted.add(_BUNDLE_V1)
    h.runner.channel_read_enforced = True
    await h.kernel.process_event(_qevent("before", thread=thread))
    assert h.runner.opened == ["before"]

    binding.deployment_id = uuid.UUID("44444444-4444-4444-8444-444444444444")
    binding.bundle_ref = _BUNDLE_V2
    _fail_pin(h, monkeypatch, "read")
    h.runner.channel_read_enforced = None
    await h.kernel.process_event(_qevent("after", thread=thread))
    assert len(h.fake_k8s.claim_envs) == 1, "the second turn did not retain the sandbox"


def test_a_retained_granted_boot_refuses_an_unenforcing_runner_after_an_ungranted_redeploy(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding, max_attempts=3) as h:
            binding.attach(h)

            await _redeploy_then_fail_the_pin(h, binding, monkeypatch, booted_granted=True)

            assert h.runner.opened == ["before"], "the retained granted turn reached the runner"
            assert _classification(h) == "channel-read-unenforced"

    asyncio.run(go())


def test_a_retained_ungranted_boot_runs_after_a_granted_redeploy(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The reverse: the sandbox runs the ungranted bundle, so there is nothing
    # to enforce even though the resolved row now grants channel read.
    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding, max_attempts=3) as h:
            binding.attach(h)

            await _redeploy_then_fail_the_pin(h, binding, monkeypatch, booted_granted=False)

            assert h.runner.opened == ["before", "after"], h.sink.last_text
            assert h.runner.event_bodies[-1]["channel_read"] is None
            assert h.sink.last_text == "ok"

    asyncio.run(go())


def test_an_unknown_booted_bundle_with_the_pin_unreadable_refuses_retryably(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Neither the route nor the claim names the bundle the sandbox booted, and
    # the pin cannot be read: which grant applies is unknown.
    from dataclasses import replace

    from curie_worker.kernel.constants import RETRYABLE_CLASSIFICATIONS

    async def go() -> None:
        binding = _ChannelReadBinding()
        async with make_harness(binding=binding, max_attempts=3) as h:
            binding.attach(h)
            h.runner.channel_read_enforced = True
            _fail_pin(h, monkeypatch, "read")

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

    asyncio.run(go())
