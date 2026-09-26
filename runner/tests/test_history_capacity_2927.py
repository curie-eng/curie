"""#2927: a one-turn coding thread must resume a review turn without a capacity refusal.

A coding turn (file reads, an edit, a pytest run, the native checkpoint) stores
tens of KiB. The state API caps the WHOLE transcript array at 64 KiB, so the old
boot compaction, which re-embedded the only turn in a summary tail, doubled the
array past the cap and refused every later turn, and two coding turns could not
share one array.

Everything here drives the real ``StateApiTranscriptStore`` over HTTP against a
fake state API that enforces the whole-array cap on append AND put, honors the
append ``reserve_bytes`` headroom, and implements compare-and-set versions. The
turn side runs through a real ``SessionRunner`` with the scripted fake model, and
boot runs through ``__main__._load_history``.

This module imports only names that exist before the fix, so it collects on the
base and fails by assertion.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import aiohttp
import anyio
import pytest
from aci_protocol import Event, Final, SessionStatus, parse_ndjson_line
from aiohttp import web
from aiohttp.test_utils import TestServer
from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner import __main__ as boot
from curie_runner.config import RunnerConfig
from curie_runner.fake import FakeModelSession
from curie_runner.history import (
    HarnessReplayState,
    HistoryAppendError,
    HistoryCapacityError,
    HistoryError,
    StateApiTranscriptStore,
    SummaryRecord,
    TurnRecord,
    build_conversation_replay,
)
from curie_runner.session import SessionRunner

_CAP = 65_536
_RESERVE = 8_192
_KEY = "/agents/A/state/transcript/t1"
_BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'


def _size(value: Any) -> int:
    return len(json.dumps(value, separators=(",", ":")).encode("utf-8"))


class _CappedCasState:
    """A transcript key on a fake state API with the real API's write rules.

    - whole-array compact-JSON cap on POST /append and on PUT (413);
    - optional ``reserve_bytes`` on append: 413 when the new array would leave
      fewer than that many bytes free under the cap;
    - a version bumped on every write, and CAS PUT with ``expected_version``
      (409 on mismatch);
    - every request recorded as ``(method, body, status)``.
    """

    def __init__(
        self, seed: list[dict[str, Any]] | None = None, *, max_bytes: int = _CAP
    ) -> None:
        self.value: list[dict[str, Any]] | None = list(seed) if seed else None
        self.max_bytes = max_bytes
        self.cap_header: str | None = str(max_bytes)
        self.version = 1 if seed else 0
        self.requests: list[tuple[str, Any, int]] = []
        # Test hooks for interleavings and forced statuses.
        self.inject_after_first_get: dict[str, Any] | None = None
        self.concurrent_item_before_first_put: dict[str, Any] | None = None
        self.force_append_status: int | None = None
        self.force_put_status: int | None = None
        self._gets = 0
        self._puts = 0

    def methods(self) -> list[tuple[str, int]]:
        return [(method, status) for method, _body, status in self.requests]

    def _write(self, value: list[dict[str, Any]]) -> None:
        self.value = value
        self.version += 1

    def _entry(self) -> dict[str, Any]:
        return {
            "namespace": "transcript",
            "key": "t1",
            "value": list(self.value or []),
            "version": self.version,
        }

    def app(self) -> web.Application:
        app = web.Application()

        async def get_key(_request: web.Request) -> web.Response:
            self._gets += 1
            headers = (
                {"X-Curie-Transcript-Max-Bytes": self.cap_header}
                if self.cap_header is not None
                else {}
            )
            if self.value is None:
                self.requests.append(("GET", None, 404))
                return web.json_response({"detail": "not found"}, status=404, headers=headers)
            response = self._entry()
            self.requests.append(("GET", None, 200))
            if self._gets == 1 and self.inject_after_first_get is not None:
                # A concurrent writer lands right after this reader's load.
                self._write([*(self.value or []), self.inject_after_first_get])
                self.inject_after_first_get = None
            return web.json_response(response, headers=headers)

        async def append_key(request: web.Request) -> web.Response:
            body = await request.json()
            if self.force_append_status is not None:
                self.requests.append(("POST", body, self.force_append_status))
                return web.json_response({"detail": "forced"}, status=self.force_append_status)
            candidate = [*(self.value or []), body["item"]]
            size = _size(candidate)
            reserve = body.get("reserve_bytes")
            if size > self.max_bytes:
                self.requests.append(("POST", body, 413))
                return web.json_response(
                    {"detail": f"value is {size} bytes, over the {self.max_bytes}-byte cap"},
                    status=413,
                )
            if reserve is not None and self.max_bytes - size < int(reserve):
                self.requests.append(("POST", body, 413))
                return web.json_response(
                    {"detail": f"value is {size} bytes, leaves under the {reserve}-byte reserve"},
                    status=413,
                )
            self._write(candidate)
            self.requests.append(("POST", body, 200))
            return web.json_response(self._entry())

        async def put_key(request: web.Request) -> web.Response:
            body = await request.json()
            self._puts += 1
            if self._puts == 1 and self.concurrent_item_before_first_put is not None:
                # Another writer appends between the compactor's GET and its PUT.
                self._write([*(self.value or []), self.concurrent_item_before_first_put])
                self.concurrent_item_before_first_put = None
            if self.force_put_status is not None:
                self.requests.append(("PUT", body, self.force_put_status))
                return web.json_response({"detail": "forced"}, status=self.force_put_status)
            expected = body.get("expected_version")
            if expected is not None and (self.value is None or expected != self.version):
                self.requests.append(("PUT", body, 409))
                return web.json_response(
                    {"detail": f"version mismatch: expected {expected}, stored {self.version}"},
                    status=409,
                )
            value = body["value"]
            size = _size(value)
            if size > self.max_bytes:
                self.requests.append(("PUT", body, 413))
                return web.json_response(
                    {"detail": f"value is {size} bytes, over the {self.max_bytes}-byte cap"},
                    status=413,
                )
            self._write(list(value))
            self.requests.append(("PUT", body, 200))
            return web.json_response(self._entry())

        app.router.add_get(_KEY, get_key)
        app.router.add_post(f"{_KEY}/append", append_key)
        app.router.add_put(_KEY, put_key)
        return app


# --- realistic coding-turn payloads ---------------------------------------------


def _source_file(lines: int) -> str:
    return "".join(
        f"    def rule_{i}(self, amount: Decimal) -> Decimal:\n"
        f'        """Pricing rule {i}: apply the tiered discount."""\n'
        f"        return (amount * Decimal('0.{i % 97:02d}')).quantize(CENTS)\n\n"
        for i in range(lines)
    )


def _pytest_output(tests: int) -> str:
    body = "".join(
        f"tests/test_pricing.py::test_rule_{i}_rounds_half_even PASSED"
        f"{' ' * 8}[{(i + 1) * 100 // tests:3d}%]\n"
        for i in range(tests)
    )
    return (
        "============================= test session starts ==============================\n"
        "platform linux -- Python 3.12.3, pytest-8.3.2, pluggy-1.5.0\n"
        f"collected {tests} items\n\n{body}\n"
        f"============================== {tests} passed in 3.41s ===============================\n"
    )


def _git_diff(hunks: int) -> str:
    return "diff --git a/src/pricing.py b/src/pricing.py\n" + "".join(
        f"@@ -{10 * i},3 +{10 * i},3 @@ class PricingRules:\n"
        f"-        return amount * Decimal('0.{i:02d}')\n"
        f"+        return (amount * Decimal('0.{i:02d}')).quantize(CENTS)\n"
        for i in range(hunks)
    )


def _result(text: str) -> ResultMessage:
    return ResultMessage(
        subtype="success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=False,
        num_turns=1,
        session_id="fake-session",
        result=text,
        usage={"input_tokens": 10, "output_tokens": 10},
    )


def _coding_turn_script(tag: str, *, scale: int = 1) -> list[Any]:
    """A turn a real coding session produces: read, edit, pytest, diff, push."""

    final = (
        f"[{tag}] Updated the pricing rules to quantize to cents and added tests; "
        "all pricing tests pass and the change is published for review. "
        + ("Details: rounding is half-even on every tier. " * 10)
    )
    return [
        AssistantMessage(
            content=[TextBlock(text=f"[{tag}] Reading the pricing module first.")],
            model="fake-model",
        ),
        AssistantMessage(
            content=[
                ToolUseBlock(id=f"{tag}-read", name="Read", input={"file_path": "src/pricing.py"})
            ],
            model="fake-model",
        ),
        UserMessage(
            content=[
                ToolResultBlock(
                    tool_use_id=f"{tag}-read",
                    content=_source_file(125 * scale),
                    is_error=False,
                )
            ]
        ),
        AssistantMessage(
            content=[
                ToolUseBlock(
                    id=f"{tag}-edit",
                    name="Edit",
                    input={
                        "file_path": "src/pricing.py",
                        "old_string": _source_file(6),
                        "new_string": _source_file(6).replace("quantize", "quantize_half_even"),
                    },
                )
            ],
            model="fake-model",
        ),
        UserMessage(
            content=[
                ToolResultBlock(
                    tool_use_id=f"{tag}-edit",
                    content="The file src/pricing.py has been updated.",
                    is_error=False,
                )
            ]
        ),
        AssistantMessage(
            content=[
                ToolUseBlock(
                    id=f"{tag}-pytest",
                    name="Bash",
                    input={"command": "uv run pytest tests/test_pricing.py -v"},
                )
            ],
            model="fake-model",
        ),
        UserMessage(
            content=[
                ToolResultBlock(
                    tool_use_id=f"{tag}-pytest",
                    content=_pytest_output(120 * scale),
                    is_error=False,
                )
            ]
        ),
        AssistantMessage(
            content=[ToolUseBlock(id=f"{tag}-diff", name="Bash", input={"command": "git diff"})],
            model="fake-model",
        ),
        UserMessage(
            content=[
                ToolResultBlock(
                    tool_use_id=f"{tag}-diff",
                    content=_git_diff(40 * scale),
                    is_error=False,
                )
            ]
        ),
        AssistantMessage(
            content=[
                ToolUseBlock(
                    id=f"{tag}-publish",
                    name="Bash",
                    input={"command": "git push origin pricing-cents"},
                )
            ],
            model="fake-model",
        ),
        UserMessage(
            content=[
                ToolResultBlock(
                    tool_use_id=f"{tag}-publish",
                    content=(
                        "To github.com:acme/pricing.git\n"
                        " * [new branch] pricing-cents -> pricing-cents\n"
                    ),
                    is_error=False,
                )
            ]
        ),
        AssistantMessage(content=[TextBlock(text=final)], model="fake-model"),
        _result(final),
    ]


class _CheckpointingFake(FakeModelSession):
    """The fake model plus the Claude harness's native checkpoint export."""

    def __init__(self, *args: Any, kind: str, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._kind = kind

    async def export_replay_state(self) -> HarnessReplayState:
        return HarnessReplayState(
            harness="claude",
            kind=self._kind,
            entries=tuple(
                {
                    "type": "assistant",
                    "uuid": f"{self._kind}-{i}",
                    "cwd": "/workspace",
                    "payload": "native-" + ("n" * 700),
                }
                for i in range(6)
            ),
        )

    def request_full_checkpoint(self) -> None:
        self._kind = "checkpoint"


def _runner(
    store: StateApiTranscriptStore,
    session: FakeModelSession,
    *,
    resumed: bool = False,
    capacity_exceeded: bool = False,
) -> SessionRunner:
    return SessionRunner(
        session_factory=lambda: session,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="history-capacity-2927",
        session_id="session-2927",
        history_store=store,
        history_resumed=resumed,
        history_capacity_exceeded=capacity_exceeded,
    )


async def _run_turn(runner: SessionRunner, text: str, ts: str) -> Final:
    await runner.start()
    lines = [
        line async for line in runner.run_inbound(Event(type="message", text=text, user="U", ts=ts))
    ]
    final = parse_ndjson_line(lines[-1])
    assert isinstance(final, Final)
    return final


def _config(tmp_path: Path, history_ref: str) -> RunnerConfig:
    plugin_dir = tmp_path / "bundle"
    plugin_dir.mkdir(exist_ok=True)
    return RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(plugin_dir),
            "CURIE_SESSION_ID": "s-2927",
            "CURIE_SANDBOX_ID": "b-2927",
            "CURIE_BUDGET": _BUDGET,
            "CURIE_HISTORY_REF": history_ref,
        }
    )


def _publication_item() -> dict[str, Any]:
    """The worker's publication outcome record (publication_loop.py), ~2000 chars."""

    return {
        "user": "Platform publication outcome",
        "assistant": "Published pull request #41 for branch pricing-cents. "
        + ("Checks queued; review requested from the code owners. " * 40)[:1_950],
        "ts": "2026-09-22T12:00:00+00:00",
        "publication_id": str(uuid.uuid4()),
    }


def _replay_text(messages: Any) -> str:
    return json.dumps([message.to_dict() for message in messages])


async def _post_item(url: str, item: dict[str, Any]) -> int:
    async with aiohttp.ClientSession() as http:
        async with http.post(f"{url}/append", json={"item": item}) as resp:
            return resp.status


# --- Test 1: the fix pin ----------------------------------------------------------


def test_one_turn_coding_thread_resumes_review_turn_without_capacity_refusal(
    tmp_path: Path,
) -> None:
    state = _CappedCasState()
    coding_text = "CODING-2927: quantize the pricing rules to cents and publish a PR"
    review_text = "REVIEW-2927: address the review comments on PR 41 and push a revision"
    publication = _publication_item()

    async def go() -> None:
        async with TestServer(state.app()) as server:
            url = str(server.make_url(_KEY))

            # Turn 1: a real coding turn through the runner and the HTTP store.
            first_store = StateApiTranscriptStore(url, token=None)
            assert await first_store.load() == []
            first = _runner(
                first_store,
                _CheckpointingFake(lambda: _coding_turn_script("coding"), kind="checkpoint"),
            )
            first_final = await _run_turn(first, coding_text, "1")
            assert first_final.status is SessionStatus.DONE
            assert first.history_durable is True
            stored_one_turn = _size(state.value)
            # The shape of a real coding turn: tens of KiB, alone under the cap.
            assert 40_000 <= stored_one_turn <= 60_000, stored_one_turn

            # Boot a fresh sandbox on this thread.
            config = _config(tmp_path, url)
            store, replay, capacity_exceeded = await boot._load_history(config)
            assert capacity_exceeded is False
            assert state.value is not None
            assert _size(state.value) <= _CAP
            assert replay.present
            assert coding_text in _replay_text(replay.messages)

            # Turn 2: the resumed review turn, also a large coding turn.
            review = _runner(
                store,
                _CheckpointingFake(
                    lambda: _coding_turn_script("review"),
                    kind="checkpoint",
                    replay_messages=replay.messages,
                ),
                resumed=replay.present,
                capacity_exceeded=capacity_exceeded,
            )
            review_final = await _run_turn(review, review_text, "2")
            assert review_final.status is SessionStatus.DONE, review_final
            assert review_final.status is not SessionStatus.CLASSIFIED_FAILURE
            assert review.history_durable is True
            assert _size(state.value) <= _CAP - _RESERVE, _size(state.value)

            # The worker's publication outcome appends WITHOUT a reserve and fits.
            assert await _post_item(url, publication) == 200

            # Third boot: still serving, and the review turn is in the replay.
            _store, third_replay, third_capacity = await boot._load_history(config)
            assert third_capacity is False
            assert review_text in _replay_text(third_replay.messages)
            assert state.value is not None
            assert _size(state.value) <= _CAP
            assert any(
                item.get("publication_id") == publication["publication_id"] for item in state.value
            ), "the worker's publication marker must survive compaction"

    anyio.run(go)


# --- Test 7: a near-cap boot summary is refused by the reserve and compacted -----


def _big_turn(tag: str, pad: int, ts: str) -> TurnRecord:
    """A stored coding-shaped turn whose tool result carries ``pad`` bytes."""

    from curie_runner.history import ConversationMessage

    return TurnRecord(
        user=f"{tag}: work item",
        assistant=f"{tag}: done",
        ts=ts,
        messages=(
            ConversationMessage(role="user", content=f"{tag}: work item"),
            ConversationMessage(
                role="assistant",
                content=[
                    {
                        "type": "tool_use",
                        "id": f"{tag}-t",
                        "name": "Bash",
                        "input": {"command": "pytest"},
                    }
                ],
            ),
            ConversationMessage(
                role="user",
                content=[{"type": "tool_result", "tool_use_id": f"{tag}-t", "content": "p" * pad}],
            ),
            ConversationMessage(
                role="assistant", content=[{"type": "text", "text": f"{tag}: done"}]
            ),
        ),
    )


def _near_cap_seed(free_after_summary: int) -> list[dict[str, Any]]:
    """Two turns whose boot summary append lands ``free_after_summary`` under the cap."""

    second = _big_turn("SECOND-2927", 18_000, "2026-09-22T00:00:02+00:00")

    def total(pad: int) -> int:
        first = _big_turn("FIRST-2927", pad, "2026-09-22T00:00:01+00:00")
        _replay, summary = build_conversation_replay([first, second])
        assert summary is not None
        return _size([first.to_dict(), second.to_dict(), summary.to_dict()])

    base_pad = 10_000
    pad = base_pad + (_CAP - free_after_summary) - total(base_pad)
    first = _big_turn("FIRST-2927", pad, "2026-09-22T00:00:01+00:00")
    seeded = [first.to_dict(), second.to_dict()]
    assert _CAP - free_after_summary - 200 <= total(pad) <= _CAP - free_after_summary + 200
    return seeded


def test_near_cap_boot_summary_is_refused_by_the_reserve_and_compacted(
    tmp_path: Path,
) -> None:
    state = _CappedCasState(_near_cap_seed(free_after_summary=1_000))
    publication = _publication_item()

    async def go() -> None:
        async with TestServer(state.app()) as server:
            url = str(server.make_url(_KEY))
            _store, replay, capacity_exceeded = await boot._load_history(_config(tmp_path, url))
            assert capacity_exceeded is False
            # Boot must not leave the key full: the reserve is free afterwards.
            assert state.value is not None
            assert _size(state.value) <= _CAP - _RESERVE, _size(state.value)
            assert "SECOND-2927" in _replay_text(replay.messages)
            # So the worker's publication outcome still appends.
            assert await _post_item(url, publication) == 200
            assert any(
                item.get("publication_id") == publication["publication_id"] for item in state.value
            )

    anyio.run(go)


# --- Test 4: store-level compaction and conflict behavior ------------------------


def _near_cap_turns() -> list[dict[str, Any]]:
    """Two stored turns that leave about 4 KiB free under the cap."""

    first = _big_turn("OLD-2927", 30_000, "2026-09-22T00:00:01+00:00").to_dict()
    second_pad = _CAP - 4_000 - _size([first, _big_turn("MID-2927", 0, "x").to_dict()])
    second = _big_turn("MID-2927", second_pad, "2026-09-22T00:00:02+00:00").to_dict()
    assert _CAP - 4_200 <= _size([first, second]) <= _CAP - 3_800
    return [first, second]


def _new_turn() -> TurnRecord:
    return _big_turn("NEW-2927", 12_000, "2026-09-22T00:00:03+00:00")


def test_turn_append_413_compacts_with_cas_and_retries_a_put_conflict_with_a_fresh_get() -> None:
    state = _CappedCasState(_near_cap_turns())
    concurrent = _publication_item()
    state.concurrent_item_before_first_put = concurrent

    async def go() -> None:
        async with TestServer(state.app()) as server:
            store = StateApiTranscriptStore(str(server.make_url(_KEY)), token=None)
            await store.load()
            await store.append(_new_turn())

    anyio.run(go)

    puts = [status for method, status in state.methods() if method == "PUT"]
    assert puts == [409, 200], state.methods()
    # The retry re-read the key between the conflicting PUT and the winning one.
    methods = state.methods()
    first_put = methods.index(("PUT", 409))
    assert ("GET", 200) in methods[first_put + 1 : methods.index(("PUT", 200))]
    # The winning PUT carried the version the fresh GET returned.
    winning_body = next(
        body for method, body, status in state.requests if (method, status) == ("PUT", 200)
    )
    assert winning_body["expected_version"] == state.version - 1
    assert state.value is not None
    assert _size(state.value) <= _CAP - _RESERVE
    # The concurrent writer's marker survived and the new turn is the kept turn.
    assert concurrent in state.value
    assert state.value[-1]["user"] == "NEW-2927: work item"


def test_turn_append_compaction_put_413_raises_history_capacity_error() -> None:
    seed = _near_cap_turns()
    state = _CappedCasState(seed)
    state.force_put_status = 413

    async def go() -> None:
        async with TestServer(state.app()) as server:
            store = StateApiTranscriptStore(str(server.make_url(_KEY)), token=None)
            await store.load()
            with pytest.raises(HistoryCapacityError) as caught:
                await store.append(_new_turn())
            assert caught.value.status == 413

    anyio.run(go)
    # Compaction was attempted, refused, and the stored log is untouched.
    assert ("PUT", 413) in state.methods(), state.methods()
    assert state.value == seed


def test_non_capacity_append_failure_does_not_compact() -> None:
    seed = _near_cap_turns()
    state = _CappedCasState(seed)
    state.force_append_status = 500

    async def go() -> None:
        async with TestServer(state.app()) as server:
            store = StateApiTranscriptStore(str(server.make_url(_KEY)), token=None)
            await store.load()
            with pytest.raises(HistoryAppendError) as caught:
                await store.append(_new_turn())
            assert not isinstance(caught.value, HistoryCapacityError)
            assert caught.value.status == 500

    anyio.run(go)
    assert [method for method, _status in state.methods()] == ["GET", "POST"]
    assert state.value == seed


def test_summary_record_append_413_raises_capacity_error_without_a_put() -> None:
    seed = _near_cap_turns()
    state = _CappedCasState(seed)
    summary = SummaryRecord(
        content="summary " + ("s" * 8_000),
        digest="0" * 64,
        source_turns=1,
        through_ts="2026-09-22T00:00:01+00:00",
    )

    async def go() -> None:
        async with TestServer(state.app()) as server:
            store = StateApiTranscriptStore(str(server.make_url(_KEY)), token=None)
            await store.load()
            with pytest.raises(HistoryCapacityError):
                await store.append(summary)

    anyio.run(go)
    assert [method for method, _status in state.methods()] == ["GET", "POST"]
    assert state.value == seed


def test_boot_compaction_conflict_reloads_and_replays_the_intervening_record(
    tmp_path: Path,
) -> None:
    """Boot compacts against the version it loaded; a record appended after that
    load makes the compaction PUT conflict, and boot reloads rather than writing
    a stale summary over it."""

    first = _big_turn("FIRST-2927", 30_000, "2026-09-22T00:00:01+00:00").to_dict()
    second = _big_turn("SECOND-2927", 25_000, "2026-09-22T00:00:02+00:00").to_dict()
    state = _CappedCasState([first, second])
    injected = TurnRecord(
        user="INJECTED-2927: late turn from another runner",
        assistant="acknowledged",
        ts="2026-09-22T00:00:03+00:00",
    ).to_dict()
    state.inject_after_first_get = injected

    async def go() -> None:
        async with TestServer(state.app()) as server:
            url = str(server.make_url(_KEY))
            _store, replay, capacity_exceeded = await boot._load_history(_config(tmp_path, url))
            assert capacity_exceeded is False
            assert "INJECTED-2927" in _replay_text(replay.messages)

    anyio.run(go)

    puts = [status for method, status in state.methods() if method == "PUT"]
    assert puts and puts[0] == 409, state.methods()
    assert puts[-1] == 200, state.methods()
    assert state.value is not None
    assert _size(state.value) <= _CAP - _RESERVE
    assert "INJECTED-2927" in json.dumps(state.value)


# A factory sized turn must persist even when tool call structure exceeds the cap.


def _tiny_tool_calls_script(calls: int, *, final: str | None = None) -> list[Any]:
    """A factory sized turn whose tool call structure outweighs its tool output."""

    script: list[Any] = []
    for i in range(calls):
        script.append(
            AssistantMessage(
                content=[ToolUseBlock(id=f"u{i}", name="Bash", input={"command": "ls"})],
                model="fake-model",
            )
        )
        script.append(
            UserMessage(
                content=[ToolResultBlock(tool_use_id=f"u{i}", content="ok", is_error=False)]
            )
        )
    final = _FACTORY_FINAL if final is None else final
    script.append(AssistantMessage(content=[TextBlock(text=final)], model="fake-model"))
    script.append(_result(final))
    return script


_FACTORY_CALLS = 400
_FACTORY_FINAL = "The implementation passed review and the pull request was published."


def test_factory_sized_turn_persists_with_old_tool_calls_compacted() -> None:
    """The HTTP store persists hundreds of tool calls and the exact final answer."""
    state = _CappedCasState()

    async def go() -> None:
        async with TestServer(state.app()) as server:
            store = StateApiTranscriptStore(str(server.make_url(_KEY)), token=None)
            assert await store.load() == []
            runner = _runner(
                store,
                FakeModelSession(lambda: _tiny_tool_calls_script(_FACTORY_CALLS)),
            )
            final = await _run_turn(runner, "Complete factory issue 3211", "1")
            assert final.status is SessionStatus.DONE, final
            assert final.text == _FACTORY_FINAL
            assert runner.history_durable is True

    anyio.run(go)
    assert state.value is not None
    assert _size(state.value) <= _CAP - _RESERVE
    stored = TurnRecord.from_dict(state.value[-1])
    assert stored.assistant == _FACTORY_FINAL
    stored_text = json.dumps(stored.to_dict())
    assert '"id": "u0"' not in stored_text
    assert f'"id": "u{_FACTORY_CALLS - 1}"' in stored_text
    assert "tool" in stored_text.lower() and "omitted" in stored_text.lower()
    seen_uses: set[str] = set()
    marker_positions: list[int] = []
    for index, message in enumerate(stored.messages):
        if not isinstance(message.content, list):
            continue
        for block in message.content:
            if block.get("type") == "tool_use":
                seen_uses.add(block["id"])
            elif block.get("type") == "tool_result":
                assert block["tool_use_id"] in seen_uses
            elif block.get("type") == "text" and "tool groups omitted" in block.get("text", ""):
                marker_positions.append(index)
    assert marker_positions
    assert marker_positions[0] < len(stored.messages) - 1
    assert stored.messages[-1].role == "assistant"
    assert stored.messages[-1].content == [{"type": "text", "text": _FACTORY_FINAL}]


def test_factory_sized_turn_preserves_long_final_answer_and_first_user() -> None:
    first_user = "Complete factory issue 3211 with its original requirements"
    final_answer = "Review complete. " + ("The requested behavior is verified. " * 100)
    assert len(final_answer) > 3_000
    state = _CappedCasState()

    async def go() -> None:
        async with TestServer(state.app()) as server:
            store = StateApiTranscriptStore(str(server.make_url(_KEY)), token=None)
            assert await store.load() == []
            runner = _runner(
                store,
                FakeModelSession(
                    lambda: _tiny_tool_calls_script(_FACTORY_CALLS, final=final_answer)
                ),
            )
            final = await _run_turn(runner, first_user, "1")
            assert final.status is SessionStatus.DONE, final
            assert final.text == final_answer
            assert runner.history_durable is True

    anyio.run(go)
    assert state.value is not None
    assert _size(state.value) <= _CAP - _RESERVE
    stored = TurnRecord.from_dict(state.value[-1])
    assert stored.user == first_user
    assert stored.messages[0].content == first_user
    assert stored.assistant == final_answer
    assert stored.messages[-1].content == [{"type": "text", "text": final_answer}]


def test_runner_uses_the_cap_advertised_by_the_state_api() -> None:
    state = _CappedCasState(max_bytes=128 * 1024)

    async def go() -> None:
        async with TestServer(state.app()) as server:
            store = StateApiTranscriptStore(str(server.make_url(_KEY)), token=None)
            assert await store.load() == []
            runner = _runner(
                store,
                FakeModelSession(lambda: _tiny_tool_calls_script(_FACTORY_CALLS)),
            )
            final = await _run_turn(runner, "Complete factory issue 3211", "1")
            assert final.status is SessionStatus.DONE, final
            assert runner.history_durable is True

    anyio.run(go)
    assert state.value is not None
    assert _CAP - _RESERVE < _size(state.value) <= state.max_bytes - _RESERVE
    stored = TurnRecord.from_dict(state.value[-1])
    assert stored.assistant == _FACTORY_FINAL
    stored_text = json.dumps(stored.to_dict())
    assert '"id": "u0"' in stored_text
    assert f'"id": "u{_FACTORY_CALLS - 1}"' in stored_text


@pytest.mark.parametrize("cap_header", [None, "invalid", "0", "8000", "8192"])
def test_store_refuses_missing_or_invalid_api_capacity(cap_header: str | None) -> None:
    state = _CappedCasState()
    state.cap_header = cap_header

    async def go() -> None:
        async with TestServer(state.app()) as server:
            store = StateApiTranscriptStore(str(server.make_url(_KEY)), token=None)
            with pytest.raises(HistoryError):
                await store.load()

    anyio.run(go)
    assert state.value is None
    assert state.methods() == [("GET", 404)]
