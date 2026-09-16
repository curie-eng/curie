"""The conversation-history port: resolution, structured replay, the state-API
store, and the per-turn append that persists a thread's transcript (#20).

The StateApiTranscriptStore is exercised against a tiny in-memory fake of the
#248 log-shaped state endpoints (GET the key, POST .../append), so load/append
round-trip over real HTTP without the API. The write side is exercised by driving
a real turn through the SessionRunner with the fake model.
"""

import hashlib
import json

import anyio
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner.adapter import ClaudeAgentSession, build_options, build_structured_resume
from curie_runner.history import (
    ApprovalContext,
    ConversationMessage,
    HarnessReplayState,
    HistoryError,
    NullTranscriptStore,
    StateApiTranscriptStore,
    TranscriptStore,
    TurnRecord,
    build_conversation_replay,
    resolve_history,
)


def _fake_state_app() -> tuple[web.Application, list]:
    """A minimal fake of the state key at /agents/A/state/transcript/t1."""
    log: list = []
    app = web.Application()
    key = "/agents/A/state/transcript/t1"

    async def get_key(request: web.Request) -> web.Response:
        if not log:
            return web.json_response({"detail": "not found"}, status=404)
        return web.json_response(
            {"namespace": "transcript", "key": "t1", "value": list(log), "version": len(log)}
        )

    async def append_key(request: web.Request) -> web.Response:
        body = await request.json()
        log.append(body["item"])
        return web.json_response(
            {"namespace": "transcript", "key": "t1", "value": list(log), "version": len(log)}
        )

    app.router.add_get(key, get_key)
    app.router.add_post(f"{key}/append", append_key)
    return app, log


_STATE_VALUE_MAX_BYTES = 65_536


def _capped_state_app() -> tuple[web.Application, list, list[int]]:
    """A state-key fake that enforces the API's whole-value JSON byte cap."""

    log: list = []
    rejected_sizes: list[int] = []
    app = web.Application()
    key = "/agents/A/state/transcript/t1"

    async def get_key(_request: web.Request) -> web.Response:
        if not log:
            return web.json_response({"detail": "not found"}, status=404)
        return web.json_response(
            {"namespace": "transcript", "key": "t1", "value": list(log), "version": 1}
        )

    async def append_key(request: web.Request) -> web.Response:
        body = await request.json()
        candidate = [*log, body["item"]]
        encoded_size = len(
            json.dumps(candidate, separators=(",", ":")).encode("utf-8")
        )
        if encoded_size > _STATE_VALUE_MAX_BYTES:
            rejected_sizes.append(encoded_size)
            return web.json_response(
                {
                    "detail": (
                        f"value is {encoded_size} bytes, over the "
                        f"{_STATE_VALUE_MAX_BYTES}-byte per-value cap"
                    )
                },
                status=413,
            )
        log.append(body["item"])
        return web.json_response(
            {"namespace": "transcript", "key": "t1", "value": list(log), "version": 1}
        )

    app.router.add_get(key, get_key)
    app.router.add_post(f"{key}/append", append_key)
    return app, log, rejected_sizes


def _assert_digest_marker(value: object, original: str) -> None:
    assert isinstance(value, str)
    assert original not in value
    assert hashlib.sha256(original.encode("utf-8")).hexdigest() in value
    assert str(len(original.encode("utf-8"))) in value


def test_turn_record_round_trip() -> None:
    rec = TurnRecord(
        user="what changed?", assistant="the deploy bumped v3", ts="2026-07-14T00:00:00+00:00"
    )
    assert TurnRecord.from_dict(rec.to_dict()) == rec


def test_structured_turn_round_trip_preserves_tools_and_approval_context() -> None:
    record = TurnRecord(
        user="deploy release",
        assistant="approval required",
        ts="2026-08-30T00:00:00Z",
        status="awaiting_approval",
        messages=(
            ConversationMessage(role="user", content="deploy release"),
            ConversationMessage(
                role="assistant",
                content=[
                    {
                        "type": "tool_use",
                        "id": "call-1",
                        "name": "Bash",
                        "input": {"command": "deploy --release"},
                    }
                ],
            ),
            ConversationMessage(
                role="user",
                content=[
                    {
                        "type": "tool_result",
                        "tool_use_id": "call-1",
                        "content": "permission denied",
                        "is_error": True,
                    }
                ],
            ),
        ),
        approval=ApprovalContext(
            summary="Deploy release",
            route="release-managers",
            gate_kind="permission",
            granted_tool="Bash",
            decision=None,
        ),
        harness_replay=HarnessReplayState(
            harness="claude",
            kind="checkpoint",
            entries=({"type": "user", "uuid": "entry-1"},),
        ),
    )

    loaded = TurnRecord.from_dict(record.to_dict())

    assert loaded == record
    assert [message.role for message in loaded.messages] == ["user", "assistant", "user"]
    assert loaded.messages[1].content[0]["type"] == "tool_use"
    assert loaded.messages[2].content[0]["type"] == "tool_result"
    assert loaded.approval == record.approval


def test_legacy_turn_becomes_structured_user_assistant_messages() -> None:
    record = TurnRecord.from_dict(
        {"user": "old question", "assistant": "old answer", "ts": "2026-07-14T00:00:00Z"}
    )

    assert record.messages == (
        ConversationMessage(role="user", content="old question"),
        ConversationMessage(
            role="assistant", content=[{"type": "text", "text": "old answer"}]
        ),
    )


def test_malformed_structured_turn_is_rejected_instead_of_falling_back_to_legacy() -> None:
    with pytest.raises(HistoryError, match="structured conversation messages"):
        TurnRecord.from_dict(
            {
                "user": "must not silently survive",
                "assistant": "legacy fallback",
                "messages": [42],
            }
        )

def test_long_history_compacts_once_then_keeps_prefix_stable_until_next_boundary() -> None:
    records = [
        TurnRecord(user=f"u{i}", assistant=f"a{i}", ts=f"2026-08-30T00:00:0{i}Z")
        for i in range(6)
    ]

    replay, summary = build_conversation_replay(records, max_turns=4, max_bytes=None)

    assert summary is not None
    assert summary.source_turns == 4
    assert all(turn.harness_replay is None for turn in summary.tail)
    assert replay.summary_digest == summary.digest
    assert replay.messages[0].role == "user"
    assert "Durable conversation summary" in str(replay.messages[0].content)
    assert replay.messages[-2:] == records[-1].messages

    persisted = [*records, summary]
    with_one_more = [
        *persisted,
        TurnRecord(user="u6", assistant="a6", ts="2026-08-30T00:00:06Z"),
    ]
    replay_after, next_summary = build_conversation_replay(
        with_one_more, max_turns=4, max_bytes=None
    )

    assert next_summary is None
    assert replay_after.messages[: len(replay.messages)] == replay.messages


def test_changed_compacted_prefix_changes_summary_digest() -> None:
    records = [TurnRecord(user=f"u{i}", assistant=f"a{i}") for i in range(6)]
    _, original = build_conversation_replay(records, max_turns=4, max_bytes=None)
    changed = list(records)
    changed[0] = TurnRecord(user="changed-u0", assistant="a0")
    _, different = build_conversation_replay(changed, max_turns=4, max_bytes=None)

    assert original is not None
    assert different is not None
    assert different.digest != original.digest


def test_summary_digest_ignores_optional_harness_checkpoint_metadata() -> None:
    def records(entry_uuid: str) -> list[TurnRecord]:
        return [
            TurnRecord(
                user=f"u{i}",
                assistant=f"a{i}",
                harness_replay=HarnessReplayState(
                    harness="claude",
                    kind="checkpoint" if i == 0 else "delta",
                    entries=({"uuid": f"{entry_uuid}-{i}"},),
                ),
            )
            for i in range(4)
        ]

    _, first = build_conversation_replay(records("runner-a"), max_turns=2)
    _, second = build_conversation_replay(records("runner-b"), max_turns=2)

    assert first is not None
    assert second is not None
    assert first.digest == second.digest


def test_structured_resume_materializes_ordered_sdk_entries_without_rendering_text(
    tmp_path,
) -> None:
    messages = (
        ConversationMessage(role="user", content="run it"),
        ConversationMessage(
            role="assistant",
            content=[
                {
                    "type": "tool_use",
                    "id": "call-1",
                    "name": "Bash",
                    "input": {"command": "echo ok"},
                }
            ],
        ),
        ConversationMessage(
            role="user",
            content=[
                {
                    "type": "tool_result",
                    "tool_use_id": "call-1",
                    "content": "ok",
                }
            ],
        ),
        ConversationMessage(
            role="assistant", content=[{"type": "text", "text": "done"}]
        ),
    )

    first = build_structured_resume(
        messages, curie_session_id="curie-thread-1", cwd=str(tmp_path)
    )
    second = build_structured_resume(
        messages, curie_session_id="curie-thread-1", cwd=str(tmp_path)
    )

    assert first.resume == first.session_id
    assert first.session_id == second.session_id
    assert first.session_store is not None
    entries = anyio.run(first.session_store.load, first.session_key)
    assert entries is not None
    assert [entry["message"]["role"] for entry in entries] == [
        "user",
        "assistant",
        "user",
        "assistant",
    ]
    assert entries[1]["message"]["content"][0]["type"] == "tool_use"
    assert entries[2]["message"]["content"][0]["type"] == "tool_result"
    assert [entry["parentUuid"] for entry in entries][1:] == [
        entry["uuid"] for entry in entries[:-1]
    ]

    fresh = build_structured_resume((), curie_session_id="curie-thread-1", cwd=str(tmp_path))
    assert fresh.resume is None
    assert fresh.session_store is not None
    assert anyio.run(fresh.session_store.load, fresh.session_key) is None
    assert fresh.session_id == first.session_id


def test_claude_native_checkpoint_is_restored_and_then_exports_only_a_delta(tmp_path) -> None:
    checkpoint = HarnessReplayState(
        harness="claude",
        kind="checkpoint",
        entries=(
            {
                "type": "user",
                "uuid": "entry-1",
                "timestamp": "1970-01-01T00:00:00.000Z",
                "message": {"role": "user", "content": "prior"},
            },
        ),
    )
    resume = build_structured_resume(
        (ConversationMessage(role="user", content="prior"),),
        curie_session_id="curie-thread-native",
        cwd=str(tmp_path),
        harness_replay=checkpoint,
    )
    assert resume.session_store is not None
    assert anyio.run(resume.session_store.load, resume.session_key) == list(
        checkpoint.entries
    )

    options = build_options(
        plugins=[],
        model=None,
        system_prompt=None,
        max_turns=2,
        max_budget_usd=1.0,
        resume=resume.resume,
        session_id=resume.session_id,
        session_store=resume.session_store,
    )
    session = ClaudeAgentSession(options)
    delta_entry = {"type": "assistant", "uuid": "entry-2"}
    anyio.run(resume.session_store.append, resume.session_key, [delta_entry])

    exported = anyio.run(session.export_replay_state)

    assert exported == HarnessReplayState(
        harness="claude", kind="delta", entries=(delta_entry,)
    )


def test_replay_folds_native_checkpoint_and_deltas_but_drops_them_at_compaction() -> None:
    turns = [
        TurnRecord(
            user="u0",
            assistant="a0",
            harness_replay=HarnessReplayState(
                harness="claude", kind="checkpoint", entries=({"uuid": "one"},)
            ),
        ),
        TurnRecord(
            user="u1",
            assistant="a1",
            harness_replay=HarnessReplayState(
                harness="claude", kind="delta", entries=({"uuid": "two"},)
            ),
        ),
    ]

    replay, summary = build_conversation_replay(turns, max_turns=4, max_bytes=None)
    assert summary is None
    assert replay.harness_replay == HarnessReplayState(
        harness="claude",
        kind="checkpoint",
        entries=({"uuid": "one"}, {"uuid": "two"}),
    )

    compacted, summary = build_conversation_replay(
        [*turns, TurnRecord(user="u2", assistant="a2")],
        max_turns=2,
        max_bytes=None,
    )
    assert summary is not None
    assert compacted.harness_replay is None


def test_resolve_absent_ref_is_null_store() -> None:
    store = resolve_history(None, {})
    assert isinstance(store, NullTranscriptStore)
    assert anyio.run(store.load) == []
    # Append on the null store is a silent no-op.
    anyio.run(store.append, TurnRecord(user="u", assistant="a"))


def test_resolve_http_ref_is_state_store() -> None:
    store = resolve_history("http://api:8000/agents/A/state/transcript/t1", {})
    assert isinstance(store, StateApiTranscriptStore)


def test_resolve_unsupported_scheme_raises() -> None:
    # An old SDK-resume id (or any non-http ref) is rejected loudly, not silently
    # dropped, so a misconfigured ref fails visibly at boot.
    with pytest.raises(HistoryError):
        resolve_history("sdk-session-abc123", {})
    with pytest.raises(HistoryError):
        resolve_history("s3://bucket/hist", {})


def test_state_store_load_empty_is_empty() -> None:
    app, _ = _fake_state_app()

    async def go() -> None:
        async with TestServer(app) as server:
            url = str(server.make_url("/agents/A/state/transcript/t1"))
            store = StateApiTranscriptStore(url, token=None)
            assert await store.load() == []

    anyio.run(go)


def test_state_store_append_then_load_round_trip() -> None:
    app, log = _fake_state_app()

    async def go() -> None:
        async with TestServer(app) as server:
            url = str(server.make_url("/agents/A/state/transcript/t1"))
            store = StateApiTranscriptStore(url, token="k")
            await store.append(
                TurnRecord(user="q1", assistant="a1", ts="2026-07-14T00:00:00+00:00")
            )
            await store.append(TurnRecord(user="q2", assistant="a2"))
            loaded = await store.load()
            assert loaded == [
                TurnRecord(user="q1", assistant="a1", ts="2026-07-14T00:00:00+00:00"),
                TurnRecord(user="q2", assistant="a2"),
            ]
            assert len(log) == 2

    anyio.run(go)


def test_oversized_first_turn_is_bounded_before_append_and_cold_replays_in_order() -> None:
    """The live-sized first record must fit before the state API sees it (#2271)."""

    from aci_protocol import Event, Final, SessionStatus, parse_ndjson_line
    from claude_agent_sdk import (
        AssistantMessage,
        ResultMessage,
        TextBlock,
        ToolResultBlock,
        ToolUseBlock,
        UserMessage,
    )
    from curie_runner import RunTracer, SideEffectClassifier
    from curie_runner.fake import FakeModelSession
    from curie_runner.session import SessionRunner

    tool_payload = "tool-output-" + ("x" * 42_000)
    assistant_payload = "assistant-output-" + ("y" * 42_000)
    native_payload = "native-checkpoint-" + ("z" * 42_000)
    harness_replay = HarnessReplayState(
        harness="claude",
        kind="checkpoint",
        entries=({"type": "assistant", "payload": native_payload},),
    )
    raw_record = TurnRecord(
        user="inspect the example workspace",
        assistant=assistant_payload,
        messages=(
            ConversationMessage(role="user", content="inspect the example workspace"),
            ConversationMessage(
                role="assistant",
                content=[
                    {
                        "type": "tool_use",
                        "id": "call-example",
                        "name": "Read",
                        "input": {"path": "example.txt"},
                    }
                ],
            ),
            ConversationMessage(
                role="user",
                content=[
                    {
                        "type": "tool_result",
                        "tool_use_id": "call-example",
                        "content": tool_payload,
                    }
                ],
            ),
            ConversationMessage(
                role="assistant",
                content=[{"type": "text", "text": assistant_payload}],
            ),
        ),
        harness_replay=harness_replay,
    )
    raw_value_size = len(
        json.dumps([raw_record.to_dict()], separators=(",", ":")).encode("utf-8")
    )
    raw_without_native = TurnRecord(
        user=raw_record.user,
        assistant=raw_record.assistant,
        messages=raw_record.messages,
    )
    assert raw_value_size > 84 * 1024
    assert (
        len(
            json.dumps(
                [raw_without_native.to_dict()], separators=(",", ":")
            ).encode("utf-8")
        )
        > _STATE_VALUE_MAX_BYTES
    )

    class ReplayExportingFake(FakeModelSession):
        async def export_replay_state(self) -> HarnessReplayState:
            return harness_replay

    script = lambda: [  # noqa: E731 - compact fixed SDK transcript fixture
        AssistantMessage(
            content=[
                ToolUseBlock(
                    id="call-example",
                    name="Read",
                    input={"path": "example.txt"},
                )
            ],
            model="fake-model",
        ),
        UserMessage(
            content=[
                ToolResultBlock(
                    tool_use_id="call-example",
                    content=tool_payload,
                    is_error=False,
                )
            ]
        ),
        AssistantMessage(
            content=[TextBlock(text=assistant_payload)], model="fake-model"
        ),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="fake-session",
            result=assistant_payload,
            usage={"input_tokens": 1, "output_tokens": 1},
        ),
    ]
    app, log, rejected_sizes = _capped_state_app()

    async def go() -> None:
        async with TestServer(app) as server:
            key_url = str(server.make_url("/agents/A/state/transcript/t1"))
            session = ReplayExportingFake(script)
            runner = SessionRunner(
                session_factory=lambda: session,
                ceiling=0,
                tracer=RunTracer(None),
                classifier=SideEffectClassifier(),
                trace_name="bounded-history",
                session_id="session-example",
                history_store=StateApiTranscriptStore(key_url, token=None),
            )
            await runner.start()
            lines = [
                line
                async for line in runner.run_inbound(
                    Event(
                        type="message",
                        text="inspect the example workspace",
                        user="U",
                        ts="1",
                    )
                )
            ]
            final = parse_ndjson_line(lines[-1])
            assert isinstance(final, Final)
            assert final.status is SessionStatus.DONE
            assert runner.history_durable is True

            # Bounding is pre-append: the API must never reject a raw attempt.
            assert rejected_sizes == []
            assert len(log) == 1
            assert (
                len(json.dumps(log, separators=(",", ":")).encode("utf-8"))
                <= _STATE_VALUE_MAX_BYTES
            )

            cold_store = StateApiTranscriptStore(key_url, token=None)
            cold_records = await cold_store.load()
            replay, summary = build_conversation_replay(
                cold_records, max_turns=None, max_bytes=None
            )
            assert summary is None
            assert replay.present is True
            assert [message.role for message in replay.messages] == [
                "user",
                "assistant",
                "user",
                "assistant",
            ]

            bounded = cold_records[0]
            assert isinstance(bounded, TurnRecord)
            assert bounded.harness_replay is None
            assert bounded.user == "inspect the example workspace"
            _assert_digest_marker(bounded.assistant, assistant_payload)
            tool_result = bounded.messages[2].content
            assert isinstance(tool_result, list)
            _assert_digest_marker(tool_result[0]["content"], tool_payload)
            assistant_text = bounded.messages[3].content
            assert isinstance(assistant_text, list)
            _assert_digest_marker(assistant_text[0]["text"], assistant_payload)

    anyio.run(go)


def test_irreducibly_large_turn_fails_durability_before_store_append(caplog) -> None:
    """Role/order overhead cannot be dropped merely to manufacture a small record."""

    from aci_protocol import Event, SessionStatus
    from claude_agent_sdk import AssistantMessage, ResultMessage

    class CountingStore(_RecordingStore):
        def __init__(self) -> None:
            super().__init__()
            self.append_attempts = 0

        async def append(self, record: TurnRecord) -> None:
            self.append_attempts += 1
            await super().append(record)

    empty_assistant_messages = 3_000
    script = lambda: [  # noqa: E731 - compact fixed SDK transcript fixture
        *(
            AssistantMessage(content=[], model="fake-model")
            for _ in range(empty_assistant_messages)
        ),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="fake-session",
            result="answer",
            usage={"input_tokens": 1, "output_tokens": 1},
        ),
    ]
    irreducible = TurnRecord(
        user="q",
        assistant="answer",
        messages=(
            ConversationMessage(role="user", content="q"),
            *(
                ConversationMessage(role="assistant", content=[])
                for _ in range(empty_assistant_messages)
            ),
        ),
    )
    assert (
        len(json.dumps([irreducible.to_dict()], separators=(",", ":")).encode("utf-8"))
        > _STATE_VALUE_MAX_BYTES
    )

    store = CountingStore()
    runner = _recording_runner(store, script=script)
    final = _run_recording_turn(
        runner, Event(type="message", text="q", user="U", ts="1")
    )

    assert final.status is SessionStatus.DONE
    assert runner.history_durable is False
    assert store.append_attempts == 0
    assert store.turns == []
    assert any(
        "history append failed" in record.getMessage()
        and "HistoryError" in record.getMessage()
        for record in caplog.records
    )


def test_history_durability_failure_is_sticky_after_later_successful_append() -> None:
    """A later turn cannot make an earlier dropped turn safe to hand off."""

    from aci_protocol import Event, Final, SessionStatus, parse_ndjson_line
    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

    class CountingStore(_RecordingStore):
        def __init__(self) -> None:
            super().__init__()
            self.append_attempts = 0

        async def append(self, record: TurnRecord) -> None:
            self.append_attempts += 1
            await super().append(record)

    oversized_messages = 3_000
    scripts = iter(
        (
            [
                *(
                    AssistantMessage(content=[], model="fake-model")
                    for _ in range(oversized_messages)
                ),
                ResultMessage(
                    subtype="success",
                    duration_ms=1,
                    duration_api_ms=1,
                    is_error=False,
                    num_turns=1,
                    session_id="fake-session",
                    result="first answer",
                    usage={"input_tokens": 1, "output_tokens": 1},
                ),
            ],
            [
                AssistantMessage(
                    content=[TextBlock(text="small answer")], model="fake-model"
                ),
                ResultMessage(
                    subtype="success",
                    duration_ms=1,
                    duration_api_ms=1,
                    is_error=False,
                    num_turns=1,
                    session_id="fake-session",
                    result="small answer",
                    usage={"input_tokens": 1, "output_tokens": 1},
                ),
            ],
        )
    )
    store = CountingStore()
    runner = _recording_runner(store, script=lambda: next(scripts))

    async def go() -> None:
        await runner.start()
        first_lines = [
            line
            async for line in runner.run_inbound(
                Event(type="message", text="oversized turn", user="U", ts="1")
            )
        ]
        first_final = parse_ndjson_line(first_lines[-1])
        assert isinstance(first_final, Final)
        assert first_final.status is SessionStatus.DONE
        assert runner.history_durable is False
        assert store.append_attempts == 0

        second_lines = [
            line
            async for line in runner.run_inbound(
                Event(type="message", text="small turn", user="U", ts="2")
            )
        ]
        second_final = parse_ndjson_line(second_lines[-1])
        assert isinstance(second_final, Final)
        assert second_final.status is SessionStatus.DONE
        assert store.append_attempts == 1
        assert len(store.turns) == 1
        assert store.turns[0].assistant == "small answer"

        # The earlier missing turn still makes replacement unsafe even though
        # this append succeeded.
        assert runner.history_durable is False

    anyio.run(go)


def test_state_store_load_rejects_non_array() -> None:
    app = web.Application()

    async def get_key(_request: web.Request) -> web.Response:
        return web.json_response(
            {"namespace": "transcript", "key": "t1", "value": {"not": "a list"}, "version": 1}
        )

    app.router.add_get("/agents/A/state/transcript/t1", get_key)

    async def go() -> None:
        async with TestServer(app) as server:
            url = str(server.make_url("/agents/A/state/transcript/t1"))
            store = StateApiTranscriptStore(url, token=None)
            with pytest.raises(HistoryError):
                await store.load()

    anyio.run(go)


def test_state_store_load_rejects_malformed_log_entry() -> None:
    app = web.Application()

    async def get_key(_request: web.Request) -> web.Response:
        return web.json_response(
            {"namespace": "transcript", "key": "t1", "value": [42], "version": 1}
        )

    app.router.add_get("/agents/A/state/transcript/t1", get_key)

    async def go() -> None:
        async with TestServer(app) as server:
            url = str(server.make_url("/agents/A/state/transcript/t1"))
            store = StateApiTranscriptStore(url, token=None)
            with pytest.raises(HistoryError, match="invalid transcript record"):
                await store.load()

    anyio.run(go)


def _recording_runner(store: TranscriptStore, *, script=None, ceiling: int = 0):
    """A SessionRunner wired to the fake model and a recording transcript store."""
    from curie_runner import RunTracer, SideEffectClassifier
    from curie_runner.fake import FakeModelSession, default_turn
    from curie_runner.session import SessionRunner

    return SessionRunner(
        session_factory=lambda: FakeModelSession(script or default_turn),
        ceiling=ceiling,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
        session_id="sess-hist",
        history_store=store,
    )


class _RecordingStore:
    def __init__(self) -> None:
        self.turns: list[TurnRecord] = []

    async def load(self) -> list[TurnRecord]:
        return list(self.turns)

    async def append(self, record: TurnRecord) -> None:
        self.turns.append(record)


def _run_recording_turn(runner, event):
    """Drive an inbound event through the real runner turn lifecycle."""
    from aci_protocol import Final, parse_ndjson_line

    async def go() -> Final:
        await runner.start()
        lines = [line async for line in runner.run_inbound(event)]
        final = parse_ndjson_line(lines[-1])
        assert isinstance(final, Final)
        return final

    return anyio.run(go)


def test_successful_turn_is_appended_to_the_transcript() -> None:
    from aci_protocol import Event, SessionStatus

    store = _RecordingStore()
    runner = _recording_runner(store)
    final = _run_recording_turn(
        runner, Event(type="message", text="what changed?", user="U", ts="1")
    )

    assert final.status is SessionStatus.DONE
    assert runner.history_durable is True
    # The fully delivered DONE final records its reply once, not once per
    # translated frame or once per store retry.
    assert len(store.turns) == 1
    assert store.turns[0].user == "what changed?"
    # default_turn's terminal result text.
    assert store.turns[0].assistant == "all done"
    assert store.turns[0].ts  # a timestamp was stamped
    assert [message.role for message in store.turns[0].messages] == [
        "user",
        "assistant",
        "assistant",
        "user",
        "assistant",
    ]
    assert any(
        isinstance(message.content, list)
        and any(block.get("type") == "tool_use" for block in message.content)
        for message in store.turns[0].messages
    )
    assert any(
        isinstance(message.content, list)
        and any(block.get("type") == "tool_result" for block in message.content)
        for message in store.turns[0].messages
    )


def test_failed_transcript_append_fails_closed_for_runner_replacement() -> None:
    from aci_protocol import Event, SessionStatus

    class FailingStore(_RecordingStore):
        async def append(self, record: TurnRecord) -> None:
            del record
            raise HistoryError("injected append failure")

    runner = _recording_runner(FailingStore())
    final = _run_recording_turn(
        runner, Event(type="message", text="what changed?", user="U", ts="1")
    )

    assert final.status is SessionStatus.DONE
    assert runner.turn_active is False
    assert runner.history_durable is False


def test_accepted_steer_is_persisted_in_ordered_structured_history() -> None:
    from aci_protocol import Event

    store = _RecordingStore()
    runner = _recording_runner(store)

    async def go() -> None:
        await runner.start()
        stream = runner.run_turn(
            Event(type="message", text="first request", user="U", ts="1")
        )
        await stream.__anext__()
        assert await runner.steer("authorized steering state") is True
        async for _ in stream:
            pass

    anyio.run(go)

    assert len(store.turns) == 1
    messages = store.turns[0].messages
    steers = [
        message
        for message in messages
        if message.role == "user" and message.content == "authorized steering state"
    ]
    assert len(steers) == 1


def test_synthetic_done_after_incomplete_turn_is_not_appended_to_the_transcript() -> None:
    # The runner synthesizes DONE when the SDK ends after streamed text but omits
    # its ResultMessage. That closes the wire stream, not an assistant reply, so
    # it must preserve the incomplete-turn history contract.
    from aci_protocol import Event, SessionStatus
    from claude_agent_sdk import AssistantMessage, TextBlock

    store = _RecordingStore()
    final = _run_recording_turn(
        _recording_runner(
            store,
            script=lambda: [
                AssistantMessage(
                    content=[TextBlock(text="partial streamed answer")], model="fake-model"
                )
            ],
        ),
        Event(type="message", text="q", user="U", ts="1"),
    )

    assert final.status is SessionStatus.DONE
    assert store.turns == []


def test_classified_failure_turn_is_not_appended_to_the_transcript() -> None:
    from aci_protocol import Event, SessionStatus
    from claude_agent_sdk import ResultMessage

    def script():
        return [
            ResultMessage(
                subtype="error_during_execution",
                duration_ms=1,
                duration_api_ms=1,
                is_error=True,
                num_turns=1,
                session_id="fake-session",
                result="model failed",
                usage=None,
            )
        ]
    store = _RecordingStore()
    final = _run_recording_turn(
        _recording_runner(store, script=script),
        Event(type="message", text="q", user="U", ts="1"),
    )

    assert final.status is SessionStatus.CLASSIFIED_FAILURE
    assert store.turns == []


def test_auth_rejection_turn_is_not_appended_to_the_transcript() -> None:
    from aci_protocol import Event, SessionStatus
    from claude_agent_sdk import AssistantMessage

    store = _RecordingStore()
    final = _run_recording_turn(
        _recording_runner(
            store,
            script=lambda: [
                AssistantMessage(content=[], model="fake-model", error="authentication_failed")
            ],
        ),
        Event(type="message", text="q", user="U", ts="1"),
    )

    assert final.status is SessionStatus.CLASSIFIED_FAILURE
    assert store.turns == []


def test_budget_halted_turn_is_not_appended_to_the_transcript() -> None:
    from aci_protocol import Event, SessionStatus
    from claude_agent_sdk import AssistantMessage, TextBlock

    store = _RecordingStore()
    final = _run_recording_turn(
        _recording_runner(
            store,
            ceiling=1,
            script=lambda: [
                AssistantMessage(
                    content=[TextBlock(text="thinking")],
                    model="fake-model",
                    usage={"output_tokens": 2},
                )
            ],
        ),
        Event(type="message", text="q", user="U", ts="1"),
    )

    assert final.status is SessionStatus.CLASSIFIED_FAILURE
    assert store.turns == []


def test_awaiting_approval_turn_is_appended_with_suspend_context() -> None:
    from aci_protocol import Event, SessionStatus
    from curie_runner.fake import approval_turn

    store = _RecordingStore()
    final = _run_recording_turn(
        _recording_runner(store, script=lambda: approval_turn("Approve the action")),
        Event(type="message", text="q", user="U", ts="1"),
    )

    assert final.status is SessionStatus.AWAITING_APPROVAL
    assert len(store.turns) == 1
    assert store.turns[0].status == SessionStatus.AWAITING_APPROVAL.value
    assert store.turns[0].approval is not None
    assert store.turns[0].approval.gate_kind == "policy"
    assert store.turns[0].approval.summary == "Approve the action"


def test_interrupted_turn_is_not_appended_to_the_transcript() -> None:
    # An interrupt reclassifies the terminal result to IDLE_AWAITING_INPUT. Drive
    # that delivered terminal through run_inbound, rather than asserting only the
    # internal state, to pin that it is not a completed assistant reply.
    from aci_protocol import Event, Final, SessionStatus, parse_ndjson_line

    store = _RecordingStore()
    runner = _recording_runner(store)

    async def go() -> Final:
        await runner.start()
        stream = runner.run_inbound(Event(type="message", text="q", user="U", ts="1"))
        await stream.__anext__()  # The first streamed frame makes the turn live.
        await runner.interrupt("user stop")
        lines = [line async for line in stream]
        final = parse_ndjson_line(lines[-1])
        assert isinstance(final, Final)
        return final

    final = anyio.run(go)

    assert final.status is SessionStatus.IDLE_AWAITING_INPUT
    assert store.turns == []


def test_compose_system_prompt_excludes_conversation_history() -> None:
    # ADR-0119: only memory and bundle instructions belong in the system prompt;
    # conversation history crosses as structured messages.
    from curie_runner.__main__ import _compose_system_prompt

    assert _compose_system_prompt("BASE", "MEM", model=None) == "MEM\n\nBASE"
    assert _compose_system_prompt("BASE", None, model=None) == "BASE"
    assert _compose_system_prompt(None, None, model=None) is None


def test_compose_system_prompt_appends_configured_model() -> None:
    from curie_runner.__main__ import _compose_system_prompt

    assert (
        _compose_system_prompt("BASE", "MEM", model="z-ai/glm-5.2")
        == "MEM\n\nBASE\n\nConfigured model: z-ai/glm-5.2"
    )


def test_build_runner_forwards_configured_model_to_session_prompt(tmp_path) -> None:
    from curie_runner import RunnerConfig
    from curie_runner.__main__ import build_runner
    from curie_runner.adapter import ClaudeAgentSession

    plugin_dir = tmp_path / ".claude-plugin"
    plugin_dir.mkdir()
    (plugin_dir / "plugin.json").write_text(
        '{"name": "modelwiring", "version": "0.1.0", "description": "test"}'
    )
    config = RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(tmp_path),
            "CURIE_SESSION_ID": "s-model",
            "CURIE_SANDBOX_ID": "b-model",
            "CURIE_BUDGET": (
                '{"max_output_tokens_per_run": 1000, "max_usd_per_day": 1.0}'
            ),
            "CURIE_MODEL": "z-ai/glm-5.2",
        }
    )
    runner = build_runner(config, fake_model=False)
    session = runner._factory()
    assert isinstance(session, ClaudeAgentSession)
    options = session._options

    assert options.system_prompt == "Configured model: z-ai/glm-5.2"


def test_record_turn_swallows_store_failure() -> None:
    # A transient store failure must never fail a turn the user already answered.
    from aci_protocol import Event
    from curie_runner import RunTracer, SideEffectClassifier
    from curie_runner.session import SessionRunner
    from curie_runner.translate import TurnState

    class _BoomStore:
        async def load(self) -> list[TurnRecord]:
            return []

        async def append(self, record: TurnRecord) -> None:
            raise HistoryError("state API unavailable")

    runner = SessionRunner(
        session_factory=lambda: None,  # type: ignore[arg-type,return-value]
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
        session_id="s",
        history_store=_BoomStore(),
    )
    state = TurnState()
    state.final_text = "answer"
    # Must not raise.
    anyio.run(lambda: runner._record_turn(Event(type="message", text="q", user="U", ts="1"), state))


def test_successful_turn_survives_transcript_store_failure() -> None:
    # A delivered successful answer remains DONE when its best-effort transcript
    # append fails. This drives the full public inbound path so an early return
    # before append cannot make the regression pass.
    from aci_protocol import Event, SessionStatus

    class _BoomStore:
        def __init__(self) -> None:
            self.append_attempts = 0

        async def load(self) -> list[TurnRecord]:
            return []

        async def append(self, record: TurnRecord) -> None:
            self.append_attempts += 1
            raise HistoryError("state API unavailable")

    store = _BoomStore()
    final = _run_recording_turn(
        _recording_runner(store),
        Event(type="message", text="what changed?", user="U", ts="1"),
    )

    assert final.status is SessionStatus.DONE
    assert store.append_attempts == 1
