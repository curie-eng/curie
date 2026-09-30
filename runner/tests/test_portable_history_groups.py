"""RUNNER-HISTORY-GROUP: portable provenance and causal replay boundaries.

Issue #3628 measured SDK 0.2.159 / CLI 2.1.281: native assistant fragments
share message.id, while role/content-only interleaving loses two tool results
in the first outgoing /v1/messages request. StreamEvent.message_start.message.id
is the observed source; AssistantMessage has no invented provider-ID attribute.
The local native transport pin separately tests the SDK's normalization.
"""

import hashlib

import anyio
import pytest
from curie_runner.adapter import build_structured_resume
from curie_runner.history import (
    ConversationMessage,
    HarnessReplayState,
    HistoryError,
    TurnRecord,
    bound_turn_record,
)

GROUP = hashlib.sha256(b"msg_acme_example").hexdigest()
OTHER = hashlib.sha256(b"msg_acme_dependent").hexdigest()


def _call(number, group=GROUP):
    raw = {
        "role": "assistant",
        "content": [
            {
                "type": "tool_use",
                "id": f"call-acme-{number}",
                "name": "Read",
                "input": {"file_path": f"/tmp/acme-{number}"},
            }
        ],
    }
    if group is not None:
        raw["assistant_group"] = group
    return ConversationMessage.from_dict(raw)


def _result(number, text="acme result", error=False):
    return ConversationMessage(
        role="user",
        content=[
            {
                "type": "tool_result",
                "tool_use_id": f"call-acme-{number}",
                "content": text,
                "is_error": error,
            }
        ],
    )


def _interleaved(group=GROUP):
    return (
        _call(1, group),
        _call(2, group),
        _result(1),
        _call(3, group),
        _result(2, error=True),
        _result(3),
    )


def _entries(messages, tmp_path, **kwargs):
    resume = build_structured_resume(
        tuple(messages), curie_session_id="acme-thread", cwd=str(tmp_path), **kwargs
    )
    return anyio.run(resume.session_store.load, resume.session_key)


def test_group_round_trip_preserves_interleaving_and_error_results(tmp_path):
    record = TurnRecord(
        user="inspect", assistant="done", ts="2026-09-30T00:00:00Z", messages=_interleaved()
    )
    loaded = TurnRecord.from_dict(record.to_dict())
    assert loaded == record
    assert [m.to_dict().get("assistant_group") for m in loaded.messages] == [
        GROUP,
        GROUP,
        None,
        GROUP,
        None,
        None,
    ]
    entries = _entries(loaded.messages, tmp_path)
    assert [(e["message"]["role"], e["message"]["content"]) for e in entries] == [
        (m.role, m.content) for m in record.messages
    ]
    ids = [entries[n]["message"]["id"] for n in (0, 1, 3)]
    assert len(set(ids)) == 1
    assert all("assistant_group" not in e["message"] for e in entries)
    assert [e["parentUuid"] for e in entries[1:]] == [e["uuid"] for e in entries[:-1]]


@pytest.mark.parametrize("bad", ["", "A" * 64, "a" * 63, "g" * 64, 42, []])
def test_malformed_persisted_group_rejected(bad):
    with pytest.raises(HistoryError):
        ConversationMessage.from_dict({"role": "assistant", "content": [], "assistant_group": bad})


def test_user_group_rejected():
    with pytest.raises(HistoryError):
        ConversationMessage.from_dict(
            {"role": "user", "content": "new request", "assistant_group": GROUP}
        )


def test_constructor_and_repr_follow_same_metadata_contract():
    message = ConversationMessage(role="assistant", content=[], assistant_group=GROUP)
    assert message.to_dict()["assistant_group"] == GROUP
    assert GROUP not in repr(message)
    with pytest.raises(HistoryError):
        ConversationMessage(role="assistant", content=[], assistant_group="bad")


def test_distinct_dependent_group_and_new_user_keep_order(tmp_path):
    messages = (
        _call(1),
        _result(1),
        ConversationMessage(role="user", content="use the previous result"),
        _call(2, OTHER),
        _result(2),
    )
    entries = _entries(messages, tmp_path)
    assert [(e["message"]["role"], e["message"]["content"]) for e in entries] == [
        (m.role, m.content) for m in messages
    ]
    assert entries[0]["message"]["id"] != entries[3]["message"]["id"]


@pytest.mark.parametrize(
    "barrier",
    [
        ConversationMessage(role="user", content="new human request"),
        ConversationMessage(role="user", content=[{"type": "text", "text": "steer"}]),
        _call(4, OTHER),
    ],
)
def test_group_reuse_after_causal_barrier_rejected(tmp_path, barrier):
    messages = [_call(1), _result(1), barrier]
    if barrier.role == "assistant":
        messages.append(_result(4))
    messages.extend([_call(2), _result(2)])
    with pytest.raises(HistoryError):
        _entries(messages, tmp_path)


def test_legacy_sequential_needs_no_native_checkpoint(tmp_path):
    messages = (_call(1, None), _result(1), _call(2, None), _result(2))
    entries = _entries(messages, tmp_path)
    assert [e["message"]["content"] for e in entries] == [m.content for m in messages]
    assert all("assistant_group" not in m.to_dict() for m in messages)


def test_legacy_ambiguous_overlap_refused_before_hydration(tmp_path):
    with pytest.raises(HistoryError, match="(?i)group"):
        _entries(_interleaved(None), tmp_path)


@pytest.mark.parametrize(
    "messages",
    [
        (_call(1), _call(1), _result(1)),
        (_call(1), _result(2)),
        (_call(1), _result(1), _result(1)),
    ],
)
def test_duplicate_or_unmatched_tool_ids_fail_closed(tmp_path, messages):
    with pytest.raises(HistoryError):
        _entries(messages, tmp_path)


def test_bounding_drops_native_but_keeps_active_group(tmp_path):
    record = TurnRecord(
        user="inspect",
        assistant="done",
        ts="2026-09-30T00:00:00Z",
        messages=(
            ConversationMessage(role="user", content="inspect"),
            _call(1),
            _result(1, "x" * 12000),
            _call(2),
            _result(2, "y" * 12000),
            ConversationMessage(role="assistant", content=[{"type": "text", "text": "done"}]),
        ),
        harness_replay=HarnessReplayState(
            harness="claude", kind="checkpoint", entries=({"padding": "z" * 12000},)
        ),
    )
    bounded = bound_turn_record(record, max_value_bytes=6000)
    assert bounded.harness_replay is None
    assert [
        m.to_dict().get("assistant_group")
        for m in bounded.messages
        if m.role == "assistant" and any(b.get("type") == "tool_use" for b in m.content)
    ] == [GROUP, GROUP]
    entries = _entries(bounded.messages, tmp_path)
    assert entries[1]["message"]["id"] == entries[3]["message"]["id"]
    assert bounded.user == record.user and bounded.assistant == record.assistant


def test_stale_prompt_and_attachment_never_restored_with_grouping(tmp_path):
    messages = _interleaved()
    checkpoint = HarnessReplayState(
        harness="claude",
        kind="checkpoint",
        entries=(
            {
                "type": "attachment",
                "attachment": {
                    "type": "prompt_snapshot",
                    "systemPrompt": ["old unavailable attachment"],
                },
            },
            {"type": "attachment", "attachment": {"type": "file", "content": "old authority"}},
        ),
    )
    entries = _entries(
        messages,
        tmp_path,
        harness_replay=checkpoint,
        system_prompt="current: attachment unavailable",
    )
    assert len(entries) == len(messages)
    assert all(e["type"] != "attachment" for e in entries)
    assert entries[0]["message"]["id"] == entries[3]["message"]["id"]


# Only the external SDK response source is scripted; normalization, SessionRunner,
# projection, and persistence remain actual candidate code.
def _stream_start(identifier):
    from claude_agent_sdk import StreamEvent

    return StreamEvent(
        uuid="acme-stream",
        session_id="acme-sdk-session",
        event={
            "type": "message_start",
            "message": {"id": identifier},
        },
    )


def _assistant(number):
    from claude_agent_sdk import AssistantMessage, ToolUseBlock

    return AssistantMessage(
        content=[
            ToolUseBlock(
                id=f"call-acme-{number}", name="Read", input={"file_path": f"/tmp/acme-{number}"}
            )
        ],
        model="acme-model",
    )


def _sdk_result(number):
    from claude_agent_sdk import ToolResultBlock, UserMessage

    return UserMessage(
        content=[
            ToolResultBlock(
                tool_use_id=f"call-acme-{number}", content="acme exact result", is_error=False
            )
        ]
    )


class _ResponseClient:
    def __init__(self, scripts):
        self.scripts = scripts
        self.queries = []

    async def connect(self):
        pass

    async def disconnect(self):
        pass

    async def query(self, text):
        self.queries.append(text)

    async def receive_response(self):
        from claude_agent_sdk import ResultMessage

        for message in self.scripts.pop(0):
            yield message
        yield ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="acme-sdk-session",
            result="done",
        )


@pytest.mark.parametrize(
    "identifier", [None, "", "contains space", "x" * 257, "nonascii-é", "\x00", 42]
)
def test_invalid_observed_start_clears_group(monkeypatch, identifier):
    from curie_runner import adapter
    from curie_runner.adapter import ClaudeAgentSession, PartialMessageBoundary, build_options

    client = _ResponseClient([[_stream_start("msg_acme_example"), _stream_start(identifier)]])
    monkeypatch.setattr(adapter, "ClaudeSDKClient", lambda _options: client)
    session = ClaudeAgentSession(
        build_options(
            plugins=[], model=None, system_prompt=None, resume=None, max_turns=1, max_budget_usd=1
        )
    )

    async def collect():
        return [m async for m in session.receive_turn() if isinstance(m, PartialMessageBoundary)]

    boundaries = anyio.run(collect)
    assert boundaries[0].assistant_group == GROUP
    assert boundaries[1].assistant_group is None
    assert "msg_acme_example" not in repr(boundaries[0])
    assert GROUP not in repr(boundaries[0])


def test_actual_session_capture_keeps_results_but_clears_user_and_new_turn(monkeypatch):
    from aci_protocol import Event
    from claude_agent_sdk import AssistantMessage, StreamEvent, TextBlock, UserMessage
    from curie_runner import RunTracer, SideEffectClassifier, adapter
    from curie_runner.adapter import ClaudeAgentSession, build_options
    from curie_runner.session import SessionRunner

    client = _ResponseClient(
        [
            [
                _stream_start("msg_acme_example"),
                _assistant(1),
                _sdk_result(1),
                StreamEvent(
                    uuid="acme-block",
                    session_id="acme-sdk-session",
                    event={"type": "content_block_start", "content_block": {"type": "text"}},
                ),
                _assistant(2),
                _sdk_result(2),
                UserMessage(content="fresh human content"),
                AssistantMessage(content=[TextBlock(text="after human")], model="acme-model"),
            ],
            [AssistantMessage(content=[TextBlock(text="next turn")], model="acme-model")],
        ]
    )
    monkeypatch.setattr(adapter, "ClaudeSDKClient", lambda _options: client)

    class Store:
        def __init__(self):
            self.records = []

        async def load(self):
            return list(self.records)

        async def append(self, record):
            self.records.append(record)
            return True

    store = Store()
    options = build_options(
        plugins=[], model=None, system_prompt=None, resume=None, max_turns=2, max_budget_usd=1
    )
    runner = SessionRunner(
        session_factory=lambda: ClaudeAgentSession(options),
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="acme-capture",
        history_store=store,
    )

    async def run():
        await runner.start()
        try:
            for n in range(2):
                _lines = [
                    line
                    async for line in runner.run_turn(
                        Event(type="message", text=f"request {n}", user="U0EXAMPLE1", ts=str(n))
                    )
                ]
        finally:
            await runner.close()

    anyio.run(run)
    assert len(store.records) == 2
    groups = [
        m.to_dict().get("assistant_group")
        for m in store.records[0].messages
        if m.role == "assistant"
    ]
    assert groups == [GROUP, GROUP, None]
    assert [
        m.to_dict().get("assistant_group")
        for m in store.records[1].messages
        if m.role == "assistant"
    ] == [None]


def test_accepted_steer_clears_capture_group(monkeypatch):
    from aci_protocol import Event
    from curie_runner import RunTracer, SideEffectClassifier, adapter
    from curie_runner.adapter import ClaudeAgentSession, build_options
    from curie_runner.session import SessionRunner

    entered, release = anyio.Event(), anyio.Event()

    class Client(_ResponseClient):
        async def receive_response(self):
            from claude_agent_sdk import ResultMessage

            yield _stream_start("msg_acme_example")
            yield _assistant(1)
            entered.set()
            await release.wait()
            yield _sdk_result(1)
            yield _assistant(2)
            yield _sdk_result(2)
            yield ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id="acme-sdk-session",
                result="done",
            )

    client = Client([])
    monkeypatch.setattr(adapter, "ClaudeSDKClient", lambda _options: client)

    class Store:
        records = []

        async def load(self):
            return list(self.records)

        async def append(self, record):
            self.records.append(record)
            return True

    store = Store()
    options = build_options(
        plugins=[], model=None, system_prompt=None, resume=None, max_turns=2, max_budget_usd=1
    )
    runner = SessionRunner(
        session_factory=lambda: ClaudeAgentSession(options),
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="acme-steer",
        history_store=store,
    )

    async def consume():
        _lines = [
            line
            async for line in runner.run_turn(
                Event(type="message", text="inspect", user="U0EXAMPLE1", ts="1")
            )
        ]

    async def run():
        await runner.start()
        try:
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(consume)
                await entered.wait()
                assert await runner.steer("fresh steer") is True
                release.set()
        finally:
            await runner.close()

    anyio.run(run)
    assert client.queries == ["inspect", "fresh steer"]
    assert [
        m.to_dict().get("assistant_group")
        for m in store.records[0].messages
        if m.role == "assistant"
    ] == [GROUP, None]
    assert any(m.role == "user" and m.content == "fresh steer" for m in store.records[0].messages)


@pytest.mark.parametrize("mismatch", [False, True])
def test_stale_native_migration_requires_full_exact_portable_correspondence(tmp_path, mismatch):
    messages = _interleaved(None)
    native = []
    for index, message in enumerate(messages):
        payload = message.to_dict()
        if message.role == "assistant":
            payload["id"] = "msg_acme_original"
        if mismatch and index == 2:
            payload["content"] = [{"type": "text", "text": "unmatched native authority"}]
        native.append({"type": message.role, "uuid": f"acme-entry-{index}", "message": payload})
    native.append(
        {
            "type": "attachment",
            "attachment": {
                "type": "prompt_snapshot",
                "systemPrompt": ["old attachment available"],
            },
        }
    )
    checkpoint = HarnessReplayState(harness="claude", kind="checkpoint", entries=tuple(native))
    if mismatch:
        with pytest.raises(HistoryError, match="(?i)group"):
            _entries(
                messages,
                tmp_path,
                harness_replay=checkpoint,
                system_prompt="current attachment unavailable",
            )
    else:
        entries = _entries(
            messages,
            tmp_path,
            harness_replay=checkpoint,
            system_prompt="current attachment unavailable",
        )
        assert len(entries) == len(messages)
        assert [e["message"]["content"] for e in entries] == [m.content for m in messages]
        assert entries[0]["message"]["id"] == entries[3]["message"]["id"]
        assert all(e["type"] != "attachment" for e in entries)


def test_compaction_omits_entire_old_assistant_group_across_rows():
    old_first = _call(1).to_dict()
    old_second = _call(2).to_dict()
    # Tool input is opaque to text reduction: the budget must prune exchanges,
    # rather than merely replace large result prose with a digest marker.
    old_first["content"][0]["input"] = {"record": "acme-first-" + "x" * 3000}
    old_second["content"][0]["input"] = {"record": "acme-second-" + "y" * 3000}
    newer_call = _call(3, OTHER)
    newer_result = _result(3, "acme newer exact result")
    historical_text = "Acme older group context remains historical."
    record = TurnRecord(
        user="inspect",
        assistant="done",
        ts="2026-09-30T00:00:00Z",
        messages=(
            ConversationMessage(role="user", content="inspect"),
            ConversationMessage.from_dict(old_first),
            _result(1),
            ConversationMessage.from_dict(
                {
                    "role": "assistant",
                    "assistant_group": GROUP,
                    "content": [{"type": "text", "text": historical_text}],
                }
            ),
            ConversationMessage.from_dict(old_second),
            _result(2),
            newer_call,
            newer_result,
            ConversationMessage(role="assistant", content=[{"type": "text", "text": "done"}]),
        ),
    )
    bounded = bound_turn_record(record, max_value_bytes=5500)
    blocks = [
        block
        for message in bounded.messages
        if isinstance(message.content, list)
        for block in message.content
    ]
    # A cap that can fit after removing only the first pair must still remove
    # both pairs of the proven same native group, including its text attribution.
    assert {b["id"] for b in blocks if b.get("type") == "tool_use"} == {"call-acme-3"}
    assert {b["tool_use_id"] for b in blocks if b.get("type") == "tool_result"} == {"call-acme-3"}
    assert all(m.to_dict().get("assistant_group") != GROUP for m in bounded.messages)
    assert newer_call in bounded.messages and newer_result in bounded.messages
    assert any(b.get("text") == historical_text for b in blocks)
    assert bounded.messages[0] == record.messages[0]
    assert bounded.messages[-1] == record.messages[-1]
    assert bounded.user == record.user and bounded.assistant == record.assistant
