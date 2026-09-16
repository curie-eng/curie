"""#2221: POST /v1/reset after a completed turn must not reuse the SDK session id.

A reset on a never-used runner already succeeds and cannot catch this. The
structured-resume envelope pins a deterministic SDK session id, and the boot
factory reused that envelope on reconnect. After a real turn the Claude Code
subprocess refuses the same id (``Session ID ... is already in use``) and the
handler surfaces 500, which is what ``curie skill eval`` hits between cases.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast

import anyio
import pytest
from aci_protocol import SessionStatus, parse_ndjson
from aiohttp.test_utils import TestClient, TestServer
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock
from claude_agent_sdk._errors import ProcessError
from curie_runner import create_app
from curie_runner.__main__ import build_runner
from curie_runner.adapter import ClaudeAgentSession
from curie_runner.config import RunnerConfig
from curie_runner.history import ConversationMessage, ConversationReplay


def _config(tmp_path: Path) -> RunnerConfig:
    plugin = tmp_path / ".claude-plugin"
    plugin.mkdir(parents=True)
    (plugin / "plugin.json").write_text(json.dumps({"name": "reset-after-turn"}))
    return RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(tmp_path),
            "CURIE_SESSION_ID": "s-reset-2221",
            "CURIE_SANDBOX_ID": "b-reset-2221",
            "CURIE_BUDGET": '{"max_output_tokens_per_run": 1000, "max_usd_per_day": 1.0}',
        }
    )


class _ReuseRefusingClient:
    """Stand-in for ClaudeSDKClient that reproduces the post-turn session-id lock.

    Connect succeeds until a query has used the id. Disconnect does not release
    it: that matches the observed Claude Code subprocess, which still refuses
    the just-closed id on reconnect.
    """

    used_after_query: set[str] = set()

    def __init__(self, options: Any) -> None:
        self.options = options

    def _sdk_session_id(self) -> str | None:
        session_id = cast("str | None", self.options.session_id)
        resume = cast("str | None", self.options.resume)
        return session_id or resume

    async def connect(self) -> None:
        session_id = self._sdk_session_id()
        if session_id is not None and session_id in _ReuseRefusingClient.used_after_query:
            raise ProcessError(
                f"Session ID {session_id} is already in use.",
                exit_code=1,
            )

    async def query(self, _text: str) -> None:
        session_id = self._sdk_session_id()
        if session_id is not None:
            _ReuseRefusingClient.used_after_query.add(session_id)

    def receive_response(self) -> AsyncIterator[Any]:
        async def _gen() -> AsyncIterator[Any]:
            yield AssistantMessage(content=[TextBlock(text="pong")], model="stub-model")
            yield ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id=self._sdk_session_id() or "stub-session",
                result="pong",
                usage=None,
            )

        return _gen()

    async def disconnect(self) -> None:
        return None

    async def interrupt(self) -> None:
        return None


@pytest.fixture(autouse=True)
def _isolate_used_ids() -> None:
    _ReuseRefusingClient.used_after_query = set()


def test_reset_after_a_completed_turn_returns_200(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The consumer path is the real adapter and the real /v1/reset handler. The
    # only mock is the SDK transport, which raises the same ProcessError the
    # live subprocess raised on origin/next.
    monkeypatch.setattr("curie_runner.adapter.ClaudeSDKClient", _ReuseRefusingClient)
    runner = build_runner(_config(tmp_path), fake_model=False)

    async def go() -> None:
        await runner.start()
        first = runner._session  # noqa: SLF001
        assert isinstance(first, ClaudeAgentSession)
        first_id = first._options.session_id or first._options.resume  # noqa: SLF001
        async with TestClient(TestServer(create_app(runner))) as client:
            event = await client.post(
                "/v1/event",
                json={
                    "kind": "event",
                    "type": "message",
                    "text": "Reply with pong",
                    "user": "U",
                    "ts": "1",
                },
            )
            assert event.status == 200
            events = parse_ndjson(await event.text())
            assert events[-1].type == "final"
            assert events[-1].status == SessionStatus.DONE

            resp = await client.post("/v1/reset")
            body = await resp.text()
            assert resp.status == 200, body
            assert json.loads(body)["ok"] is True

            second = runner._session  # noqa: SLF001
            assert isinstance(second, ClaudeAgentSession)
            second_id = second._options.session_id or second._options.resume  # noqa: SLF001
            assert second is not first
            assert second_id != first_id

    anyio.run(go)


def test_reset_without_a_turn_still_succeeds_on_the_real_adapter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Negative / second path: a never-used runner already reset cleanly. The
    # stub still allows reconnect on an unused id, so this must stay 200.
    monkeypatch.setattr("curie_runner.adapter.ClaudeSDKClient", _ReuseRefusingClient)
    runner = build_runner(_config(tmp_path), fake_model=False)

    async def go() -> None:
        await runner.start()
        async with TestClient(TestServer(create_app(runner))) as client:
            resp = await client.post("/v1/reset")
            assert resp.status == 200
            assert (await resp.json())["ok"] is True

    anyio.run(go)


def test_reset_after_a_turn_still_rehydrates_thread_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Sibling path (ADR-0051): a thread with recovered history still rehydrates
    # that prefix on reset, but under a new SDK session id so reconnect cannot
    # collide with the just-closed envelope.
    monkeypatch.setattr("curie_runner.adapter.ClaudeSDKClient", _ReuseRefusingClient)
    replay = ConversationReplay(
        messages=(
            ConversationMessage(role="user", content="prior question"),
            ConversationMessage(
                role="assistant", content=[{"type": "text", "text": "prior answer"}]
            ),
        ),
        source_turns=1,
    )
    runner = build_runner(_config(tmp_path), conversation_replay=replay, fake_model=False)

    async def go() -> None:
        await runner.start()
        first = runner._session  # noqa: SLF001
        assert isinstance(first, ClaudeAgentSession)
        assert first._options.resume is not None  # noqa: SLF001
        first_id = first._options.resume  # noqa: SLF001
        async with TestClient(TestServer(create_app(runner))) as client:
            event = await client.post(
                "/v1/event",
                json={
                    "kind": "event",
                    "type": "message",
                    "text": "Reply with pong",
                    "user": "U",
                    "ts": "1",
                },
            )
            assert event.status == 200
            resp = await client.post("/v1/reset")
            body = await resp.text()
            assert resp.status == 200, body
            second = runner._session  # noqa: SLF001
            assert isinstance(second, ClaudeAgentSession)
            assert second._options.resume is not None  # noqa: SLF001
            assert second._options.resume != first_id  # noqa: SLF001
            assert second._options.session_store is not None  # noqa: SLF001

    anyio.run(go)
