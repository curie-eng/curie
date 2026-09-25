"""#2927 assembled: a one-turn coding thread resumes a review turn, end to end.

The runner-side regression drives ``StateApiTranscriptStore`` against a fake
state API. This one puts nothing fake inside the changed boundary: the real
curie-api app (state routes, reserve and compare-and-set, disposable Postgres)
is served over a real socket by uvicorn, and the real runner store, boot path
(``__main__._load_history``) and ``SessionRunner`` talk to it over HTTP. The
scripted model is the only fake, and it sits outside the boundary.
"""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import aiohttp
import anyio
import pytest
import uvicorn
from curie_api.config import get_settings
from curie_api.main import create_app

_CAP = 65_536
_RESERVE = 8_192
_BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'
_PUBLICATION_TEXT = "Published PR #41 at https://github.com/acme-corp/pricing/pull/41"


def _size(value: Any) -> int:
    return len(json.dumps(value, separators=(",", ":")).encode("utf-8"))


@pytest.fixture
def served_api(_disposable_db: Any, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """The real API app on 127.0.0.1, served by uvicorn in its own thread and loop."""

    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    get_settings.cache_clear()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(64)
    base_url = f"http://127.0.0.1:{sock.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(create_app(), log_level="warning", access_log=False))
    thread = threading.Thread(target=lambda: asyncio.run(server.serve(sockets=[sock])), daemon=True)
    thread.start()
    deadline = time.monotonic() + 30
    while not server.started:
        assert thread.is_alive(), "the API server exited during startup"
        assert time.monotonic() < deadline, "the API server did not start"
        time.sleep(0.05)
    try:
        yield base_url
    finally:
        server.should_exit = True
        thread.join(timeout=15)
        sock.close()
        get_settings.cache_clear()


# --- realistic coding-turn payloads (copied from the runner regression) ----------


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


def _coding_turn_script(tag: str) -> list[Any]:
    """A turn a real coding session produces: read, edit, pytest, diff, push."""

    from claude_agent_sdk import (
        AssistantMessage,
        ResultMessage,
        TextBlock,
        ToolResultBlock,
        ToolUseBlock,
        UserMessage,
    )

    final = (
        f"[{tag}] Updated the pricing rules to quantize to cents and added tests; "
        "all pricing tests pass and the change is published for review. "
        + ("Details: rounding is half-even on every tier. " * 10)
    )

    def tool(call_id: str, name: str, tool_input: dict[str, Any], output: str) -> list[Any]:
        return [
            AssistantMessage(
                content=[ToolUseBlock(id=call_id, name=name, input=tool_input)],
                model="fake-model",
            ),
            UserMessage(
                content=[ToolResultBlock(tool_use_id=call_id, content=output, is_error=False)]
            ),
        ]

    return [
        AssistantMessage(
            content=[TextBlock(text=f"[{tag}] Reading the pricing module first.")],
            model="fake-model",
        ),
        *tool(f"{tag}-read", "Read", {"file_path": "src/pricing.py"}, _source_file(125)),
        *tool(
            f"{tag}-edit",
            "Edit",
            {
                "file_path": "src/pricing.py",
                "old_string": _source_file(6),
                "new_string": _source_file(6).replace("quantize", "quantize_half_even"),
            },
            "The file src/pricing.py has been updated.",
        ),
        *tool(
            f"{tag}-pytest",
            "Bash",
            {"command": "uv run pytest tests/test_pricing.py -v"},
            _pytest_output(120),
        ),
        *tool(f"{tag}-diff", "Bash", {"command": "git diff"}, _git_diff(40)),
        *tool(
            f"{tag}-publish",
            "Bash",
            {"command": "git push origin pricing-cents"},
            "To github.com:acme/pricing.git\n * [new branch] pricing-cents -> pricing-cents\n",
        ),
        AssistantMessage(content=[TextBlock(text=final)], model="fake-model"),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="fake-session",
            result=final,
            usage={"input_tokens": 10, "output_tokens": 10},
        ),
    ]


def _checkpointing_fake(*tags: str, replay_messages: Any = ()) -> Any:
    """The scripted model plus the Claude harness's native checkpoint export.

    Each turn the live session serves plays the next tag's coding turn.
    """

    from curie_runner.fake import FakeModelSession
    from curie_runner.history import HarnessReplayState

    class CheckpointingFake(FakeModelSession):
        async def export_replay_state(self) -> HarnessReplayState:
            return HarnessReplayState(
                harness="claude",
                kind="checkpoint",
                entries=tuple(
                    {
                        "type": "assistant",
                        "uuid": f"{tags[0]}-{i}",
                        "cwd": "/workspace",
                        "payload": "native-" + ("n" * 700),
                    }
                    for i in range(6)
                ),
            )

    turns = iter(tags)

    def next_turn() -> list[Any]:
        return _coding_turn_script(next(turns))

    return CheckpointingFake(next_turn, replay_messages=replay_messages)


def _runner(
    store: Any, session: Any, *, resumed: bool = False, capacity_exceeded: bool = False
) -> Any:
    from curie_runner import RunTracer, SideEffectClassifier
    from curie_runner.session import SessionRunner

    return SessionRunner(
        session_factory=lambda: session,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="history-capacity-2927-assembled",
        session_id="session-2927-assembled",
        history_store=store,
        history_resumed=resumed,
        history_capacity_exceeded=capacity_exceeded,
    )


async def _run_turn(runner: Any, text: str, ts: str) -> Any:
    from aci_protocol import Event, Final, parse_ndjson_line

    await runner.start()
    lines = [
        line async for line in runner.run_inbound(Event(type="message", text=text, user="U", ts=ts))
    ]
    final = parse_ndjson_line(lines[-1])
    assert isinstance(final, Final)
    return final


def _replay_text(messages: Any) -> str:
    return json.dumps([message.to_dict() for message in messages])


class _Thread:
    """One transcript key on the served API, plus the runner boot config for it."""

    def __init__(
        self,
        client: Any,
        auth_headers: dict[str, str],
        served_api: str,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from curie_runner.config import RunnerConfig
        from curie_runner.history import HISTORY_TOKEN_ENV

        agent = client.post(
            "/agents",
            json={
                "name": f"history-2927-{uuid.uuid4().hex[:8]}",
                "channel": {"kind": "slack", "address": "C0EXAMPLE9"},
            },
            headers=auth_headers,
        )
        assert agent.status_code == 201, agent.text
        self.client = client
        self.auth_headers = auth_headers
        self.key_path = f"/agents/{agent.json()['id']}/state/transcript/t1"
        self.key_url = f"{served_api}{self.key_path}"
        self.api_key = auth_headers["X-API-Key"]
        # The boot path resolves the store's credential from the process env.
        monkeypatch.setenv(HISTORY_TOKEN_ENV, self.api_key)
        plugin_dir = tmp_path / "bundle"
        plugin_dir.mkdir(exist_ok=True)
        self.config = RunnerConfig.from_env(
            {
                "CURIE_PLUGIN_DIR": str(plugin_dir),
                "CURIE_SESSION_ID": "s-2927-assembled",
                "CURIE_SANDBOX_ID": "b-2927-assembled",
                "CURIE_BUDGET": _BUDGET,
                "CURIE_HISTORY_REF": self.key_url,
            }
        )
        self.publication = {
            "user": "Platform publication outcome",
            "assistant": (
                f"{_PUBLICATION_TEXT} for branch pricing-cents. "
                + "Checks queued; review requested from the code owners. " * 30
            )[:1_990],
            "ts": "2026-09-22T12:00:00+00:00",
            "publication_id": str(uuid.uuid4()),
        }

    def stored(self) -> list[Any]:
        response = self.client.get(self.key_path, headers=self.auth_headers)
        assert response.status_code == 200, response.text
        value: list[Any] = response.json()["value"]
        return value

    def first_turn(self, text: str) -> None:
        from aci_protocol import SessionStatus
        from curie_runner.history import StateApiTranscriptStore

        async def go() -> None:
            store = StateApiTranscriptStore(self.key_url, token=self.api_key)
            assert await store.load() == []
            runner = _runner(
                store,
                _checkpointing_fake("coding"),
            )
            final = await _run_turn(runner, text, "1")
            assert final.status is SessionStatus.DONE, final
            assert runner.history_durable is True

        anyio.run(go)

    def boot_and_turn(self, tag: str, text: str, ts: str, *, expect_in_replay: str) -> str:
        """Boot a fresh sandbox, check the replay, run one large turn; return the replay."""

        from aci_protocol import SessionStatus
        from curie_runner import __main__ as boot

        async def go() -> str:
            store, replay, capacity_exceeded = await boot._load_history(self.config)
            assert capacity_exceeded is False
            assert replay.present
            replay_text = _replay_text(replay.messages)
            assert expect_in_replay in replay_text
            runner = _runner(
                store,
                _checkpointing_fake(tag, replay_messages=replay.messages),
                resumed=replay.present,
                capacity_exceeded=capacity_exceeded,
            )
            final = await _run_turn(runner, text, ts)
            assert final.status is SessionStatus.DONE, final
            assert runner.history_durable is True
            return replay_text

        return anyio.run(go)

    async def append_publication(self) -> int:
        # The worker's publication outcome append carries no reserve.
        async with aiohttp.ClientSession() as http:
            async with http.post(
                f"{self.key_url}/append",
                json={"item": self.publication},
                headers={"X-API-Key": self.api_key},
            ) as resp:
                return resp.status

    def publish_outcome(self) -> int:
        return anyio.run(self.append_publication)

    def boot_replay(self) -> str:
        from curie_runner import __main__ as boot

        async def go() -> str:
            _store, replay, capacity_exceeded = await boot._load_history(self.config)
            assert capacity_exceeded is False
            return _replay_text(replay.messages)

        return anyio.run(go)

    def marker_survives(self) -> bool:
        return any(
            item.get("publication_id") == self.publication["publication_id"]
            for item in self.stored()
        )


_CODING_TEXT = "CODING-2927: quantize the pricing rules to cents and publish a PR"
_REVIEW_TEXT = "REVIEW-2927: address the review comments on PR 41 and push a revision"
_FOLLOWUP_TEXT = "FOLLOWUP-2927: rebase the pricing branch and rerun the full suite"


def test_assembled_factory_sized_turn_persists_through_the_api(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    served_api: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from aci_protocol import SessionStatus
    from claude_agent_sdk import (
        AssistantMessage,
        ResultMessage,
        TextBlock,
        ToolResultBlock,
        ToolUseBlock,
        UserMessage,
    )
    from curie_runner.fake import FakeModelSession
    from curie_runner.history import StateApiTranscriptStore, TurnRecord

    thread = _Thread(client, auth_headers, served_api, tmp_path, monkeypatch)
    final_text = "The factory work passed review and the pull request was published."

    def factory_turn() -> list[Any]:
        script: list[Any] = []
        for index in range(400):
            script.append(
                AssistantMessage(
                    content=[
                        ToolUseBlock(
                            id=f"factory{index}", name="Bash", input={"command": "ls"}
                        )
                    ],
                    model="fake-model",
                )
            )
            script.append(
                UserMessage(
                    content=[
                        ToolResultBlock(
                            tool_use_id=f"factory{index}", content="ok", is_error=False
                        )
                    ]
                )
            )
        script.append(AssistantMessage(content=[TextBlock(text=final_text)], model="fake-model"))
        script.append(
            ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id="fake-session",
                result=final_text,
                usage={"input_tokens": 10, "output_tokens": 10},
            )
        )
        return script

    async def go() -> None:
        store = StateApiTranscriptStore(thread.key_url, token=thread.api_key)
        assert await store.load() == []
        runner = _runner(store, FakeModelSession(factory_turn))
        final = await _run_turn(runner, "Complete factory issue", "1")
        assert final.status is SessionStatus.DONE, final
        assert final.text == final_text
        assert runner.history_durable is True

    anyio.run(go)
    value = thread.stored()
    assert _size(value) <= _CAP - _RESERVE
    turn = TurnRecord.from_dict(value[-1])
    assert turn.assistant == final_text
    stored_text = json.dumps(turn.to_dict())
    assert '"id": "factory0"' not in stored_text
    assert '"id": "factory399"' in stored_text

    async def resume() -> None:
        from curie_runner import __main__ as boot

        store, replay, capacity_exceeded = await boot._load_history(thread.config)
        assert capacity_exceeded is False
        assert final_text in _replay_text(replay.messages)
        runner = _runner(
            store,
            _checkpointing_fake("followup", replay_messages=replay.messages),
            resumed=replay.present,
            capacity_exceeded=capacity_exceeded,
        )
        final = await _run_turn(runner, "Review the published change", "2")
        assert final.status is SessionStatus.DONE, final
        assert runner.history_durable is True

    anyio.run(resume)
    assert _size(thread.stored()) <= _CAP - _RESERVE
    assert "Review the published change" in thread.boot_replay()


def test_assembled_one_turn_coding_thread_resumes_review_turn(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    served_api: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    thread = _Thread(client, auth_headers, served_api, tmp_path, monkeypatch)

    thread.first_turn(_CODING_TEXT)
    stored_one_turn = _size(thread.stored())
    assert 40_000 <= stored_one_turn <= 60_000, stored_one_turn

    thread.boot_and_turn("review", _REVIEW_TEXT, "2", expect_in_replay=_CODING_TEXT)
    assert _size(thread.stored()) <= _CAP - _RESERVE, _size(thread.stored())

    assert thread.publish_outcome() == 200

    third_replay = thread.boot_replay()
    assert _REVIEW_TEXT in third_replay
    assert _PUBLICATION_TEXT in third_replay, "the publication outcome vanished from the replay"
    assert _size(thread.stored()) <= _CAP
    assert thread.marker_survives(), "the worker's publication marker must survive compaction"


def test_assembled_publication_outcome_survives_a_live_turn_compaction(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    served_api: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The worker appends the publication outcome while the review sandbox is
    still live; that sandbox's next large turn is refused at the reserve and
    compacts with the marker in the stored value. The next boot's replay must
    still tell the model the pull request was published."""

    from aci_protocol import Event, Final, SessionStatus, parse_ndjson_line
    from curie_runner import __main__ as boot

    thread = _Thread(client, auth_headers, served_api, tmp_path, monkeypatch)
    thread.first_turn(_CODING_TEXT)

    async def live_sandbox() -> None:
        store, replay, capacity_exceeded = await boot._load_history(thread.config)
        assert capacity_exceeded is False
        # One live sandbox and model session serve both turns.
        runner = _runner(
            store,
            _checkpointing_fake("review", "followup", replay_messages=replay.messages),
            resumed=replay.present,
        )
        final = await _run_turn(runner, _REVIEW_TEXT, "2")
        assert final.status is SessionStatus.DONE, final
        # The worker's publication outcome lands between the two turns.
        assert await thread.append_publication() == 200
        lines = [
            line
            async for line in runner.run_inbound(
                Event(type="message", text=_FOLLOWUP_TEXT, user="U", ts="3")
            )
        ]
        final = parse_ndjson_line(lines[-1])
        assert isinstance(final, Final)
        assert final.status is SessionStatus.DONE, final
        assert runner.history_durable is True

    anyio.run(live_sandbox)
    assert _size(thread.stored()) <= _CAP - _RESERVE, _size(thread.stored())
    assert thread.marker_survives(), "the worker's publication marker must survive compaction"

    next_replay = thread.boot_replay()
    assert _FOLLOWUP_TEXT in next_replay
    assert _PUBLICATION_TEXT in next_replay, "the publication outcome vanished from the replay"
