"""A posted question round ends the turn, on the real bundled CLI (#5785).

ADR 0286 decision 2 ends the request when the factory asks, and decision 11
says the platform enforces that ending. Each case runs one real turn through
the real session and the real bundled Claude CLI. A local stand-in for
/v1/messages scripts the model: it calls ``ask_questions``, then tries a
``Bash`` call that would create a marker file, then ends with text. The
questions route is a local HTTP API, so the tool's request and the answer it
reads are the real HTTP boundary. No credential and no network.

The session is wired the way ``__main__`` wires a factory implementation
session: the gate a managed workspace arms, the completion contract bound
from the issue read capability with the gate's halt, the ``curie`` server
carrying the bound question tool, the approval hook and the permission
callback.

The frames the halted turn emits are pinned to
``tests/vectors/question-halt-turn.json``, which the worker's handoff test
replays through its completion path.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import anyio
import pytest
from aci_protocol import Event, Final, SessionStatus, parse_ndjson
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.adapter import ClaudeAgentSession, build_options
from curie_runner.approval import APPROVAL_SERVER_NAME, ApprovalGate, build_approval_gate
from curie_runner.completion_contract import bind_completion_contract
from curie_runner.harness.claude.approval import (
    build_approval_hook,
    build_approval_server,
    build_can_use_tool,
)
from curie_runner.harness.claude.question_tool import wrap_questions_handler
from curie_runner.issue_read import CAPABILITY_HEADER
from curie_runner.session import SessionRunner

VECTOR = Path(__file__).resolve().parents[2] / "tests" / "vectors" / "question-halt-turn.json"
CAPABILITY = "wir.example-capability"
ISSUE_READ_PATH = "/work-items/issue-read"
CONTRACT_PATH = "/work-items/completion-contract"
QUESTIONS_PATH = "/work-items/questions"
IMPLEMENTATION = {
    "completion_contract": "implementation",
    "execution": None,
    "rule_proposal": False,
}
ASK_ID = "toolu_ask01"
BASH_ID = "toolu_bash01"
PUBLISH_ID = "toolu_publish01"
PUBLISH_TOOL = f"mcp__{APPROVAL_SERVER_NAME}__publish_changes"
QUESTION_TOOL = f"mcp__{APPROVAL_SERVER_NAME}__ask_questions"
ASK_INPUT = {
    "summary": "The importer reproduces the duplicate on a second run.",
    "questions": ["Should a re-run replace the rows or append them?"],
}
_TURN_TIMEOUT_S = 90

_BODIES = web.AppKey("bodies", list[dict[str, Any]])
_MARKER = web.AppKey("marker", str)
_ANSWER = web.AppKey("answer", tuple[int, Any])
_ASKED = web.AppKey("asked", list[tuple[Any, str | None]])
_SCRIPT = web.AppKey("script", str)


def _sse(event: str, data: dict[str, Any]) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


def _tool_results(body: dict[str, Any]) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for message in body.get("messages") or ():
        content = message.get("content") if isinstance(message, dict) else None
        if isinstance(content, list):
            found.extend(
                block
                for block in content
                if isinstance(block, dict) and block.get("type") == "tool_result"
            )
    return found


def _ask_block() -> tuple[dict[str, Any], dict[str, Any]]:
    return (
        {"type": "tool_use", "id": ASK_ID, "name": QUESTION_TOOL, "input": {}},
        {"type": "input_json_delta", "partial_json": json.dumps(ASK_INPUT)},
    )


def _bash_block(marker: str) -> tuple[dict[str, Any], dict[str, Any]]:
    command = {"command": f"touch {marker}", "description": "keep working"}
    return (
        {"type": "tool_use", "id": BASH_ID, "name": "Bash", "input": {}},
        {"type": "input_json_delta", "partial_json": json.dumps(command)},
    )


def _publish_block() -> tuple[dict[str, Any], dict[str, Any]]:
    proposal = {"title": "Replace the rows on a re-run", "body": "Done."}
    return (
        {"type": "tool_use", "id": PUBLISH_ID, "name": PUBLISH_TOOL, "input": {}},
        {"type": "input_json_delta", "partial_json": json.dumps(proposal)},
    )


def _text_block() -> tuple[dict[str, Any], dict[str, Any]]:
    return (
        {"type": "text", "text": ""},
        {"type": "text_delta", "text": "Kept working after asking."},
    )


def _step(app: web.Application, answered: int) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """What the scripted model answers after ``answered`` tool results."""

    marker = app[_MARKER]
    script = app[_SCRIPT]
    if script == "sequential":
        steps = [[_ask_block()], [_bash_block(marker)]]
    elif script == "same-message-bash":
        steps = [[_ask_block(), _bash_block(marker)]]
    else:
        steps = [[_ask_block(), _publish_block()]]
    taken = 0
    for blocks in steps:
        if answered == taken:
            return blocks
        taken += len(blocks)
    return [_text_block()]


async def _messages(request: web.Request) -> web.StreamResponse:
    """The scripted model, one step per request, chosen by the results so far."""

    body = await request.json()
    request.app[_BODIES].append(body)
    blocks = _step(request.app, len(_tool_results(body)))
    response = web.StreamResponse(headers={"content-type": "text/event-stream"})
    await response.prepare(request)
    message = {
        "id": f"msg_stand_in_{len(request.app[_BODIES])}",
        "type": "message",
        "role": "assistant",
        "model": body.get("model", "stand-in"),
        "content": [],
        "stop_reason": None,
        "stop_sequence": None,
        "usage": {"input_tokens": 1, "output_tokens": 1},
    }
    stop_reason = "tool_use" if blocks[0][0]["type"] == "tool_use" else "end_turn"
    events: list[tuple[str, dict[str, Any]]] = [
        ("message_start", {"type": "message_start", "message": message})
    ]
    for index, (block, delta) in enumerate(blocks):
        events += [
            (
                "content_block_start",
                {"type": "content_block_start", "index": index, "content_block": block},
            ),
            (
                "content_block_delta",
                {"type": "content_block_delta", "index": index, "delta": delta},
            ),
            ("content_block_stop", {"type": "content_block_stop", "index": index}),
        ]
    events += [
        (
            "message_delta",
            {
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": None},
                "usage": {"output_tokens": 1},
            },
        ),
        ("message_stop", {"type": "message_stop"}),
    ]
    for event, data in events:
        await response.write(_sse(event, data))
    await response.write_eof()
    return response


async def _contract(_request: web.Request) -> web.Response:
    return web.json_response(IMPLEMENTATION)


async def _questions(request: web.Request) -> web.Response:
    request.app[_ASKED].append((await request.json(), request.headers.get(CAPABILITY_HEADER)))
    status, payload = request.app[_ANSWER]
    return web.json_response(payload, status=status)


async def _anything_else(_request: web.Request) -> web.Response:
    return web.json_response({"input_tokens": 1})


class _Turn:
    def __init__(
        self,
        final: Final,
        frames: list[Any],
        bodies: list[dict[str, Any]],
        asked: list[tuple[Any, str | None]],
        gate: ApprovalGate,
        marker: Path,
    ) -> None:
        self.final = final
        self.frames = frames
        self.bodies = bodies
        self.asked = asked
        self.gate = gate
        self.marker = marker


def _run_turn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    answer: tuple[int, Any],
    script: str = "sequential",
) -> _Turn:
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-config"))
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    cwd = tmp_path / "workspace"
    (cwd / ".git").mkdir(parents=True)
    marker = tmp_path / "written-after-asking"
    # The gate a factory implementation session arms: a managed workspace.
    gate = build_approval_gate(operator_tools=[], policy_routes={}, managed_workspace=True)
    assert gate is not None

    async def scenario() -> tuple[Final, list[Any], list[dict[str, Any]], list[Any]]:
        app = web.Application()
        app[_BODIES] = []
        app[_MARKER] = str(marker)
        app[_ANSWER] = answer
        app[_ASKED] = []
        app[_SCRIPT] = script
        app.router.add_post("/v1/messages", _messages)
        app.router.add_post(CONTRACT_PATH, _contract)
        app.router.add_post(QUESTIONS_PATH, _questions)
        app.router.add_route("*", "/{tail:.*}", _anything_else)
        async with TestServer(app, host="127.0.0.1") as server:
            bound = await bind_completion_contract(
                (str(server.make_url(ISSUE_READ_PATH)), CAPABILITY),
                on_questions_posted=gate.halt_for_questions,
            )
            assert bound.question_handler is not None
            options = build_options(
                plugins=[],
                model="claude-sonnet-5",
                system_prompt="You build the acme ticket.",
                max_turns=5,
                max_budget_usd=None,
                resume=None,
                cwd=str(cwd),
                hooks=build_approval_hook(gate),
                can_use_tool=build_can_use_tool(gate),
                mcp_servers={
                    APPROVAL_SERVER_NAME: build_approval_server(
                        gate,
                        managed_workspace=True,
                        include_request_approval=False,
                        question_tool=wrap_questions_handler(bound.question_handler),
                    )
                },
                env={
                    "ANTHROPIC_API_KEY": "sk-ant-placeholder",
                    "ANTHROPIC_BASE_URL": str(server.make_url("")).rstrip("/"),
                    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                    "DISABLE_TELEMETRY": "1",
                },
            )
            runner = SessionRunner(
                max_usd_per_day=None,
                held_secrets=frozenset(),
                session_factory=lambda: ClaudeAgentSession(options),
                ceiling=0,
                tracer=RunTracer(None),
                classifier=SideEffectClassifier(),
                trace_name="curie-run:acme-questions",
                session_id="session-PLACEHOLDER",
                model="claude-sonnet-5",
                approval_gate=gate,
            )
            lines: list[str] = []
            await runner.start()
            try:
                with anyio.fail_after(_TURN_TIMEOUT_S):
                    async for line in runner.run_turn(
                        Event(type="message", text="build the ticket", user="U0EXAMPLE1", ts="1")
                    ):
                        lines.append(line)
            finally:
                await runner.close()
            frames = list(parse_ndjson("".join(lines)))
            finals = [frame for frame in frames if isinstance(frame, Final)]
            assert len(finals) == 1, lines
            return finals[0], frames, list(app[_BODIES]), list(app[_ASKED])

    final, frames, bodies, asked = anyio.run(scenario)
    return _Turn(final, frames, bodies, asked, gate, marker)


def _pinned(frames: list[Any]) -> list[dict[str, Any]]:
    """The frames as the vector pins them: no version, token counts or arguments."""

    pinned: list[dict[str, Any]] = []
    for frame in frames:
        data = frame.model_dump(mode="json", exclude_none=True)
        for key in ("version", "input_tokens", "output_tokens", "arguments"):
            data.pop(key, None)
        pinned.append(data)
    return pinned


def test_a_posted_round_ends_the_turn_before_the_model_is_asked_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Decision 11: after a 200 the turn ends at once; the next call never runs.

    Red without the halt: the model is asked again, its Bash call creates the
    marker, and it ends on its own text.
    """

    turn = _run_turn(tmp_path, monkeypatch, answer=(200, {"round": 1, "rounds_limit": 3}))

    assert turn.asked == [(ASK_INPUT, CAPABILITY)]
    assert not turn.marker.exists(), "a tool call ran after the questions were posted"
    # The model is never asked again: no request carries the ask's result.
    assert len(turn.bodies) == 1
    assert turn.gate.questions_posted is True
    assert turn.final.status is SessionStatus.DONE
    assert turn.final.approval_summary is None

    # The worker's handoff test replays exactly these frames.
    vector = json.loads(VECTOR.read_text(encoding="utf-8"))
    assert _pinned(turn.frames) == vector["runner_frames"]


def test_a_same_body_replay_also_ends_the_turn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A replay answers 200 too: the round is posted, so the turn still ends."""

    turn = _run_turn(tmp_path, monkeypatch, answer=(200, {"round": 2, "rounds_limit": 3}))

    assert not turn.marker.exists()
    assert len(turn.bodies) == 1
    assert turn.final.status is SessionStatus.DONE


def test_a_call_sent_beside_a_posted_ask_never_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The model sends Bash in the same message as the ask: it is refused."""

    turn = _run_turn(
        tmp_path,
        monkeypatch,
        answer=(200, {"round": 1, "rounds_limit": 3}),
        script="same-message-bash",
    )

    assert turn.asked == [(ASK_INPUT, CAPABILITY)]
    assert not turn.marker.exists(), "a sibling call ran after the questions were posted"
    assert len(turn.bodies) == 1
    assert turn.final.status is SessionStatus.DONE


def test_a_publish_sent_beside_a_posted_ask_does_not_park_the_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A publication proposal beside the ask is no approval: the turn ends done.

    Red without the session rule: the stream observer records the proposal
    and the final pauses for approval, so the request is held, not settled.
    """

    turn = _run_turn(
        tmp_path,
        monkeypatch,
        answer=(200, {"round": 1, "rounds_limit": 3}),
        script="same-message-publish",
    )

    assert turn.asked == [(ASK_INPUT, CAPABILITY)]
    assert len(turn.bodies) == 1
    assert turn.final.status is SessionStatus.DONE
    assert turn.final.approval_summary is None
    assert turn.final.approval_granted_tool is None


@pytest.mark.parametrize(
    "answer",
    [
        (409, {"code": "already_asked"}),
        (409, {"detail": {"code": "question_rounds_exhausted"}}),
        (503, {"code": "questions_unavailable"}),
        (500, {}),
    ],
    ids=["409-already-asked", "409-rounds-exhausted", "503", "500"],
)
def test_a_refused_or_failed_ask_does_not_halt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, answer: tuple[int, Any]
) -> None:
    """Liveness: nothing was posted, so the model keeps its turn and the call runs."""

    turn = _run_turn(tmp_path, monkeypatch, answer=answer)

    assert turn.asked == [(ASK_INPUT, CAPABILITY)]
    assert turn.marker.exists(), "the turn was stopped although nothing was posted"
    assert turn.gate.questions_posted is False
    assert turn.final.status is SessionStatus.DONE
    assert len(turn.bodies) == 3


def test_a_refused_publish_beside_a_refused_ask_still_pauses_for_approval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Liveness for the session rule: with nothing posted, publication pauses as before."""

    turn = _run_turn(
        tmp_path,
        monkeypatch,
        answer=(409, {"code": "already_asked"}),
        script="same-message-publish",
    )

    assert turn.gate.questions_posted is False
    assert turn.final.status is SessionStatus.AWAITING_APPROVAL
    assert turn.final.approval_granted_tool == PUBLISH_TOOL
