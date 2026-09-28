"""The platform ``report_progress`` tool (#3077): phases, activity, the wire body.

The tool reads the bundle's ``progress/phases.json``, validates the model's
phase and round against it, adds the runner's own activity counters, and POSTs
the report to the api with the request-bound token. Every HTTP case runs against
a real local aiohttp server that records what it received; nothing of ours is
mocked.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import anyio
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from claude_agent_sdk import AssistantMessage, TextBlock, ToolUseBlock
from curie_runner import SideEffectClassifier
from curie_runner.progress import (
    PROGRESS_TOKEN_ENV,
    PROGRESS_URL_ENV,
    ProgressActivity,
    build_progress_tool,
    load_phase_declaration,
    resolve_progress,
)
from curie_runner.translate import TurnState, translate_message

REPO_ROOT = Path(__file__).resolve().parents[2]
BUNDLE = REPO_ROOT / "examples" / "dark-factory"
TOKEN = "sbx.example-progress-token.signature"
BUNDLE_PHASES = [
    "read_issue",
    "pin_criteria",
    "plan",
    "plan_review",
    "failing_test",
    "implement",
    "review_diff",
    "publish",
    "wait_ci",
]


def _bundle(tmp_path: Path, declaration: object | str) -> Path:
    progress = tmp_path / "progress"
    progress.mkdir(parents=True)
    text = declaration if isinstance(declaration, str) else json.dumps(declaration)
    (progress / "phases.json").write_text(text)
    return tmp_path


def _text(result: dict[str, Any]) -> str:
    return " ".join(str(item.get("text") or "") for item in result.get("content") or [])


class _Recorder:
    def __init__(self, statuses: list[int] | None = None) -> None:
        self.statuses = list(statuses or [])
        self.received: list[tuple[dict[str, Any], str | None]] = []

    def app(self) -> web.Application:
        app = web.Application()

        async def report(request: web.Request) -> web.Response:
            self.received.append((await request.json(), request.headers.get("X-API-Key")))
            status = self.statuses.pop(0) if self.statuses else 201
            if status == 201:
                return web.json_response(
                    {"recorded": True, "request_id": "example-request"}, status=201
                )
            return web.json_response({"code": "example"}, status=status)

        app.router.add_post("/v1/work-item-progress/{request_id}", report)
        return app


# --- the phase declaration ---------------------------------------------------


def test_the_dark_factory_bundle_declares_nine_phases_and_three_loops() -> None:
    declared = load_phase_declaration(BUNDLE)
    assert declared is not None
    assert [phase["id"] for phase in declared["phases"]] == BUNDLE_PHASES
    assert [(loop["start"], loop["review"], loop["cap"]) for loop in declared["loops"]] == [
        ("plan", "plan_review", 3),
        ("implement", "review_diff", 3),
        ("implement", "wait_ci", 3),
    ]
    assert [(stage["id"], stage["phases"]) for stage in declared["stages"]] == [
        ("plan", ["read_issue", "pin_criteria", "plan"]),
        ("plan_review", ["plan_review"]),
        ("implement", ["failing_test", "implement"]),
        ("review_diff", ["review_diff", "publish"]),
        ("wait_ci", ["wait_ci"]),
    ]


def test_a_bundle_without_a_phase_file_declares_nothing(tmp_path: Path) -> None:
    assert load_phase_declaration(tmp_path) is None


@pytest.mark.parametrize(
    "declaration",
    [
        "{not json",
        {"phases": []},
        {"phases": [{"id": "Bad-Id", "label": "Bad"}]},
        {"phases": [{"id": "a", "label": "A"}, {"id": "a", "label": "Again"}]},
        {"phases": [{"id": "a", "label": "x" * 41}]},
        {"phases": [{"id": f"p{i}", "label": "P"} for i in range(13)]},
        {
            "phases": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}],
            "loops": [{"start": "b", "review": "a", "cap": 3}],
        },
        {
            "phases": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}],
            "loops": [{"start": "a", "review": "missing", "cap": 3}],
        },
        {
            "phases": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}],
            "loops": [{"start": "a", "review": "b", "cap": 6}],
        },
        {
            "phases": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}],
            "stages": [{"id": "only", "label": "Only", "phases": ["a"]}],
        },
        {
            "phases": [{"id": "a", "label": "A"}],
            "stages": [{"id": "only", "label": "Only", "phases": ["a", "a"]}],
        },
        {
            "phases": [{"id": "a", "label": "A"}],
            "stages": [{"id": "only", "label": "Only", "phases": ["missing"]}],
        },
    ],
)
def test_a_malformed_phase_file_raises(tmp_path: Path, declaration: object) -> None:
    with pytest.raises(ValueError):
        load_phase_declaration(_bundle(tmp_path, declaration))


def test_progress_needs_both_env_vars_and_a_declaration(tmp_path: Path) -> None:
    env = {PROGRESS_URL_ENV: "http://api:8000/v1/work-item-progress/x", PROGRESS_TOKEN_ENV: TOKEN}
    assert resolve_progress(env, BUNDLE) is not None
    assert resolve_progress({PROGRESS_URL_ENV: env[PROGRESS_URL_ENV]}, BUNDLE) is None
    assert resolve_progress({PROGRESS_TOKEN_ENV: TOKEN}, BUNDLE) is None
    assert resolve_progress(env, tmp_path) is None


# --- the tool ----------------------------------------------------------------


def _tool(url: str, activity: ProgressActivity | None = None) -> Any:
    resolved = resolve_progress({PROGRESS_URL_ENV: url, PROGRESS_TOKEN_ENV: TOKEN}, BUNDLE)
    assert resolved is not None
    client, declaration = resolved
    return build_progress_tool(declaration, client, activity or ProgressActivity())


def test_the_tool_is_named_report_progress_and_enumerates_the_phases() -> None:
    tool = _tool("http://127.0.0.1:9/v1/work-item-progress/x")
    assert tool.name == "report_progress"
    schema = tool.input_schema
    assert schema["properties"]["phase"]["enum"] == BUNDLE_PHASES
    assert schema["properties"]["note"]["maxLength"] == 280


@pytest.mark.parametrize(
    ("args", "needle"),
    [
        ({"phase": "explore_repo"}, "read_issue"),
        ({"phase": "read_issue", "round": 2}, "round"),
        ({"phase": "plan", "round": 4}, "round"),
        ({"phase": "plan", "round": 0}, "round"),
    ],
)
def test_the_tool_refuses_an_undeclared_phase_or_a_bad_round_without_posting(
    args: dict[str, Any], needle: str
) -> None:
    recorder = _Recorder()

    async def go() -> dict[str, Any]:
        async with TestServer(recorder.app()) as server:
            tool = _tool(str(server.make_url("/v1/work-item-progress/example-request")))
            return await tool.handler(args)

    result = anyio.run(go)
    assert result.get("is_error") is True
    assert needle in _text(result)
    assert recorder.received == []


def test_a_valid_report_posts_the_wire_body_with_the_token_header() -> None:
    recorder = _Recorder()
    activity = ProgressActivity()
    activity.model = "glm-5.3-flash"
    activity.observe_assistant_message()
    activity.observe_tool("mcp__github__get_issue")
    activity.observe_tool("Bash")

    async def go() -> dict[str, Any]:
        async with TestServer(recorder.app()) as server:
            tool = _tool(str(server.make_url("/v1/work-item-progress/example-request")), activity)
            return await tool.handler(
                {"phase": "plan", "note": "Drafting the plan", "round": 2}
            )

    result = anyio.run(go)
    assert result.get("is_error") is not True
    assert _text(result) == "recorded plan"
    assert len(recorder.received) == 1
    body, key = recorder.received[0]
    assert key == TOKEN
    declared = load_phase_declaration(BUNDLE)
    assert body == {
        "phase": "plan",
        "note": "Drafting the plan",
        "round": 2,
        "declaration": declared,
        "activity": {
            "model": "glm-5.3-flash",
            "turns": 1,
            "tool_calls": 2,
            "last_tool": "Bash",
        },
    }


def test_a_report_without_note_or_round_omits_them() -> None:
    recorder = _Recorder()

    async def go() -> dict[str, Any]:
        async with TestServer(recorder.app()) as server:
            tool = _tool(str(server.make_url("/v1/work-item-progress/example-request")))
            return await tool.handler({"phase": "read_issue"})

    result = anyio.run(go)
    assert _text(result) == "recorded read_issue"
    body, _key = recorder.received[0]
    assert body["phase"] == "read_issue"
    assert "note" not in body or body["note"] is None
    assert "round" not in body or body["round"] is None


def test_a_server_error_is_retried_once_then_surfaced_as_a_tool_error() -> None:
    recorder = _Recorder(statuses=[500, 500, 500])

    async def go() -> dict[str, Any]:
        async with TestServer(recorder.app()) as server:
            tool = _tool(str(server.make_url("/v1/work-item-progress/example-request")))
            return await tool.handler({"phase": "implement", "round": 1})

    result = anyio.run(go)
    assert len(recorder.received) == 2
    assert result.get("is_error") is True
    assert "500" in _text(result)
    assert TOKEN not in _text(result)


def test_a_5xx_then_success_records_once_retried() -> None:
    recorder = _Recorder(statuses=[503, 201])

    async def go() -> dict[str, Any]:
        async with TestServer(recorder.app()) as server:
            tool = _tool(str(server.make_url("/v1/work-item-progress/example-request")))
            return await tool.handler({"phase": "publish"})

    result = anyio.run(go)
    assert len(recorder.received) == 2
    assert _text(result) == "recorded publish"


@pytest.mark.parametrize("status", [401, 409, 422, 429])
def test_a_refused_report_is_a_tool_error_naming_the_status_and_not_retried(
    status: int,
) -> None:
    recorder = _Recorder(statuses=[status])

    async def go() -> dict[str, Any]:
        async with TestServer(recorder.app()) as server:
            tool = _tool(str(server.make_url("/v1/work-item-progress/example-request")))
            return await tool.handler({"phase": "wait_ci"})

    result = anyio.run(go)
    assert len(recorder.received) == 1
    assert result.get("is_error") is True
    assert str(status) in _text(result)


def test_an_unreachable_api_is_a_tool_error_not_an_exception() -> None:
    tool = _tool("http://127.0.0.1:9/v1/work-item-progress/example-request")

    async def go() -> dict[str, Any]:
        return await tool.handler({"phase": "read_issue"})

    result = anyio.run(go)
    assert result.get("is_error") is True


# --- activity counters through translation -----------------------------------


def test_translate_feeds_tool_calls_and_turns_into_the_activity() -> None:
    activity = ProgressActivity()
    message = AssistantMessage(
        content=[
            TextBlock(text="Looking at the issue."),
            ToolUseBlock(id="1", name="mcp__github__get_issue", input={}),
            ToolUseBlock(id="2", name="Read", input={}),
        ],
        model="m",
    )
    translate_message(message, TurnState(), SideEffectClassifier(), None, activity=activity)
    assert activity.tool_calls == 2
    assert activity.last_tool == "Read"
    assert activity.turns == 1

    translate_message(
        AssistantMessage(
            content=[ToolUseBlock(id="3", name="mcp__github__get_issue", input={})], model="m"
        ),
        TurnState(),
        SideEffectClassifier(),
        None,
        activity=activity,
    )
    assert activity.tool_calls == 3
    assert activity.last_tool == "get_issue"
    assert activity.turns == 2
