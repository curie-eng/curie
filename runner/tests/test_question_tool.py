"""The ``ask_questions`` platform tool (#5785, decisions 1, 2 and 16).

An implementation run that holds the issue read capability gets
``mcp__curie__ask_questions``. Every HTTP case runs against a real local
aiohttp server that records what it received; nothing of ours is mocked.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from pathlib import Path
from typing import Any

import anyio
import mcp.types as mcp_types
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner.__main__ import build_runner
from curie_runner.approval import QUESTION_TOOL_NAME, is_platform_owned_tool
from curie_runner.completion_contract import bind_completion_contract, sibling_urls
from curie_runner.issue_read import CAPABILITY_HEADER, ISSUE_READ_TOKEN_ENV, ISSUE_READ_URL_ENV
from curie_runner.question_tool import question_input_issues, questions_handler
from curie_runner.side_effects import (
    PLATFORM_IDEMPOTENT_TOOLS,
    PLATFORM_SIDE_EFFECTING_TOOLS,
    SideEffectClassifier,
)
from runner_boot_session import initialized_session
from test_completion_contract import (
    CAPABILITY,
    IMPLEMENTATION,
    ISSUE_READ_PATH,
    PROBE,
    RECOVERY,
    _Api,
    _boot,  # noqa: F401 - the autouse boot fixture of the sibling tests
    _boot_session,
    _config,
    _curie_tools,
    _ModelSession,
    _text,
)

QUESTIONS_PATH = "/work-items/questions"


class _QuestionsApi(_Api):
    """The contract api plus a recording ``questions`` route."""

    def __init__(
        self,
        contract: list[tuple[int, Any]] | None = None,
        *,
        answer: tuple[int, Any] = (200, {"round": 1, "rounds_limit": 3}),
        drop: bool = False,
    ) -> None:
        super().__init__(contract)
        self.answer = answer
        self.drop = drop
        self.question_calls: list[tuple[Any, str | None]] = []

    def app(self) -> web.Application:
        app = super().app()

        async def questions(request: web.Request) -> web.Response:
            self.question_calls.append(
                (await request.json(), request.headers.get(CAPABILITY_HEADER))
            )
            if self.drop:
                assert request.transport is not None
                request.transport.close()
                raise ConnectionResetError
            status, payload = self.answer
            return web.json_response(payload, status=status)

        app.router.add_post(QUESTIONS_PATH, questions)
        return app


def _ask(api: _QuestionsApi, args: dict[str, Any]) -> dict[str, Any]:
    async def go() -> dict[str, Any]:
        async with TestServer(api.app()) as server:
            handler = questions_handler(str(server.make_url(QUESTIONS_PATH)), CAPABILITY)
            return await handler(args)

    return anyio.run(go)


def test_parallel_calls_reach_the_route_one_at_a_time() -> None:
    # The route lets a retry of a round still posting write again, so two
    # parallel calls from one turn must not both be in flight.
    in_flight = 0
    most = 0
    overlapped = asyncio.Event()

    async def slow(request: web.Request) -> web.Response:
        nonlocal in_flight, most
        in_flight += 1
        most = max(most, in_flight)
        if in_flight > 1:
            overlapped.set()
        # Hold the first call open long enough for an unserialized second call
        # to arrive; serialized, nothing arrives and the wait times out.
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(overlapped.wait(), timeout=0.5)
        in_flight -= 1
        return web.json_response({"round": 1, "rounds_limit": 3})

    async def go() -> list[dict[str, Any]]:
        app = web.Application()
        app.router.add_post(QUESTIONS_PATH, slow)
        async with TestServer(app) as server:
            handler = questions_handler(str(server.make_url(QUESTIONS_PATH)), CAPABILITY)
            return list(await asyncio.gather(handler(_valid()), handler(_valid())))

    results = anyio.run(go)
    assert most == 1
    assert all(not result.get("is_error") for result in results)


def _valid() -> dict[str, Any]:
    return {"summary": "Reproduced the crash on the main branch.", "questions": ["Which one?"]}


# --- Mounting --------------------------------------------------------------------


def test_an_implementation_contract_with_the_capability_mounts_the_tool(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    session = _boot_session(monkeypatch, tmp_path, _QuestionsApi([(200, IMPLEMENTATION)]))
    assert isinstance(session, _ModelSession)
    tools = _curie_tools(session.options)
    assert "ask_questions" in tools


def test_a_publication_recovery_contract_does_not_mount_the_tool(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    session = _boot_session(monkeypatch, tmp_path, _QuestionsApi([(200, RECOVERY)]))
    # A publication recovery constructs no model session at all.
    assert not isinstance(session, _ModelSession)


def test_without_the_capability_the_tool_is_not_mounted_and_nothing_is_called(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    api = _QuestionsApi()
    session = _boot_session(monkeypatch, tmp_path, api, issue_read_path=None)
    assert isinstance(session, _ModelSession)
    assert "ask_questions" not in _curie_tools(session.options)
    assert api.contract_calls == []


def test_the_binding_sets_the_handler_only_for_an_implementation_contract() -> None:
    async def bind(payload: dict[str, Any]) -> Any:
        api = _QuestionsApi([(200, payload)])
        async with TestServer(api.app()) as server:
            return await bind_completion_contract(
                (str(server.make_url("/work-items/issue-read")), CAPABILITY)
            )

    implementation = anyio.run(bind, IMPLEMENTATION)
    assert implementation.question_handler is not None
    assert anyio.run(bind, RECOVERY).question_handler is None
    assert anyio.run(bind_completion_contract, None).question_handler is None


def test_the_questions_route_sits_beside_the_issue_read_route() -> None:
    urls = sibling_urls("https://api.example:8443/v1/runner/work-items/issue-read")
    assert urls.questions == "https://api.example:8443/v1/runner/work-items/questions"


# --- Bounds ----------------------------------------------------------------------

_LONG_QUESTION = "q" * 1_001

VIOLATIONS: list[tuple[str, dict[str, Any]]] = [
    ("empty summary", {"summary": "", "questions": ["Which one?"]}),
    ("whitespace summary", {"summary": " \t\n ", "questions": ["Which one?"]}),
    ("long summary", {"summary": "s" * 601, "questions": ["Which one?"]}),
    ("no questions", {"summary": "found it", "questions": []}),
    ("eleven questions", {"summary": "found it", "questions": ["Which?"] * 11}),
    ("empty question", {"summary": "found it", "questions": [""]}),
    ("whitespace question", {"summary": "found it", "questions": ["  "]}),
    ("long question", {"summary": "found it", "questions": [_LONG_QUESTION]}),
    ("blank line", {"summary": "found it", "questions": ["One\n\nTwo"]}),
    ("blank line with spaces", {"summary": "found it", "questions": ["One\n \t\nTwo"]}),
    ("crlf blank line", {"summary": "found it", "questions": ["One\r\n\r\nTwo"]}),
    # Every line break form the API normalizes before its blank-line rule, and
    # a line that only an invisible character keeps from being blank.
    ("line separator blank line", {"summary": "found it", "questions": ["One\u2028\u2028Two"]}),
    ("lone cr blank line", {"summary": "found it", "questions": ["One\r\rTwo"]}),
    ("next line blank line", {"summary": "found it", "questions": ["One\x85\x85Two"]}),
    ("form feed blank line", {"summary": "found it", "questions": ["One\n\x0c\nTwo"]}),
    ("vertical tab blank line", {"summary": "found it", "questions": ["One\x0b\x0bTwo"]}),
    ("zero width blank line", {"summary": "found it", "questions": ["One\n\u200b\nTwo"]}),
    ("invisible question", {"summary": "found it", "questions": ["\u200b\u2060"]}),
    ("extra key", {"summary": "found it", "questions": ["Which?"], "note": "x"}),
    ("non string question", {"summary": "found it", "questions": ["Which?", 7]}),
    ("missing summary", {"questions": ["Which?"]}),
    ("missing questions", {"summary": "found it"}),
]


@pytest.mark.parametrize(("label", "args"), VIOLATIONS, ids=[label for label, _ in VIOLATIONS])
def test_input_outside_the_bounds_is_refused_without_a_request(
    label: str, args: dict[str, Any]
) -> None:
    api = _QuestionsApi()
    result = _ask(api, args)
    assert result["is_error"] is True
    assert "outside its bounds" in _text(result)
    assert api.question_calls == []
    assert question_input_issues(args)


EDGES: list[tuple[str, dict[str, Any]]] = [
    ("summary at the limit", {"summary": "s" * 600, "questions": ["Which?"]}),
    ("ten questions", {"summary": "found it", "questions": [f"Question {n}?" for n in range(10)]}),
    ("question at the limit", {"summary": "found it", "questions": ["q" * 1_000]}),
    ("single line break", {"summary": "found it", "questions": ["One\nTwo", "Three\r\nFour"]}),
    (
        "single separator line breaks",
        {
            "summary": "found it",
            "questions": ["One\u2028Two", "Three\rFour", "Five\x85Six", "Seven\u2029Eight"],
        },
    ),
]


@pytest.mark.parametrize(("label", "args"), EDGES, ids=[label for label, _ in EDGES])
def test_the_edge_values_are_accepted_and_sent(label: str, args: dict[str, Any]) -> None:
    api = _QuestionsApi()
    result = _ask(api, args)
    assert "is_error" not in result
    assert question_input_issues(args) == []
    assert api.question_calls == [(args, CAPABILITY)]


# --- Answers ---------------------------------------------------------------------


def test_the_request_carries_the_capability_and_exactly_the_two_fields() -> None:
    api = _QuestionsApi()
    _ask(api, _valid())
    [(body, token)] = api.question_calls
    assert token == CAPABILITY
    assert set(body) == {"summary", "questions"}
    assert body == _valid()


def test_an_accepted_post_reports_the_round_and_ends_the_session() -> None:
    result = _ask(_QuestionsApi(answer=(200, {"round": 2, "rounds_limit": 3})), _valid())
    assert "is_error" not in result
    text = _text(result)
    assert "round 2 of 3" in text
    assert "session ends now" in text
    assert "without opening a pull request" in text


def test_a_posted_size_refusal_tells_the_model_to_shorten_the_questions() -> None:
    # The pre-check passed; the platform refused the comment it would post.
    api = _QuestionsApi(answer=(409, {"detail": {"code": "invalid_questions"}}))
    result = _ask(api, _valid())
    assert result["is_error"] is True
    text = _text(result)
    assert "too long" in text and "Shorten" in text
    assert len(api.question_calls) == 1


def test_exhausted_rounds_tell_the_model_to_end_with_could_not_complete() -> None:
    api = _QuestionsApi(
        answer=(409, {"detail": {"code": "question_rounds_exhausted"}}),
    )
    result = _ask(api, _valid())
    assert result["is_error"] is True
    assert "Could not complete:" in _text(result)


def test_another_409_renders_its_code() -> None:
    result = _ask(_QuestionsApi(answer=(409, {"detail": {"code": "ticket_closed"}})), _valid())
    assert result["is_error"] is True
    assert "ticket_closed" in _text(result)


def test_a_409_code_outside_the_pattern_is_not_rendered() -> None:
    result = _ask(_QuestionsApi(answer=(409, {"detail": {"code": "Bad Code!"}})), _valid())
    assert result["is_error"] is True
    assert "Bad Code" not in _text(result)


def test_a_503_asks_for_a_retry() -> None:
    result = _ask(_QuestionsApi(answer=(503, {})), _valid())
    assert result["is_error"] is True
    assert "Retry shortly" in _text(result)


def test_another_status_is_refused_with_its_number() -> None:
    result = _ask(_QuestionsApi(answer=(404, {})), _valid())
    assert result["is_error"] is True
    assert "404" in _text(result)


def test_a_transport_failure_asks_for_a_retry_and_never_names_the_endpoint() -> None:
    seen: list[str] = []

    async def go() -> dict[str, Any]:
        async with TestServer(_QuestionsApi(drop=True).app()) as server:
            url = str(server.make_url(QUESTIONS_PATH))
            seen.append(url)
            return await questions_handler(url, CAPABILITY)(_valid())

    result = anyio.run(go)
    text = _text(result)
    assert result["is_error"] is True
    assert "Retry shortly" in text
    assert seen[0] not in text
    assert "127.0.0.1" not in text
    assert CAPABILITY not in text
    json.dumps(result)


# --- The halt a posted round sets (ADR 0286 decision 11) ---------------------------


async def _bash_decisions(options: Any) -> list[dict[str, Any]]:
    """Every PreToolUse answer the booted session gives one Bash call."""

    decisions: list[dict[str, Any]] = []
    for matcher in options.hooks["PreToolUse"]:
        for callback in matcher.hooks:
            decisions.append(
                await callback(
                    {
                        "hook_event_name": "PreToolUse",
                        "tool_name": "Bash",
                        "tool_input": {"command": "git commit -am more"},
                    },
                    "toolu_after_ask",
                    {"signal": None},
                )
            )
    return decisions


async def _after_ask_decisions(options: Any) -> list[dict[str, Any]]:
    """Every PostToolUse answer the booted session gives the ask itself."""

    decisions: list[dict[str, Any]] = []
    for matcher in options.hooks.get("PostToolUse", []):
        for callback in matcher.hooks:
            decisions.append(
                await callback(
                    {
                        "hook_event_name": "PostToolUse",
                        "tool_name": "mcp__curie__ask_questions",
                        "tool_input": _valid(),
                        "tool_response": {},
                    },
                    "toolu_ask",
                    {"signal": None},
                )
            )
    return decisions


def _ends(decision: dict[str, Any]) -> bool:
    return decision.get("continue_") is False


def _stops(decision: dict[str, Any]) -> bool:
    output = decision.get("hookSpecificOutput") or {}
    return output.get("permissionDecision") == "deny" and decision.get("continue_") is False


@pytest.mark.parametrize(
    ("answer", "halts", "managed"),
    [
        ((200, {"round": 1, "rounds_limit": 3}), True, True),
        ((200, {"round": 1, "rounds_limit": 3}), True, False),
        ((409, {"code": "already_asked"}), False, True),
        ((503, {}), False, True),
    ],
    ids=["200", "200-no-workspace", "409", "503"],
)
def test_the_booted_session_stops_the_turn_after_a_posted_round(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    answer: tuple[int, Any],
    halts: bool,
    managed: bool,
) -> None:
    """``__main__`` hands the tool the gate's halt: after a 200 the next call stops the turn.

    A factory boot (an issue read capability and a managed workspace) mounts
    ``ask_questions``; the mounted tool is called the way the CLI calls it, then
    the session's own PreToolUse hooks decide a Bash call. Red without the
    halt: no hook refuses. A refusal or a 503 posts nothing and stops nothing.
    A factory boot with no managed workspace, approval tools or tool policy has
    no policy gate and must still stop.
    """

    workspace = tmp_path / "workspace"
    if managed:
        (workspace / ".git").mkdir(parents=True)
    api = _QuestionsApi([(200, IMPLEMENTATION)], answer=answer)

    async def go() -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
        async with TestServer(api.app()) as server:
            monkeypatch.setenv(ISSUE_READ_URL_ENV, str(server.make_url(ISSUE_READ_PATH)))
            monkeypatch.setenv(ISSUE_READ_TOKEN_ENV, CAPABILITY)
            runner = build_runner(
                _config(tmp_path),
                mcp_capability=PROBE,
                workspace_path=workspace if managed else None,
            )
            session = await initialized_session(runner)
            assert isinstance(session, _ModelSession)
            before = await _bash_decisions(session.options)
            entry = session.options.mcp_servers["curie"]["instance"].get_request_handler(
                "tools/call"
            )
            assert entry is not None
            called = await entry.handler(
                None,
                mcp_types.CallToolRequestParams(name="ask_questions", arguments=_valid()),
            )
            after = await _bash_decisions(session.options)
            ended = await _after_ask_decisions(session.options)
            return before, after + ended, called.model_dump()

    before, after, called = anyio.run(go)

    assert len(api.question_calls) == 1
    assert not any(_stops(decision) for decision in before)
    assert any(_stops(decision) for decision in after) is halts
    # The ask's own PostToolUse ends the turn before the model is asked again.
    assert (
        any(_ends(decision) and "hookSpecificOutput" not in decision for decision in after) is halts
    )
    assert bool(called.get("isError") or called.get("is_error")) is not halts


# --- Classification --------------------------------------------------------------


def test_the_tool_is_always_side_effecting() -> None:
    assert QUESTION_TOOL_NAME == "mcp__curie__ask_questions"
    assert "mcp__curie__ask_questions" in PLATFORM_SIDE_EFFECTING_TOOLS
    assert not PLATFORM_SIDE_EFFECTING_TOOLS & PLATFORM_IDEMPOTENT_TOOLS
    assert SideEffectClassifier().is_side_effecting("mcp__curie__ask_questions")
    declared = SideEffectClassifier(readonly_tools={"mcp__curie__ask_questions", "read"})
    assert declared.is_side_effecting("mcp__curie__ask_questions")
    assert not declared.is_side_effecting("read")


def test_the_tool_is_a_platform_owned_name() -> None:
    assert is_platform_owned_tool(QUESTION_TOOL_NAME, state_server_mounted=False)
    assert is_platform_owned_tool(QUESTION_TOOL_NAME, state_server_mounted=True)
