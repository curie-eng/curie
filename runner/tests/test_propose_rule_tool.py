"""The ``propose_rule`` platform tool (ADR 0287, #5796, decisions 3 to 5).

A factory run mounts ``mcp__curie__propose_rule`` only when the API's
completion contract answer says ``rule_proposal: true`` and the session is
not the fake model. The tool posts exactly ``rule`` and ``reason`` to the
``/work-items/rule-proposal`` sibling of the issue read route, and turns every
refusal into a readable ``is_error`` result that never repeats the rule.

This file is the must-not-regress record decision 4 names, kept apart from
``test_tracker_result.py`` so a rename of that module or its tests does not
move it. Its helpers are copied, not imported, for the same reason.

Every HTTP case runs against a real local aiohttp server that records what it
received; nothing of ours is mocked.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import anyio
import mcp.types as mcp_types
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner import RunnerConfig, tracker_result
from curie_runner import __main__ as boot
from curie_runner.__main__ import build_runner
from curie_runner.approval import (
    APPROVAL_SERVER_NAME,
    PROPOSE_RULE_TOOL_NAME,
    is_platform_owned_tool,
)
from curie_runner.fake import FakeModelSession
from curie_runner.issue_read import CAPABILITY_HEADER, ISSUE_READ_TOKEN_ENV, ISSUE_READ_URL_ENV
from curie_runner.mcp_tool_capability import McpToolCapabilityProbe
from curie_runner.propose_rule import PROPOSE_RULE_SCHEMA, propose_rule_handler
from curie_runner.side_effects import (
    PLATFORM_IDEMPOTENT_TOOLS,
    PLATFORM_SIDE_EFFECTING_TOOLS,
    SideEffectClassifier,
)
from curie_runner.tracker_result import BoundContract
from runner_boot_session import initialized_session

CAPABILITY = "wir.example-capability.signature"
CONTRACT_PATH = "/work-items/completion-contract"
ISSUE_READ_PATH = "/work-items/issue-read"
RULE_PATH = "/work-items/rule-proposal"
BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'

IMPLEMENTATION: dict[str, Any] = {
    "completion_contract": "implementation",
    "result_kinds": [],
    "session_usd_allowance": None,
}
TRACKER: dict[str, Any] = {
    "completion_contract": "tracker_result",
    "result_kinds": ["ticket", "findings"],
    "session_usd_allowance": 2.5,
}

RULE = "Every CLI flag that takes a time parses it with parse_timestamp in cli/src/time.rs."
REASON = "The reviewer stated it for every time flag, not only this change."


class _Api:
    """A local API answering the contract and rule proposal routes, recording each call."""

    def __init__(
        self,
        contract: dict[str, Any] | None = None,
        *,
        answer: tuple[int, Any] = (200, {"status": "candidate"}),
        drop: bool = False,
    ) -> None:
        self.contract = contract if contract is not None else IMPLEMENTATION
        self.answer = answer
        self.drop = drop
        self.contract_calls: list[Any] = []
        self.rule_calls: list[tuple[Any, str | None]] = []

    def app(self) -> web.Application:
        app = web.Application()

        async def contract(request: web.Request) -> web.Response:
            body = await request.read()
            self.contract_calls.append(json.loads(body) if body else None)
            return web.json_response(self.contract)

        async def rule(request: web.Request) -> web.StreamResponse:
            self.rule_calls.append((await request.json(), request.headers.get(CAPABILITY_HEADER)))
            if self.drop:
                assert request.transport is not None
                request.transport.close()
                raise ConnectionResetError
            status, payload = self.answer
            if isinstance(payload, bytes):
                return web.Response(status=status, body=payload)
            return web.json_response(payload, status=status)

        app.router.add_post(CONTRACT_PATH, contract)
        app.router.add_post(RULE_PATH, rule)
        return app


def _text(result: dict[str, Any]) -> str:
    return " ".join(str(item.get("text") or "") for item in result.get("content") or [])


# --- Boot ----------------------------------------------------------------------


class _ModelSession:
    """The real-model session stand in: records the options it was built with."""

    def __init__(self, options: Any) -> None:
        self.options = options

    async def connect(self) -> None:
        return None

    async def query(self, _text: str) -> None:
        return None

    async def interrupt(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def abort(self) -> None:
        return None

    async def receive_turn(self) -> Any:
        if False:
            yield None


# The bundle's MCP surface, given rather than probed: these boots run inside
# an event loop beside the local API.
PROBE = McpToolCapabilityProbe(
    complete=True,
    has_potential_write_tool=True,
    tool_count=1,
    observed_tools=frozenset(),
    readonly_tools=frozenset(),
)


async def _no_delay(_seconds: float) -> None:
    return None


@pytest.fixture
def bound(monkeypatch: pytest.MonkeyPatch) -> list[BoundContract]:
    """Every contract the boot bound, as it bound it."""

    recorded: list[BoundContract] = []

    async def recording(
        issue_read: tuple[str, str] | None, *, fake_model: bool = False
    ) -> BoundContract:
        # The contract call's retry waits are injected, never slept.
        result = await tracker_result.bind_completion_contract(
            issue_read, fake_model=fake_model, delay=_no_delay
        )
        recorded.append(result)
        return result

    monkeypatch.setattr(boot, "ClaudeAgentSession", _ModelSession)
    monkeypatch.setattr(boot, "bind_completion_contract", recording)
    monkeypatch.delenv(ISSUE_READ_URL_ENV, raising=False)
    monkeypatch.delenv(ISSUE_READ_TOKEN_ENV, raising=False)
    return recorded


def _config(tmp_path: Path) -> RunnerConfig:
    plugin = tmp_path / "bundle"
    (plugin / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    (plugin / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "revise", "version": "0.1.0", "description": "t"}), encoding="utf-8"
    )
    return RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(plugin),
            "CURIE_SESSION_ID": "s-revise",
            "CURIE_SANDBOX_ID": "b-revise",
            "CURIE_BUDGET": BUDGET,
        }
    )


def _boot_session(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, api: _Api, *, fake_model: bool = False
) -> Any:
    """Boot a runner against ``api`` and return the session it would connect."""

    async def go() -> Any:
        async with TestServer(api.app()) as server:
            monkeypatch.setenv(ISSUE_READ_URL_ENV, str(server.make_url(ISSUE_READ_PATH)))
            monkeypatch.setenv(ISSUE_READ_TOKEN_ENV, CAPABILITY)
            runner = build_runner(_config(tmp_path), fake_model=fake_model, mcp_capability=PROBE)
            return await initialized_session(runner)

    return anyio.run(go)


def _curie_tools(options: Any) -> set[str]:
    server = options.mcp_servers[APPROVAL_SERVER_NAME]

    async def names() -> set[str]:
        entry = server["instance"].get_request_handler("tools/list")
        assert entry is not None
        result = await entry.handler(None, mcp_types.PaginatedRequestParams())
        return {tool["name"] for tool in result.model_dump()["tools"]}

    return anyio.run(names)


# --- Mounting (decision 3, acceptance criterion 5) --------------------------------


@pytest.mark.parametrize(
    ("contract", "fake_model", "mounted"),
    [
        pytest.param({**IMPLEMENTATION, "rule_proposal": True}, False, True, id="true"),
        pytest.param({**TRACKER, "rule_proposal": True}, False, True, id="tracker-true"),
        pytest.param({**IMPLEMENTATION, "rule_proposal": False}, False, False, id="false"),
        # A runner newer than its API: the field is absent and reads false.
        pytest.param(IMPLEMENTATION, False, False, id="absent"),
        pytest.param({**IMPLEMENTATION, "rule_proposal": True}, True, False, id="fake-model"),
    ],
)
def test_propose_rule_mounts_only_when_the_contract_offers_it(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    bound: list[BoundContract],
    contract: dict[str, Any],
    fake_model: bool,
    mounted: bool,
) -> None:
    api = _Api(contract)
    session = _boot_session(monkeypatch, tmp_path, api, fake_model=fake_model)
    assert api.contract_calls == [{}]
    assert len(bound) == 1
    assert bound[0].contract.rule_proposal is (contract.get("rule_proposal") is True)
    assert (bound[0].rule_handler is not None) is mounted
    if fake_model:
        # The offline fake never gets the tool: no handler is bound, and the
        # fake's session holds no SDK options to mount one on.
        assert isinstance(session, FakeModelSession)
    else:
        assert isinstance(session, _ModelSession)
        tools = _curie_tools(session.options)
        assert ("propose_rule" in tools) is mounted
        # The contract's own tool still mounts beside it.
        tracker = contract["completion_contract"] == "tracker_result"
        assert ("submit_tracker_result" in tools) is tracker
        assert ("ask_questions" in tools) is not tracker
    # Booting never proposes anything.
    assert api.rule_calls == []


def test_the_tool_is_a_platform_owned_side_effecting_name() -> None:
    assert PROPOSE_RULE_TOOL_NAME == "mcp__curie__propose_rule"
    for state in (False, True):
        assert is_platform_owned_tool(PROPOSE_RULE_TOOL_NAME, state_server_mounted=state)
    assert PROPOSE_RULE_TOOL_NAME in PLATFORM_SIDE_EFFECTING_TOOLS
    assert PROPOSE_RULE_TOOL_NAME not in PLATFORM_IDEMPOTENT_TOOLS
    # Even a harness declaring it read-only cannot make it idempotent.
    assert SideEffectClassifier(
        readonly_tools=frozenset({PROPOSE_RULE_TOOL_NAME})
    ).is_side_effecting(PROPOSE_RULE_TOOL_NAME)


def test_the_schema_takes_exactly_rule_and_reason() -> None:
    assert set(PROPOSE_RULE_SCHEMA["properties"]) == {"rule", "reason"}
    assert sorted(PROPOSE_RULE_SCHEMA["required"]) == ["reason", "rule"]
    assert PROPOSE_RULE_SCHEMA["additionalProperties"] is False


# --- The handler (decision 5) -------------------------------------------------------


def _propose(api: _Api, args: Any) -> dict[str, Any]:
    async def go() -> dict[str, Any]:
        async with TestServer(api.app()) as server:
            handler = propose_rule_handler(str(server.make_url(RULE_PATH)), CAPABILITY)
            return await handler(args)

    return anyio.run(go)


def test_the_post_carries_the_capability_and_only_rule_and_reason() -> None:
    api = _Api()
    result = _propose(
        api,
        {
            "rule": RULE,
            "reason": REASON,
            "scope_key": "github:example:1",
            "agent_id": "00000000-0000-4000-8000-000000000001",
            "url": "https://example.invalid/pull/1",
            "state": "accepted",
        },
    )
    assert api.rule_calls == [({"rule": RULE, "reason": REASON}, CAPABILITY)]
    assert not result.get("is_error")


def test_an_accepted_proposal_reads_as_a_candidate() -> None:
    result = _propose(_Api(), {"rule": RULE, "reason": REASON})
    assert not result.get("is_error")
    text = _text(result)
    assert "candidate" in text
    assert RULE not in text
    assert REASON not in text


REFUSALS = [
    (403, "rule_proposal_not_eligible"),
    (409, "rule_already_proposed"),
    (409, "invalid_context"),
    (422, "invalid_text"),
    (422, "too_large"),
    (422, "reserved_text"),
    (422, "invalid_body"),
    (401, "invalid_capability"),
]


@pytest.mark.parametrize(("status", "code"), REFUSALS, ids=[code for _, code in REFUSALS])
def test_a_refusal_is_a_readable_error_that_never_repeats_the_rule(
    status: int, code: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    answer = {"detail": {"code": code, "message": "refused"}}
    result = _propose(_Api(answer=(status, answer)), {"rule": RULE, "reason": REASON})
    assert result.get("is_error") is True
    text = _text(result)
    assert code in text
    # Readable: a sentence telling the model what to do next.
    assert "propose_rule" in text or "call again" in text
    for secret in (RULE, REASON, CAPABILITY):
        assert secret not in text
        assert secret not in caplog.text


@pytest.mark.parametrize(
    "answer",
    [(429, {}), (503, {"detail": {"code": "unavailable"}}), (500, b"not json"), (404, {})],
    ids=["429", "503", "500", "404"],
)
def test_other_failures_are_readable_errors(answer: tuple[int, Any]) -> None:
    result = _propose(_Api(answer=answer), {"rule": RULE, "reason": REASON})
    assert result.get("is_error") is True
    text = _text(result)
    assert str(answer[0]) in text
    assert RULE not in text
    assert REASON not in text


def test_a_200_without_the_candidate_status_is_an_error() -> None:
    result = _propose(_Api(answer=(200, {"status": "accepted"})), {"rule": RULE, "reason": REASON})
    assert result.get("is_error") is True


def test_a_transport_failure_never_names_the_endpoint() -> None:
    result = _propose(_Api(drop=True), {"rule": RULE, "reason": REASON})
    assert result.get("is_error") is True
    text = _text(result)
    assert "127.0.0.1" not in text
    assert "rule-proposal" not in text
    assert RULE not in text


@pytest.mark.parametrize(
    "args",
    [
        {"reason": REASON},
        {"rule": RULE},
        {"rule": 7, "reason": REASON},
        {"rule": RULE, "reason": ["why"]},
        {},
    ],
    ids=["no-rule", "no-reason", "rule-not-string", "reason-not-string", "empty"],
)
def test_a_missing_or_non_string_argument_is_refused_without_a_request(
    args: dict[str, Any],
) -> None:
    api = _Api()
    result = _propose(api, args)
    assert result.get("is_error") is True
    assert "rule and reason" in _text(result)
    assert api.rule_calls == []
