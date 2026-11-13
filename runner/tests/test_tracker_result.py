"""The completion contract and ``submit_tracker_result`` (ADR 0277, #5586).

A factory execution learns from the API, over the issue read capability the
worker already injects, whether it ends in an implementation or a tracker
result. A tracker result session mounts ``submit_tracker_result``, cannot use
``Task`` or ``Agent``, spends at most the API's allowance, and gets exactly one
model session per request. Every way the contract can fail blocks the model.

Every HTTP case runs against a real local aiohttp server that records what it
received; nothing of ours is mocked.
"""

from __future__ import annotations

import asyncio
import functools
import json
from pathlib import Path
from typing import Any

import anyio
import mcp.types as mcp_types
import pytest
from aci_protocol import BootEnv
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner import RunnerConfig, tracker_result
from curie_runner import __main__ as boot
from curie_runner.__main__ import build_runner
from curie_runner.approval import (
    APPROVAL_SERVER_NAME,
    TRACKER_RESULT_TOOL_NAME,
    is_platform_owned_tool,
)
from curie_runner.fake import FakeModelSession
from curie_runner.harness.claude.approval import build_approval_server
from curie_runner.harness.claude.preflight_blocked import PreflightBlockedSession
from curie_runner.harness.claude.tracker_result_tool import build_tracker_result_tool
from curie_runner.issue_read import CAPABILITY_HEADER, ISSUE_READ_TOKEN_ENV, ISSUE_READ_URL_ENV
from curie_runner.mcp_tool_capability import McpToolCapabilityProbe
from curie_runner.side_effects import PLATFORM_IDEMPOTENT_TOOLS
from curie_runner.tracker_result import (
    CONTRACT_ATTEMPTS,
    REASON_ALLOWANCE_INVALID,
    REASON_BODY_INVALID,
    REASON_CONTRACT_UNKNOWN,
    REASON_DEADLINE_EXCEEDED,
    REASON_INTERNAL_ERROR,
    REASON_RESULT_KINDS_INVALID,
    REASON_ROUTE_INVALID,
    REASON_SESSION_ALREADY_STARTED,
    REASON_STATUS_REFUSED,
    REASON_STATUS_TRANSIENT,
    REASON_TRANSPORT_FAILED,
    TRACKER_RESULT_DISALLOWED_TOOLS,
    CompletionContract,
    ContractUnavailable,
    fetch_completion_contract,
    session_limits,
    sibling_urls,
)
from runner_boot_session import initialized_session

CAPABILITY = "wir.example-capability.signature"
CONTRACT_PATH = "/work-items/completion-contract"
RESULT_PATH = "/work-items/tracker-result"
ISSUE_READ_PATH = "/work-items/issue-read"
BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'
ALLOWANCE = 4.25

TRACKER = {
    "completion_contract": "tracker_result",
    "result_kinds": ["ticket", "findings"],
    "session_usd_allowance": ALLOWANCE,
}
IMPLEMENTATION = {
    "completion_contract": "implementation",
    "result_kinds": [],
    "session_usd_allowance": None,
}

# The boot env field set before #5586: the runner learns the contract from the
# API through the existing issue read fields, never a new boot field.
BOOT_ENV_KEYS = frozenset(
    {
        "ANTHROPIC_BASE_URL",
        "CURIE_APPROVAL_DECISION",
        "CURIE_APPROVAL_GRANT_ARGUMENTS",
        "CURIE_APPROVAL_GRANT_TOOL",
        "CURIE_APPROVAL_REQUIRED_TOOLS",
        "CURIE_APPROVAL_RESUMED_KIND",
        "CURIE_ATTACHMENTS_MANIFEST",
        "CURIE_ATTACHMENT_ACCESS_ENABLED",
        "CURIE_BUDGET",
        "CURIE_BUNDLE_REF",
        "CURIE_BUNDLE_VERSION",
        "CURIE_CHANNEL_BOUND",
        "CURIE_CHANNEL_KIND",
        "CURIE_CHANNEL_MEMORY_REF",
        "CURIE_CONNECTOR_AGENT",
        "CURIE_CONNECTOR_CALLER_TOKEN",
        "CURIE_CONNECTOR_NAMESPACE",
        "CURIE_CONNECTOR_RELEASE",
        "CURIE_CONNECTOR_SECRET_KEYS",
        "CURIE_CREDENTIALS",
        "CURIE_DEPLOYMENT_ENVIRONMENT",
        "CURIE_FAKE_MODEL",
        "CURIE_HISTORY_MAX_BYTES",
        "CURIE_HISTORY_MAX_TURNS",
        "CURIE_HISTORY_REF",
        "CURIE_HISTORY_TOKEN",
        "CURIE_ISSUE_READ_TOKEN",
        "CURIE_ISSUE_READ_URL",
        "CURIE_MAX_TURNS",
        "CURIE_MEMORY_MAX_FACTS",
        "CURIE_MEMORY_REF",
        "CURIE_MEMORY_TOKEN",
        "CURIE_MEMORY_WRITES",
        "CURIE_MODEL",
        "CURIE_MODEL_API_BACKEND",
        "CURIE_MODEL_ENV_KEY",
        "CURIE_PLUGIN_DIR",
        "CURIE_PROGRESS_TOKEN",
        "CURIE_PROGRESS_URL",
        "CURIE_REPO_CA_BUNDLE",
        "CURIE_REPO_ORIGIN",
        "CURIE_REPO_PATH",
        "CURIE_REVIEWER_MODEL",
        "CURIE_RUNNER_PORT",
        "CURIE_RUNNER_TOKEN",
        "CURIE_SANDBOX_ID",
        "CURIE_SESSION_ID",
        "CURIE_STATE_TOKEN",
        "CURIE_STATE_URL",
        "CURIE_THINKING",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_HEADERS",
        "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
    }
)


def _text(result: dict[str, Any]) -> str:
    return " ".join(str(item.get("text") or "") for item in result.get("content") or [])


class _Api:
    """A local API answering the contract and result routes, recording each call.

    ``drops`` closes the connection on that many first contract calls (a real
    transport failure). ``contract`` is a list of answers, one per contract
    call; the last one repeats.
    """

    def __init__(
        self,
        contract: list[tuple[int, Any]] | None = None,
        *,
        result: tuple[int, Any] | None = None,
        drops: int = 0,
        stall: bool = False,
    ) -> None:
        self.contract = contract or [(200, IMPLEMENTATION)]
        self.result = result or (200, {})
        self.drops = drops
        self.stall = stall
        self.contract_calls: list[tuple[Any, str | None]] = []
        self.result_calls: list[tuple[Any, str | None]] = []

    def app(self) -> web.Application:
        app = web.Application()

        async def contract(request: web.Request) -> web.StreamResponse:
            body = await request.read()
            self.contract_calls.append(
                (json.loads(body) if body else None, request.headers.get(CAPABILITY_HEADER))
            )
            if self.stall:
                await asyncio.Event().wait()
            if len(self.contract_calls) <= self.drops:
                assert request.transport is not None
                request.transport.close()
                raise ConnectionResetError
            status, payload = self.contract[min(len(self.contract_calls), len(self.contract)) - 1]
            if isinstance(payload, bytes):
                return web.Response(status=status, body=payload)
            return web.json_response(payload, status=status)

        async def result(request: web.Request) -> web.Response:
            self.result_calls.append((await request.json(), request.headers.get(CAPABILITY_HEADER)))
            status, payload = self.result
            if isinstance(payload, bytes):
                return web.Response(status=status, body=payload)
            return web.json_response(payload, status=status)

        app.router.add_post(CONTRACT_PATH, contract)
        app.router.add_post(RESULT_PATH, result)
        return app


async def _no_delay(_seconds: float) -> None:
    return None


# --- Boot ----------------------------------------------------------------------


class _ModelSession:
    """The real-model session stand in: records every model session started."""

    started: list[_ModelSession] = []

    def __init__(self, options: Any) -> None:
        self.options = options
        _ModelSession.started.append(self)

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


class _FakeSession(FakeModelSession):
    """The offline fake, recording every fake model session started."""

    started: list[_FakeSession] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        _FakeSession.started.append(self)


# The bundle's MCP surface, given rather than probed: these boots run inside
# an event loop beside the local API.
PROBE = McpToolCapabilityProbe(
    complete=True,
    has_potential_write_tool=True,
    tool_count=1,
    observed_tools=frozenset(),
    readonly_tools=frozenset(),
)


@pytest.fixture(autouse=True)
def _boot(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(boot, "ClaudeAgentSession", _ModelSession)
    monkeypatch.setattr(boot, "FakeModelSession", _FakeSession)
    # The contract call's retry waits are injected, never slept.
    monkeypatch.setattr(
        boot,
        "bind_completion_contract",
        functools.partial(tracker_result.bind_completion_contract, delay=_no_delay),
    )
    monkeypatch.delenv(ISSUE_READ_URL_ENV, raising=False)
    monkeypatch.delenv(ISSUE_READ_TOKEN_ENV, raising=False)
    _ModelSession.started = []
    _FakeSession.started = []


def _config(tmp_path: Path) -> RunnerConfig:
    plugin = tmp_path / "bundle"
    (plugin / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    (plugin / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "triage", "version": "0.1.0", "description": "t"}), encoding="utf-8"
    )
    return RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(plugin),
            "CURIE_SESSION_ID": "s-triage",
            "CURIE_SANDBOX_ID": "b-triage",
            "CURIE_BUDGET": BUDGET,
        }
    )


def _boot_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    api: _Api,
    *,
    fake_model: bool = False,
    issue_read_path: str | None = ISSUE_READ_PATH,
) -> Any:
    """Boot a runner against ``api`` and return the session it would connect."""

    async def go() -> Any:
        async with TestServer(api.app()) as server:
            if issue_read_path is not None:
                monkeypatch.setenv(ISSUE_READ_URL_ENV, str(server.make_url(issue_read_path)))
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


def test_the_boot_env_field_set_is_unchanged() -> None:
    assert frozenset(BootEnv.env_keys()) == BOOT_ENV_KEYS


def test_a_tracker_result_contract_mounts_the_tool_denies_subagents_and_caps_spend(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    api = _Api([(200, TRACKER)])
    session = _boot_session(monkeypatch, tmp_path, api)
    assert isinstance(session, _ModelSession)
    assert api.contract_calls == [({}, CAPABILITY)]
    assert {"submit_tracker_result", "get_issue"} <= _curie_tools(session.options)
    assert {"Task", "Agent"} <= set(session.options.disallowed_tools)
    assert session.options.max_budget_usd == ALLOWANCE


def test_an_implementation_contract_boots_exactly_as_before(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    api = _Api([(200, IMPLEMENTATION)])
    session = _boot_session(monkeypatch, tmp_path, api)
    assert isinstance(session, _ModelSession)
    assert api.contract_calls == [({}, CAPABILITY)]
    tools = _curie_tools(session.options)
    assert "get_issue" in tools
    assert "submit_tracker_result" not in tools
    assert not {"Task", "Agent"} & set(session.options.disallowed_tools)
    assert session.options.max_budget_usd == 1.0


def test_no_capability_makes_no_contract_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    api = _Api([(200, TRACKER)])
    session = _boot_session(monkeypatch, tmp_path, api, issue_read_path=None)
    assert isinstance(session, _ModelSession)
    assert api.contract_calls == []
    assert "submit_tracker_result" not in _curie_tools(session.options)
    assert not {"Task", "Agent"} & set(session.options.disallowed_tools)


def test_the_fake_model_without_a_capability_stays_offline(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Only the route without its capability: resolve_issue_read answers None,
    # so the fake makes no contract call at all.
    api = _Api([(200, TRACKER)])

    async def go() -> Any:
        async with TestServer(api.app()) as server:
            monkeypatch.setenv(ISSUE_READ_URL_ENV, str(server.make_url(ISSUE_READ_PATH)))
            runner = build_runner(_config(tmp_path), fake_model=True, mcp_capability=PROBE)
            return await initialized_session(runner)

    session = anyio.run(go)
    assert isinstance(session, _FakeSession)
    assert api.contract_calls == []
    assert not {"Task", "Agent"} & set(session._disallowed_tools)  # noqa: SLF001


@pytest.mark.parametrize(("contract", "denied"), [(TRACKER, True), (IMPLEMENTATION, False)])
def test_the_fake_model_session_denies_subagents_only_for_tracker_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, contract: dict[str, Any], denied: bool
) -> None:
    api = _Api([(200, contract)])
    session = _boot_session(monkeypatch, tmp_path, api, fake_model=True)
    assert isinstance(session, _FakeSession)
    assert api.contract_calls == [({}, CAPABILITY)]
    disallowed = set(session._disallowed_tools)  # noqa: SLF001 - the fake's catalogue
    assert ({"Task", "Agent"} <= disallowed) is denied
    assert bool({"Task", "Agent"} & disallowed) is denied


@pytest.mark.parametrize(
    ("api", "path"),
    [
        pytest.param(_Api([(200, TRACKER)]), "/work-items/issue-reader", id="bad-suffix"),
        pytest.param(_Api([(200, TRACKER)]), "/issue-read", id="no-work-items"),
        pytest.param(_Api([(200, TRACKER)], drops=99), ISSUE_READ_PATH, id="transport"),
        pytest.param(_Api([(500, {"code": "boom"})]), ISSUE_READ_PATH, id="500"),
        pytest.param(_Api([(200, b"not json")]), ISSUE_READ_PATH, id="non-json"),
        pytest.param(_Api([(200, ["tracker_result"])]), ISSUE_READ_PATH, id="non-object"),
        pytest.param(
            _Api([(200, {**TRACKER, "completion_contract": "review"})]),
            ISSUE_READ_PATH,
            id="unknown-contract",
        ),
        pytest.param(
            _Api([(200, {k: v for k, v in TRACKER.items() if k != "session_usd_allowance"})]),
            ISSUE_READ_PATH,
            id="missing-allowance",
        ),
        pytest.param(
            _Api([(200, {**TRACKER, "session_usd_allowance": 0})]),
            ISSUE_READ_PATH,
            id="zero-allowance",
        ),
        pytest.param(
            _Api([(409, {"detail": {"code": "session_already_started"}})]),
            ISSUE_READ_PATH,
            id="session-already-started",
        ),
    ],
)
@pytest.mark.parametrize("fake_model", [False, True])
def test_every_contract_failure_blocks_the_model(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    api: _Api,
    path: str,
    fake_model: bool,
) -> None:
    api.contract_calls = []
    session = _boot_session(monkeypatch, tmp_path, api, fake_model=fake_model, issue_read_path=path)
    assert isinstance(session, PreflightBlockedSession)
    assert _ModelSession.started == []
    assert _FakeSession.started == []


def test_a_blocked_session_answers_offline_without_the_capability(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    session = _boot_session(monkeypatch, tmp_path, _Api([(500, {})]))
    assert isinstance(session, PreflightBlockedSession)

    async def turn() -> list[Any]:
        await session.query("go")
        return [message async for message in session.receive_turn()]

    rendered = repr(anyio.run(turn))
    assert CAPABILITY not in rendered
    assert "127.0.0.1" not in rendered


# --- One model session per tracker result request --------------------------------


def _runner_against(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, api: _Api, scenario: Any
) -> None:
    async def go() -> None:
        async with TestServer(api.app()) as server:
            monkeypatch.setenv(ISSUE_READ_URL_ENV, str(server.make_url(ISSUE_READ_PATH)))
            monkeypatch.setenv(ISSUE_READ_TOKEN_ENV, CAPABILITY)
            runner = build_runner(_config(tmp_path), mcp_capability=PROBE)
            try:
                await runner.start()
                await scenario(runner)
            finally:
                await runner.close()

    anyio.run(go)


@pytest.mark.parametrize(("contract", "sessions"), [(TRACKER, 1), (IMPLEMENTATION, 2)])
def test_reset_starts_no_second_model_session_for_tracker_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, contract: dict[str, Any], sessions: int
) -> None:
    async def scenario(runner: Any) -> None:
        await runner.reset()

    _runner_against(monkeypatch, tmp_path, _Api([(200, contract)]), scenario)
    assert len(_ModelSession.started) == sessions


@pytest.mark.parametrize(("contract", "sessions"), [(TRACKER, 1), (IMPLEMENTATION, 2)])
def test_session_replacement_starts_no_second_model_session_for_tracker_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, contract: dict[str, Any], sessions: int
) -> None:
    async def scenario(runner: Any) -> None:
        await runner._replace_session()  # noqa: SLF001 - the replacement path is the subject

    _runner_against(monkeypatch, tmp_path, _Api([(200, contract)]), scenario)
    assert len(_ModelSession.started) == sessions


def test_a_second_process_for_the_request_starts_no_model_session(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # The API reserves the one session at the first contract call; a restarted
    # runner process for the same request is refused and starts nothing.
    api = _Api([(200, TRACKER), (409, {"detail": {"code": "session_already_started"}})])

    async def go() -> tuple[Any, Any]:
        async with TestServer(api.app()) as server:
            monkeypatch.setenv(ISSUE_READ_URL_ENV, str(server.make_url(ISSUE_READ_PATH)))
            monkeypatch.setenv(ISSUE_READ_TOKEN_ENV, CAPABILITY)
            first = await initialized_session(build_runner(_config(tmp_path), mcp_capability=PROBE))
            second = await initialized_session(
                build_runner(_config(tmp_path), mcp_capability=PROBE)
            )
            return first, second

    first, second = anyio.run(go)
    assert isinstance(first, _ModelSession)
    assert isinstance(second, PreflightBlockedSession)
    assert len(_ModelSession.started) == 1
    assert len(api.contract_calls) == 2


# --- The contract call ------------------------------------------------------------


def _fetch(api: _Api, **kwargs: Any) -> CompletionContract:
    async def go() -> CompletionContract:
        async with TestServer(api.app()) as server:
            return await fetch_completion_contract(
                str(server.make_url(CONTRACT_PATH)), CAPABILITY, **kwargs
            )

    return anyio.run(go)


def _refusal(api: _Api, **kwargs: Any) -> ContractUnavailable:
    with pytest.raises(ContractUnavailable) as caught:
        _fetch(api, **kwargs)
    assert CAPABILITY not in str(caught.value)
    assert caught.value.__cause__ is None
    return caught.value


def test_sibling_urls_replace_only_the_last_segment() -> None:
    urls = sibling_urls("https://api.example:8443/v1/runner/work-items/issue-read")
    assert urls.completion_contract == (
        "https://api.example:8443/v1/runner/work-items/completion-contract"
    )
    assert urls.tracker_result == "https://api.example:8443/v1/runner/work-items/tracker-result"


@pytest.mark.parametrize(
    "url",
    [
        "http://api.example/work-items/issue-read/",
        "http://api.example/work-items/issue-reads",
        "http://api.example/items/issue-read",
        "http://api.example/work-items/issue-read?x=1",
        "http://api.example/work-items/issue-read#frag",
        "http://user:pw@api.example/work-items/issue-read",
        "ftp://api.example/work-items/issue-read",
        "/work-items/issue-read",
        "",
    ],
)
def test_any_other_issue_read_url_is_a_contract_failure(url: str) -> None:
    with pytest.raises(ContractUnavailable) as caught:
        sibling_urls(url)
    assert caught.value.reason == REASON_ROUTE_INVALID


def test_the_contract_call_presents_the_capability_with_an_empty_body() -> None:
    api = _Api([(200, TRACKER)])
    contract = _fetch(api)
    assert api.contract_calls == [({}, CAPABILITY)]
    assert contract.tracker_result
    assert contract.result_kinds == ("findings", "ticket")
    assert contract.session_usd_allowance == ALLOWANCE


def test_an_implementation_answer_is_the_implementation_contract() -> None:
    contract = _fetch(_Api([(200, IMPLEMENTATION)]))
    assert not contract.tracker_result
    assert contract.session_usd_allowance is None


def test_transport_failures_retry_with_the_injected_delay_then_succeed() -> None:
    waits: list[float] = []

    async def delay(seconds: float) -> None:
        waits.append(seconds)

    api = _Api([(200, TRACKER)], drops=CONTRACT_ATTEMPTS - 1)
    assert _fetch(api, delay=delay).tracker_result
    assert len(api.contract_calls) == CONTRACT_ATTEMPTS
    assert len(waits) == CONTRACT_ATTEMPTS - 1


def test_transport_failure_after_the_bounded_attempts_is_refused() -> None:
    waits: list[float] = []

    async def delay(seconds: float) -> None:
        waits.append(seconds)

    api = _Api([(200, TRACKER)], drops=99)
    assert _refusal(api, delay=delay).reason == REASON_TRANSPORT_FAILED
    assert len(api.contract_calls) == CONTRACT_ATTEMPTS
    assert len(waits) == CONTRACT_ATTEMPTS - 1


@pytest.mark.parametrize(
    ("answer", "reason"),
    [
        ((403, {"code": "forbidden"}), REASON_STATUS_REFUSED),
        ((404, {"detail": "Not Found"}), REASON_STATUS_REFUSED),
        ((302, {}), REASON_STATUS_REFUSED),
        ((409, {"detail": {"code": "invalid_context"}}), REASON_STATUS_REFUSED),
        ((409, {"code": "session_already_started"}), REASON_SESSION_ALREADY_STARTED),
        ((409, {"detail": {"code": "session_already_started"}}), REASON_SESSION_ALREADY_STARTED),
        ((200, b"{not json"), REASON_BODY_INVALID),
        ((200, b"x" * 70_000), REASON_BODY_INVALID),
        ((200, ["implementation"]), REASON_BODY_INVALID),
        ((200, {"completion_contract": "review"}), REASON_CONTRACT_UNKNOWN),
        ((200, {}), REASON_CONTRACT_UNKNOWN),
        ((200, {**TRACKER, "result_kinds": []}), REASON_RESULT_KINDS_INVALID),
        ((200, {**TRACKER, "result_kinds": ["pull_request"]}), REASON_RESULT_KINDS_INVALID),
        ((200, {**TRACKER, "result_kinds": ["ticket", "ticket"]}), REASON_RESULT_KINDS_INVALID),
        ((200, {**TRACKER, "result_kinds": "ticket"}), REASON_RESULT_KINDS_INVALID),
        ((200, {**TRACKER, "session_usd_allowance": None}), REASON_ALLOWANCE_INVALID),
        ((200, {**TRACKER, "session_usd_allowance": -1}), REASON_ALLOWANCE_INVALID),
        ((200, {**TRACKER, "session_usd_allowance": 0.0}), REASON_ALLOWANCE_INVALID),
        ((200, {**TRACKER, "session_usd_allowance": "5"}), REASON_ALLOWANCE_INVALID),
        ((200, {**TRACKER, "session_usd_allowance": True}), REASON_ALLOWANCE_INVALID),
    ],
)
def test_every_unusable_answer_is_refused_and_never_retried(
    answer: tuple[int, Any], reason: str
) -> None:
    api = _Api([answer])
    assert _refusal(api, delay=_no_delay).reason == reason
    assert len(api.contract_calls) == 1


def test_a_transient_status_then_success_returns_the_contract() -> None:
    waits: list[float] = []

    async def delay(seconds: float) -> None:
        waits.append(seconds)

    api = _Api([(500, {"code": "boom"}), (200, IMPLEMENTATION)])
    contract = _fetch(api, delay=delay)
    assert not contract.tracker_result
    assert len(api.contract_calls) == 2
    assert len(waits) == 1


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_a_transient_status_on_every_attempt_is_refused_as_transient(status: int) -> None:
    api = _Api([(status, {})])
    assert _refusal(api, delay=_no_delay).reason == REASON_STATUS_TRANSIENT
    assert len(api.contract_calls) == CONTRACT_ATTEMPTS


def test_a_forbidden_answer_is_final_after_one_call() -> None:
    api = _Api([(403, {}), (200, IMPLEMENTATION)])
    assert _refusal(api, delay=_no_delay).reason == REASON_STATUS_REFUSED
    assert len(api.contract_calls) == 1


def test_session_already_started_is_final_after_one_call() -> None:
    api = _Api([(409, {"detail": {"code": "session_already_started"}}), (200, IMPLEMENTATION)])
    assert _refusal(api, delay=_no_delay).reason == REASON_SESSION_ALREADY_STARTED
    assert len(api.contract_calls) == 1


def test_the_deadline_fires_when_the_server_stalls() -> None:
    api = _Api([(200, TRACKER)], stall=True)
    assert _refusal(api, delay=_no_delay, deadline_s=0.2).reason == REASON_DEADLINE_EXCEEDED


def test_the_internal_deadline_is_refused() -> None:
    assert _refusal(_Api([(200, TRACKER)]), deadline_s=0).reason == REASON_DEADLINE_EXCEEDED


def test_the_gate_denies_on_its_own_exception() -> None:
    async def broken(_seconds: float) -> None:
        raise RuntimeError("delay broke")

    api = _Api([(200, TRACKER)], drops=1)
    assert _refusal(api, delay=broken).reason == REASON_INTERNAL_ERROR


def test_session_limits_change_only_a_tracker_result_session() -> None:
    configured = ("Write",)
    implementation = session_limits(
        CompletionContract("implementation"), disallowed_tools=configured, max_budget_usd=2.0
    )
    assert implementation.disallowed_tools == configured
    assert implementation.max_budget_usd == 2.0
    tracker = session_limits(
        CompletionContract("tracker_result", ("findings",), 0.5),
        disallowed_tools=(*configured, "Task"),
        max_budget_usd=2.0,
    )
    assert tracker.disallowed_tools == ("Write", "Task", "Agent")
    assert set(TRACKER_RESULT_DISALLOWED_TOOLS) <= set(tracker.disallowed_tools)
    assert tracker.max_budget_usd == 0.5


# --- The submission tool ------------------------------------------------------------

TICKET = {
    "kind": "ticket",
    "title": "Crash on empty config",
    "body": "The service crashes when the config file is empty.",
    "reproduction": {
        "command": "make repro",
        "observed": "KeyError: 'name'",
        "revision": "a" * 40,
    },
}
FINDINGS = {
    "kind": "findings",
    "reason": "not_reproduced",
    "tried": "Ran the reported command at the reported revision.",
    "missing": "A config that triggers the crash.",
}


def _submit(api: _Api, args: dict[str, Any]) -> dict[str, Any]:
    async def go() -> dict[str, Any]:
        async with TestServer(api.app()) as server:
            tool = build_tracker_result_tool(str(server.make_url(RESULT_PATH)), CAPABILITY)
            return await tool.handler(args)

    return anyio.run(go)


def test_the_curie_server_carries_the_tool_only_when_given() -> None:
    async def names(server: Any) -> set[str]:
        entry = server["instance"].get_request_handler("tools/list")
        assert entry is not None
        result = await entry.handler(None, mcp_types.PaginatedRequestParams())
        return {tool["name"] for tool in result.model_dump()["tools"]}

    tool = build_tracker_result_tool("http://api/work-items/tracker-result", CAPABILITY)
    assert TRACKER_RESULT_TOOL_NAME == "mcp__curie__submit_tracker_result"
    with_tool = anyio.run(names, build_approval_server(tracker_result_tool=tool))
    without = anyio.run(names, build_approval_server())
    assert with_tool == without | {"submit_tracker_result"}


def test_the_tool_is_platform_owned_and_never_idempotent() -> None:
    assert is_platform_owned_tool(TRACKER_RESULT_TOOL_NAME, state_server_mounted=False)
    assert TRACKER_RESULT_TOOL_NAME not in PLATFORM_IDEMPOTENT_TOOLS


@pytest.mark.parametrize("proposal", [TICKET, FINDINGS])
def test_a_submission_posts_the_proposal_with_the_capability(proposal: dict[str, Any]) -> None:
    api = _Api(
        result=(
            200,
            {
                "result_kind": proposal["kind"],
                "state": "delivered",
                "result_comment_url": "https://tracker.example/issues/7#comment-1",
                "ticket_url": (
                    "https://tracker.example/issues/8" if proposal["kind"] == "ticket" else None
                ),
            },
        )
    )
    result = _submit(api, proposal)
    assert result.get("is_error") is not True, _text(result)
    assert api.result_calls == [({"proposal": proposal}, CAPABILITY)]
    text = _text(result)
    assert proposal["kind"] in text
    assert "https://tracker.example/issues/7#comment-1" in text
    assert ("https://tracker.example/issues/8" in text) is (proposal["kind"] == "ticket")
    assert CAPABILITY not in text


def test_an_owed_delivery_is_accepted() -> None:
    api = _Api(
        result=(
            200,
            {
                "result_kind": "findings",
                "state": "owed",
                "result_comment_url": None,
                "ticket_url": None,
            },
        )
    )
    result = _submit(api, FINDINGS)
    assert result.get("is_error") is not True
    assert "findings" in _text(result)


def test_a_failed_delivery_is_a_tool_error() -> None:
    api = _Api(
        result=(
            200,
            {
                "result_kind": "ticket",
                "state": "failed",
                "result_comment_url": None,
                "ticket_url": None,
            },
        )
    )
    assert _submit(api, TICKET).get("is_error") is True


def test_link_values_that_are_not_plain_web_urls_are_dropped() -> None:
    api = _Api(
        result=(
            200,
            {
                "result_kind": "ticket",
                "state": "delivered",
                "result_comment_url": "javascript:alert(1)",
                "ticket_url": "https://user:secret@tracker.example/issues/8",
            },
        )
    )
    text = _text(_submit(api, TICKET))
    assert "javascript" not in text
    assert "secret" not in text


@pytest.mark.parametrize(
    "code", ["result_already_submitted", "source_fingerprint_changed", "not_tracker_result"]
)
@pytest.mark.parametrize("wrapped", [False, True])
def test_a_409_refusal_renders_its_code(code: str, wrapped: bool) -> None:
    body = {"code": code, "message": "refused"}
    api = _Api(result=(409, {"detail": body} if wrapped else body))
    result = _submit(api, TICKET)
    assert result.get("is_error") is True
    assert code in _text(result)
    assert "refused" in _text(result)


def test_a_409_code_that_is_not_a_plain_code_is_not_echoed() -> None:
    api = _Api(result=(409, {"code": f"echo {CAPABILITY}"}))
    result = _submit(api, TICKET)
    assert result.get("is_error") is True
    assert CAPABILITY not in _text(result)


def test_a_422_renders_field_and_code_pairs_only() -> None:
    api = _Api(
        result=(
            422,
            {
                "detail": {
                    "code": "proposal_invalid",
                    "issues": [
                        {"field": "reproduction.revision", "code": "revision"},
                        {"field": "", "code": "unknown_field"},
                        {"field": "title", "code": "length", "message": "LEAKED TEXT"},
                        {"field": "body; DROP", "code": "type"},
                    ],
                }
            },
        )
    )
    result = _submit(api, TICKET)
    text = _text(result)
    assert result.get("is_error") is True
    assert "reproduction.revision: revision" in text
    assert "unknown_field" in text
    assert "title: length" in text
    assert "LEAKED TEXT" not in text
    assert "DROP" not in text


def test_a_503_and_a_transport_failure_ask_for_a_retry_without_diagnostics() -> None:
    result = _submit(_Api(result=(503, {"code": "tracker_unavailable"})), TICKET)
    assert result.get("is_error") is True
    tool = build_tracker_result_tool("http://127.0.0.1:9" + RESULT_PATH, CAPABILITY)
    unreachable = anyio.run(tool.handler, TICKET)
    assert unreachable.get("is_error") is True
    assert "127.0.0.1" not in _text(unreachable)
    assert CAPABILITY not in _text(unreachable)


def test_an_unreadable_answer_is_a_tool_error() -> None:
    assert _submit(_Api(result=(200, b"<html>")), TICKET).get("is_error") is True
    assert _submit(_Api(result=(200, {"state": "elsewhere"})), TICKET).get("is_error") is True
