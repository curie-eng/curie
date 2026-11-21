"""The completion contract (ADR 0277, #5586).

A factory execution learns from the API, over the issue read capability the
worker already injects, how it must end: an implementation, or a publication
recovery that runs no model. Every way the contract can fail, an answer naming
a contract the runner does not know included, blocks the model.

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
from curie_runner import RunnerConfig, completion_contract
from curie_runner import __main__ as boot
from curie_runner.__main__ import build_runner
from curie_runner.approval import APPROVAL_SERVER_NAME
from curie_runner.completion_contract import (
    CONTRACT_ATTEMPTS,
    PUBLICATION_RECOVERY_EXPLANATION,
    REASON_BODY_INVALID,
    REASON_CONTRACT_UNKNOWN,
    REASON_DEADLINE_EXCEEDED,
    REASON_INTERNAL_ERROR,
    REASON_ROUTE_INVALID,
    REASON_STATUS_REFUSED,
    REASON_STATUS_TRANSIENT,
    REASON_TRANSPORT_FAILED,
    CompletionContract,
    ContractUnavailable,
    fetch_completion_contract,
    sibling_urls,
)
from curie_runner.fake import FakeModelSession
from curie_runner.harness.claude.preflight_blocked import PreflightBlockedSession
from curie_runner.issue_read import CAPABILITY_HEADER, ISSUE_READ_TOKEN_ENV, ISSUE_READ_URL_ENV
from curie_runner.mcp_tool_capability import McpToolCapabilityProbe
from curie_runner.requester_context import ExecutionBinding
from runner_boot_session import initialized_session

CAPABILITY = "wir.example-capability.signature"
CONTRACT_PATH = "/work-items/completion-contract"
ISSUE_READ_PATH = "/work-items/issue-read"
BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'

# The negative control of this file: the triage contract the API stopped
# answering. It is written once, here, and referenced by name below.
RETIRED_TRIAGE_CONTRACT = "tracker_result"

TENANT = "00000000-0000-4000-8000-000000000001"
WORK_ITEM = "00000000-0000-4000-8000-000000000002"
REQUEST = "00000000-0000-4000-8000-000000000003"
EXECUTION = {
    "tenant_id": TENANT,
    "work_item_id": WORK_ITEM,
    "execution_request_id": REQUEST,
    "execution_fence": 1,
}
# The answer keys the API sends: the contract, the execution binding and the
# rule proposal offer, and nothing else.
IMPLEMENTATION = {
    "completion_contract": "implementation",
    "execution": None,
    "rule_proposal": False,
}
RECOVERY = {
    "completion_contract": "publication_recovery",
    "execution": None,
    "rule_proposal": False,
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
    """A local API answering the contract route, recording each call.

    ``drops`` closes the connection on that many first contract calls (a real
    transport failure). ``contract`` is a list of answers, one per contract
    call; the last one repeats.
    """

    def __init__(
        self,
        contract: list[tuple[int, Any]] | None = None,
        *,
        drops: int = 0,
        stall: bool = False,
    ) -> None:
        self.contract = contract or [(200, IMPLEMENTATION)]
        self.drops = drops
        self.stall = stall
        self.contract_calls: list[tuple[Any, str | None]] = []

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

        app.router.add_post(CONTRACT_PATH, contract)
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
        functools.partial(completion_contract.bind_completion_contract, delay=_no_delay),
    )
    monkeypatch.delenv(ISSUE_READ_URL_ENV, raising=False)
    monkeypatch.delenv(ISSUE_READ_TOKEN_ENV, raising=False)
    _ModelSession.started = []
    _FakeSession.started = []


def _config(tmp_path: Path) -> RunnerConfig:
    plugin = tmp_path / "bundle"
    (plugin / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    (plugin / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "contract", "version": "0.1.0", "description": "t"}), encoding="utf-8"
    )
    return RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(plugin),
            "CURIE_SESSION_ID": "s-contract",
            "CURIE_SANDBOX_ID": "b-contract",
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


def test_an_implementation_contract_boots_a_model_session_with_the_configured_limits(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    api = _Api([(200, IMPLEMENTATION)])
    session = _boot_session(monkeypatch, tmp_path, api)
    assert isinstance(session, _ModelSession)
    assert api.contract_calls == [({}, CAPABILITY)]
    assert "get_issue" in _curie_tools(session.options)
    assert not {"Task", "Agent"} & set(session.options.disallowed_tools)
    assert session.options.max_budget_usd == 1.0


def test_no_capability_makes_no_contract_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    api = _Api([(200, IMPLEMENTATION)])
    session = _boot_session(monkeypatch, tmp_path, api, issue_read_path=None)
    assert isinstance(session, _ModelSession)
    assert api.contract_calls == []
    assert not {"Task", "Agent"} & set(session.options.disallowed_tools)


def test_the_fake_model_without_a_capability_stays_offline(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Only the route without its capability: resolve_issue_read answers None,
    # so the fake makes no contract call at all.
    api = _Api([(200, IMPLEMENTATION)])

    async def go() -> Any:
        async with TestServer(api.app()) as server:
            monkeypatch.setenv(ISSUE_READ_URL_ENV, str(server.make_url(ISSUE_READ_PATH)))
            runner = build_runner(_config(tmp_path), fake_model=True, mcp_capability=PROBE)
            return await initialized_session(runner)

    session = anyio.run(go)
    assert isinstance(session, _FakeSession)
    assert api.contract_calls == []
    assert not {"Task", "Agent"} & set(session._disallowed_tools)  # noqa: SLF001


def test_the_fake_model_session_keeps_the_configured_tool_catalogue(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    api = _Api([(200, IMPLEMENTATION)])
    session = _boot_session(monkeypatch, tmp_path, api, fake_model=True)
    assert isinstance(session, _FakeSession)
    assert api.contract_calls == [({}, CAPABILITY)]
    assert not {"Task", "Agent"} & set(session._disallowed_tools)  # noqa: SLF001 - the fake's catalogue


@pytest.mark.parametrize(
    ("api", "path"),
    [
        pytest.param(_Api([(200, IMPLEMENTATION)]), "/work-items/issue-reader", id="bad-suffix"),
        pytest.param(_Api([(200, IMPLEMENTATION)]), "/issue-read", id="no-work-items"),
        pytest.param(_Api([(200, IMPLEMENTATION)], drops=99), ISSUE_READ_PATH, id="transport"),
        pytest.param(_Api([(500, {"code": "boom"})]), ISSUE_READ_PATH, id="500"),
        pytest.param(_Api([(200, b"not json")]), ISSUE_READ_PATH, id="non-json"),
        pytest.param(_Api([(200, ["implementation"])]), ISSUE_READ_PATH, id="non-object"),
        pytest.param(
            _Api([(200, {**IMPLEMENTATION, "completion_contract": "review"})]),
            ISSUE_READ_PATH,
            id="unknown-contract",
        ),
        pytest.param(
            _Api([(200, {**IMPLEMENTATION, "completion_contract": RETIRED_TRIAGE_CONTRACT})]),
            ISSUE_READ_PATH,
            id="retired-triage-contract",
        ),
        pytest.param(
            _Api([(409, {"detail": {"code": "invalid_context"}})]),
            ISSUE_READ_PATH,
            id="refused-409",
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


# --- A publication recovery process runs no model (#5429) ------------------------


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


@pytest.mark.parametrize("fake_model", [False, True])
def test_a_publication_recovery_contract_constructs_no_model_session(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fake_model: bool
) -> None:
    api = _Api([(200, RECOVERY)])
    session = _boot_session(monkeypatch, tmp_path, api, fake_model=fake_model)
    assert isinstance(session, PreflightBlockedSession)
    assert api.contract_calls == [({}, CAPABILITY)]
    assert _ModelSession.started == []
    assert _FakeSession.started == []

    async def turn() -> list[Any]:
        await session.connect()
        await session.query("publish the candidate")
        return [message async for message in session.receive_turn()]

    messages = anyio.run(turn)
    assert [getattr(message, "result", None) for message in messages][-1] == (
        PUBLICATION_RECOVERY_EXPLANATION
    )
    assert _ModelSession.started == []


@pytest.mark.parametrize(("contract", "sessions"), [(RECOVERY, 0), (IMPLEMENTATION, 3)])
def test_reset_and_replacement_construct_no_model_session_for_publication_recovery(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, contract: dict[str, Any], sessions: int
) -> None:
    async def scenario(runner: Any) -> None:
        await runner.reset()
        await runner._replace_session()  # noqa: SLF001 - the replacement path is the subject

    _runner_against(monkeypatch, tmp_path, _Api([(200, contract)]), scenario)
    # The implementation control: boot, reset and replacement each construct one.
    assert len(_ModelSession.started) == sessions


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
    base = "https://api.example:8443/v1/runner/work-items/"
    assert urls.completion_contract == base + "completion-contract"
    assert urls.questions == base + "questions"
    assert urls.rule_proposal == base + "rule-proposal"


def test_the_sibling_routes_are_exactly_the_surviving_set() -> None:
    urls = sibling_urls("https://api.example/work-items/issue-read")
    assert set(vars(urls)) == {"completion_contract", "questions", "rule_proposal"}


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
    api = _Api([(200, IMPLEMENTATION)])
    contract = _fetch(api)
    assert api.contract_calls == [({}, CAPABILITY)]
    assert contract.kind == "implementation"


def test_an_implementation_answer_carries_its_execution_binding_and_rule_proposal() -> None:
    answer = {**IMPLEMENTATION, "execution": EXECUTION, "rule_proposal": True}
    contract = _fetch(_Api([(200, answer)]))
    assert contract.kind == "implementation"
    assert contract.execution == ExecutionBinding(
        tenant_id=TENANT,
        work_item_id=WORK_ITEM,
        execution_request_id=REQUEST,
        execution_fence=1,
    )
    assert contract.rule_proposal is True
    assert not contract.publication_recovery


def test_an_implementation_answer_without_either_is_the_bare_implementation_contract() -> None:
    contract = _fetch(_Api([(200, {"completion_contract": "implementation"})]))
    assert contract == completion_contract.IMPLEMENTATION
    assert contract.execution is None
    assert contract.rule_proposal is False


@pytest.mark.parametrize(
    "execution",
    [
        "not-an-object",
        {**EXECUTION, "tenant_id": "not-a-uuid"},
        {**EXECUTION, "execution_fence": 0},
        {**EXECUTION, "execution_fence": True},
        {key: value for key, value in EXECUTION.items() if key != "work_item_id"},
    ],
)
def test_a_malformed_execution_binding_is_refused(execution: Any) -> None:
    answer = {**IMPLEMENTATION, "execution": execution}
    assert _refusal(_Api([(200, answer)]), delay=_no_delay).reason == REASON_BODY_INVALID


def test_transport_failures_retry_with_the_injected_delay_then_succeed() -> None:
    waits: list[float] = []

    async def delay(seconds: float) -> None:
        waits.append(seconds)

    api = _Api([(200, IMPLEMENTATION)], drops=CONTRACT_ATTEMPTS - 1)
    assert _fetch(api, delay=delay).kind == "implementation"
    assert len(api.contract_calls) == CONTRACT_ATTEMPTS
    assert len(waits) == CONTRACT_ATTEMPTS - 1


def test_transport_failure_after_the_bounded_attempts_is_refused() -> None:
    waits: list[float] = []

    async def delay(seconds: float) -> None:
        waits.append(seconds)

    api = _Api([(200, IMPLEMENTATION)], drops=99)
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
        ((409, {"code": "invalid_context"}), REASON_STATUS_REFUSED),
        ((409, {}), REASON_STATUS_REFUSED),
        ((200, b"{not json"), REASON_BODY_INVALID),
        ((200, b"x" * 70_000), REASON_BODY_INVALID),
        ((200, ["implementation"]), REASON_BODY_INVALID),
        ((200, {"completion_contract": "review"}), REASON_CONTRACT_UNKNOWN),
        ((200, {}), REASON_CONTRACT_UNKNOWN),
    ],
)
def test_every_unusable_answer_is_refused_and_never_retried(
    answer: tuple[int, Any], reason: str
) -> None:
    api = _Api([answer])
    assert _refusal(api, delay=_no_delay).reason == reason
    assert len(api.contract_calls) == 1


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param({}, id="bare"),
        pytest.param({"execution": EXECUTION}, id="with-an-execution-binding"),
        # The execution binding is malformed: the contract is still unknown,
        # never a body error, so no later parse step can map the value away.
        pytest.param({"execution": "not-an-object"}, id="with-a-malformed-binding"),
        pytest.param({"rule_proposal": True}, id="offering-a-rule-proposal"),
    ],
)
def test_the_retired_triage_contract_is_unknown(extra: dict[str, Any]) -> None:
    answer = {"completion_contract": RETIRED_TRIAGE_CONTRACT, **extra}
    api = _Api([(200, answer)])
    assert _refusal(api, delay=_no_delay).reason == REASON_CONTRACT_UNKNOWN
    assert len(api.contract_calls) == 1


def test_the_retired_triage_contract_starts_no_model_session_and_is_never_an_implementation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # A runner booted against the former answer, with everything an
    # implementation answer carries, still starts no model and mounts nothing.
    answer = {**IMPLEMENTATION, "completion_contract": RETIRED_TRIAGE_CONTRACT}
    session = _boot_session(monkeypatch, tmp_path, _Api([(200, answer)]))
    assert isinstance(session, PreflightBlockedSession)
    assert _ModelSession.started == []
    assert _FakeSession.started == []


def test_a_publication_recovery_answer_is_the_recovery_contract() -> None:
    contract = _fetch(_Api([(200, RECOVERY)]))
    assert contract.publication_recovery
    assert contract.execution is None
    # The bare kind, with nothing else, is the same contract.
    assert _fetch(_Api([(200, {"completion_contract": "publication_recovery"})])) == contract


def test_a_publication_recovery_answer_carrying_an_execution_binding_is_refused() -> None:
    answer = {**RECOVERY, "execution": EXECUTION}
    assert _refusal(_Api([(200, answer)]), delay=_no_delay).reason == REASON_BODY_INVALID


# --- The rule proposal offer (ADR 0287, #5796) -----------------------------------


@pytest.mark.parametrize(
    ("offer", "offered"),
    [
        pytest.param({"rule_proposal": True}, True, id="true"),
        pytest.param({"rule_proposal": False}, False, id="false"),
        pytest.param({}, False, id="absent"),
        pytest.param({"rule_proposal": None}, False, id="null"),
        pytest.param({"rule_proposal": "true"}, False, id="string"),
        pytest.param({"rule_proposal": 1}, False, id="one"),
        pytest.param({"rule_proposal": {"on": True}}, False, id="object"),
    ],
)
def test_only_json_true_offers_a_rule_proposal_and_nothing_else_blocks_the_run(
    offer: dict[str, Any], offered: bool
) -> None:
    # Absent reads false, so a runner newer than its API mounts nothing; a
    # malformed value reads false too and never fails the contract.
    answer = {key: value for key, value in IMPLEMENTATION.items() if key != "rule_proposal"}
    contract = _fetch(_Api([(200, {**answer, **offer})]))
    assert contract.rule_proposal is offered
    assert contract.kind == "implementation"


def test_a_publication_recovery_answer_with_rule_proposal_false_parses() -> None:
    # The API always sends the field now; for this kind it is false.
    assert _fetch(_Api([(200, {**RECOVERY, "rule_proposal": False})])).publication_recovery


@pytest.mark.parametrize("value", [True, "false", 0])
def test_a_publication_recovery_answer_offering_a_rule_proposal_is_refused(value: Any) -> None:
    answer = {**RECOVERY, "rule_proposal": value}
    assert _refusal(_Api([(200, answer)]), delay=_no_delay).reason == REASON_BODY_INVALID


def test_a_transient_status_then_success_returns_the_contract() -> None:
    waits: list[float] = []

    async def delay(seconds: float) -> None:
        waits.append(seconds)

    api = _Api([(500, {"code": "boom"}), (200, IMPLEMENTATION)])
    contract = _fetch(api, delay=delay)
    assert contract.kind == "implementation"
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


def test_the_deadline_fires_when_the_server_stalls() -> None:
    api = _Api([(200, IMPLEMENTATION)], stall=True)
    assert _refusal(api, delay=_no_delay, deadline_s=0.2).reason == REASON_DEADLINE_EXCEEDED


def test_the_internal_deadline_is_refused() -> None:
    assert _refusal(_Api([(200, IMPLEMENTATION)]), deadline_s=0).reason == REASON_DEADLINE_EXCEEDED


def test_the_gate_denies_on_its_own_exception() -> None:
    async def broken(_seconds: float) -> None:
        raise RuntimeError("delay broke")

    api = _Api([(200, IMPLEMENTATION)], drops=1)
    assert _refusal(api, delay=broken).reason == REASON_INTERNAL_ERROR
