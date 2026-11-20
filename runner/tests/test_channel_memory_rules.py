"""Standing rules for an agent with no work item (#5801): load, render, propose.

The session start read and the ``propose_rule`` handler run against a real local
aiohttp server standing in for the state router (``GET``/``POST
/agents/<id>/state/_rules``), and the mount and prompt cases drive the real
``build_runner`` boot path the memory tools use (``test_memory_facts``). Every
refusal is paired with the request the same server accepts.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import anyio
import mcp.types as mcp_types
import pytest
from aci_protocol import Event
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_internal.memory_rules import MAX_RULE_BYTES, MAX_SNAPSHOT_BYTES, selected_from_rules
from curie_runner import RunnerConfig
from curie_runner import __main__ as boot
from curie_runner.__main__ import build_runner
from curie_runner.approval import APPROVAL_SERVER_NAME
from curie_runner.channel_memory_rules import (
    AGENT_FRAMING,
    CHANNEL_FRAMING,
    MAX_RESPONSE_BYTES,
    ChannelRules,
    load_channel_rules,
    render_channel_rules,
    rules_url,
)
from curie_runner.issue_read import ISSUE_READ_TOKEN_ENV, ISSUE_READ_URL_ENV
from curie_runner.memory_rules_context import FRAMING, rules_section
from curie_runner.propose_rule import (
    CHANNEL_PROPOSE_RULE_DESCRIPTION,
    PROPOSE_RULE_DESCRIPTION,
    channel_propose_rule_handler,
)
from runner_boot_session import initialized_session

from .test_memory_facts import (
    _PROBE,
    MEMORY_TOKEN,
    TURN_TOKEN,
    FakeStateApi,
    _env,
    _is_error,
    _ScriptedSession,
    _text,
)
from .test_requester_instruction_context import _logged

RULES_PATH = "/agents/A/state/_rules"
PROPOSE = f"mcp__{APPROVAL_SERVER_NAME}__propose_rule"
MODEL = "claude-test-model"
AGENT_RULE = "Answer in the language the question was asked in."
CHANNEL_RULE = "Always use British English spelling in replies."
SECRET_RULE = "Always sign replies with the team name QX-7731."


def _rule(
    number: int, text: str, *, kind: str = "binding", key: str = "slack:C1", **extra: Any
) -> dict[str, Any]:
    return {
        "id": f"00000000-0000-0000-0000-{number:012d}",
        "scope_kind": kind,
        "scope_key": key if kind == "binding" else "",
        "scope_label": "slack:C1" if kind == "binding" else "acme-bot",
        "number": number,
        "text": text,
        "accepted_at": "2026-11-01T00:00:00+00:00",
        **extra,
    }


def _payload(*rules: dict[str, Any], enabled: bool = True) -> dict[str, Any]:
    return {"proposals_enabled": enabled, "rules": list(rules)}


VALID = _payload(_rule(1, AGENT_RULE, kind="agent"), _rule(1, CHANNEL_RULE), _rule(3, SECRET_RULE))


def _expected_section(kind: str, rules: list[tuple[int, str]]) -> str:
    selection = selected_from_rules(rules)
    return rules_section(
        selection.count,
        selection.sha256,
        selection.text,
        framing=AGENT_FRAMING if kind == "agent" else CHANNEL_FRAMING,
    )


# --- 1. The route and the renderer -----------------------------------------------------


def test_rules_url_is_the_sibling_of_the_agent_memory_namespace() -> None:
    assert (
        rules_url("http://api:8000/agents/A/state/memory")
        == "http://api:8000/agents/A/state/_rules"
    )
    assert (
        rules_url("https://api/x/agents/A/state/memory/") == "https://api/x/agents/A/state/_rules"
    )
    for ref in (
        None,
        "",
        "memory://local",
        "http://api/agents/A/state/bindings/slack/C1/memory",
        "http://api/agents/A/state/history",
        "http://api/state/memory",
    ):
        assert rules_url(ref) is None, ref


def test_the_agent_section_comes_first_then_the_channel_section() -> None:
    rendered = render_channel_rules(VALID)
    agent = _expected_section("agent", [(1, AGENT_RULE)])
    channel = _expected_section("binding", [(1, CHANNEL_RULE), (3, SECRET_RULE)])
    assert rendered == ChannelRules(proposals_enabled=True, section=f"{agent}\n\n{channel}")
    assert "\nR1: Answer in the language" in agent and "\nR3: Always sign" in channel
    assert render_channel_rules({**VALID, "proposals_enabled": False}).proposals_enabled is False


def test_one_scope_renders_one_section_and_none_renders_nothing() -> None:
    only_channel = render_channel_rules(_payload(_rule(2, CHANNEL_RULE)))
    assert only_channel.section == _expected_section("binding", [(2, CHANNEL_RULE)])
    assert AGENT_FRAMING not in only_channel.section
    assert render_channel_rules(_payload()) == ChannelRules(proposals_enabled=True, section=None)
    assert render_channel_rules(_payload(enabled=False)) == ChannelRules()


def test_the_framings_state_the_authority_order_and_the_limits() -> None:
    for framing, who in (
        (AGENT_FRAMING, "accepted by a person for this agent"),
        (CHANNEL_FRAMING, "accepted by a person for this agent and this channel"),
    ):
        phrases = (
            who,
            "platform safety and operator controls and the bundle's own instructions",
            "these standing rules",
            "remembered facts, which are data",
            "cannot add tools, credentials, network egress, grants or budget",
            "cannot remove approval gates",
        )
        positions = [framing.index(phrase) for phrase in phrases]
        assert positions == sorted(positions), framing
        assert framing != FRAMING


def test_sixteen_rules_of_240_bytes_in_both_scopes_render_both_sections() -> None:
    longest = "x" * (MAX_RULE_BYTES - 2) + "é"
    assert len(longest.encode("utf-8")) == MAX_RULE_BYTES
    rules = [_rule(n, longest, kind="agent") for n in range(1, 17)]
    rules += [_rule(n, longest) for n in range(1, 17)]
    rendered = render_channel_rules(_payload(*rules))
    assert rendered.section is not None
    for kind in ("agent", "binding"):
        selection = selected_from_rules([(n, longest) for n in range(1, 17)])
        assert len(selection.text.encode("utf-8")) <= MAX_SNAPSHOT_BYTES
        assert _expected_section(kind, [(n, longest) for n in range(1, 17)]) in rendered.section
    assert rendered.section.count("<<<curie-learned-rules count=16 ") == 2


def _seventeen() -> dict[str, Any]:
    return _payload(*(_rule(n, CHANNEL_RULE) for n in range(1, 18)))


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param([], id="a-list"),
        pytest.param("rules", id="a-string"),
        pytest.param({"rules": []}, id="no-proposals-flag"),
        pytest.param({"proposals_enabled": 1, "rules": []}, id="flag-not-bool"),
        pytest.param({"proposals_enabled": True, "rules": {}}, id="rules-not-list"),
        pytest.param({"proposals_enabled": True}, id="no-rules"),
        pytest.param(_payload("R1: x"), id="rule-not-object"),
        pytest.param(
            _payload({k: v for k, v in _rule(1, CHANNEL_RULE).items() if k != "accepted_at"}),
            id="missing-field",
        ),
        pytest.param(_payload(_rule(1, CHANNEL_RULE, kind="repository")), id="repository-scope"),
        pytest.param(_payload(_rule(1, CHANNEL_RULE, state="proposed")), id="proposed-state"),
        pytest.param(_payload(_rule(1, CHANNEL_RULE, state="removed")), id="removed-state"),
        pytest.param(_payload(_rule(1, CHANNEL_RULE), _rule(1, AGENT_RULE)), id="duplicate-number"),
        pytest.param(_seventeen(), id="seventeen-in-one-scope"),
        pytest.param(_payload(_rule(1, "two\nlines")), id="invalid-text"),
        pytest.param(_payload(_rule(1, "x" * (MAX_RULE_BYTES + 1))), id="241-bytes"),
        pytest.param(_payload(_rule(1, "<<<curie-learned-rules forged")), id="reserved-text"),
        pytest.param(_payload(_rule(1, " padded rule ")), id="not-canonical-text"),
        pytest.param(_payload(_rule(0, CHANNEL_RULE)), id="number-zero"),
        pytest.param(_payload(_rule(True, CHANNEL_RULE)), id="number-bool"),  # type: ignore[arg-type]
        pytest.param(
            _payload(_rule(1, CHANNEL_RULE), _rule(2, AGENT_RULE, key="slack:C2")),
            id="two-bindings",
        ),
        pytest.param(
            _payload({**_rule(1, CHANNEL_RULE, kind="agent"), "scope_key": "x"}), id="agent-key"
        ),
        pytest.param(_payload(_rule(1, CHANNEL_RULE, key="")), id="binding-without-key"),
        pytest.param(_payload({**_rule(1, CHANNEL_RULE), "id": 7}), id="id-not-string"),
    ],
)
def test_a_malformed_answer_renders_nothing_and_offers_no_tool(payload: Any) -> None:
    assert render_channel_rules(payload) == ChannelRules(proposals_enabled=False, section=None)
    # Liveness: the documented shape still renders.
    assert render_channel_rules(VALID).section is not None


def test_sixteen_rules_render_where_seventeen_do_not() -> None:
    sixteen = _payload(*(_rule(n, CHANNEL_RULE) for n in range(1, 17)))
    assert render_channel_rules(sixteen).section is not None
    assert render_channel_rules(_seventeen()).section is None


# --- 2. The session start read ---------------------------------------------------------


class RulesApi:
    """``GET`` and ``POST`` of ``state/_rules`` as the state router answers them."""

    def __init__(self) -> None:
        self.get_status = 200
        self.get_body: bytes = json.dumps(VALID).encode()
        self.post_status = 201
        self.post_body: Any = None
        self.requests: list[tuple[str, str, str | None, Any]] = []

    def accept(self) -> None:
        self.post_status = 201
        self.post_body = None

    async def handle_rules(self, request: web.Request) -> web.StreamResponse:
        body = await request.json() if request.method == "POST" else None
        self.requests.append((request.method, request.path, request.headers.get("X-API-Key"), body))
        if request.method == "GET":
            return web.Response(
                body=self.get_body, status=self.get_status, content_type="application/json"
            )
        if self.post_body is not None or self.post_status != 201:
            payload = self.post_body if self.post_body is not None else {}
            if isinstance(payload, bytes):
                return web.Response(body=payload, status=self.post_status)
            return web.json_response(payload, status=self.post_status)
        return web.json_response(
            {"id": "r-1", "number": 1, "state": "proposed", "text": body["rule"]}, status=201
        )

    def app(self, state: FakeStateApi | None = None) -> web.Application:
        app = state.app() if state is not None else web.Application()

        @web.middleware
        async def rules(request: web.Request, handler: Any) -> web.StreamResponse:
            if request.path == RULES_PATH:
                return await self.handle_rules(request)
            if request.path == "/work-items/completion-contract":
                # A factory run's contract: an implementation with no execution.
                return web.json_response({"completion_contract": "implementation"})
            return await handler(request)

        app.middlewares.append(rules)
        if state is None:

            async def missing(_request: web.Request) -> web.Response:
                return web.Response(status=404)

            app.router.add_route("*", "/{tail:.*}", missing)
        return app


def _load(api: RulesApi, *, path: str = RULES_PATH) -> ChannelRules:
    async def go() -> ChannelRules:
        async with TestServer(api.app()) as server:
            return await load_channel_rules(str(server.make_url(path)), MEMORY_TOKEN)

    return anyio.run(go)


def test_the_read_sends_the_boot_token_and_renders_the_answer() -> None:
    api = RulesApi()
    assert _load(api) == render_channel_rules(VALID)
    assert [(m, p, t) for m, p, t, _ in api.requests] == [("GET", RULES_PATH, MEMORY_TOKEN)]


def test_the_loaded_rules_are_redacted_from_logs() -> None:
    line = f"R3: {SECRET_RULE}"
    assert line in _logged(line)
    rendered = _load(RulesApi())
    assert rendered.section is not None
    logged = _logged(line, SECRET_RULE + " tail", rendered.section)
    assert line not in logged and rendered.section not in logged


@pytest.mark.parametrize(
    ("status", "body"),
    [
        pytest.param(500, b"{}", id="server-error"),
        pytest.param(403, b'{"detail": "memory credential is read-only"}', id="refused"),
        pytest.param(404, b'{"detail": "not found"}', id="older-api"),
        pytest.param(200, b"not json", id="not-json"),
        pytest.param(200, b"[]", id="kv-namespace-shape"),
        pytest.param(200, b"x" * (MAX_RESPONSE_BYTES + 1), id="oversized"),
    ],
)
def test_a_failed_read_degrades_to_nothing_and_logs_no_rule(
    status: int, body: bytes, caplog: pytest.LogCaptureFixture
) -> None:
    api = RulesApi()
    api.get_status, api.get_body = status, body
    with caplog.at_level(logging.INFO, logger="curie_runner"):
        assert _load(api) == ChannelRules()
    assert "channel rules not loaded" in caplog.text
    assert CHANNEL_RULE not in caplog.text
    # Liveness: the same server with the documented answer renders.
    assert _load(RulesApi()).section is not None


def test_an_unreachable_api_degrades_to_nothing(caplog: pytest.LogCaptureFixture) -> None:
    async def go() -> ChannelRules:
        async with TestServer(RulesApi().app()) as server:
            url = str(server.make_url(RULES_PATH))
        return await load_channel_rules(url, MEMORY_TOKEN)

    with caplog.at_level(logging.WARNING, logger="curie_runner"):
        assert anyio.run(go) == ChannelRules()
    assert "error_class=ClientConnectorError" in caplog.text


# --- 3. The handler --------------------------------------------------------------------


def _propose(
    api: RulesApi, args: dict[str, Any], *, token: str | None = TURN_TOKEN
) -> dict[str, Any]:
    async def go() -> dict[str, Any]:
        async with TestServer(api.app()) as server:
            handler = channel_propose_rule_handler(str(server.make_url(RULES_PATH)), lambda: token)
            return await handler(args)

    return anyio.run(go)


def _result_text(result: dict[str, Any]) -> str:
    return " ".join(block["text"] for block in result["content"])


ARGS = {"rule": CHANNEL_RULE, "reason": "The person asked for it from now on."}


def test_the_handler_posts_the_three_fields_with_the_turn_credential() -> None:
    api = RulesApi()
    result = _propose(api, {**ARGS, "scope_key": "slack:C9", "state": "accepted"})
    assert not result.get("is_error"), result
    assert "A person must accept it" in _result_text(result)
    [(method, path, key, body)] = api.requests
    assert (method, path, key) == ("POST", RULES_PATH, TURN_TOKEN)
    assert body == {**ARGS, "scope": "channel"}
    agent = RulesApi()
    assert not _propose(agent, {**ARGS, "scope": "agent"}).get("is_error")
    assert agent.requests[0][3] == {**ARGS, "scope": "agent"}


@pytest.mark.parametrize(
    ("args", "token"),
    [
        pytest.param(ARGS, None, id="no-turn-credential"),
        pytest.param(ARGS, "", id="empty-turn-credential"),
        pytest.param({**ARGS, "scope": "repository"}, TURN_TOKEN, id="unknown-scope"),
        pytest.param({"rule": CHANNEL_RULE}, TURN_TOKEN, id="no-reason"),
        pytest.param({"rule": 7, "reason": "r"}, TURN_TOKEN, id="rule-not-string"),
    ],
)
def test_a_local_refusal_sends_nothing(args: dict[str, Any], token: str | None) -> None:
    api = RulesApi()
    result = _propose(api, args, token=token)
    assert result.get("is_error") and api.requests == []
    assert CHANNEL_RULE not in _result_text(result)
    # Liveness: a valid call with the turn credential reaches the route.
    assert not _propose(api, ARGS).get("is_error") and len(api.requests) == 1


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        pytest.param(
            403, {"detail": {"code": "rule_proposal_not_eligible"}}, "turned off", id="switch-off"
        ),
        pytest.param(
            409, {"detail": {"code": "rule_already_proposed"}}, "already proposed", id="second"
        ),
        pytest.param(422, {"detail": {"code": "too_large"}}, "too long", id="too-large"),
        pytest.param(422, {"detail": {"code": "invalid_text"}}, "not valid text", id="text"),
        pytest.param(422, {"detail": {"code": "reserved_text"}}, "reserved", id="reserved"),
        pytest.param(422, {"detail": {"code": "invalid_body"}}, "malformed", id="body"),
        pytest.param(
            503, {"detail": {"code": "rule_proposal_unavailable"}}, "right now", id="unavailable"
        ),
        pytest.param(
            403, {"detail": "memory credential is read-only"}, "status 403", id="read-only"
        ),
        pytest.param(403, {"detail": "turn has ended"}, "status 403", id="turn-ended"),
        pytest.param(403, {"detail": {"code": f"Bad {CHANNEL_RULE}"}}, "status 403", id="odd-code"),
        pytest.param(201, {"state": "candidate"}, "could not be read", id="unreadable-201"),
        pytest.param(201, b"not json", "could not be read", id="non-json-201"),
        pytest.param(200, {"state": "proposed"}, "status 200", id="wrong-success-status"),
    ],
)
def test_each_refusal_is_reported_without_the_rule(
    status: int, body: Any, expected: str, caplog: pytest.LogCaptureFixture
) -> None:
    api = RulesApi()
    api.post_status, api.post_body = status, body
    with caplog.at_level(logging.INFO, logger="curie_runner"):
        result = _propose(api, ARGS)
    text = _result_text(result)
    assert result.get("is_error"), result
    assert expected in text, text
    assert CHANNEL_RULE not in text and "read-only" not in text and "ended" not in text
    assert CHANNEL_RULE not in caplog.text
    # Liveness: the same server, accepting, takes the same call.
    api.accept()
    assert not _propose(api, ARGS).get("is_error")


def test_a_transport_failure_is_reported_without_the_endpoint() -> None:
    async def go() -> dict[str, Any]:
        async with TestServer(RulesApi().app()) as server:
            url = str(server.make_url(RULES_PATH))
        return await channel_propose_rule_handler(url, lambda: TURN_TOKEN)(ARGS)

    result = anyio.run(go)
    assert result.get("is_error") and "could not be sent" in _result_text(result)
    assert "127.0.0.1" not in _result_text(result)


# --- 4. The boot: mount and prompt ---------------------------------------------------


def _boot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    rules: ChannelRules | None,
    pass_rules: bool = True,
    writes: bool = True,
    channel: bool = True,
    wir: bool = False,
    script: list[tuple[str, dict[str, Any]]] | None = None,
    event: Event | None = None,
    rules_api: RulesApi | None = None,
) -> tuple[Any, str | None]:
    """Boot the real runner; optionally run one turn calling the script's tools."""

    captured: dict[str, Any] = {}
    _ScriptedSession.script = script or []
    _ScriptedSession.results = []
    _ScriptedSession.listed = set()
    api = rules_api or RulesApi()

    async def go() -> None:
        monkeypatch.delenv(ISSUE_READ_URL_ENV, raising=False)
        monkeypatch.delenv(ISSUE_READ_TOKEN_ENV, raising=False)
        async with TestServer(api.app(FakeStateApi())) as server:
            env = _env(
                monkeypatch,
                tmp_path,
                server,
                channel=channel,
                writes=writes,
                extra={"CURIE_MODEL": MODEL},
            )
            if wir:
                monkeypatch.setenv(
                    ISSUE_READ_URL_ENV, str(server.make_url("/work-items/issue-read"))
                )
                monkeypatch.setenv(ISSUE_READ_TOKEN_ENV, "wir-capability")
            config = RunnerConfig.from_env(env)
            monkeypatch.setattr(boot, "ClaudeAgentSession", _ScriptedSession)
            fetches = await boot._load_boot_fetches(config, True, None)
            kwargs: dict[str, Any] = {
                "memory_facts_preamble": fetches.memory_facts_preamble,
                "memory_guidance": fetches.memory_guidance,
                "mcp_capability": _PROBE,
            }
            if pass_rules:
                kwargs["channel_rules"] = rules
            runner = build_runner(config, fake_model=False, **kwargs)
            if event is None:
                session = await initialized_session(runner)
                captured["options"] = session.options
                return
            await runner.start()
            try:
                async for _line in runner.run_turn(event):
                    pass
            finally:
                await runner.close()

    anyio.run(go)
    options = captured.get("options")
    return options, options.system_prompt if options is not None else None


def _published(options: Any) -> dict[str, str]:
    async def listed(instance: Any) -> list[tuple[str, str]]:
        entry = instance.get_request_handler("tools/list")
        result = await entry.handler(None, mcp_types.PaginatedRequestParams())
        return [(tool.name, tool.description or "") for tool in result.tools]

    instance = options.mcp_servers[APPROVAL_SERVER_NAME]["instance"]
    return {
        f"mcp__{APPROVAL_SERVER_NAME}__{name}": description
        for name, description in anyio.run(listed, instance)
    }


ON = ChannelRules(proposals_enabled=True, section=render_channel_rules(VALID).section)


def test_the_tool_mounts_when_memory_tools_mount_and_proposals_are_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    options, _prompt = _boot(monkeypatch, tmp_path, rules=ON)
    published = _published(options)
    assert published[PROPOSE] == CHANNEL_PROPOSE_RULE_DESCRIPTION


@pytest.mark.parametrize(
    "case",
    ["proposals-off", "no-read", "writes-off", "no-channel-ref", "wir-run"],
)
def test_the_tool_is_absent_when_any_condition_fails(
    case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    kwargs: dict[str, Any] = {"rules": ON}
    if case == "proposals-off":
        kwargs["rules"] = ChannelRules(proposals_enabled=False, section=ON.section)
    elif case == "no-read":
        kwargs["rules"] = None
    elif case == "writes-off":
        kwargs["writes"] = False
    elif case == "no-channel-ref":
        kwargs["channel"] = False
    else:
        kwargs["wir"] = True
    options, _prompt = _boot(monkeypatch, tmp_path / case, **kwargs)
    published = _published(options)
    assert PROPOSE not in published, published
    # Liveness: the same boot with every condition met mounts it.
    on, _prompt = _boot(monkeypatch, tmp_path / f"{case}-on", rules=ON)
    assert PROPOSE in _published(on)


def test_the_factory_description_is_not_the_channel_one() -> None:
    assert CHANNEL_PROPOSE_RULE_DESCRIPTION != PROPOSE_RULE_DESCRIPTION
    assert CHANNEL_PROPOSE_RULE_DESCRIPTION.startswith(
        "Propose one standing rule for this channel (or, with scope agent, for every "
        "channel you work in) when someone states how you should always work from now on."
    )


def test_accepted_rules_sit_right_after_the_model_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _options, prompt = _boot(monkeypatch, tmp_path, rules=ON)
    assert prompt is not None and ON.section is not None
    assert prompt.endswith(f"Configured model: {MODEL}\n\n{ON.section}")
    assert prompt.count("<<<curie-learned-rules count=") == 2
    assert prompt.index(AGENT_FRAMING) < prompt.index(CHANNEL_FRAMING)


@pytest.mark.parametrize(
    ("rules", "wir"),
    [
        pytest.param(ChannelRules(proposals_enabled=True, section=None), False, id="no-rules"),
        pytest.param(ChannelRules(), False, id="nothing-read"),
        pytest.param(ON, True, id="factory-run-with-rules"),
    ],
)
def test_no_rules_or_a_factory_run_keeps_the_prompt_byte_identical(
    rules: ChannelRules, wir: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _options, before = _boot(
        monkeypatch, tmp_path / "before", rules=None, pass_rules=False, wir=wir
    )
    _options, after = _boot(monkeypatch, tmp_path / "after", rules=rules, wir=wir)
    assert before is not None and after is not None
    assert after.encode("utf-8") == before.encode("utf-8")
    assert "<<<curie-learned-rules" not in after
    # Control: the same non-factory boot with accepted rules carries the section.
    _options, control = _boot(monkeypatch, tmp_path / "control", rules=ON)
    assert control is not None and ON.section is not None and ON.section in control


def test_a_proposal_in_a_turn_writes_with_the_turn_credential(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = RulesApi()
    _boot(
        monkeypatch,
        tmp_path,
        rules=ON,
        rules_api=api,
        script=[(PROPOSE, {**ARGS, "scope": "agent"})],
        event=Event(type="message", text="from now on", user="U1", ts="1", memory_token=TURN_TOKEN),
    )
    assert "propose_rule" in _ScriptedSession.listed
    [result] = _ScriptedSession.results
    assert not _is_error(result), _text(result)
    posts = [r for r in api.requests if r[0] == "POST"]
    assert posts == [("POST", RULES_PATH, TURN_TOKEN, {**ARGS, "scope": "agent"})]


def test_a_turn_without_a_credential_proposes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = RulesApi()
    _boot(
        monkeypatch,
        tmp_path,
        rules=ON,
        rules_api=api,
        script=[(PROPOSE, ARGS)],
        event=Event(type="message", text="from now on", user="U1", ts="1"),
    )
    [result] = _ScriptedSession.results
    assert _is_error(result) and "no credential" in _text(result)
    assert [r for r in api.requests if r[0] == "POST"] == []


# --- 5. The boot read: when it runs ------------------------------------------------------


def _fetch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    fake_model: bool = False,
    wir: bool = False,
    token: bool = True,
) -> tuple[ChannelRules | None, RulesApi]:
    api = RulesApi()

    async def probe(*_args: Any, **_kwargs: Any) -> Any:
        return _PROBE

    async def go() -> ChannelRules | None:
        monkeypatch.delenv(ISSUE_READ_URL_ENV, raising=False)
        monkeypatch.delenv(ISSUE_READ_TOKEN_ENV, raising=False)
        async with TestServer(api.app(FakeStateApi())) as server:
            env = _env(monkeypatch, tmp_path, server)
            if not token:
                env.pop("CURIE_MEMORY_TOKEN")
                monkeypatch.delenv("CURIE_MEMORY_TOKEN")
            if wir:
                monkeypatch.setenv(
                    ISSUE_READ_URL_ENV, str(server.make_url("/work-items/issue-read"))
                )
                monkeypatch.setenv(ISSUE_READ_TOKEN_ENV, "wir-capability")
            monkeypatch.setattr(boot, "probe_mcp_tool_capability", probe)
            fetches = await boot._load_boot_fetches(RunnerConfig.from_env(env), fake_model, None)
            return fetches.channel_rules

    return anyio.run(go), api


@pytest.mark.parametrize("case", ["fake-model", "wir-run", "no-memory-token"])
def test_the_boot_read_runs_only_without_wir_with_a_token_on_the_real_path(
    case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    loaded, api = _fetch(
        monkeypatch,
        tmp_path / case,
        fake_model=case == "fake-model",
        wir=case == "wir-run",
        token=case != "no-memory-token",
    )
    assert loaded is None and api.requests == []
    # Liveness: the real path with a token and no wir reads once, with the boot token.
    loaded, api = _fetch(monkeypatch, tmp_path / f"{case}-on")
    assert loaded == render_channel_rules(VALID)
    assert [(m, p, t) for m, p, t, _ in api.requests] == [("GET", RULES_PATH, MEMORY_TOKEN)]


def test_build_runner_takes_what_the_boot_read_returns() -> None:
    import dataclasses
    import inspect

    fields = {f.name for f in dataclasses.fields(boot._BootFetches)}
    assert "channel_rules" in fields
    assert "channel_rules" in inspect.signature(build_runner).parameters
