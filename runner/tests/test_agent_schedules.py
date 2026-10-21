"""Agent-managed schedules in the runner: URL mapping, client, tools, guidance, boot.

The client is driven against a real aiohttp test server, like ``test_memory_facts.py``.
The tools are built with ``build_agent_schedule_tools`` and invoked through each
tool's ``handler``, with a fake client (for refusal and error mapping) or the real
client (for the credential rule).

Names this file pins beyond the plan's interface contract: the tool argument names
(``prompt``, ``cron``, ``run_at``, ``timezone``, ``id``, ``expected_revision``), and
that ``load_for_session`` is a coroutine.
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Any

import anyio
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner import agent_schedules as sched
from curie_runner.agent_schedules import (
    AgentSchedule,
    AgentSchedulesClient,
    AgentSchedulesConfig,
    AgentSchedulesError,
    agent_schedules_url,
    schedules_guidance,
)
from curie_runner.approval import platform_tool_names
from curie_runner.harness.claude.agent_schedule_tools import (
    JOB_REFUSAL,
    build_agent_schedule_tools,
)
from curie_runner.memory_facts import NO_PERSON, MemoryTurn
from curie_runner.side_effects import PLATFORM_IDEMPOTENT_TOOLS
from curie_runner.state import RESERVED_NAMESPACES, STATE_TOKEN_ENV, STATE_URL_ENV
from curie_runner.tool_names import (
    AGENT_SCHEDULE_TOOL_SHORT_NAMES,
    AGENT_SCHEDULES_SERVER_NAME,
)

READ_TOKEN = "read-tok"
WRITE_TOKEN = "write-tok"
SCHEDULES_PATH = "/agents/A/schedules"
SCHEDULE_ID = "11111111-1111-4111-8111-111111111111"
LOCAL_LABEL = "Mon 2026-10-26 08:00 EDT (America/New_York)"

LIVE_NAMES = {f"mcp__{AGENT_SCHEDULES_SERVER_NAME}__{n}" for n in AGENT_SCHEDULE_TOOL_SHORT_NAMES}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _schedule_json(**over: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "id": SCHEDULE_ID,
        "agent": "A",
        "agent_id": "A",
        "revision": 1,
        "state": "active",
        "cron": "0 8 * * 1",
        "run_at": None,
        "timezone": "America/New_York",
        "prompt": "post the weekly summary",
        "created_by": "U1",
        "created_source": "agent",
        "destination": {"kind": "slack", "address": "C1", "adapter": None},
        "next_run_at": "2026-10-26T12:00:00+00:00",
        "next_run_local": LOCAL_LABEL,
        "paused": False,
        "consecutive_failures": 0,
        "last_fire_at": None,
        "last_outcome": None,
        "last_reason": None,
        "created_at": "2026-10-20T00:00:00+00:00",
        "updated_at": "2026-10-20T00:00:00+00:00",
        "updated_by": "U1",
    }
    body.update(over)
    return body


def _make_schedule(**over: Any) -> AgentSchedule:
    """Build an AgentSchedule by field name, defaulting unknown fields to None."""

    data = _schedule_json(**over)
    kwargs: dict[str, Any] = {}
    for field in dataclasses.fields(AgentSchedule):
        if field.name in data:
            kwargs[field.name] = data[field.name]
        elif field.default is dataclasses.MISSING and field.default_factory is dataclasses.MISSING:
            kwargs[field.name] = None
    return AgentSchedule(**kwargs)


class FakeSchedulesApi:
    def __init__(self) -> None:
        self.requests: list[tuple[str, str, str | None, Any]] = []
        self.config: dict[str, Any] = {
            "enabled": True,
            "timezone": "America/New_York",
            "max_active": 7,
            "min_interval_minutes": 30,
            "schedules": [],
        }

    def writes(self) -> list[tuple[str, str]]:
        return [(m, p) for m, p, _, _ in self.requests if m != "GET"]

    def app(self) -> web.Application:
        async def handle(request: web.Request) -> web.StreamResponse:
            body = await request.json() if request.can_read_body else None
            self.requests.append(
                (request.method, request.path, request.headers.get("X-API-Key"), body)
            )
            path = request.path
            if path == SCHEDULES_PATH and request.method == "GET":
                return web.json_response(self.config)
            if path == SCHEDULES_PATH and request.method == "POST":
                return web.json_response(_schedule_json(prompt=body["prompt"]), status=201)
            if path.endswith("/cancel") and request.method == "POST":
                return web.json_response(_schedule_json(state="cancelled", revision=2))
            if path.endswith("/revert") and request.method == "POST":
                return web.json_response(_schedule_json(revision=3))
            if path.startswith(SCHEDULES_PATH + "/") and request.method == "PATCH":
                return web.json_response(_schedule_json(revision=2))
            return web.json_response({"detail": "no route"}, status=404)

        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", handle)
        return app


class FakeClient:
    """Stands in for AgentSchedulesClient; records calls, optionally raises."""

    def __init__(self, *, raises: AgentSchedulesError | None = None) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.raises = raises

    async def _do(self, name: str, result: Any, *args: Any, **kwargs: Any) -> Any:
        self.calls.append((name, args, kwargs))
        if self.raises is not None:
            raise self.raises
        return result

    async def list(self, *args: Any, **kwargs: Any) -> Any:
        return await self._do("list", [_make_schedule()], *args, **kwargs)

    async def create(self, *args: Any, **kwargs: Any) -> Any:
        return await self._do("create", _make_schedule(), *args, **kwargs)

    async def update(self, *args: Any, **kwargs: Any) -> Any:
        return await self._do("update", _make_schedule(revision=2), *args, **kwargs)

    async def cancel(self, *args: Any, **kwargs: Any) -> Any:
        return await self._do("cancel", _make_schedule(state="cancelled"), *args, **kwargs)

    async def revert(self, *args: Any, **kwargs: Any) -> Any:
        return await self._do("revert", _make_schedule(revision=3), *args, **kwargs)


def _turn(author: str = "U1") -> MemoryTurn:
    turn = MemoryTurn()
    turn.author = author
    turn.write_token = WRITE_TOKEN
    return turn


def _tools(client: Any, turn: MemoryTurn) -> dict[str, Any]:
    return {t.name: t for t in build_agent_schedule_tools(client=client, turn=turn)}


def _text(result: dict[str, Any]) -> str:
    return "".join(part.get("text", "") for part in result["content"])


def _call(tool: Any, args: dict[str, Any]) -> dict[str, Any]:
    async def go() -> dict[str, Any]:
        return await tool.handler(args)

    return anyio.run(go)


CREATE_ARGS = {
    "prompt": "post the weekly summary",
    "cron": "0 8 * * 1",
    "timezone": "America/New_York",
}
UPDATE_ARGS = {"id": SCHEDULE_ID, "expected_revision": 1, "prompt": "x"}
CANCEL_ARGS = {"id": SCHEDULE_ID}
REVERT_ARGS = {"id": SCHEDULE_ID, "expected_revision": 2}


# --------------------------------------------------------------------------- #
# 1. URL mapping
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "state_url",
    [
        "http://api:8000/agents/abc/state",
        "http://api:8000/agents/abc/state/x",
    ],
)
def test_agent_schedules_url_maps_state_urls(state_url: str) -> None:
    assert agent_schedules_url(state_url) == "http://api:8000/agents/abc/schedules"


@pytest.mark.parametrize(
    "state_url",
    [
        "http://api:8000/agents/abc",
        "http://api:8000/state",
        "http://api:8000/agents/abc/memory",
        "",
    ],
)
def test_agent_schedules_url_other_shapes_are_none(state_url: str) -> None:
    assert agent_schedules_url(state_url) is None


# --------------------------------------------------------------------------- #
# 2. Client
# --------------------------------------------------------------------------- #


def _client(server: TestServer, write: str | None = WRITE_TOKEN) -> AgentSchedulesClient:
    return AgentSchedulesClient(str(server.make_url(SCHEDULES_PATH)), READ_TOKEN, lambda: write)


def test_client_get_uses_read_token() -> None:
    api = FakeSchedulesApi()

    async def go() -> None:
        async with TestServer(api.app()) as server:
            await _client(server).list()
        assert api.requests
        assert {tok for m, _, tok, _ in api.requests if m == "GET"} == {READ_TOKEN}

    anyio.run(go)


def test_client_writes_use_write_token() -> None:
    api = FakeSchedulesApi()

    async def go() -> None:
        async with TestServer(api.app()) as server:
            client = _client(server)
            await client.create("post the weekly summary", cron="0 8 * * 1")
            await client.update(SCHEDULE_ID, 1, prompt="new prompt")
            await client.cancel(SCHEDULE_ID, 2)
            await client.revert(SCHEDULE_ID, 2, to_revision=1)
        writes = [r for r in api.requests if r[0] != "GET"]
        assert len(writes) == 4
        assert {tok for _, _, tok, _ in writes} == {WRITE_TOKEN}
        methods = [(m, p.rsplit("/", 1)[-1]) for m, p, _, _ in writes]
        assert methods[0] == ("POST", "schedules")
        assert methods[1][0] == "PATCH"
        assert methods[2] == ("POST", "cancel")
        assert methods[3] == ("POST", "revert")
        assert writes[1][3]["expected_revision"] == 1
        assert writes[3][3] == {"expected_revision": 2, "to_revision": 1}

    anyio.run(go)


def test_client_write_without_write_token_is_403_and_sends_nothing() -> None:
    api = FakeSchedulesApi()

    async def go() -> None:
        async with TestServer(api.app()) as server:
            client = _client(server, write=None)
            for call in (
                lambda: client.create("p", cron="0 8 * * 1"),
                lambda: client.update(SCHEDULE_ID, 1, prompt="p"),
                lambda: client.cancel(SCHEDULE_ID),
            ):
                with pytest.raises(AgentSchedulesError) as excinfo:
                    await call()
                assert excinfo.value.status == 403
        assert api.requests == []

    anyio.run(go)


def test_load_config_parses_fields() -> None:
    api = FakeSchedulesApi()

    async def go() -> None:
        async with TestServer(api.app()) as server:
            config = await _client(server).load_config()
        assert isinstance(config, AgentSchedulesConfig)
        assert config.enabled is True
        assert config.timezone == "America/New_York"
        assert config.max_active == 7
        assert config.min_interval_minutes == 30

    anyio.run(go)


def test_load_config_disabled() -> None:
    api = FakeSchedulesApi()
    api.config["enabled"] = False

    async def go() -> None:
        async with TestServer(api.app()) as server:
            config = await _client(server).load_config()
        assert config.enabled is False

    anyio.run(go)


# --------------------------------------------------------------------------- #
# 3. Tools
# --------------------------------------------------------------------------- #


def test_builds_the_five_tools() -> None:
    tools = _tools(FakeClient(), _turn())
    assert set(tools) == set(AGENT_SCHEDULE_TOOL_SHORT_NAMES)


def test_create_schedule_for_a_person_names_id_revision_next_run_and_cancel_offer() -> None:
    client = FakeClient()
    result = _call(_tools(client, _turn("U1"))["create_schedule"], CREATE_ARGS)
    assert not result.get("is_error")
    text = _text(result)
    assert SCHEDULE_ID in text
    assert "revision 1" in text.lower() or "revision: 1" in text.lower()
    assert LOCAL_LABEL in text
    assert "cancel" in text.lower()
    assert [c[0] for c in client.calls] == ["create"]


@pytest.mark.parametrize("author", [NO_PERSON, "cron:agent-schedule:x"])
def test_scheduled_or_personless_turn_cannot_create_update_or_revert(author: str) -> None:
    client = FakeClient()
    tools = _tools(client, _turn(author))
    for name, args in (
        ("create_schedule", CREATE_ARGS),
        ("update_schedule", UPDATE_ARGS),
        ("revert_schedule", REVERT_ARGS),
    ):
        result = _call(tools[name], args)
        assert result["is_error"] is True
        assert JOB_REFUSAL in _text(result)
    assert client.calls == []


def test_scheduled_refusal_logs_each_refused_tool(caplog: pytest.LogCaptureFixture) -> None:
    """The refusal is observable outside the model's reply: one log line per
    refused call, naming the tool and the scheduled run's author."""

    author = "cron:agent-schedule:6f9619ff-8b86-4011-b42d-00c04fc964ff"
    tools = _tools(FakeClient(), _turn(author))
    with caplog.at_level(logging.INFO, logger="curie_runner.harness.claude.agent_schedule_tools"):
        for name, args in (
            ("create_schedule", CREATE_ARGS),
            ("update_schedule", UPDATE_ARGS),
            ("revert_schedule", REVERT_ARGS),
        ):
            _call(tools[name], args)
        _call(tools["list_schedules"], {})
    refused = [r.getMessage() for r in caplog.records if "refused" in r.getMessage()]
    assert refused == [
        f"agent schedule write refused tool=create_schedule author={author} reason=scheduled_run",
        f"agent schedule write refused tool=update_schedule author={author} reason=scheduled_run",
        f"agent schedule write refused tool=revert_schedule author={author} reason=scheduled_run",
    ]


@pytest.mark.parametrize("author", [NO_PERSON, "cron:agent-schedule:x"])
def test_scheduled_or_personless_turn_can_still_list_and_cancel(author: str) -> None:
    client = FakeClient()
    tools = _tools(client, _turn(author))
    listed = _call(tools["list_schedules"], {})
    cancelled = _call(tools["cancel_schedule"], CANCEL_ARGS)
    assert not listed.get("is_error")
    assert not cancelled.get("is_error")
    assert [c[0] for c in client.calls] == ["list", "cancel"]


def test_update_cancel_and_revert_send_the_id_argument() -> None:
    client = FakeClient()
    tools = _tools(client, _turn())
    assert not _call(tools["update_schedule"], UPDATE_ARGS).get("is_error")
    assert not _call(tools["cancel_schedule"], CANCEL_ARGS).get("is_error")
    assert not _call(tools["revert_schedule"], REVERT_ARGS).get("is_error")
    assert [(c[0], c[1][0]) for c in client.calls] == [
        ("update", SCHEDULE_ID),
        ("cancel", SCHEDULE_ID),
        ("revert", SCHEDULE_ID),
    ]


def test_revert_reads_back_and_passes_the_named_revision() -> None:
    client = FakeClient()
    result = _call(
        _tools(client, _turn("U1"))["revert_schedule"], {**REVERT_ARGS, "to_revision": 1}
    )
    assert not result.get("is_error")
    assert "Reverted schedule" in _text(result)
    assert "revision 3" in _text(result)
    assert client.calls == [("revert", (SCHEDULE_ID, 2, 1), {})]


def test_schedule_id_is_not_an_alias_for_id() -> None:
    # The tools take only ``id``; a ``schedule_id`` argument names nothing.
    client = FakeClient()
    tools = _tools(client, _turn())
    _call(tools["update_schedule"], {"schedule_id": SCHEDULE_ID, "expected_revision": 1})
    _call(tools["cancel_schedule"], {"schedule_id": SCHEDULE_ID})
    for _name, args, _kwargs in client.calls:
        assert SCHEDULE_ID not in args, client.calls


def test_no_write_credential_is_an_error_refusal_and_sends_nothing() -> None:
    api = FakeSchedulesApi()
    turn = _turn("U1")
    turn.write_token = None

    async def go() -> dict[str, Any]:
        async with TestServer(api.app()) as server:
            client = AgentSchedulesClient(
                str(server.make_url(SCHEDULES_PATH)), READ_TOKEN, lambda: turn.write_token
            )
            tool = _tools(client, turn)["create_schedule"]
            return await tool.handler(CREATE_ARGS)

    result = anyio.run(go)
    assert result["is_error"] is True
    assert api.requests == []


def test_conflict_asks_to_reread_and_retry() -> None:
    client = FakeClient(raises=AgentSchedulesError("schedule is at revision 3", status=409))
    result = _call(_tools(client, _turn())["update_schedule"], UPDATE_ARGS)
    assert result["is_error"] is True
    text = _text(result).lower()
    assert "retry" in text
    assert "read" in text


def test_validation_detail_is_surfaced_verbatim() -> None:
    detail = "schedules must be at least 15 minutes apart"
    client = FakeClient(raises=AgentSchedulesError(detail, status=422))
    result = _call(_tools(client, _turn())["create_schedule"], CREATE_ARGS)
    assert result["is_error"] is True
    assert detail in _text(result)


def test_api_refusal_is_logged_with_tool_status_and_detail(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An API refusal is observable outside the model's reply."""

    detail = "schedules must be at least 15 minutes apart"
    client = FakeClient(raises=AgentSchedulesError(detail, status=422))
    with caplog.at_level(logging.INFO, logger="curie_runner.harness.claude.agent_schedule_tools"):
        _call(_tools(client, _turn())["create_schedule"], CREATE_ARGS)
    assert [r.getMessage() for r in caplog.records if "refused by the API" in r.getMessage()] == [
        f"agent schedule write refused by the API tool=create_schedule status=422 detail={detail}"
    ]


@pytest.mark.parametrize("status", [500, 503])
def test_server_error_says_could_not_confirm(status: int) -> None:
    client = FakeClient(raises=AgentSchedulesError("boom", status=status))
    result = _call(_tools(client, _turn())["create_schedule"], CREATE_ARGS)
    assert result["is_error"] is True
    assert "could not confirm" in _text(result).lower()


# --------------------------------------------------------------------------- #
# 4. Tool names, approval classification, reserved namespace
# --------------------------------------------------------------------------- #


def test_platform_tool_names_includes_live_schedule_names_when_mounted() -> None:
    names = platform_tool_names(state_server_mounted=False, agent_schedules_mounted=True)
    assert LIVE_NAMES <= names
    assert len(LIVE_NAMES) == 5


def test_platform_tool_names_excludes_schedule_names_when_not_mounted() -> None:
    names = platform_tool_names(state_server_mounted=False)
    assert not (LIVE_NAMES & names)
    names = platform_tool_names(state_server_mounted=True, agent_schedules_mounted=False)
    assert not (LIVE_NAMES & names)


def test_only_list_schedules_is_idempotent() -> None:
    prefix = f"mcp__{AGENT_SCHEDULES_SERVER_NAME}__"
    assert f"{prefix}list_schedules" in PLATFORM_IDEMPOTENT_TOOLS
    assert f"{prefix}create_schedule" not in PLATFORM_IDEMPOTENT_TOOLS
    assert f"{prefix}update_schedule" not in PLATFORM_IDEMPOTENT_TOOLS
    assert f"{prefix}cancel_schedule" not in PLATFORM_IDEMPOTENT_TOOLS
    assert f"{prefix}revert_schedule" not in PLATFORM_IDEMPOTENT_TOOLS


def test_agent_schedules_is_a_reserved_state_namespace() -> None:
    assert "agent_schedules" in RESERVED_NAMESPACES


# --------------------------------------------------------------------------- #
# 5. Guidance
# --------------------------------------------------------------------------- #


def test_schedules_guidance_content() -> None:
    text = schedules_guidance("America/New_York")
    lower = text.lower()
    assert "America/New_York" in text
    assert "cancel" in lower
    assert "read" in lower and "zone" in lower
    assert "test run" in lower
    assert "recurring" in lower
    assert "ambiguous" in lower
    assert "clarif" in lower or "ask" in lower


# --------------------------------------------------------------------------- #
# 6. load_for_session
# --------------------------------------------------------------------------- #


def test_load_for_session_none_with_fake_model_or_no_state_url() -> None:
    api = FakeSchedulesApi()

    async def go() -> None:
        async with TestServer(api.app()) as server:
            env = {
                STATE_URL_ENV: str(server.make_url("/agents/A/state")),
                STATE_TOKEN_ENV: READ_TOKEN,
            }
            assert await sched.load_for_session(env, True) is None
            assert await sched.load_for_session({}, False) is None
        assert api.requests == []

    anyio.run(go)


def test_load_for_session_returns_config_when_store_answers() -> None:
    api = FakeSchedulesApi()

    async def go() -> None:
        async with TestServer(api.app()) as server:
            env = {
                STATE_URL_ENV: str(server.make_url("/agents/A/state")),
                STATE_TOKEN_ENV: READ_TOKEN,
            }
            config = await sched.load_for_session(env, False)
        assert isinstance(config, AgentSchedulesConfig)
        assert config.enabled is True
        assert config.max_active == 7
        assert [p for m, p, _, _ in api.requests] == [SCHEDULES_PATH]

    anyio.run(go)
