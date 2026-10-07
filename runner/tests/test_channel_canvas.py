"""The runner half of ADR 0200 canvas tools (#3819).

Three canvas tools ride the platform ``curie-slack`` server beside the ADR 0100
history tools, each behind its own bundle grant: ``canvasList`` mounts
``list_channel_canvases``, ``canvasRead`` mounts ``read_canvas`` and
``canvasEdit`` mounts ``edit_canvas_cell``. Any one grant mounts the server;
history stays behind ``channelRead`` alone.

Real paths throughout, as in ``test_channel_read``: the production
``build_runner``, the real aiohttp ACI app, and tool handlers calling a local
server playing the platform's ``POST /channel-canvas`` route. The route double
answers in the platform's wire shape: a ``ChannelCanvasResult`` on 200, and the
``{"detail": {"code": "channel_read.<name>", "message": ...}}`` body on every
refusal. Only the SDK client is scripted.
"""

from __future__ import annotations

import contextlib
import itertools
import json
import logging
import os
import socket
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from pathlib import Path
from typing import Any

import anyio
import mcp.types as mcp_types
import pytest
from aci_protocol import ChannelReadCapability
from aiohttp.test_utils import TestClient, TestServer
from curie_runner import RunnerConfig, SideEffectClassifier, create_app
from curie_runner.__main__ import build_runner
from curie_runner.approval import APPROVAL_SERVER_NAME, is_platform_owned_tool
from curie_runner.harness.claude.platform_slack import build_channel_read_server
from curie_runner.platform_slack.capability import ChannelReadTurn, canvas_url
from curie_runner.plugin import load_bundle_platform_slack_grants
from curie_runner.tool_names import (
    CANVAS_TOOL_NAMES,
    PLATFORM_SLACK_TOOL_NAMES,
    PLATFORM_SLACK_TOOLS_BY_GRANT,
    STATE_SERVER_NAME,
    platform_slack_tool_names,
)
from plugin_format import CHANNEL_READ_SERVER_NAME, PLATFORM_SLACK_GRANT_FIELDS

from .test_channel_read import (
    _TOKEN,
    NO_CAP,
    SENTINEL,
    _assert_refused,
    _call_server,
    _ChannelApi,
    _event,
    _origin,
    _patch_sdk,
    _Rig,
    _Sdk,
    _status_fields,
    _Store,
    _token,
    _tool_results,
    _url,
)
from .test_connectors import _boot_env, _published_live_tool_names
from .test_tool_policy_enforcement import _interception_reason

# Wire names spelled out: a pin derived from the constant it pins agrees with itself.
HISTORY_TOOL = "mcp__curie-slack__read_channel_history"
THREAD_TOOL = "mcp__curie-slack__read_thread_replies"
MESSAGE_TOOL = "mcp__curie-slack__read_channel_message"
LIST_TOOL = "mcp__curie-slack__list_channel_canvases"
READ_TOOL = "mcp__curie-slack__read_canvas"
EDIT_TOOL = "mcp__curie-slack__edit_canvas_cell"
HISTORY_NAMES = frozenset({HISTORY_TOOL, THREAD_TOOL, MESSAGE_TOOL})
CANVAS_NAMES = frozenset({LIST_TOOL, READ_TOOL, EDIT_TOOL})
ALL_NAMES = HISTORY_NAMES | CANVAS_NAMES
TOOLS_BY_GRANT: dict[str, frozenset[str]] = {
    "channelRead": HISTORY_NAMES,
    "canvasList": frozenset({LIST_TOOL}),
    "canvasRead": frozenset({READ_TOOL}),
    "canvasEdit": frozenset({EDIT_TOOL}),
}
GRANTS = ("channelRead", "canvasList", "canvasRead", "canvasEdit")
ALL_GRANTS = frozenset(GRANTS)
SUBSETS = [frozenset(combo) for size in range(5) for combo in itertools.combinations(GRANTS, size)]


def _ids(subset: frozenset[str]) -> str:
    return "+".join(name for name in GRANTS if name in subset) or "none"


def _expected(subset: frozenset[str]) -> frozenset[str]:
    return frozenset().union(*(TOOLS_BY_GRANT[name] for name in subset))


def _short(names: frozenset[str]) -> set[str]:
    return {name.removeprefix("mcp__curie-slack__") for name in names}


# Anonymized ids in the shape the platform answers with.
CANVAS = "F0EXAMPLE01"
SECTION = "temp:C:EXA0a1b2c3d4e5f"
# Every canvas text the route double returns carries this, so a leak shows in a search.
MARKER = "canvas-text-marker-5b8e1d"
OTHER_CHANNEL = {"kind": "slack", "address": "C0EXAMPLE2"}


def _cell(section: str | None, text: str) -> dict[str, Any]:
    return {"section_id": section, "text": text, "truncated": False}


def _result(operation: str, **fields: Any) -> dict[str, Any]:
    """A ``ChannelCanvasResult`` as the API serializes it: every field, unset ones null."""

    keys = (
        "canvases",
        "has_more",
        "canvas_id",
        "title",
        "tables",
        "paragraphs",
        "section_id",
        "edited",
    )
    return {"operation": operation, **dict.fromkeys(keys), **fields}


LIST_RESULT = _result(
    "list",
    canvases=[{"id": CANVAS, "title": f"Weekly plan {MARKER}", "created": "2026-10-05T12:00:00Z"}],
    has_more=False,
)
READ_RESULT = _result(
    "read",
    canvas_id=CANVAS,
    title=f"Weekly plan {MARKER}",
    tables=[
        {
            "header": [
                _cell("temp:C:EXA00000001", "Item"),
                _cell("temp:C:EXA00000002", "Owner"),
                _cell("temp:C:EXA00000003", "Status"),
            ],
            "rows": [
                [
                    _cell("temp:C:EXA00000004", "Weekly plan review"),
                    _cell("temp:C:EXA00000005", f"Platform {MARKER}"),
                    _cell(SECTION, "sentinel-baseline"),
                ]
            ],
        }
    ],
    paragraphs=[{"section_id": None, "text": f"Plan for the week {MARKER}", "truncated": False}],
)
EDIT_RESULT = _result("edit", canvas_id=CANVAS, section_id=SECTION, edited=True)

EDIT_ARGS = {"canvas_id": CANVAS, "section_id": SECTION, "text": "sentinel-3f9a1c2e"}
# (tool, model arguments, the route's 200 answer, the exact body the route must receive)
CANVAS_CASES: list[tuple[str, dict[str, Any], dict[str, Any], dict[str, Any]]] = [
    ("list_channel_canvases", {}, LIST_RESULT, {"operation": "list"}),
    (
        "list_channel_canvases",
        {"channel": OTHER_CHANNEL},
        LIST_RESULT,
        {"operation": "list", "channel": OTHER_CHANNEL},
    ),
    ("read_canvas", {"canvas_id": CANVAS}, READ_RESULT, {"operation": "read", "canvas_id": CANVAS}),
    (
        "read_canvas",
        {"canvas_id": CANVAS, "kind": "slack"},
        READ_RESULT,
        {"operation": "read", "canvas_id": CANVAS, "kind": "slack"},
    ),
    ("edit_canvas_cell", EDIT_ARGS, EDIT_RESULT, {"operation": "edit", **EDIT_ARGS}),
]
_CASE_IDS = ["list-default", "list-named", "read", "read-kind", "edit"]
ONE_CALL_EACH = [CANVAS_CASES[0], CANVAS_CASES[2], CANVAS_CASES[4]]

# The capability's route sits under a path prefix, so the derivation is not a bare rename.
READ_ROUTE = "/api/v1/channel-read"
CANVAS_ROUTE = "/api/v1/channel-canvas"


# --- The route double and the built server ---


@contextlib.asynccontextmanager
async def _served(
    api: _ChannelApi,
    *,
    grants: frozenset[str] = ALL_GRANTS,
    capability: bool = True,
    token: str | None = None,
) -> AsyncIterator[tuple[TestServer, ChannelReadTurn, Any]]:
    """The route double, a holder trusting it, and the server built with ``grants``."""

    async with TestServer(api.app()) as server:
        turn = ChannelReadTurn(trusted_origin=_origin(server))
        if capability:
            turn.begin(_event({"url": _url(server, READ_ROUTE), "token": token or _token()}))
        yield server, turn, build_channel_read_server(turn, grants)["instance"]


def _call_once(
    api: _ChannelApi, name: str, args: dict[str, Any], token: str | None = None
) -> dict[str, Any]:
    async def go() -> dict[str, Any]:
        async with _served(api, token=token) as (_, _, instance):
            return await _call_server(instance, name, args)

    return anyio.run(go)


async def _listed(instance: Any) -> list[dict[str, Any]]:
    entry = instance.get_request_handler("tools/list")
    assert entry is not None
    result = await entry.handler(None, mcp_types.PaginatedRequestParams())
    return [tool.model_dump() for tool in result.tools]


def _catalogue(grants: frozenset[str]) -> list[dict[str, Any]]:
    turn = ChannelReadTurn(trusted_origin=("http", "h", 8080))
    return anyio.run(_listed, build_channel_read_server(turn, grants)["instance"])


# --- Grant vocabulary and the catalogue ---


def test_the_tool_map_keys_are_the_grant_vocabulary() -> None:
    assert set(PLATFORM_SLACK_TOOLS_BY_GRANT) == set(PLATFORM_SLACK_GRANT_FIELDS) == set(GRANTS)
    for grant, names in TOOLS_BY_GRANT.items():
        assert set(PLATFORM_SLACK_TOOLS_BY_GRANT[grant]) == _short(names), grant
    assert frozenset(CANVAS_TOOL_NAMES) == CANVAS_NAMES
    assert frozenset(PLATFORM_SLACK_TOOL_NAMES) == ALL_NAMES


@pytest.mark.parametrize("subset", SUBSETS, ids=[_ids(s) for s in SUBSETS])
def test_platform_slack_tool_names_are_exactly_the_granted_tools(subset: frozenset[str]) -> None:
    assert platform_slack_tool_names(subset) == _expected(subset)


@pytest.mark.parametrize("subset", SUBSETS[1:], ids=[_ids(s) for s in SUBSETS[1:]])
def test_each_tool_is_in_the_catalogue_only_with_its_own_grant(subset: frozenset[str]) -> None:
    listed = {tool["name"] for tool in _catalogue(subset)}
    assert listed == _short(_expected(subset))
    # History stays behind channelRead alone; no canvas grant brings it in.
    assert bool(listed & _short(HISTORY_NAMES)) is ("channelRead" in subset)


def test_an_empty_grant_set_builds_no_server() -> None:
    turn = ChannelReadTurn(trusted_origin=("http", "h", 8080))
    with pytest.raises(ValueError):
        build_channel_read_server(turn, frozenset())
    # Liveness: one grant is enough to build it.
    assert {tool["name"] for tool in _catalogue(frozenset({"canvasList"}))} == {
        "list_channel_canvases"
    }


def test_canvas_tool_schemas_take_no_credential_and_name_their_required_fields() -> None:
    tools = {tool["name"]: tool for tool in _catalogue(ALL_GRANTS)}
    assert set(tools) == _short(ALL_NAMES)
    for name, tool in tools.items():
        schema = tool.get("inputSchema") or tool.get("input_schema") or {}
        properties = {key.lower() for key in (schema.get("properties") or {})}
        assert not properties & {"token", "url", "capability", "credential"}, name
    schemas = {
        name: tools[name].get("inputSchema") or tools[name].get("input_schema") or {}
        for name in _short(CANVAS_NAMES)
    }
    assert "canvas_id" in set(schemas["read_canvas"].get("required") or [])
    edit_required = set(schemas["edit_canvas_cell"].get("required") or [])
    assert {"canvas_id", "section_id", "text"} <= edit_required
    assert not set(schemas["list_channel_canvases"].get("required") or [])


@pytest.mark.parametrize(
    ("value", "granted"),
    [(True, True), (False, False), ("true", False), (1, False), (None, False)],
    ids=["true", "false", "string-true", "one", "null"],
)
@pytest.mark.parametrize("grant", GRANTS[1:])
def test_only_a_literal_true_canvas_grant_grants(
    tmp_path: Path, grant: str, value: Any, granted: bool
) -> None:
    manifest_dir = tmp_path / ".claude-plugin"
    manifest_dir.mkdir()
    manifest = {"name": "b", "version": "0.1.0", "description": "t", grant: value}
    (manifest_dir / "plugin.json").write_text(json.dumps(manifest), encoding="utf-8")
    assert load_bundle_platform_slack_grants(str(tmp_path)) == (
        frozenset({grant}) if granted else frozenset()
    )
    assert load_bundle_platform_slack_grants(None) == frozenset()


# --- Mount, catalogue and advertisement through the production boot ---


def _config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    api_base: str = "http://state.invalid",
    *,
    grants: Mapping[str, Any],
    policy: dict[str, list[str]] | None = None,
) -> RunnerConfig:
    """The eligible boot env with the manifest declaring ``grants`` verbatim."""

    env = _boot_env(monkeypatch, tmp_path, "channel-canvas")
    monkeypatch.setenv("CURIE_STATE_URL", f"{api_base}/agents/a/state")
    manifest_path = Path(env["CURIE_PLUGIN_DIR"]) / ".claude-plugin" / "plugin.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.update(grants)
    if policy is not None:
        manifest["toolPolicy"] = {"enforcement": "curie/mcp-tool-policy@1", **policy}
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return RunnerConfig.from_env(env)


def _boot_options(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, **config: Any
) -> tuple[Any, Any]:
    sessions = _patch_sdk(monkeypatch)
    runner = build_runner(_config(monkeypatch, tmp_path, **config), fake_model=False)
    session = runner._factory()  # noqa: SLF001 - the options the SDK receives
    assert isinstance(session, _Sdk)
    assert sessions[-1] is session
    return runner, session.options


@pytest.mark.parametrize("subset", SUBSETS, ids=[_ids(s) for s in SUBSETS])
def test_any_grant_mounts_curie_slack_with_exactly_the_granted_tools(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, subset: frozenset[str]
) -> None:
    mounted = bool(subset)
    runner, options = _boot_options(monkeypatch, tmp_path, grants={name: True for name in subset})
    servers = {APPROVAL_SERVER_NAME, STATE_SERVER_NAME, CHANNEL_READ_SERVER_NAME}
    assert set(options.mcp_servers) == servers - (set() if mounted else {CHANNEL_READ_SERVER_NAME})
    published = _published_live_tool_names(options.mcp_servers)
    slack = {name for name in published if name.startswith("mcp__curie-slack__")}
    assert slack == _expected(subset)
    # With no policy, nothing mounted is hidden from the catalogue.
    assert not set(options.disallowed_tools or ()) & slack
    assert [value is True for value in _status_fields(runner)] == [mounted, mounted]


def test_a_fake_model_boot_never_mounts_even_with_every_grant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(monkeypatch, tmp_path, grants={name: True for name in GRANTS})
    runner = build_runner(config, fake_model=True)
    assert [value is True for value in _status_fields(runner)] == [False, False]


_EVERY_GRANT = {name: True for name in GRANTS}


def test_policy_can_deny_the_cell_edit_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner, options = _boot_options(
        monkeypatch,
        tmp_path,
        grants=_EVERY_GRANT,
        policy={"allow": ["curie-slack/*"], "deny": ["curie-slack/edit_canvas_cell"]},
    )
    assert ALL_NAMES & set(options.disallowed_tools) == {EDIT_TOOL}
    gate = runner._approval_gate  # noqa: SLF001 - the gate the boot built
    assert gate is not None
    for interceptor in ("hook", "callback"):
        reason = _interception_reason(gate, EDIT_TOOL, interceptor)
        assert "not permitted for this agent" in reason, (interceptor, reason)
        # Liveness: the read-only canvas tools are not refused.
        assert _interception_reason(gate, READ_TOOL, interceptor) == "", interceptor
        assert _interception_reason(gate, LIST_TOOL, interceptor) == "", interceptor


@pytest.mark.parametrize("interceptor", ["hook", "callback"])
def test_policy_can_require_approval_for_the_cell_edit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, interceptor: str
) -> None:
    runner, options = _boot_options(
        monkeypatch,
        tmp_path,
        grants=_EVERY_GRANT,
        policy={
            "allow": ["curie-slack/list_channel_canvases", "curie-slack/read_canvas"],
            "approvalRequired": ["curie-slack/edit_canvas_cell"],
        },
    )
    # Gated, not hidden: the model can still ask for the edit.
    assert EDIT_TOOL not in set(options.disallowed_tools or ())
    gate = runner._approval_gate  # noqa: SLF001 - the gate the boot built
    assert gate is not None
    assert _interception_reason(gate, READ_TOOL, interceptor) == ""
    reason = _interception_reason(gate, EDIT_TOOL, interceptor, dict(EDIT_ARGS))
    assert reason
    assert "not permitted for this agent" not in reason
    assert gate.pending_summary is not None


def test_canvas_tools_are_never_platform_exempt() -> None:
    for name in CANVAS_NAMES:
        assert not is_platform_owned_tool(name, state_server_mounted=True)
        assert not is_platform_owned_tool(
            name, state_server_mounted=True, memory_tools_mounted=True
        )


def test_list_and_read_are_safe_and_the_cell_edit_is_a_side_effect() -> None:
    classifier = SideEffectClassifier()
    assert not classifier.is_side_effecting(LIST_TOOL)
    assert not classifier.is_side_effecting(READ_TOOL)
    assert classifier.is_side_effecting(EDIT_TOOL)


# --- The canvas route URL ---


def _capability(url: str) -> ChannelReadCapability:
    return ChannelReadCapability(url=url, token=_token())


@pytest.mark.parametrize(
    ("url", "derived"),
    [
        ("http://h:8080/x/channel-read", "http://h:8080/x/channel-canvas"),
        ("https://api.example.com/channel-read", "https://api.example.com/channel-canvas"),
        (
            "https://api.example.com/api/v1/channel-read",
            "https://api.example.com/api/v1/channel-canvas",
        ),
    ],
)
def test_the_canvas_route_is_derived_from_the_admitted_read_route(url: str, derived: str) -> None:
    assert canvas_url(_capability(url)) == derived


@pytest.mark.parametrize(
    "url",
    [
        "https://api.example.com/channel-canvas",
        "https://api.example.com/x",
        "https://api.example.com/channel-read/extra",
    ],
)
def test_a_capability_not_on_the_read_route_derives_nothing(url: str) -> None:
    with pytest.raises(ValueError):
        canvas_url(_capability(url))


# --- The tool handlers against the platform route double ---


@pytest.mark.parametrize(
    "state",
    ["never-begun", "begun-without", "ended", "other-origin", "other-path", "canvas-path"],
)
def test_a_canvas_tool_without_a_usable_capability_refuses_without_http(state: str) -> None:
    api, foreign = _ChannelApi(), _ChannelApi()

    async def go() -> tuple[list[dict[str, Any]], bool]:
        async with (
            _served(api, capability=state == "ended") as (server, turn, instance),
            TestServer(foreign.app()) as foreign_server,
        ):
            if state == "begun-without":
                turn.begin(_event(None))
            elif state == "ended":
                turn.end()
            elif state in {"other-origin", "other-path", "canvas-path"}:
                # A capability is never presented anywhere but the trusted read route,
                # not even to the canvas path it would derive.
                other = {
                    "other-origin": _url(foreign_server, READ_ROUTE),
                    "other-path": _url(server, "/x"),
                    "canvas-path": _url(server, CANVAS_ROUTE),
                }
                turn.begin(_event({"url": other[state], "token": _token()}))
            results = [await _call_server(instance, n, a) for n, a, _, _ in ONE_CALL_EACH]
            return results, turn.replay_tainted

    results, tainted = anyio.run(go)
    for result in results:
        _assert_refused(result, NO_CAP)
    assert api.received == foreign.received == []
    assert tainted is False


@pytest.mark.parametrize(("name", "args", "answer", "body"), CANVAS_CASES, ids=_CASE_IDS)
def test_a_canvas_tool_posts_to_the_derived_route_and_labels_content_untrusted(
    name: str, args: dict[str, Any], answer: dict[str, Any], body: dict[str, Any]
) -> None:
    api, token = _ChannelApi(), _token()
    api.payload = answer

    async def go() -> tuple[dict[str, Any], bool]:
        async with _served(api, token=token) as (_, turn, instance):
            result = await _call_server(instance, name, args)
            return result, turn.replay_tainted

    result, tainted = anyio.run(go)
    assert result["is_error"] is False, result
    [(path, header, sent)] = api.received
    assert (path, header) == (CANVAS_ROUTE, token)
    assert sent == body
    assert SENTINEL not in json.dumps(sent)
    payload = json.loads(result["text"])
    assert (payload["content_trust"], payload["source"]) == ("untrusted", "canvas")
    assert payload["operation"] == answer["operation"]
    assert isinstance(payload.get("notice"), str) and payload["notice"]
    assert {k: payload[k] for k, v in answer.items() if v is not None} == {
        k: v for k, v in answer.items() if v is not None
    }
    assert SENTINEL not in result["text"]
    # Canvas text reached the SDK session, so native replay must stay off.
    assert tainted is True


def test_history_tools_still_post_to_the_read_route_beside_canvas_tools() -> None:
    api = _ChannelApi()
    result = _call_once(api, "read_channel_history", {"oldest": "2026-10-03T00:00:00Z"})
    assert result["is_error"] is False, result
    assert [path for path, _, _ in api.received] == [READ_ROUTE]


_MALFORMED: list[tuple[str, dict[str, Any], Any]] = [
    ("list_channel_canvases", {}, _result("list", canvases="x", has_more=False)),
    ("list_channel_canvases", {}, {"operation": "list", "canvases": []}),
    ("read_canvas", {"canvas_id": CANVAS}, _result("read", canvas_id=CANVAS, paragraphs=[])),
    (
        "read_canvas",
        {"canvas_id": CANVAS},
        _result("read", canvas_id=None, tables=[], paragraphs=[f"{MARKER}"]),
    ),
    ("edit_canvas_cell", EDIT_ARGS, _result("edit", canvas_id=CANVAS, edited=False)),
    ("edit_canvas_cell", EDIT_ARGS, _result("edit", canvas_id=CANVAS, edited="true")),
    ("read_canvas", {"canvas_id": CANVAS}, [MARKER]),
]


@pytest.mark.parametrize(
    ("name", "args", "answer"),
    _MALFORMED,
    ids=[
        "list-canvases-not-list",
        "list-no-has-more",
        "read-no-tables",
        "read-no-canvas-id",
        "edit-not-edited",
        "edit-string-true",
        "not-an-object",
    ],
)
def test_a_malformed_200_is_unavailable(name: str, args: dict[str, Any], answer: Any) -> None:
    api = _ChannelApi()
    api.payload = answer
    result = _call_once(api, name, args)
    _assert_refused(result, "channel_read.unavailable")
    assert MARKER not in result["text"]
    assert len(api.received) == 1


def _api_body(payload: Any) -> bytes:
    """Serialized as FastAPI's JSONResponse does: raw UTF-8, compact separators."""

    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _paragraphs(count: int) -> dict[str, Any]:
    text = "a" * 3988 + " [truncated]"
    paragraphs = [{"section_id": None, "text": text, "truncated": True}] * count
    return {**READ_RESULT, "paragraphs": paragraphs}


def test_the_canvas_ceiling_refuses_oversize_and_reads_a_large_legal_canvas_whole() -> None:
    api = _ChannelApi()
    oversized, large = _paragraphs(2400), _paragraphs(1000)

    async def go() -> tuple[dict[str, Any], dict[str, Any]]:
        async with _served(api) as (_, _, instance):
            api.raw = _api_body(oversized)
            assert len(api.raw) > 8 * 1_048_576
            refused = await _call_server(instance, "read_canvas", {"canvas_id": CANVAS})
            api.raw = _api_body(large)
            # Past the history page ceiling (under 3 MiB), so the canvas tools carry their own.
            assert 3 * 1_048_576 < len(api.raw) < 8 * 1_048_576
            whole = await _call_server(instance, "read_canvas", {"canvas_id": CANVAS})
            return refused, whole

    refused, whole = anyio.run(go)
    _assert_refused(refused, "channel_read.unavailable")
    assert "a" * 100 not in refused["text"]
    assert whole["is_error"] is False, whole["text"][:300]
    assert json.loads(whole["text"])["paragraphs"] == large["paragraphs"]
    assert len(api.received) == 2


_REFUSALS: list[tuple[str, dict[str, Any], int, str, dict[str, Any], str | None]] = [
    ("read_canvas", {"canvas_id": CANVAS}, 403, "channel_read.canvas_not_bound", {}, None),
    ("read_canvas", {"canvas_id": CANVAS}, 403, "channel_read.canvas_read_not_granted", {}, None),
    ("list_channel_canvases", {}, 403, "channel_read.canvas_list_not_granted", {}, None),
    ("edit_canvas_cell", EDIT_ARGS, 403, "channel_read.canvas_edit_not_granted", {}, None),
    ("edit_canvas_cell", EDIT_ARGS, 409, "channel_read.section_not_read", {}, None),
    ("edit_canvas_cell", EDIT_ARGS, 422, "channel_read.cell_text_invalid", {}, None),
    ("edit_canvas_cell", EDIT_ARGS, 502, "channel_read.edit_outcome_unknown", {}, None),
    ("read_canvas", {"canvas_id": CANVAS}, 404, "channel_read.canvas_not_found", {}, None),
    (
        "list_channel_canvases",
        {},
        429,
        "channel_read.provider_rate_limited",
        {"retry_after": 7},
        "7",
    ),
]


@pytest.mark.parametrize(
    ("name", "args", "status", "code", "extra", "expect"),
    _REFUSALS,
    ids=[code.removeprefix("channel_read.") for _, _, _, code, _, _ in _REFUSALS],
)
def test_api_refusals_surface_their_code_to_the_model(
    name: str,
    args: dict[str, Any],
    status: int,
    code: str,
    extra: dict[str, Any],
    expect: str | None,
) -> None:
    api = _ChannelApi()
    api.refuse(status, code, **extra)
    result = _call_once(api, name, args)
    _assert_refused(result, code)
    if expect is not None:
        assert expect in result["text"]
    assert MARKER not in result["text"]
    assert [path for path, _, _ in api.received] == [CANVAS_ROUTE]


def test_the_token_stays_out_of_canvas_arguments_results_and_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    api = _ChannelApi()
    seen: list[str] = []

    async def go() -> None:
        async with _served(api) as (_, turn, instance):
            for name, args, answer, _ in ONE_CALL_EACH:
                api.payload = answer
                seen.append(json.dumps(await _call_server(instance, name, args)))
            api.refuse(403, "channel_read.canvas_not_bound")
            refused = await _call_server(instance, "read_canvas", {"canvas_id": CANVAS})
            seen.append(json.dumps(refused))
            seen.extend((repr(turn), str(turn)))

        # Transport failure: the trusted origin has nothing listening.
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = int(sock.getsockname()[1])
        turn = ChannelReadTurn(trusted_origin=("http", "127.0.0.1", port))
        turn.begin(_event({"url": f"http://127.0.0.1:{port}/channel-read", "token": _token()}))
        instance = build_channel_read_server(turn, ALL_GRANTS)["instance"]
        for name, args, _, _ in ONE_CALL_EACH:
            failed = await _call_server(instance, name, args)
            _assert_refused(failed, "channel_read.unavailable")
            assert str(port) not in failed["text"]  # the endpoint is not disclosed either
            assert "/channel-canvas" not in failed["text"]
            seen.append(json.dumps(failed))

    with caplog.at_level(logging.DEBUG):
        anyio.run(go)

    assert len(api.received) == 4
    assert SENTINEL not in json.dumps([body for _, _, body in api.received])
    for text in seen:
        assert SENTINEL not in text, text
    assert SENTINEL not in caplog.text
    assert MARKER not in caplog.text
    assert all(SENTINEL not in repr(record.args) for record in caplog.records)


# --- Retrieved canvas text and the token stay out of everything persisted ---


_Drive = Callable[[Callable[[_Rig], Awaitable[None]]], None]


@pytest.fixture
def drive(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> _Drive:
    """Run a scenario against a production boot granting every tool, and the real ACI app."""

    async def go(scenario: Callable[[_Rig], Awaitable[None]]) -> None:
        api, sessions, store = _ChannelApi(), _patch_sdk(monkeypatch), _Store()
        async with TestServer(api.app()) as api_server:
            base = str(api_server.make_url("")).rstrip("/")
            config = _config(monkeypatch, tmp_path, base, grants=_EVERY_GRANT)
            # The boot runs its own event loop (the connector probe), so build it off this one.
            runner = await anyio.to_thread.run_sync(
                lambda: build_runner(config, fake_model=False, history_store=store)
            )
            await runner.start()
            async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
                await scenario(_Rig(api, base, client, runner, sessions, store))

    return lambda scenario: anyio.run(go, scenario)


_FORBIDDEN_STUB_KEYS = {"title", "tables", "paragraphs", "text", "notice"}


def test_persisted_records_keep_canvas_ids_not_canvas_text_or_the_token(
    drive: _Drive, caplog: pytest.LogCaptureFixture
) -> None:
    async def scenario(rig: _Rig) -> None:
        # Liveness: a turn with no canvas call exports native replay.
        await rig.close_turn(await rig.open(rig.capability(turn="evt-0")))

        turn = await rig.open(rig.capability(turn="evt-1"))
        rig.api.payload = LIST_RESULT
        listed = await rig.sdk.tool_round("call-list", "list_channel_canvases", {})
        rig.api.payload = READ_RESULT
        read = await rig.sdk.tool_round("call-read", "read_canvas", {"canvas_id": CANVAS})
        rig.api.payload = EDIT_RESULT
        edited = await rig.sdk.tool_round("call-edit", "edit_canvas_cell", EDIT_ARGS)
        for result in (listed, read, edited):
            assert result["is_error"] is False, result
        assert MARKER in listed["text"] and MARKER in read["text"]
        rig.api.refuse(403, "channel_read.canvas_not_bound")
        refused = await rig.sdk.tool_round("call-refused", "read_canvas", {"canvas_id": CANVAS})
        assert refused["is_error"] is True
        await rig.close_turn(turn)

        records = rig.store.records
        assert [record.harness_replay is not None for record in records] == [True, False]
        results = _tool_results(records[1])

        list_stub = json.loads(results["call-list"][0])
        assert list_stub["bodies_retained"] is False
        assert list_stub["canvases"] == [{"id": CANVAS, "created": "2026-10-05T12:00:00Z"}]
        read_stub = json.loads(results["call-read"][0])
        assert read_stub["bodies_retained"] is False
        assert read_stub["canvas_id"] == CANVAS
        edit_stub = json.loads(results["call-edit"][0])
        assert edit_stub["bodies_retained"] is False
        assert (edit_stub["canvas_id"], edit_stub["section_id"]) == (CANVAS, SECTION)
        for stub in (list_stub, read_stub, edit_stub):
            assert not set(stub) & _FORBIDDEN_STUB_KEYS, stub
            assert isinstance(stub.get("note"), str) and stub["note"]
        # An error carries no body and is kept verbatim.
        assert results["call-refused"] == (refused["text"], True)

        assert [path for path, _, _ in rig.api.received] == ["/channel-canvas"] * 4
        persisted = json.dumps([record.to_dict() for record in records])
        assert MARKER not in persisted and SENTINEL not in persisted
        assert SENTINEL not in json.dumps([body for _, _, body in rig.api.received])
        assert not any(SENTINEL in value for value in os.environ.values())
        assert SENTINEL not in json.dumps(dict(rig.sdk.options.env or {}))

    with caplog.at_level(logging.DEBUG):
        drive(scenario)
    assert SENTINEL not in caplog.text
    assert MARKER not in caplog.text
    assert all(SENTINEL not in repr(record.args) for record in caplog.records)


def test_a_canvas_capability_header_is_the_minted_token(drive: _Drive) -> None:
    async def scenario(rig: _Rig) -> None:
        turn = await rig.open(rig.capability(turn="evt-1"))
        rig.api.payload = READ_RESULT
        assert (await rig.sdk.call("read_canvas", {"canvas_id": CANVAS}))["is_error"] is False
        await rig.close_turn(turn)
        # After the turn the capability is gone: refused with no request.
        _assert_refused(await rig.sdk.call("read_canvas", {"canvas_id": CANVAS}), NO_CAP)
        assert rig.headers == [_token(turn="evt-1")]
        assert [(path, header) for path, header, _ in rig.api.received] == [
            ("/channel-canvas", _token(turn="evt-1"))
        ]

    drive(scenario)
