"""Runner half of the frozen runner executor route vector (ACTION-EXECUTOR-24).

@spec ACTION-EXECUTOR-6 @spec ACTION-EXECUTOR-24 @spec ACTION-EXECUTOR-7
@spec ACTION-EXECUTOR-15. In executor mode (``RUNNER_MODE_ENV`` =
``EXECUTOR_MODE``) the runner serves ``create_executor_app``: ``/healthz``, the
two status routes with the executor mode body, and ``POST /v1/execute``; every
other control route answers 409 naming the mode. The worker's client and boot
env builder read ``tests/vectors/runner-execute.json`` in another image
(``apps/worker/tests/test_runner_execute_vector.py``).

``create_executor_app(connectors, token, attestation)`` takes the connector
MCP entries an ordinary boot derives (name to entry), the per-claim bearer and
the credential-free boot attestation. Each test drives the real route against
``fixtures/mcp_executor_connector.py`` over a real stdio MCP session; the
fixture logs every ``tools/call`` it receives, which is how a refusal is
proved to have dialed nothing. The canonical argument texts come from
``tests/vectors/action-canonical-arguments.json`` and the ``observe_version``
replies from ``tests/vectors/executor-restore-calls.json``.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import socket
import sys
import threading
import time
from collections.abc import Awaitable, Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
import uvicorn
from aiohttp.test_utils import TestClient, TestServer

_VECTORS = Path(__file__).resolve().parents[2] / "tests" / "vectors"
_ROUTE = json.loads((_VECTORS / "runner-execute.json").read_text("utf-8"))
_CANONICAL = json.loads((_VECTORS / "action-canonical-arguments.json").read_text("utf-8"))
_CALLS = json.loads((_VECTORS / "executor-restore-calls.json").read_text("utf-8"))
_FIXTURE = Path(__file__).parent / "fixtures" / "mcp_executor_connector.py"
_TOKEN = "example-runner-token"
_GRANT_HEADER = "X-Curie-Connector-Grant"
_CONNECTOR = _ROUTE["phases"]["list"]["request"]["connector"]
_ATTESTATION = {
    "session_id": "example-session",
    "sandbox_id": "example-sandbox",
    "managed_workspace": False,
    "cwd": None,
}
_ROUTE_KEYS = {
    "comment",
    "mode_variable",
    "route",
    "serves",
    "request_keys",
    "phases",
    "sequences",
    "refused_sequences",
    "refused_tool_sequences",
    "refused_call_consumes",
    "bounds",
    "refusal_body_key",
    "refusals",
    "call_transport_failure",
    "absent_route",
    "unauthenticated",
    "status_body",
    "other_routes",
}


def _server():  # noqa: ANN202 - the production module under test
    from curie_runner import server

    return server


def test_the_route_vector_has_only_known_keys() -> None:
    unknown = set(_ROUTE) - _ROUTE_KEYS
    assert not unknown, (
        f"unknown keys in runner-execute.json: {sorted(unknown)}. Teach them to this test "
        "and apps/worker/tests/test_runner_execute_vector.py."
    )
    assert set(_ROUTE) == _ROUTE_KEYS


def test_the_runner_reads_the_frozen_mode_variable() -> None:
    """@spec ACTION-EXECUTOR-5: the runner-private variable, outside ``BootEnv``."""

    server = _server()
    assert server.RUNNER_MODE_ENV == _ROUTE["mode_variable"]["name"]
    assert server.EXECUTOR_MODE == _ROUTE["mode_variable"]["value"]


# --------------------------------------------------------------------------- #
# Harness
# --------------------------------------------------------------------------- #


def _load_fixture() -> Any:
    spec = importlib.util.spec_from_file_location("_mcp_executor_connector", _FIXTURE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_CONNECTOR_FIXTURE = _load_fixture()
_ENV_SETTINGS = {
    "CURIE_TEST_EXECUTOR_TOOLS": ("tools", str),
    "CURIE_TEST_OBSERVE_REPLY": ("observe_reply", json.loads),
    "CURIE_TEST_CALL_REPLY": ("call_reply", json.loads),
    "CURIE_TEST_LIST_PAGES": ("list_pages", int),
    "CURIE_TEST_CALL_RESULT_BYTES": ("call_result_bytes", int),
}


class _Hosted:
    """The fixture connector over streamable HTTP on a loopback port."""

    def __init__(self, settings: dict[str, Any]) -> None:
        self.requests: list[dict[str, Any]] = []
        app = _CONNECTOR_FIXTURE.http_app(settings, self.requests)
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(32)
        self._socket = listener
        host, port = listener.getsockname()[:2]
        self._server = uvicorn.Server(
            uvicorn.Config(app, host=host, port=port, access_log=False, log_level="critical")
        )
        self._thread = threading.Thread(
            target=self._server.run, kwargs={"sockets": [listener]}, daemon=True
        )
        self._thread.start()
        for _ in range(250):
            if self._server.started:
                break
            time.sleep(0.02)
        else:
            raise RuntimeError("fixture connector did not start")
        self.url = f"http://{host}:{port}/mcp"

    def calls(self) -> list[dict[str, Any]]:
        return [
            {
                "name": request["params"]["name"],
                "arguments": request["params"].get("arguments") or {},
                "headers": request["headers"],
            }
            for request in self.requests
            if request["method"] == "tools/call"
        ]

    def stop(self) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=5)
        self._socket.close()


_HOSTED: dict[Path, _Hosted] = {}


@pytest.fixture(autouse=True)
def _stop_hosted_connectors() -> Iterator[None]:
    yield
    while _HOSTED:
        _HOSTED.popitem()[1].stop()


def _connector(tmp_path: Path, **env: str) -> dict[str, Any]:
    """A hosted (URL) connector, the only kind a ``call`` grant can ride to."""

    settings = {
        key: parse(env[name]) for name, (key, parse) in _ENV_SETTINGS.items() if name in env
    }
    hosted = _Hosted(settings)
    _HOSTED[tmp_path] = hosted
    return {"type": "http", "url": hosted.url}


def _stdio_connector(tmp_path: Path, **env: str) -> dict[str, Any]:
    """A stdio connector: reachable for ``list`` and ``observe``, but not by URL."""

    return {
        "command": sys.executable,
        "args": [str(_FIXTURE)],
        "env": {"CURIE_TEST_EXECUTOR_CALLS": str(tmp_path / "calls.jsonl"), **env},
    }


def _calls(tmp_path: Path) -> list[dict[str, Any]]:
    if tmp_path in _HOSTED:
        return _HOSTED[tmp_path].calls()
    log = tmp_path / "calls.jsonl"
    if not log.exists():
        return []
    return [json.loads(line) for line in log.read_text("utf-8").splitlines() if line]


def _list_requests(tmp_path: Path) -> int:
    return sum(1 for request in _HOSTED[tmp_path].requests if request["method"] == "tools/list")


Drive = Callable[[TestClient], Awaitable[None]]


def _drive(connectors: dict[str, Any], drive: Drive) -> None:
    async def go() -> None:
        app = _server().create_executor_app(connectors, _TOKEN, _ATTESTATION)
        async with TestClient(TestServer(app)) as client:
            await drive(client)

    asyncio.run(go())


def _auth() -> dict[str, str]:
    return {_ROUTE["route"]["auth_header"]: f"{_ROUTE['route']['auth_scheme']} {_TOKEN}"}


def _request(phase: str, **overrides: Any) -> dict[str, Any]:
    request = dict(_ROUTE["phases"][phase]["request"])
    request.update(overrides)
    return request


def _forward_call(**overrides: Any) -> dict[str, Any]:
    scale = next(case for case in _CANONICAL["vectors"] if case["name"] == "flat_object")
    return _request("call", **{"tool": "scale", "arguments": scale["canonical"], **overrides})


async def _post(client: TestClient, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    response = await client.post(_ROUTE["route"]["path"], json=body, headers=_AUTH)
    return response.status, await response.json()


_AUTH = _auth()


def _refusal(code: str) -> dict[str, Any]:
    return next(refusal for refusal in _ROUTE["refusals"] if refusal["code"] == code)


def _assert_refused(status: int, body: dict[str, Any], code: str) -> None:
    refusal = _refusal(code)
    assert status == refusal["status"], body
    assert body == {_ROUTE["refusal_body_key"]: code}


# --------------------------------------------------------------------------- #
# Status, auth and the other control routes
# --------------------------------------------------------------------------- #


def test_the_executor_status_body_is_frozen(tmp_path: Path) -> None:
    """@spec ACTION-EXECUTOR-6: no capacity admission fields; attestation on /v1/status."""

    spec = _ROUTE["status_body"]

    async def drive(client: TestClient) -> None:
        for path, headers in (("/status", {}), ("/v1/status", _AUTH)):
            response = await client.get(path, headers=headers)
            assert response.status == 200, path
            body = await response.json()
            expected_keys = set(spec["keys"])
            if path == "/v1/status":
                expected_keys |= set(spec["attestation_keys_on_v1_status"])
            assert set(body) == expected_keys, path
            for key, value in spec["fixed"].items():
                assert body[key] == value, key
            for key in spec["booleans"]:
                assert isinstance(body[key], bool), key
            assert body["turn_active"] is False
            assert not set(spec["never"]) & set(body)
            if path == "/v1/status":
                for key in spec["attestation_keys_on_v1_status"]:
                    assert body[key] == _ATTESTATION[key], key
        health = await client.get("/healthz")
        assert health.status == 200

    _drive({_CONNECTOR: _connector(tmp_path)}, drive)


def test_execute_and_v1_status_require_the_bearer(tmp_path: Path) -> None:
    """@spec ACTION-EXECUTOR-6: ``/v1/execute`` joins ``_GATED_PATHS``."""

    async def drive(client: TestClient) -> None:
        response = await client.post(_ROUTE["route"]["path"], json=_request("list"))
        assert response.status == _ROUTE["unauthenticated"]["status"]
        status = await client.get("/v1/status")
        assert status.status == _ROUTE["unauthenticated"]["status"]

    _drive({_CONNECTOR: _connector(tmp_path)}, drive)
    assert _calls(tmp_path) == []


@pytest.mark.parametrize("route", _ROUTE["other_routes"]["routes"])
def test_every_other_control_route_is_refused_naming_the_mode(tmp_path: Path, route: str) -> None:
    """@spec ACTION-EXECUTOR-6: ``/v1/event`` and the rest answer 409 in executor mode."""

    method, path = route.split(" ", 1)
    spec = _ROUTE["other_routes"]

    async def drive(client: TestClient) -> None:
        response = await client.request(method, path, json={}, headers=_AUTH)
        assert response.status == spec["status"]
        body = await response.json()
        assert body[spec["body_mode_key"]] == _ROUTE["mode_variable"]["value"]

    _drive({_CONNECTOR: _connector(tmp_path)}, drive)


# --------------------------------------------------------------------------- #
# Phases
# --------------------------------------------------------------------------- #


def test_a_restore_runs_list_observe_call_with_the_frozen_shapes(tmp_path: Path) -> None:
    """@spec ACTION-EXECUTOR-6 @spec ACTION-EXECUTOR-15: one dial per phase, frozen bodies."""

    phases = _ROUTE["phases"]
    reply = phases["call"]["response"]["structured"]
    observed = phases["observe"]["response"]["version"]
    connector = _connector(
        tmp_path,
        CURIE_TEST_OBSERVE_REPLY=json.dumps({"version": observed}),
        CURIE_TEST_CALL_REPLY=json.dumps(reply),
    )

    async def drive(client: TestClient) -> None:
        status, listed = await _post(client, phases["list"]["request"])
        assert status == 200
        assert listed == phases["list"]["response"]
        for tool in listed["tools"]:
            expected = set(phases["list"]["tool_keys"])
            if tool["name"] in phases["list"]["schema_tools"]:
                expected |= {"input_schema"}
            assert set(tool) == expected, tool["name"]

        status, seen = await _post(client, phases["observe"]["request"])
        assert status == 200
        assert seen == phases["observe"]["response"]

        status, called = await _post(client, phases["call"]["request"])
        assert status == 200
        assert called == phases["call"]["response"]
        assert set(called) == set(phases["call"]["response_keys"])

    _drive({_CONNECTOR: connector}, drive)
    calls = _calls(tmp_path)
    assert [call["name"] for call in calls] == [_CALLS["observe_tool"], _CALLS["restore_tool"]]
    assert calls[0]["arguments"] == {"target": phases["observe"]["request"]["target"]}
    assert calls[0]["arguments"] == _CALLS["observe_arguments"]
    assert calls[1]["arguments"] == json.loads(phases["call"]["request"]["arguments"])
    # The grant rides only the write call, to the hosted connector.
    grant = _GRANT_HEADER.lower()
    assert calls[1]["headers"][grant] == phases["call"]["request"]["grant"]
    assert grant not in calls[0]["headers"]


def test_a_forward_action_runs_list_then_call(tmp_path: Path) -> None:
    """@spec ACTION-EXECUTOR-6: the forward sequence dials exactly one write call."""

    async def drive(client: TestClient) -> None:
        assert (await _post(client, _request("list")))[0] == 200
        status, called = await _post(client, _forward_call())
        assert status == 200
        assert set(called) == set(_ROUTE["phases"]["call"]["response_keys"])

    _drive({_CONNECTOR: _connector(tmp_path)}, drive)
    assert [call["name"] for call in _calls(tmp_path)] == ["scale"]


@pytest.mark.parametrize("reply", _CALLS["observe_replies"], ids=lambda reply: reply["name"])
def test_observe_returns_the_frozen_version(tmp_path: Path, reply: dict[str, Any]) -> None:
    """@spec ACTION-EXECUTOR-15: a string passes unjudged; anything else is null."""

    env = {}
    if reply["structured"] is not None:
        env["CURIE_TEST_OBSERVE_REPLY"] = json.dumps(reply["structured"])

    async def drive(client: TestClient) -> None:
        assert (await _post(client, _request("list")))[0] == 200
        status, body = await _post(client, _request("observe"))
        assert status == 200
        assert body == {"phase": "observe", "version": reply["version"]}

    _drive({_CONNECTOR: _connector(tmp_path, **env)}, drive)


def _step(phase: str, before: list[str]) -> dict[str, Any]:
    """The request for ``phase`` after ``before``: a ``call`` after ``observe`` is a restore.

    @spec ACTION-EXECUTOR-6 (amended): ``observe`` is accepted only before a
    restore, so a sequence's ``call`` takes the restore request once ``observe``
    has run and the forward request otherwise.
    """

    if phase != "call":
        return _request(phase)
    return _request("call") if "observe" in before else _forward_call()


@pytest.mark.parametrize(
    "sequence", _ROUTE["refused_sequences"], ids=lambda sequence: "-".join(sequence)
)
def test_an_out_of_order_phase_is_refused_without_dialing(
    tmp_path: Path, sequence: list[str]
) -> None:
    """@spec ACTION-EXECUTOR-6: anything but the accepted orders, a second call included."""

    *prefix, last = sequence
    observe_reply = json.dumps({"version": "rv-1041"})

    async def drive(client: TestClient) -> None:
        for index, phase in enumerate(prefix):
            status, body = await _post(client, _step(phase, prefix[:index]))
            assert status == 200, (phase, body)
        before = len(_calls(tmp_path))
        status, body = await _post(client, _step(last, prefix))
        _assert_refused(status, body, "phase_out_of_order")
        assert len(_calls(tmp_path)) == before

    _drive({_CONNECTOR: _connector(tmp_path, CURIE_TEST_OBSERVE_REPLY=observe_reply)}, drive)


@pytest.mark.parametrize(
    "case",
    _CANONICAL["non_canonical_texts"] + _CANONICAL["not_an_object_texts"],
    ids=lambda case: case["name"],
)
def test_a_non_canonical_argument_text_is_refused_before_the_call(
    tmp_path: Path, case: dict[str, Any]
) -> None:
    """@spec ACTION-EXECUTOR-7: the text must already be its own canonical form."""

    async def drive(client: TestClient) -> None:
        assert (await _post(client, _request("list")))[0] == 200
        status, body = await _post(client, _forward_call(arguments=case["text"]))
        _assert_refused(status, body, _CANONICAL["refusal"])

    _drive({_CONNECTOR: _connector(tmp_path)}, drive)
    assert _calls(tmp_path) == []


def test_an_unadvertised_tool_is_refused_before_the_call(tmp_path: Path) -> None:
    """@spec ACTION-EXECUTOR-6: preflight, the tool is advertised."""

    async def drive(client: TestClient) -> None:
        assert (await _post(client, _request("list")))[0] == 200
        status, body = await _post(client, _forward_call(tool="example_unadvertised"))
        _assert_refused(status, body, "tool_not_advertised")

    _drive({_CONNECTOR: _connector(tmp_path)}, drive)
    assert _calls(tmp_path) == []


@pytest.mark.parametrize(
    ("tools", "code"),
    [("no_restore", "restore_not_advertised"), ("readonly_restore", "restore_schema_mismatch")],
)
def test_a_restore_that_fails_the_capability_rule_is_refused(
    tmp_path: Path, tools: str, code: str
) -> None:
    """@spec ACTION-EXECUTOR-6 @spec ACTION-EXECUTOR-13: the rule is rechecked before a restore."""

    connector = _connector(
        tmp_path,
        CURIE_TEST_EXECUTOR_TOOLS=tools,
        CURIE_TEST_OBSERVE_REPLY=json.dumps({"version": "rv-1041"}),
    )

    async def drive(client: TestClient) -> None:
        assert (await _post(client, _request("list")))[0] == 200
        assert (await _post(client, _request("observe")))[0] == 200
        status, body = await _post(client, _request("call"))
        _assert_refused(status, body, code)

    _drive({_CONNECTOR: connector}, drive)
    assert _CALLS["restore_tool"] not in [call["name"] for call in _calls(tmp_path)]


@pytest.mark.parametrize(
    "body",
    [
        pytest.param({**_ROUTE["phases"]["list"]["request"], "extra": 1}, id="unknown_key"),
        pytest.param(
            {k: v for k, v in _ROUTE["phases"]["list"]["request"].items() if k != "grant"},
            id="missing_key",
        ),
        pytest.param({**_ROUTE["phases"]["list"]["request"], "phase": "write"}, id="unknown_phase"),
    ],
)
def test_a_malformed_request_is_invalid_without_dialing(
    tmp_path: Path, body: dict[str, Any]
) -> None:
    """@spec ACTION-EXECUTOR-24: exactly the frozen request keys and phases."""

    async def drive(client: TestClient) -> None:
        status, answer = await _post(client, body)
        _assert_refused(status, answer, "invalid_request")

    _drive({_CONNECTOR: _connector(tmp_path)}, drive)
    assert _calls(tmp_path) == []


def test_an_unreachable_connector_is_refused_on_list(tmp_path: Path) -> None:
    """@spec ACTION-EXECUTOR-20: ``connector_unreachable`` before any write."""

    broken = {"command": str(tmp_path / "example-missing-connector"), "args": []}

    async def drive(client: TestClient) -> None:
        status, body = await _post(client, _request("list"))
        _assert_refused(status, body, "connector_unreachable")

    _drive({_CONNECTOR: broken}, drive)


# --------------------------------------------------------------------------- #
# Review round: one call per sandbox, forward order, grant, bounds
# --------------------------------------------------------------------------- #


def test_a_call_refused_by_preflight_still_spends_the_sandbox_call(tmp_path: Path) -> None:
    """@spec ACTION-EXECUTOR-6: a refused ``call`` consumes the sequence; nothing dials."""

    spec = _ROUTE["refused_call_consumes"]

    async def drive(client: TestClient) -> None:
        assert (await _post(client, _request("list")))[0] == 200
        status, body = await _post(client, _forward_call(tool=spec["first_tool"]))
        _assert_refused(status, body, spec["first_refusal"])
        status, body = await _post(client, _forward_call(tool=spec["second_tool"]))
        _assert_refused(status, body, spec["second_refusal"])

    _drive({_CONNECTOR: _connector(tmp_path)}, drive)
    assert _calls(tmp_path) == []


@pytest.mark.parametrize("case", _ROUTE["refused_tool_sequences"], ids=lambda case: case["name"])
def test_the_call_tool_decides_whether_observe_belongs(
    tmp_path: Path, case: dict[str, Any]
) -> None:
    """@spec ACTION-EXECUTOR-6: forward is ``list`` then ``call``; restore needs ``observe``."""

    *prefix, last = case["phases"]
    assert last == "call"

    async def drive(client: TestClient) -> None:
        for phase in prefix:
            assert (await _post(client, _request(phase)))[0] == 200, phase
        before = [call["name"] for call in _calls(tmp_path)]
        request = _request("call") if case["tool"] == _CALLS["restore_tool"] else _forward_call()
        status, body = await _post(client, {**request, "tool": case["tool"]})
        _assert_refused(status, body, case["refusal"])
        assert [call["name"] for call in _calls(tmp_path)] == before

    connector = _connector(tmp_path, CURIE_TEST_OBSERVE_REPLY=json.dumps({"version": "rv-1041"}))
    _drive({_CONNECTOR: connector}, drive)
    assert case["tool"] not in [call["name"] for call in _calls(tmp_path)]


def test_a_call_the_grant_cannot_ride_to_is_refused_before_dispatch(tmp_path: Path) -> None:
    """@spec ACTION-EXECUTOR-6: a connector with no URL never gets an ungranted write."""

    async def drive(client: TestClient) -> None:
        assert (await _post(client, _request("list")))[0] == 200
        status, body = await _post(client, _forward_call())
        _assert_refused(status, body, "connector_not_hosted")

    _drive({_CONNECTOR: _stdio_connector(tmp_path)}, drive)
    assert _calls(tmp_path) == []


def test_list_pagination_past_the_bound_is_refused_without_retrying(tmp_path: Path) -> None:
    """@spec ACTION-EXECUTOR-6: at most ``list_pages`` pages, then a refusal, no retry."""

    bounds = _ROUTE["bounds"]
    pages = bounds["list_pages"]

    async def drive(client: TestClient) -> None:
        status, body = await _post(client, _request("list"))
        _assert_refused(status, body, bounds["list_over_bound"]["refusal"])

    _drive({_CONNECTOR: _connector(tmp_path, CURIE_TEST_LIST_PAGES=str(pages * 3))}, drive)
    assert pages <= _list_requests(tmp_path) <= pages + 1


def test_list_pagination_at_the_bound_is_served(tmp_path: Path) -> None:
    """@spec ACTION-EXECUTOR-6: the bound is inclusive."""

    pages = _ROUTE["bounds"]["list_pages"]

    async def drive(client: TestClient) -> None:
        status, body = await _post(client, _request("list"))
        assert status == 200, body
        assert body == _ROUTE["phases"]["list"]["response"]

    _drive({_CONNECTOR: _connector(tmp_path, CURIE_TEST_LIST_PAGES=str(pages))}, drive)
    assert _list_requests(tmp_path) == pages


def test_a_call_result_past_the_bound_fails_without_retrying(tmp_path: Path) -> None:
    """@spec ACTION-EXECUTOR-6: the write landed once; the oversized result is an error."""

    bounds = _ROUTE["bounds"]

    async def drive(client: TestClient) -> None:
        assert (await _post(client, _request("list")))[0] == 200
        status, body = await _post(client, _forward_call())
        assert status == 200, body
        assert body == bounds["call_over_bound"]["response"]

    connector = _connector(tmp_path, CURIE_TEST_CALL_RESULT_BYTES=str(bounds["call_result_bytes"]))
    _drive({_CONNECTOR: connector}, drive)
    assert [call["name"] for call in _calls(tmp_path)] == ["scale"]


@pytest.mark.parametrize("case", _CANONICAL["non_finite_texts"], ids=lambda case: case["name"])
def test_a_non_finite_argument_text_is_refused_before_the_call(
    tmp_path: Path, case: dict[str, Any]
) -> None:
    """@spec ACTION-EXECUTOR-7: NaN and infinities are never canonical."""

    async def drive(client: TestClient) -> None:
        assert (await _post(client, _request("list")))[0] == 200
        status, body = await _post(client, _forward_call(arguments=case["text"]))
        _assert_refused(status, body, _CANONICAL["refusal"])

    _drive({_CONNECTOR: _connector(tmp_path)}, drive)
    assert _calls(tmp_path) == []
