"""Worker half of the frozen runner executor route vector (ACTION-EXECUTOR-24).

@spec ACTION-EXECUTOR-24 @spec ACTION-EXECUTOR-6 @spec ACTION-EXECUTOR-5. The
worker's ``execute`` client and the runner's ``/v1/execute`` route ship in
different images, as do the writer and reader of the mode variable, so both
sides read ``tests/vectors/runner-execute.json``
(``runner/tests/test_runner_execute_vector.py`` is the runner half).

The client is ``RunnerClient.execute(base_url, request, *, token)``: it posts
the request with exactly the frozen keys and bearer, returns the phase's
response body, and raises ``ExecuteRefused`` whose ``code`` is the pre-dispatch
code the worker reports for a route refusal, a runner without the route, or a
call whose outcome was lost. The mode variable the worker sets on an executor
claim is ``RUNNER_MODE_ENV`` = ``EXECUTOR_MODE`` in the same module.

Each test serves the frozen runner answer from a real aiohttp server, so the
client's real HTTP path is what is read.

@spec AUTOMATED-REMEDIATION-12 @spec AUTOMATED-REMEDIATION-26. The remediation
``read`` phase (executor amendments E3, E4 and E6): every request carries
``pointer``, the client posts and reads the ``read`` phase, maps
``tool_not_read_only`` to its pre-dispatch code, and never reports a read as
``response_lost``, because a read cannot write.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_worker import runner_client
from curie_worker.runner_client import RunnerClient

_VECTOR = json.loads(
    (Path(__file__).resolve().parents[3] / "tests" / "vectors" / "runner-execute.json").read_text(
        "utf-8"
    )
)
_TOKEN = "example-runner-token"
_KEYS = {
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
    "read",
    "remediation_codes",
}


def test_the_vector_has_only_known_keys() -> None:
    unknown = set(_VECTOR) - _KEYS
    assert not unknown, (
        f"unknown keys in runner-execute.json: {sorted(unknown)}. Teach them to this test "
        "and runner/tests/test_runner_execute_vector.py."
    )
    assert set(_VECTOR) == _KEYS
    for phase, spec in _VECTOR["phases"].items():
        assert set(spec["request"]) == set(_VECTOR["request_keys"]), phase
        assert spec["request"]["phase"] == phase


def test_the_worker_sets_the_frozen_mode_variable() -> None:
    """@spec ACTION-EXECUTOR-5: the runner-private variable, outside ``BootEnv``."""

    assert runner_client.RUNNER_MODE_ENV == _VECTOR["mode_variable"]["name"]
    assert runner_client.EXECUTOR_MODE == _VECTOR["mode_variable"]["value"]


Answer = Callable[[web.Request], Awaitable[web.StreamResponse]]


def _run(answer: Answer, request: dict[str, Any]) -> tuple[Any, list[dict[str, Any]]]:
    """Post ``request`` through the real client to a server answering with ``answer``."""

    seen: list[dict[str, Any]] = []

    async def handler(http_request: web.Request) -> web.StreamResponse:
        seen.append(
            {
                "method": http_request.method,
                "path": http_request.path,
                "authorization": http_request.headers.get("Authorization"),
                "body": await http_request.json(),
            }
        )
        return await answer(http_request)

    async def go() -> Any:
        app = web.Application()
        route = _VECTOR["route"]
        app.router.add_route(route["method"], route["path"], handler)
        server = TestServer(app)
        await server.start_server()
        client = RunnerClient(total_timeout_s=10.0)
        try:
            return await client.execute(str(server.make_url("")).rstrip("/"), request, token=_TOKEN)
        finally:
            await client.close()
            await server.close()

    return asyncio.run(go()), seen


@pytest.mark.parametrize("phase", sorted(_VECTOR["phases"]))
def test_each_phase_posts_the_frozen_request_and_reads_the_frozen_response(phase: str) -> None:
    """@spec ACTION-EXECUTOR-6: one request and one response shape per phase."""

    spec = _VECTOR["phases"][phase]

    async def answer(_request: web.Request) -> web.StreamResponse:
        return web.json_response(spec["response"])

    response, seen = _run(answer, spec["request"])
    (sent,) = seen
    route = _VECTOR["route"]
    assert sent["method"] == route["method"]
    assert sent["path"] == route["path"]
    assert sent["authorization"] == f"{route['auth_scheme']} {_TOKEN}"
    assert sent["body"] == spec["request"]
    assert set(sent["body"]) == set(_VECTOR["request_keys"])
    assert response == spec["response"]
    assert set(response) == set(spec["response_keys"])


@pytest.mark.parametrize("refusal", _VECTOR["refusals"], ids=lambda refusal: refusal["code"])
def test_each_route_refusal_maps_to_its_frozen_worker_code(refusal: dict[str, Any]) -> None:
    """@spec ACTION-EXECUTOR-20: a route refusal becomes one pre-dispatch code."""

    phase = refusal["phases"][0]
    body = {_VECTOR["refusal_body_key"]: refusal["code"]}

    async def answer(_request: web.Request) -> web.StreamResponse:
        return web.json_response(body, status=refusal["status"])

    with pytest.raises(runner_client.ExecuteRefused) as refused:
        _run(answer, _VECTOR["phases"][phase]["request"])
    assert refused.value.code == refusal["worker_code"]


def test_a_runner_without_the_route_is_runner_unavailable_not_a_crash() -> None:
    """@spec ACTION-EXECUTOR-24: an older runner image answers 404."""

    absent = _VECTOR["absent_route"]

    async def answer(_request: web.Request) -> web.StreamResponse:
        return web.json_response({"error": "not found"}, status=absent["status"])

    with pytest.raises(runner_client.ExecuteRefused) as refused:
        _run(answer, _VECTOR["phases"]["list"]["request"])
    assert refused.value.code == absent["worker_code"]


def test_a_lost_call_outcome_is_response_lost() -> None:
    """@spec ACTION-EXECUTOR-17: a call that may have reached the connector is never a refusal."""

    failure = _VECTOR["call_transport_failure"]

    async def answer(_request: web.Request) -> web.StreamResponse:
        return web.json_response(failure["body"], status=failure["status"])

    with pytest.raises(runner_client.ExecuteRefused) as refused:
        _run(answer, _VECTOR["phases"]["call"]["request"])
    assert refused.value.code == failure["worker_code"]


def test_an_oversized_call_result_fails_the_execution() -> None:
    """@spec ACTION-EXECUTOR-6: past the result bound the call failed; it is never retried."""

    from curie_worker.action_executor import call_outcome

    over = _VECTOR["bounds"]["call_over_bound"]

    async def answer(_request: web.Request) -> web.StreamResponse:
        return web.json_response(over["response"])

    response, seen = _run(answer, _VECTOR["phases"]["call"]["request"])
    assert len(seen) == 1
    assert response == over["response"]
    state, code = call_outcome(is_error=response["is_error"], structured=response["structured"])
    assert (state, code) == (over["worker_state"], over["worker_code"])


def test_the_client_sends_exactly_the_frozen_request_keys() -> None:
    """@spec AUTOMATED-REMEDIATION-12: ``pointer`` joins the one request shape."""

    assert set(runner_client.EXECUTE_REQUEST_KEYS) == set(_VECTOR["request_keys"])
    assert set(runner_client.EXECUTE_PHASES) == set(_VECTOR["phases"])


def test_an_unrecognized_read_refusal_is_never_response_lost() -> None:
    """@spec AUTOMATED-REMEDIATION-12: a read never writes, so it is a pre-dispatch code."""

    read = _VECTOR["read"]
    failure = _VECTOR["call_transport_failure"]

    async def answer(_request: web.Request) -> web.StreamResponse:
        return web.json_response(failure["body"], status=failure["status"])

    with pytest.raises(runner_client.ExecuteRefused) as refused:
        _run(answer, _VECTOR["phases"]["read"]["request"])
    assert refused.value.code == read["unknown_refusal_worker_code"]


def test_the_read_refusal_codes_are_pre_dispatch_codes() -> None:
    """@spec AUTOMATED-REMEDIATION-12: ``tool_not_read_only`` is reported as itself.

    Executor amendment E6: the code joins the closed pre-dispatch set; the
    worker's code for it is the vector's.
    """

    added = set(_VECTOR["remediation_codes"]["pre_dispatch_added"])
    read_refusals = [r for r in _VECTOR["refusals"] if "read" in r["phases"]]
    assert {r["code"] for r in read_refusals} >= {_VECTOR["read"]["not_read_only_refusal"]}
    for refusal in read_refusals:
        if refusal["code"] in added:
            assert refusal["worker_code"] == refusal["code"]
