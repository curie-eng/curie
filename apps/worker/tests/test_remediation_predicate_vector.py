"""Worker half of the frozen remediation predicate vector.

@spec AUTOMATED-REMEDIATION-12 @spec AUTOMATED-REMEDIATION-17
@spec AUTOMATED-REMEDIATION-26. The worker sits between the runner's pointer
extraction and the API's evaluator: it reads each ``read`` phase answer through
``RunnerClient.execute`` and relays the sample, unjudged and unchanged, to
``POST /action-executions/{id}/samples``. The runner and API halves are
``runner/tests/test_runner_execute_vector.py`` and
``apps/api/tests/test_remediation_predicate_vector.py``.

The reader is ``curie_worker.action_executor.sample_report(response, *,
lease_owner, attempt, index)``: the closed sample body for one ``read`` phase
response, which raises ``ValueError`` for a response that is not a frozen sample
(an unknown kind, a non-scalar value, or a value on a kind that carries none).
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_worker.runner_client import RunnerClient

_VECTORS = Path(__file__).resolve().parents[3] / "tests" / "vectors"
_VECTOR = json.loads((_VECTORS / "remediation-predicate.json").read_text("utf-8"))
_ROUTE = json.loads((_VECTORS / "runner-execute.json").read_text("utf-8"))
_KEYS = {
    "comment",
    "comparators",
    "ordering_comparators",
    "numeric_string_grammar",
    "in_list_max",
    "value_max_chars",
    "sample_kinds",
    "successful",
    "results",
    "extractions",
    "invalid_pointers",
    "sample_report",
    "evaluations",
}
_REPORT = _VECTOR["sample_report"]


def _sample_report() -> Any:  # noqa: ANN401 - the production function under test
    from curie_worker.action_executor import sample_report

    return sample_report


def test_the_vector_has_only_known_keys() -> None:
    unknown = set(_VECTOR) - _KEYS
    assert not unknown, (
        f"unknown keys in remediation-predicate.json: {sorted(unknown)}. Teach them to this "
        "test, apps/api/tests/test_remediation_predicate_vector.py and "
        "runner/tests/test_runner_execute_vector.py."
    )
    assert set(_VECTOR) == _KEYS


def _read_response(case: dict[str, Any]) -> dict[str, Any]:
    return {"phase": "read", **case["sample"]}


def _through_the_client(response: dict[str, Any]) -> Any:  # noqa: ANN401 - a JSON body
    route = _ROUTE["route"]

    async def handler(_request: web.Request) -> web.StreamResponse:
        return web.json_response(response)

    async def go() -> Any:  # noqa: ANN401 - a JSON body
        app = web.Application()
        app.router.add_route(route["method"], route["path"], handler)
        server = TestServer(app)
        await server.start_server()
        client = RunnerClient(total_timeout_s=10.0)
        try:
            return await client.execute(
                str(server.make_url("")).rstrip("/"),
                _ROUTE["phases"]["read"]["request"],
                token="example-runner-token",
            )
        finally:
            await client.close()
            await server.close()

    return asyncio.run(go())


@pytest.mark.parametrize("case", _VECTOR["extractions"], ids=lambda case: case["name"])
def test_each_sample_is_read_and_relayed_unchanged(case: dict[str, Any]) -> None:
    """@spec AUTOMATED-REMEDIATION-12: the worker never judges or reshapes a sample."""

    response = _through_the_client(_read_response(case))
    body = _sample_report()(
        response,
        lease_owner=_REPORT["fence"]["lease_owner"],
        attempt=_REPORT["fence"]["attempt"],
        index=_REPORT["index"],
    )
    assert set(body) == set(_REPORT["keys"])
    expected = {**_REPORT["fence"], "index": _REPORT["index"], **case["sample"]}
    # Exact JSON, so 1.0 stays a float, true stays a boolean and null stays null.
    assert json.dumps(body, sort_keys=True) == json.dumps(expected, sort_keys=True)


@pytest.mark.parametrize(
    "response",
    [
        pytest.param({"phase": "read", "sample": "example_unknown", "value": None}, id="kind"),
        pytest.param({"phase": "read", "sample": "value", "value": {"x": 1}}, id="non_scalar"),
        pytest.param(
            {"phase": "read", "sample": "pointer_absent", "value": 1}, id="value_on_absent"
        ),
        pytest.param({"phase": "read", "sample": "value"}, id="missing_value"),
    ],
)
def test_a_response_that_is_not_a_frozen_sample_is_refused(response: dict[str, Any]) -> None:
    """@spec AUTOMATED-REMEDIATION-12: only the closed sample kinds cross to the API."""

    with pytest.raises(ValueError):
        _sample_report()(
            response,
            lease_owner=_REPORT["fence"]["lease_owner"],
            attempt=_REPORT["fence"]["attempt"],
            index=_REPORT["index"],
        )
