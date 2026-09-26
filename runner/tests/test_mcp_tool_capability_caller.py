"""A hosted connector's caller proxy refusing this sandbox, named as such.

ADR-0168 decision 7. The proxy's refusal shape is frozen in
tests/vectors/connector-caller-refusal.json; this reads the same file the
proxy's tests do, so the runner cannot misread a refusal the proxy sends.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner.connectors import CALLER_HEADER
from curie_runner.mcp_tool_capability import (
    ConnectorCapabilityFailure,
    probe_mcp_tool_capability,
    reprobe_connector_failures,
)

_REFUSAL = json.loads(
    (
        Path(__file__).resolve().parents[2] / "tests" / "vectors" / "connector-caller-refusal.json"
    ).read_text(encoding="utf-8")
)
_TOKEN_ENV = "CURIE_CONNECTOR_CALLER_TOKEN"


@asynccontextmanager
async def _answering(
    status: int, body: bytes, content_type: str
) -> AsyncIterator[tuple[str, list[str]]]:
    seen: list[str] = []

    async def answer(request: web.Request) -> web.Response:
        seen.append(request.path)
        return web.Response(status=status, body=body, content_type=content_type)

    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", answer)
    server = TestServer(app)
    await server.start_server()
    try:
        yield str(server.make_url("/mcp")), seen
    finally:
        await server.close()


def _derived(url: str) -> dict[str, dict[str, Any]]:
    return {
        "grafana": {
            "type": "http",
            "url": url,
            "headers": {CALLER_HEADER: f"${{{_TOKEN_ENV}}}"},
        }
    }


# @spec ADR-0168 d7
def test_the_refusal_vector_names_the_header_the_runner_sends() -> None:
    assert set(_REFUSAL) == {"comment", "header", "status", "content_type", "vectors"}
    assert _REFUSAL["header"] == CALLER_HEADER


# @spec ADR-0168 d7
@pytest.mark.parametrize("vector", _REFUSAL["vectors"], ids=lambda v: v["refusal"])
def test_a_refused_caller_is_reported_as_its_own_reason(vector: dict[str, Any]) -> None:
    async def go() -> tuple[ConnectorCapabilityFailure, ...]:
        async with _answering(
            _REFUSAL["status"], json.dumps(vector["body"]).encode(), _REFUSAL["content_type"]
        ) as (url, _seen):
            probe = await probe_mcp_tool_capability(
                None, _derived(url), {_TOKEN_ENV: "cct.payload.signature"}
            )
        return probe.connector_failures

    [failure] = asyncio.run(go())
    assert (failure.connector, failure.reason, failure.refusal) == (
        "grafana",
        "caller_refused",
        vector["refusal"],
    )
    message = failure.caller_message()
    assert message.startswith("declared connector 'grafana' refused this sandbox: ")
    assert "cct.payload.signature" not in message


# @spec ADR-0168 d7
def test_any_other_error_answer_stays_a_probe_failure() -> None:
    async def go() -> tuple[ConnectorCapabilityFailure, ...]:
        async with _answering(403, b"forbidden", "text/plain") as (url, _seen):
            probe = await probe_mcp_tool_capability(
                None, _derived(url), {_TOKEN_ENV: "cct.payload.signature"}
            )
        return probe.connector_failures

    [failure] = asyncio.run(go())
    assert (failure.reason, failure.refusal) == ("probe_failed", None)


# @spec ADR-0168 d7
def test_a_refused_caller_is_not_redialed_at_turn_start() -> None:
    # The token and the list are fixed for this sandbox's life, and the
    # bundled CLI stops dialling a server that refused it, so a re-dial could
    # only report the same refusal.
    async def go() -> tuple[tuple[ConnectorCapabilityFailure, ...], list[str]]:
        async with _answering(200, b"{}", "application/json") as (url, seen):
            failure = ConnectorCapabilityFailure(
                connector="grafana",
                credential_names=(_TOKEN_ENV,),
                reason="caller_refused",
                refusal="not_admitted",
            )
            still = await reprobe_connector_failures(
                (failure,), _derived(url), {_TOKEN_ENV: "cct.payload.signature"}
            )
        return still, seen

    still, seen = asyncio.run(go())
    assert [f.refusal for f in still] == ["not_admitted"]
    assert seen == []
