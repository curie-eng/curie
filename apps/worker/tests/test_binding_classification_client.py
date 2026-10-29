"""Synthetic HTTP boundary tests for the worker's binding classification question.

@spec HTTP-INVOCATION-EXECUTION-22.
MockTransport is an external API fixture, not installation qualification.
"""

from __future__ import annotations

import asyncio
import copy
import json
from typing import Any

import httpx
import pytest

UUID = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"
TOKEN = "SyntheticPrivateWorkerToken"
MARKER = "SyntheticUntrustedResponseMarker"
BASE = "https://core.example.test"
PATH = "/v1/internal/identity/binding-classification/check"


def api() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-22: the existing worker authority client."""
    from curie_worker import identity_authority

    assert callable(
        getattr(identity_authority.HttpTurnAuthorityClient, "classify_binding", None)
    ), "worker binding classification client is not implemented"
    return identity_authority


def fixed_error(error: Any) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-22: no supplied bytes or chaining."""
    assert error.value.__cause__ is None
    assert error.value.__context__ is None or error.value.__suppress_context__
    assert not any(value in str(error.value) + repr(error.value) for value in (TOKEN, MARKER))


def contract() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-22: missing DTOs fail in the test body."""
    from curie_internal import identity_authority

    for symbol in ("BindingClassificationRequestV1", "BindingClassificationResponseV1"):
        assert hasattr(identity_authority, symbol), f"shared DTO missing {symbol}"
    return identity_authority


def wire_request() -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-22: independent complete request fixture."""
    return {
        "schema_version": 1,
        "request_id": UUID,
        "scope": {
            "tenant_id": UUID,
            "agent_id": OTHER,
            "binding_id": UUID,
            "binding_generation": 7,
        },
    }


def echo(request: dict[str, Any], **changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-22: derive the echo from the submitted wire."""
    return {
        "schema_version": 1,
        "request_id": request["request_id"],
        "scope": copy.deepcopy(request["scope"]),
        "http_identity_required": False,
        **changes,
    }


def _run(handler: Any) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-22: one classification through MockTransport."""
    module = api()
    dto = contract()

    async def go() -> Any:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=60) as http:
            client = module.HttpTurnAuthorityClient(
                api_base_url=BASE, worker_token=TOKEN, client=http
            )
            result = await client.classify_binding(
                dto.BindingClassificationRequestV1.model_validate(wire_request())
            )
            assert not http.is_closed
            return result

    return asyncio.run(go())


@pytest.mark.parametrize("required", [True, False])
def test_exact_post_and_echo_return_the_classification(required: bool) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-22: one POST, worker credential only."""
    dto = contract()
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200, json=echo(json.loads(request.content), http_identity_required=required)
        )

    result = _run(handler)
    assert isinstance(result, dto.BindingClassificationResponseV1)
    assert result.http_identity_required is required
    with pytest.raises(TypeError):
        bool(result)
    assert len(seen) == 1
    request = seen[0]
    assert request.method == "POST" and str(request.url) == BASE + PATH
    assert request.headers["X-Curie-Worker-Token"] == TOKEN
    assert not any(name in request.headers for name in ("Authorization", "X-API-Key", "Cookie"))
    assert json.loads(request.content) == wire_request()


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(409, json={"detail": "binding_scope_changed"}),
        httpx.Response(500, text=MARKER),
        httpx.Response(200, text="not json " + MARKER),
        httpx.Response(200, json=echo(wire_request(), request_id=OTHER)),
        httpx.Response(200, json=echo(wire_request(), http_identity_required=1)),
        httpx.Response(
            200,
            json=echo(
                wire_request(),
                scope={**wire_request()["scope"], "binding_generation": 8},
            ),
        ),
        httpx.Response(200, content=b"{" + b" " * 70_000 + b"}"),
    ],
)
def test_every_failure_is_the_fixed_unavailable_error(response: httpx.Response) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-22: unanswered is unavailable, never legacy."""
    module = api()

    async def handler(_request: httpx.Request) -> httpx.Response:
        return response

    with pytest.raises(module.TurnAuthorityUnavailable) as error:
        _run(handler)
    fixed_error(error)


def test_transport_failure_is_the_fixed_unavailable_error() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-22: network failure carries no supplied bytes."""
    module = api()

    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(MARKER, request=request)

    with pytest.raises(module.TurnAuthorityUnavailable) as error:
        _run(handler)
    fixed_error(error)


def test_an_unchecked_request_refuses_before_any_http() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-22: only the shared request model is accepted."""
    module = api()

    async def handler(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("no request may be sent")

    async def go() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = module.HttpTurnAuthorityClient(
                api_base_url=BASE, worker_token=TOKEN, client=http
            )
            with pytest.raises(ValueError, match="^invalid_binding_classification_request$"):
                await client.classify_binding(wire_request())

    asyncio.run(go())
