"""Synthetic HTTP boundary tests for per-stage current authority observations.

@spec HTTP-INVOCATION-EXECUTION-8 HTTP-INVOCATION-EXECUTION-16.
MockTransport is an external API fixture, not provider or installation qualification.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

UUID = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"
CONTEXT = "SyntheticPrivateContextMarker"
TOKEN = "SyntheticPrivateWorkerToken"
MARKER = "SyntheticUntrustedResponseMarker"
BASE = "https://core.example.test"
PATH = "/v1/internal/identity/turn-authority/check"


def api() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-16: absence is a body assertion, never setup failure."""
    name = "curie_worker.identity_authority"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "worker current-authority client is not implemented"
        raise
    for symbol in ("HttpTurnAuthorityClient", "TurnAuthorityUnavailable"):
        assert hasattr(module, symbol), f"worker authority client missing {symbol}"
    return module


def contract() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-16."""
    name = "curie_internal.identity_authority"
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "shared turn authority DTOs are not implemented"
        raise


def wire_request() -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-16: independent complete request fixture."""
    return {
        "schema_version": 1,
        "request_id": UUID,
        "stage": "prepare",
        "context": CONTEXT,
        "payload_sha256": "a" * 64,
        "event_id": "synthetic-event",
        "approval_id": None,
        "scope": {
            "tenant_id": UUID,
            "agent_id": OTHER,
            "binding_id": UUID,
            "binding_generation": 7,
        },
    }


def echo(request: dict[str, Any], **changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-16: derive echo from actual submitted wire."""
    return {
        "schema_version": 1,
        "request_id": request["request_id"],
        "stage": request["stage"],
        "event_id": request["event_id"],
        "scope": copy.deepcopy(request["scope"]),
        "context_sha256": hashlib.sha256(request["context"].encode("ascii")).hexdigest(),
        "decision": "continue",
        "reason": "current_authority",
        **changes,
    }


def unavailable(module: Any) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-16: fixed exception without supplied bytes/chaining."""
    return pytest.raises(module.TurnAuthorityUnavailable, match="^identity_authority_unavailable$")


def fixed_error(error: Any) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16."""
    assert error.value.__cause__ is None
    assert error.value.__context__ is None or error.value.__suppress_context__
    assert not any(
        value in str(error.value) + repr(error.value) for value in (TOKEN, CONTEXT, MARKER)
    )


class Chunks(httpx.AsyncByteStream):
    """@spec HTTP-INVOCATION-EXECUTION-16: observe streaming and response ownership."""

    def __init__(self, chunks: list[bytes], *, fail: bool = False, block: bool = False) -> None:
        self.chunks = chunks
        self.fail = fail
        self.block = block
        self.entered = asyncio.Event()
        self.read = 0
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """@spec HTTP-INVOCATION-EXECUTION-16."""
        for chunk in self.chunks:
            self.read += 1
            yield chunk
        self.entered.set()
        if self.fail:
            raise httpx.ReadError(MARKER)
        if self.block:
            await asyncio.Event().wait()

    async def aclose(self) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-16."""
        self.closed = True


@pytest.mark.parametrize(
    "decision,reason",
    [
        ("continue", "current_authority"),
        ("refuse", "caller_not_allowed"),
        ("refuse", "invalid_context"),
        ("refuse", "unsupported_resume"),
        ("unavailable", "authority_unavailable"),
    ],
)
@pytest.mark.parametrize("stage", ["prepare", "claim_or_resume", "start", "steer"])
def test_exact_post_echo_and_all_valid_decisions_remain_boundary_observations(
    decision: str,
    reason: str,
    stage: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-8/16: a refused response never becomes legacy allow."""
    module = api()
    dto = contract()

    async def go() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-16."""
        seen = []

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(
                200, json=echo(json.loads(request.content), decision=decision, reason=reason)
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=60) as http:
            client = module.HttpTurnAuthorityClient(
                api_base_url=BASE, worker_token=TOKEN, client=http
            )
            wire = {**wire_request(), "stage": stage}
            result = await client.check(dto.TurnAuthorityRequestV1.model_validate(wire))
            assert isinstance(result, dto.TurnAuthorityResponseV1)
            assert result.model_dump(mode="json") == echo(wire, decision=decision, reason=reason)
            with pytest.raises(TypeError):
                bool(result)
            assert not http.is_closed, "the supplied client's lifespan belongs to its worker owner"
        assert len(seen) == 1
        request = seen[0]
        assert request.method == "POST" and str(request.url) == BASE + PATH
        assert request.headers["X-Curie-Worker-Token"] == TOKEN
        assert not any(name in request.headers for name in ("Authorization", "X-API-Key", "Cookie"))
        assert json.loads(request.content) == wire

    asyncio.run(go())


def test_shared_client_auth_platform_key_cookie_and_redirect_policy_cannot_widen_check() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: shared client defaults supply no other authority."""
    module = api()
    dto = contract()

    async def go() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-16."""
        seen = []

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(307, headers={"Location": "https://other.example.test/collect"})

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            follow_redirects=True,
            auth=httpx.BasicAuth("synthetic-user", "synthetic-password"),
            headers={
                "Authorization": "Bearer SyntheticAmbientAuth",
                "X-API-Key": "SyntheticPlatformKey",
                "Cookie": "synthetic=ambient",
            },
            cookies={"jar-cookie": "ambient"},
        ) as http:
            client = module.HttpTurnAuthorityClient(
                api_base_url=BASE, worker_token=TOKEN, client=http
            )
            with unavailable(module) as error:
                await client.check(dto.TurnAuthorityRequestV1.model_validate(wire_request()))
            fixed_error(error)
            assert not http.is_closed
        assert len(seen) == 1 and str(seen[0].url) == BASE + PATH
        assert seen[0].headers["X-Curie-Worker-Token"] == TOKEN
        assert not any(name in seen[0].headers for name in ("Authorization", "X-API-Key", "Cookie"))

    asyncio.run(go())


@pytest.mark.parametrize(
    "status", [201, 204, 301, 302, 307, 308, 400, 401, 404, 409, 429, 500, 503]
)
def test_non_200_status_never_retries_or_follows_redirect(status: int) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16."""
    module = api()
    dto = contract()

    async def go() -> None:
        seen = []

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(
                status,
                json=echo(json.loads(request.content)),
                headers={"Location": "https://other.example.test/collect"},
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), follow_redirects=True
        ) as http:
            client = module.HttpTurnAuthorityClient(
                api_base_url=BASE, worker_token=TOKEN, client=http
            )
            with unavailable(module) as error:
                await client.check(dto.TurnAuthorityRequestV1.model_validate(wire_request()))
            fixed_error(error)
        assert len(seen) == 1

    asyncio.run(go())


@pytest.mark.parametrize(
    "path,value",
    [
        ("request_id", OTHER),
        ("stage", "start"),
        ("event_id", "different-event"),
        ("context_sha256", "b" * 64),
        ("scope.tenant_id", OTHER),
        ("scope.agent_id", UUID),
        ("scope.binding_id", OTHER),
        ("scope.binding_generation", 8),
    ],
)
def test_each_echo_component_must_match_the_exact_current_request(path: str, value: Any) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: a valid but unrelated response grants nothing."""
    module = api()
    dto = contract()

    async def go() -> None:
        seen = []

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            response = echo(json.loads(request.content))
            node = response
            pieces = path.split(".")
            for piece in pieces[:-1]:
                node = node[piece]
            node[pieces[-1]] = value
            return httpx.Response(200, json=response)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = module.HttpTurnAuthorityClient(
                api_base_url=BASE, worker_token=TOKEN, client=http
            )
            with unavailable(module) as error:
                await client.check(dto.TurnAuthorityRequestV1.model_validate(wire_request()))
            fixed_error(error)
        assert len(seen) == 1

    asyncio.run(go())


@pytest.mark.parametrize(
    "damage",
    [
        "invalid_json",
        "array",
        "null",
        "unknown",
        "schema_bool",
        "missing",
        "duplicate_root",
        "duplicate_nested",
        "nan",
        "infinity",
        "bad_reason",
        "oversized",
    ],
)
def test_malformed_duplicate_nonfinite_and_oversized_responses_are_fixed_unavailable(
    damage: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: strict decoding precedes echo acceptance."""
    caplog.set_level(logging.DEBUG)
    module = api()
    dto = contract()

    async def go() -> None:
        seen = []

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            response = echo(json.loads(request.content))
            if damage == "unknown":
                response["unknown"] = MARKER
            elif damage == "schema_bool":
                response["schema_version"] = True
            elif damage == "missing":
                del response["reason"]
            elif damage == "bad_reason":
                response["reason"] = "caller_not_allowed"
            raw = json.dumps(response).encode()
            if damage == "invalid_json":
                raw = MARKER.encode()
            elif damage in {"array", "null"}:
                raw = b"[]" if damage == "array" else b"null"
            elif damage == "duplicate_root":
                raw = raw[:-1] + b', "decision":"continue"}'
            elif damage == "duplicate_nested":
                raw = raw.replace(
                    b'"binding_generation": 7',
                    b'"binding_generation":7,"binding_generation":7',
                )
            elif damage in {"nan", "infinity"}:
                raw = raw.replace(
                    b'"binding_generation": 7',
                    b'"binding_generation": ' + (b"NaN" if damage == "nan" else b"Infinity"),
                )
            elif damage == "oversized":
                raw = b" " * 65537 + raw
            return httpx.Response(200, content=raw)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = module.HttpTurnAuthorityClient(
                api_base_url=BASE, worker_token=TOKEN, client=http
            )
            with unavailable(module) as error:
                await client.check(dto.TurnAuthorityRequestV1.model_validate(wire_request()))
            fixed_error(error)
        assert len(seen) == 1

    asyncio.run(go())
    assert not any(value in caplog.text for value in (TOKEN, CONTEXT, MARKER))


@pytest.mark.parametrize("pad_to_limit", [False, True])
def test_streamed_valid_response_is_decoded_and_response_closed_without_closing_client(
    pad_to_limit: bool,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: exercise a response not already buffered by HTTPX."""
    module = api()
    dto = contract()

    async def go() -> None:
        response_body = json.dumps(echo(wire_request())).encode()
        if pad_to_limit:
            response_body += b" " * (65536 - len(response_body))
        stream = Chunks([response_body[:31], response_body[31:]])
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, stream=stream),
            )
        ) as http:
            client = module.HttpTurnAuthorityClient(
                api_base_url=BASE, worker_token=TOKEN, client=http
            )
            result = await client.check(dto.TurnAuthorityRequestV1.model_validate(wire_request()))
            assert result.decision == "continue" and stream.read == 2 and stream.closed
            assert not http.is_closed

    asyncio.run(go())


def test_oversized_stream_stops_at_bound_and_closes_without_reading_following_chunks() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: prebuffering cannot bypass the response bound."""
    module = api()
    dto = contract()

    async def go() -> None:
        stream = Chunks([b" " * 32768, b" " * 32768, b"x", MARKER.encode()])
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, stream=stream),
            )
        ) as http:
            client = module.HttpTurnAuthorityClient(
                api_base_url=BASE, worker_token=TOKEN, client=http
            )
            with unavailable(module) as error:
                await client.check(dto.TurnAuthorityRequestV1.model_validate(wire_request()))
            fixed_error(error)
            assert stream.read == 3 and stream.closed
            assert not http.is_closed

    asyncio.run(go())


@pytest.mark.parametrize("failure", ["connect", "headers_timeout", "stream_read"])
def test_network_and_stream_failures_are_fixed_unavailable_without_retry(failure: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: a transport failure starts no alternate attempt."""
    module = api()
    dto = contract()

    async def go() -> None:
        seen = []
        stream = Chunks([b"{"], fail=True)

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            if failure == "connect":
                raise httpx.ConnectError(MARKER, request=request)
            if failure == "headers_timeout":
                raise httpx.ReadTimeout(MARKER, request=request)
            return httpx.Response(200, stream=stream)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = module.HttpTurnAuthorityClient(
                api_base_url=BASE, worker_token=TOKEN, client=http
            )
            with unavailable(module) as error:
                await client.check(dto.TurnAuthorityRequestV1.model_validate(wire_request()))
            fixed_error(error)
            assert not http.is_closed
            if failure == "stream_read":
                assert stream.closed
        assert len(seen) == 1

    asyncio.run(go())


@pytest.mark.parametrize("damage", ["mapping", "none", "root_copy", "scope_copy", "scope_id_copy"])
def test_invalid_and_unchecked_requests_refuse_before_any_http(damage: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: revalidation crosses the complete nested request."""
    module = api()
    dto = contract()

    async def go() -> None:
        seen = []

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json=echo(json.loads(request.content)))

        request = dto.TurnAuthorityRequestV1.model_validate(wire_request())
        if damage == "mapping":
            request = wire_request()
        elif damage == "none":
            request = None
        elif damage == "root_copy":
            request = request.model_copy(update={"context": "é"})
        elif damage == "scope_copy":
            request = request.model_copy(
                update={
                    "scope": request.scope.model_copy(
                        update={
                            "binding_generation": True,
                        }
                    )
                }
            )
        else:
            request = request.model_copy(
                update={
                    "scope": request.scope.model_copy(update={"binding_id": True}),
                }
            )
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = module.HttpTurnAuthorityClient(
                api_base_url=BASE, worker_token=TOKEN, client=http
            )
            with pytest.raises(ValueError, match="^invalid_turn_authority_request$") as error:
                await client.check(request)
            fixed_error(error)
        assert seen == []

    asyncio.run(go())


@pytest.mark.parametrize("boundary", ["headers", "stream"])
def test_caller_cancellation_propagates_at_headers_and_stream_boundaries(boundary: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: cancellation is never an unavailable response."""
    module = api()
    dto = contract()

    async def go() -> None:
        entered = asyncio.Event()
        seen = []
        stream = Chunks([b"{"], block=True)

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            entered.set()
            if boundary == "headers":
                await asyncio.Event().wait()
            return httpx.Response(200, stream=stream)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=None) as http:
            client = module.HttpTurnAuthorityClient(
                api_base_url=BASE, worker_token=TOKEN, client=http
            )
            task = asyncio.create_task(
                client.check(dto.TurnAuthorityRequestV1.model_validate(wire_request()))
            )
            try:
                async with asyncio.timeout(1):
                    await (entered.wait() if boundary == "headers" else stream.entered.wait())
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert len(seen) == 1 and not http.is_closed
                if boundary == "stream":
                    assert stream.closed
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    asyncio.run(go())


def test_request_is_detached_before_network_and_echo_uses_that_captured_request() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: later unchecked mutation cannot change expectations."""
    module = api()
    dto = contract()

    async def go() -> None:
        original = dto.TurnAuthorityRequestV1.model_validate(wire_request())
        seen = []

        async def handler(request: httpx.Request) -> httpx.Response:
            submitted = json.loads(request.content)
            seen.append(submitted)
            object.__setattr__(original.scope, "binding_id", OTHER)
            object.__setattr__(original, "event_id", "SyntheticMutatedEventAfterDispatch")
            return httpx.Response(200, json=echo(submitted))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = module.HttpTurnAuthorityClient(
                api_base_url=BASE, worker_token=TOKEN, client=http
            )
            response = await client.check(original)
            assert response.model_dump(mode="json") == echo(wire_request())
        assert seen == [wire_request()]

    asyncio.run(go())


def test_one_total_five_second_deadline_covers_headers_and_blocked_stream() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-16: one bounded wall-clock observation, no clock patch."""
    import time

    module = api()
    dto = contract()

    async def go() -> None:
        seen = []
        stream = Chunks([b"{"], block=True)

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            # A new five-second budget after headers would exceed the outer bound.
            await asyncio.sleep(2)
            return httpx.Response(200, stream=stream)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=None) as http:
            client = module.HttpTurnAuthorityClient(
                api_base_url=BASE, worker_token=TOKEN, client=http
            )
            started = time.monotonic()
            async with asyncio.timeout(6.5):
                with unavailable(module) as error:
                    await client.check(dto.TurnAuthorityRequestV1.model_validate(wire_request()))
            elapsed = time.monotonic() - started
            fixed_error(error)
            assert 4.5 <= elapsed < 6.5
            assert stream.entered.is_set() and stream.closed
            assert len(seen) == 1 and not http.is_closed

    asyncio.run(go())


# -- Version 2: the verified requester projection. ---------------------------------------
# ADR 0270 decision 1. The handler replays the exact shape the API route emits
# for an opted-in agent (apps/api/tests/test_http_requester_projection.py):
# every version 1 field, schema_version 2 and a required-nullable requester.

REQUESTER = {
    "tenant_id": UUID,
    "issuer": "https://identity.example.test/directory",
    "subject": "SyntheticRequesterSubject",
}


def echo_v2(request: dict[str, Any], *, human: bool = True, **changes: Any) -> dict[str, Any]:
    """@spec CONNECTOR-REQUESTER-1: the API's V2 answer for this exact submitted request."""
    return {
        **echo(request),
        "schema_version": 2,
        "requester": copy.deepcopy(REQUESTER) if human else None,
        **changes,
    }


def answered(body: Any, *, raw: bytes | None = None) -> Any:
    """@spec CONNECTOR-REQUESTER-1: one check against a handler that derives its answer."""
    module = api()
    dto = contract()
    outcome: dict[str, Any] = {}

    async def go() -> None:
        """@spec CONNECTOR-REQUESTER-1."""
        seen = []

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            if raw is not None:
                return httpx.Response(200, content=raw)
            return httpx.Response(200, json=body(json.loads(request.content)))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = module.HttpTurnAuthorityClient(
                api_base_url=BASE, worker_token=TOKEN, client=http
            )
            try:
                outcome["result"] = await client.check(
                    dto.TurnAuthorityRequestV1.model_validate(wire_request()),
                )
            except module.TurnAuthorityUnavailable as error:
                outcome["error"] = error
        assert len(seen) == 1

    asyncio.run(go())
    return outcome


def assert_unavailable(outcome: dict[str, Any]) -> None:
    """@spec CONNECTOR-REQUESTER-1: a fixed unavailable, never a parsed observation."""
    assert "result" not in outcome, outcome
    error = outcome["error"]
    assert str(error) == "identity_authority_unavailable"
    assert error.__cause__ is None
    assert error.__context__ is None or error.__suppress_context__


@pytest.mark.parametrize("human", [True, False], ids=["human", "no-human"])
@pytest.mark.parametrize("stage", ["prepare", "claim_or_resume", "start", "steer"])
def test_a_v2_answer_is_returned_as_v2_by_its_explicit_schema_version(
    human: bool, stage: str,
) -> None:
    """@spec CONNECTOR-REQUESTER-1: the body's schema_version selects the model."""
    module = api()
    dto = contract()

    async def go() -> None:
        """@spec CONNECTOR-REQUESTER-1."""

        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=echo_v2(json.loads(request.content), human=human))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = module.HttpTurnAuthorityClient(
                api_base_url=BASE, worker_token=TOKEN, client=http
            )
            wire = {**wire_request(), "stage": stage}
            result = await client.check(dto.TurnAuthorityRequestV1.model_validate(wire))
            assert type(result) is dto.TurnAuthorityResponseV2
            assert result.model_dump(mode="json") == echo_v2(wire, human=human)
            if human:
                assert type(result.requester) is dto.TurnRequesterV1
                assert result.requester.model_dump() == REQUESTER
            else:
                assert result.requester is None
            with pytest.raises(TypeError):
                bool(result)

    asyncio.run(go())


def test_a_v1_answer_is_still_returned_as_exactly_v1() -> None:
    """@spec CONNECTOR-REQUESTER-1 HTTP-INVOCATION-EXECUTION-16: version 1 is unchanged."""
    dto = contract()
    outcome = answered(echo)
    result = outcome["result"]
    assert type(result) is dto.TurnAuthorityResponseV1
    assert "requester" not in result.model_dump()


@pytest.mark.parametrize(
    ("decision", "reason"),
    [
        ("refuse", "caller_not_allowed"),
        ("refuse", "invalid_context"),
        ("refuse", "unsupported_resume"),
        ("unavailable", "authority_unavailable"),
    ],
)
def test_a_v2_refusal_carries_no_requester_and_stays_an_observation(
    decision: str, reason: str,
) -> None:
    """@spec CONNECTOR-REQUESTER-1: a refused V2 answer parses, a requester on it does not."""
    dto = contract()
    live = answered(lambda request: echo_v2(request, human=False, decision=decision, reason=reason))
    assert type(live["result"]) is dto.TurnAuthorityResponseV2
    assert live["result"].decision == decision and live["result"].requester is None
    assert_unavailable(
        answered(lambda request: echo_v2(request, decision=decision, reason=reason)),
    )


@pytest.mark.parametrize("version", [3, 0, "2", 2.0, True, None, "missing"])
@pytest.mark.parametrize("human", [True, False], ids=["human", "no-human"])
def test_any_other_schema_version_is_unavailable(version: Any, human: bool) -> None:
    """@spec CONNECTOR-REQUESTER-1: no reader guesses between versions."""

    def body(request: dict[str, Any]) -> dict[str, Any]:
        """@spec CONNECTOR-REQUESTER-1."""
        value = echo_v2(request, human=human)
        if version == "missing":
            del value["schema_version"]
        else:
            value["schema_version"] = version
        return value

    assert_unavailable(answered(body))


@pytest.mark.parametrize(
    "path,value",
    [
        ("request_id", OTHER),
        ("stage", "start"),
        ("event_id", "different-event"),
        ("context_sha256", "b" * 64),
        ("scope.tenant_id", OTHER),
        ("scope.agent_id", UUID),
        ("scope.binding_id", OTHER),
        ("scope.binding_generation", 8),
    ],
)
def test_each_v2_echo_component_must_match_the_exact_current_request(
    path: str, value: Any,
) -> None:
    """@spec CONNECTOR-REQUESTER-1: a valid V2 answer to another request projects nothing."""

    def body(request: dict[str, Any]) -> dict[str, Any]:
        """@spec CONNECTOR-REQUESTER-1."""
        response = echo_v2(request)
        node = response
        pieces = path.split(".")
        for piece in pieces[:-1]:
            node = node[piece]
        node[pieces[-1]] = value
        if path == "scope.tenant_id":
            # Keep the projection bound to the changed scope, so only the echo differs.
            response["requester"]["tenant_id"] = value
        return response

    assert_unavailable(answered(body))


@pytest.mark.parametrize(
    "damage",
    [
        "requester-other-tenant",
        "requester-missing",
        "requester-empty",
        "requester-extra",
        "requester-partial",
        "requester-string",
        "v1-with-requester",
        "v1-with-null-requester",
    ],
)
def test_a_malformed_requester_projection_is_unavailable(damage: str) -> None:
    """@spec CONNECTOR-REQUESTER-1: the worker never uses an unbound or partial projection."""

    def body(request: dict[str, Any]) -> dict[str, Any]:
        """@spec CONNECTOR-REQUESTER-1."""
        response = echo_v2(request)
        if damage == "requester-other-tenant":
            response["requester"]["tenant_id"] = OTHER
        elif damage == "requester-missing":
            del response["requester"]
        elif damage == "requester-empty":
            response["requester"] = {}
        elif damage == "requester-extra":
            response["requester"]["groups"] = [MARKER]
        elif damage == "requester-partial":
            del response["requester"]["subject"]
        elif damage == "requester-string":
            response["requester"] = REQUESTER["subject"]
        elif damage == "v1-with-requester":
            response["schema_version"] = 1
        else:
            response["schema_version"] = 1
            response["requester"] = None
        return response

    assert_unavailable(answered(body))


@pytest.mark.parametrize("where", ["root", "requester"])
def test_a_duplicate_key_in_a_v2_answer_is_unavailable(where: str) -> None:
    """@spec CONNECTOR-REQUESTER-1: strict decoding precedes version selection."""
    request = wire_request()
    encoded = json.dumps(echo_v2(request), separators=(",", ":")).encode()
    if where == "root":
        raw = encoded[:-1] + b',"schema_version":2}'
    else:
        subject = b'"subject":"SyntheticRequesterSubject"'
        assert subject in encoded
        raw = encoded.replace(subject, subject + b',"subject":"SyntheticOtherSubject"')
    assert json.loads(raw)["schema_version"] == 2
    assert_unavailable(answered(None, raw=raw))


GRANT_PATH = "/v1/internal/identity/approval-grant/check"


def grant_wire() -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-12: a complete grant request for a resume context."""
    return {
        "schema_version": 1,
        "check": {**wire_request(), "approval_id": UUID},
        "grant": {
            "approval_id": UUID,
            "agent_id": OTHER,
            "conversation_id": "synthetic-conversation",
            "gate_kind": "permission",
            "granted_tool": "synthetic_read",
            "granted_arguments_json": "{}",
        },
    }


@pytest.mark.parametrize("check", ["v1", "v2-human", "v2-no-human"])
def test_the_grant_reader_accepts_only_a_v1_check(check: str) -> None:
    """@spec CONNECTOR-REQUESTER-1 HTTP-APPROVAL-CONTINUITY-12: a V2 check is unavailable there.

    The API never projects a requester through the grant check; a reader that
    only knows version 1 maps a V2 body to unavailable rather than continue.
    """
    module = api()
    dto = importlib.import_module("curie_internal.identity_approval")
    outcome: dict[str, Any] = {}

    async def go() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        seen = []

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            submitted = json.loads(request.content)
            answer = echo(submitted["check"]) if check == "v1" else echo_v2(
                submitted["check"], human=check == "v2-human",
            )
            grant = json.dumps(
                submitted["grant"], ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                allow_nan=False,
            ).encode()
            return httpx.Response(200, json={
                "schema_version": 1, "check": answer,
                "grant_sha256": hashlib.sha256(grant).hexdigest(),
            })

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = module.HttpTurnAuthorityClient(
                api_base_url=BASE, worker_token=TOKEN, client=http
            )
            request = dto.ApprovalGrantAuthorityRequestV1.model_validate(grant_wire())
            try:
                outcome["result"] = await client.check_grant(request)
            except module.TurnAuthorityUnavailable as error:
                outcome["error"] = error
        assert len(seen) == 1
        assert str(seen[0].url) == BASE + GRANT_PATH

    asyncio.run(go())
    if check == "v1":
        result = outcome["result"]
        assert type(result) is dto.ApprovalGrantAuthorityResponseV1
        assert result.check.schema_version == 1 and result.check.decision == "continue"
    else:
        assert_unavailable(outcome)
