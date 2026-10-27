"""Synthetic external HTTP boundaries, not provider/Slack qualification.

@spec HTTP-APPROVAL-CONTINUITY-2 HTTP-APPROVAL-CONTINUITY-3
@spec HTTP-APPROVAL-CONTINUITY-12.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib
import inspect
import json
import logging
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

UUID = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"
TOKEN = "SyntheticPrivateWorkerToken"
CONTEXT = "SyntheticPrivateContext"
PAYLOAD = '{ "original": "é", "order": 1 }'
MARKER = "SyntheticUntrustedResponse"
BASE = "https://core.example.test"
CREATE_PATH = "/v1/internal/identity/approvals"
GRANT_PATH = "/v1/internal/identity/approval-grant/check"


def modules() -> tuple[Any, Any, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-3 HTTP-APPROVAL-CONTINUITY-12."""
    return tuple(
        importlib.import_module(name)
        for name in (
            "curie_worker.approvals",
            "curie_worker.identity_authority",
            "curie_internal.identity_approval",
        )
    )


def scope() -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-3 HTTP-APPROVAL-CONTINUITY-12."""
    return {"tenant_id": UUID, "agent_id": OTHER, "binding_id": UUID, "binding_generation": 7}


def approval() -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-3: unchanged ACI request."""
    from aci_protocol import ApprovalRequest

    return ApprovalRequest.model_validate(
        {
            "agent_id": OTHER,
            "conversation_id": "1712345678.000001",
            "author": "original-invoker",
            "summary": "Synthetic permission",
            "reply_kind": "slack",
            "reply_channel": "CExample",
            "reply_placeholder": None,
            "dedupe_key": "event-1",
            "gate_kind": "permission",
            "granted_tool": "SyntheticTool",
            "granted_arguments": {},
        }
    )


def identity() -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-3."""
    return modules()[2].ApprovalCreationIdentityV1.model_validate(
        {
            "schema_version": 1,
            "context": CONTEXT,
            "payload": PAYLOAD,
            "scope": scope(),
        }
    )


def grant_request() -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-12."""
    return modules()[2].ApprovalGrantAuthorityRequestV1.model_validate(
        {
            "schema_version": 1,
            "check": {
                "schema_version": 1,
                "request_id": UUID,
                "stage": "prepare",
                "context": CONTEXT,
                "payload_sha256": hashlib.sha256(PAYLOAD.encode()).hexdigest(),
                "event_id": f"approval-{UUID}-resolved",
                "approval_id": UUID,
                "scope": scope(),
            },
            "grant": {
                "approval_id": UUID,
                "agent_id": OTHER,
                "conversation_id": "1712345678.000001",
                "gate_kind": "permission",
                "granted_tool": "SyntheticTool",
                "granted_arguments_json": '{"array":[3,1],"é":"value"}',
            },
        }
    )


def canonical(value: Any) -> bytes:
    """@spec HTTP-APPROVAL-CONTINUITY-2 HTTP-APPROVAL-CONTINUITY-12."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


def response_body(request: dict[str, Any], *, grant: bool = False) -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-3 HTTP-APPROVAL-CONTINUITY-12: independent wire oracle."""
    if not grant:
        return {
            "schema_version": 1,
            "request_id": request["request_id"],
            "id": OTHER,
            "status": "pending",
            "duplicate": False,
            "requested_by": None,
        }
    check = request["check"]
    return {
        "schema_version": 1,
        "grant_sha256": hashlib.sha256(canonical(request["grant"])).hexdigest(),
        "check": {
            "schema_version": 1,
            "request_id": check["request_id"],
            "stage": check["stage"],
            "event_id": check["event_id"],
            "scope": copy.deepcopy(check["scope"]),
            "context_sha256": hashlib.sha256(check["context"].encode("ascii")).hexdigest(),
            "decision": "continue",
            "reason": "current_authority",
        },
    }


def client(http: httpx.AsyncClient, *, grant: bool = False) -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-3 HTTP-APPROVAL-CONTINUITY-12."""
    creation, authority, _ = modules()
    if grant:
        result = authority.HttpTurnAuthorityClient(
            api_base_url=BASE, worker_token=TOKEN, client=http
        )
        assert callable(getattr(result, "check_grant", None)), (
            "approval grant client is not implemented"
        )
        return result
    assert "identity" in inspect.signature(creation.ApprovalClient.create).parameters, (
        "identity-aware approval creation client is not implemented"
    )
    return creation.ApprovalClient(
        api_base_url=BASE,
        api_key="SyntheticPlatformKey",
        client=http,
        read_timeout_s=30,
        worker_token=TOKEN,
    )


async def invoke(owner: Any, *, grant: bool = False, budget: Any = 1.0) -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-3 HTTP-APPROVAL-CONTINUITY-12."""
    if grant:
        return await owner.check_grant(grant_request())
    return await owner.create(approval(), identity=identity(), budget_s=budget)


def private_error(caught: Any, expected: str) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-3 HTTP-APPROVAL-CONTINUITY-12."""
    assert str(caught.value) == expected
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None or caught.value.__suppress_context__
    assert not any(
        secret in str(caught.value) + repr(caught.value)
        for secret in (TOKEN, CONTEXT, PAYLOAD, MARKER)
    )


def assert_request(request: httpx.Request, path: str, maximum: int) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-3 HTTP-APPROVAL-CONTINUITY-12."""
    assert request.method == "POST" and str(request.url) == BASE + path
    assert request.headers["X-Curie-Worker-Token"] == TOKEN
    assert not any(header in request.headers for header in ("Authorization", "Cookie", "X-API-Key"))
    assert len(request.content) <= maximum
    assert all(
        0 < value <= 5 for value in request.extensions["timeout"].values() if value is not None
    )


class Stream(httpx.AsyncByteStream):
    """@spec HTTP-APPROVAL-CONTINUITY-3 HTTP-APPROVAL-CONTINUITY-12: actual HTTPX body ownership."""

    def __init__(
        self,
        raw: bytes,
        *,
        block: bool = False,
        fault: bool = False,
        close_block: bool = False,
        close_fault: bool = False,
    ) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-3 HTTP-APPROVAL-CONTINUITY-12."""
        self.raw, self.block, self.fault = raw, block, fault
        self.close_block, self.close_fault = close_block, close_fault
        self.entered = asyncio.Event()
        self.close_entered = asyncio.Event()
        self.finished = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """@spec HTTP-APPROVAL-CONTINUITY-3 HTTP-APPROVAL-CONTINUITY-12."""
        self.entered.set()
        if self.block:
            await asyncio.Event().wait()
        yield self.raw
        if self.fault:
            raise httpx.ReadError(MARKER)

    async def aclose(self) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-3 HTTP-APPROVAL-CONTINUITY-12."""
        self.close_entered.set()
        try:
            if self.close_block:
                await asyncio.Event().wait()
            if self.close_fault:
                raise RuntimeError(MARKER)
        finally:
            self.finished = True


@pytest.mark.parametrize(("status", "historical"), [(201, "pending"), (200, "approved")])
def test_creation_exact_envelope_fresh_request_id_and_stripped_client_defaults(
    status: int, historical: str
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-3."""

    async def go() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-3."""
        seen, bodies = [], []

        async def handle(request: httpx.Request) -> httpx.Response:
            """@spec HTTP-APPROVAL-CONTINUITY-3."""
            seen.append(request)
            body = response_body(json.loads(request.content))
            body.update(status=historical, duplicate=status == 200)
            stream = Stream(canonical(body))
            bodies.append(stream)
            return httpx.Response(status, stream=stream)

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handle),
            auth=httpx.BasicAuth("u", "p"),
            headers={
                "Authorization": "Bearer Ambient",
                "X-API-Key": "Ambient",
                "Cookie": "ambient=1",
            },
            cookies={"jar": "ambient"},
            follow_redirects=True,
        ) as http:
            owner = client(http)
            for budget in (1.0, 9.0):
                result = await invoke(owner, budget=budget)
                assert isinstance(result, modules()[0].CreatedApproval)
                assert (result.id, result.status, result.requested_by, result.requester_known) == (
                    OTHER,
                    historical,
                    None,
                    True,
                )
            assert not http.is_closed
        assert len(seen) == 2 and all(body.finished for body in bodies)
        ids = set()
        for request, budget in zip(seen, (1.0, 9.0), strict=True):
            assert_request(request, CREATE_PATH, 2_097_152)
            assert all(
                value <= min(5, budget)
                for value in request.extensions["timeout"].values()
                if value is not None
            )
            wire = json.loads(request.content)
            assert set(wire) == {"schema_version", "request_id", "approval", "identity"}
            assert wire["schema_version"] == 1
            assert wire["approval"] == approval().model_dump(mode="json")
            assert wire["identity"] == identity().model_dump(mode="json")
            assert str(uuid.UUID(wire["request_id"])) == wire["request_id"]
            ids.add(wire["request_id"])
        assert len(ids) == 2

    asyncio.run(go())


@pytest.mark.parametrize(
    ("status", "detail"),
    [
        (403, "caller_not_allowed"),
        (409, "approval_provenance_conflict"),
        (409, "unsupported_approval"),
        (413, "body_too_large"),
        (422, "invalid_approval_context"),
    ],
)
def test_only_validated_fixed_api_denials_become_private_continuity_refusal(
    status: int, detail: str
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-3."""

    async def go() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-3."""
        seen, streams = [], []

        async def handle(request: httpx.Request) -> httpx.Response:
            """@spec HTTP-APPROVAL-CONTINUITY-3."""
            seen.append(request)
            stream = Stream(canonical({"detail": detail}))
            streams.append(stream)
            return httpx.Response(status, stream=stream)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            owner = client(http)
            error_type = getattr(modules()[0], "ApprovalContinuityRefused", None)
            assert error_type is not None, "private approval continuity refusal is not implemented"
            assert issubclass(error_type, RuntimeError)
            assert not issubclass(error_type, modules()[0].ApprovalBackendError)
            with pytest.raises(error_type) as error:
                await invoke(owner)
            private_error(error, "identity_approval_refused")
        assert len(seen) == 1 and all(stream.finished for stream in streams)

    asyncio.run(go())


@pytest.mark.parametrize(
    "fault", ["correlation", "status", "shape", "duplicate", "utf8", "oversize"]
)
def test_creation_invalid_receipt_is_unavailable_not_legacy_retry(fault: str) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-2 HTTP-APPROVAL-CONTINUITY-3."""

    async def go() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-3."""
        seen, streams = [], []

        async def handle(request: httpx.Request) -> httpx.Response:
            """@spec HTTP-APPROVAL-CONTINUITY-3."""
            seen.append(request)
            body = response_body(json.loads(request.content))
            if fault == "correlation":
                body["request_id"] = OTHER
            if fault == "status":
                body["duplicate"] = True
            if fault == "shape":
                body["extra"] = MARKER
            raw = canonical(body)
            if fault == "duplicate":
                raw = b'{"schema_version":1,' + raw[1:]
            if fault == "utf8":
                raw = b"\xff"
            if fault == "oversize":
                raw = b" " * 65_537
            stream = Stream(raw)
            streams.append(stream)
            return httpx.Response(201, stream=stream)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            owner = client(http)
            with pytest.raises(modules()[1].TurnAuthorityUnavailable) as error:
                await invoke(owner)
            private_error(error, "identity_authority_unavailable")
        assert len(seen) == 1 and seen[0].url.path == CREATE_PATH
        assert all(stream.finished for stream in streams)

    asyncio.run(go())


def test_auth_route_transport_redirect_and_malformed_error_never_fall_back(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-3."""

    async def go() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-3."""
        for status, detail in [
            (401, "invalid_credential"),
            (404, MARKER),
            (503, "authority_unavailable"),
            (307, MARKER),
            (403, "unsupported_approval"),
            (409, MARKER),
            (0, MARKER),
        ]:
            seen = []

            async def handle(
                request: httpx.Request,
                *,
                seen: Any = seen,
                status: int = status,
                detail: str = detail,
            ) -> httpx.Response:
                """@spec HTTP-APPROVAL-CONTINUITY-3."""
                seen.append(request)
                if status == 0:
                    raise httpx.ConnectError(MARKER)
                return httpx.Response(
                    status, json={"detail": detail}, headers={"Location": BASE + "/approvals"}
                )

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handle), follow_redirects=True
            ) as http:
                owner = client(http)
                with pytest.raises(modules()[1].TurnAuthorityUnavailable) as error:
                    await invoke(owner)
                private_error(error, "identity_authority_unavailable")
            assert len(seen) == 1 and seen[0].url.path == CREATE_PATH

    with caplog.at_level(logging.DEBUG):
        asyncio.run(go())
    assert not any(secret in caplog.text for secret in (CONTEXT, TOKEN, PAYLOAD, MARKER))


def test_invalid_request_or_nonfinite_bool_budget_refuses_before_http() -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-2 HTTP-APPROVAL-CONTINUITY-3."""

    async def go() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-3."""
        seen = []

        async def handle(request: httpx.Request) -> httpx.Response:
            """@spec HTTP-APPROVAL-CONTINUITY-3."""
            seen.append(request)
            raise AssertionError("invalid local input reached HTTP")

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            owner = client(http)
            for budget in (True, False, 0, -1, float("nan"), float("inf"), 10**1000, "1"):
                with pytest.raises(ValueError) as error:
                    await invoke(owner, budget=budget)
                private_error(error, "invalid_approval_creation_request")
            for request, provenance in (
                ({}, identity()),
                (approval(), {}),
                (approval(), identity().model_copy(update={"schema_version": True})),
                (approval().model_copy(update={"summary": "x" * 2_097_152}), identity()),
            ):
                with pytest.raises(ValueError) as error:
                    await owner.create(request, identity=provenance)
                private_error(error, "invalid_approval_creation_request")
        assert seen == []

    asyncio.run(go())


def test_legacy_creation_without_identity_preserves_existing_endpoint_and_result() -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-3."""

    async def go() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-3."""
        seen = []

        async def handle(request: httpx.Request) -> httpx.Response:
            """@spec HTTP-APPROVAL-CONTINUITY-3."""
            seen.append(request)
            return httpx.Response(
                201, json={"id": OTHER, "status": "pending", "requested_by": "legacy"}
            )

        creation = modules()[0]
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            owner = creation.ApprovalClient(
                api_base_url=BASE,
                api_key="LegacyKey",
                client=http,
                read_timeout_s=30,
                worker_token=TOKEN,
            )
            result = await owner.create(approval())
            assert (result.id, result.status, result.requested_by) == (OTHER, "pending", "legacy")
        assert len(seen) == 1 and seen[0].url.path == "/approvals"
        assert seen[0].headers["X-API-Key"] == "LegacyKey"
        assert json.loads(seen[0].content) == approval().model_dump(mode="json")

    asyncio.run(go())


@pytest.mark.parametrize(
    ("decision", "reason"),
    [
        ("continue", "current_authority"),
        ("refuse", "caller_not_allowed"),
        ("unavailable", "authority_unavailable"),
    ],
)
def test_grant_current_check_echoes_complete_snapshot_without_ambient_authority(
    decision: str, reason: str
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-12."""

    async def go() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        seen, streams = [], []

        async def handle(request: httpx.Request) -> httpx.Response:
            """@spec HTTP-APPROVAL-CONTINUITY-12."""
            seen.append(request)
            body = response_body(json.loads(request.content), grant=True)
            body["check"].update(decision=decision, reason=reason)
            stream = Stream(canonical(body))
            streams.append(stream)
            return httpx.Response(200, stream=stream)

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handle),
            auth=httpx.BasicAuth("u", "p"),
            headers={
                "Authorization": "Bearer Ambient",
                "X-API-Key": "Ambient",
                "Cookie": "ambient=1",
            },
            cookies={"jar": "ambient"},
            follow_redirects=True,
        ) as http:
            result = await invoke(client(http, grant=True), grant=True)
            assert isinstance(result, modules()[2].ApprovalGrantAuthorityResponseV1)
            assert result.check.decision == decision
            assert result.model_dump(mode="json") == response_body(
                json.loads(seen[0].content), grant=True
            ) | {
                "check": {
                    **response_body(json.loads(seen[0].content), grant=True)["check"],
                    "decision": decision,
                    "reason": reason,
                }
            }
            with pytest.raises(TypeError):
                bool(result)
            assert not http.is_closed
        assert len(seen) == 1 and all(stream.finished for stream in streams)
        assert_request(seen[0], GRANT_PATH, 65_536)
        assert json.loads(seen[0].content) == grant_request().model_dump(mode="json")

    asyncio.run(go())


@pytest.mark.parametrize(
    "fault", ["echo", "digest", "duplicate", "nonfinite", "oversize", "status"]
)
def test_grant_malformed_or_mismatched_response_is_fixed_unavailable(fault: str) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-2 HTTP-APPROVAL-CONTINUITY-12."""

    async def go() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        # Every echoed field gets an independently mismatched real HTTP response.
        paths = (
            (
                "request_id",
                "stage",
                "event_id",
                "context_sha256",
                "scope.tenant_id",
                "scope.agent_id",
                "scope.binding_id",
                "scope.binding_generation",
            )
            if fault == "echo"
            else (None,)
        )
        for path in paths:
            seen, streams = [], []

            async def handle(
                request: httpx.Request,
                *,
                seen: Any = seen,
                streams: Any = streams,
                path: Any = path,
            ) -> httpx.Response:
                """@spec HTTP-APPROVAL-CONTINUITY-12."""
                seen.append(request)
                body = response_body(json.loads(request.content), grant=True)
                if path:
                    target = body["check"]
                    parts = path.split(".")
                    if len(parts) == 2:
                        target = target[parts[0]]
                    target[parts[-1]] = {
                        "request_id": OTHER,
                        "stage": "start",
                        "event_id": "other-event",
                        "context_sha256": "b" * 64,
                        "tenant_id": OTHER,
                        "agent_id": UUID,
                        "binding_id": OTHER,
                        "binding_generation": 8,
                    }[parts[-1]]
                if fault == "digest":
                    body["grant_sha256"] = "b" * 64
                raw = canonical(body)
                if fault == "duplicate":
                    raw = b'{"schema_version":1,' + raw[1:]
                if fault == "nonfinite":
                    raw = b'{"extra":NaN,' + raw[1:]
                if fault == "oversize":
                    raw = b" " * 65_537
                stream = Stream(raw)
                streams.append(stream)
                return httpx.Response(
                    307 if fault == "status" else 200,
                    stream=stream,
                    headers={"Location": BASE + "/other"},
                )

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handle), follow_redirects=True
            ) as http:
                owner = client(http, grant=True)
                with pytest.raises(modules()[1].TurnAuthorityUnavailable) as error:
                    await invoke(owner, grant=True)
                private_error(error, "identity_authority_unavailable")
            assert len(seen) == 1 and all(stream.finished for stream in streams)

    asyncio.run(go())


def test_creation_total_budget_includes_hung_receipt_and_hung_close_with_no_orphan() -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-3: one actual-clock deadline test, no fake clock."""

    async def go() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-3."""
        for close_block in (False, True):
            seen, streams = [], []

            async def handle(
                request: httpx.Request,
                *,
                seen: Any = seen,
                streams: Any = streams,
                close_block: bool = close_block,
            ) -> httpx.Response:
                """@spec HTTP-APPROVAL-CONTINUITY-3."""
                seen.append(request)
                stream = Stream(
                    canonical(response_body(json.loads(request.content))),
                    block=not close_block,
                    close_block=close_block,
                )
                streams.append(stream)
                return httpx.Response(201, stream=stream)

            async with httpx.AsyncClient(transport=httpx.MockTransport(handle), timeout=60) as http:
                owner = client(http)
                began = time.monotonic()
                # The outer bound is a test watchdog, not the client budget.
                async with asyncio.timeout(0.18):
                    with pytest.raises(modules()[1].TurnAuthorityUnavailable) as error:
                        await invoke(owner, budget=0.04)
                private_error(error, "identity_authority_unavailable")
                assert time.monotonic() - began < 0.18
                assert not http.is_closed
            assert len(seen) == 1 and streams[0].close_entered.is_set() and streams[0].finished
            assert not [
                task
                for task in asyncio.all_tasks()
                if task is not asyncio.current_task() and not task.done()
            ]

    asyncio.run(go())


@pytest.mark.parametrize("grant", [False, True])
def test_cancellation_closes_response_and_cleanup_fault_never_masks_cancelled_error(
    grant: bool,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-3 HTTP-APPROVAL-CONTINUITY-12."""

    async def go() -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-3 HTTP-APPROVAL-CONTINUITY-12."""
        stream = Stream(b"", block=True, close_fault=True)
        seen = []

        async def handle(request: httpx.Request) -> httpx.Response:
            """@spec HTTP-APPROVAL-CONTINUITY-3 HTTP-APPROVAL-CONTINUITY-12."""
            seen.append(request)
            return httpx.Response(200 if grant else 201, stream=stream)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            owner = client(http, grant=grant)
            task = asyncio.create_task(invoke(owner, grant=grant))
            try:
                await asyncio.wait_for(stream.entered.wait(), 0.5)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 0.5)
                assert stream.close_entered.is_set() and stream.finished
                assert not http.is_closed
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        assert len(seen) == 1

    asyncio.run(go())
