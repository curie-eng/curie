"""Independent synthetic API/Slack boundaries for the selected HTTP receiver.

@spec HTTP-INVOCATION-EXECUTION-1/2/3/15/19.
No actual Slack installation, provider freshness or protected key custody is qualified.
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import pickle
import threading
import time
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
from curie_internal import identity_context as codec
from slack_sdk.web import WebClient

RECEIVER = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
IDENTITY = "11111111-1111-4111-8111-111111111111"
INSTALLATION = "22222222-2222-4222-8222-222222222222"
BINDING = "33333333-3333-4333-8333-333333333333"
NAMESPACE = "44444444-4444-4444-8444-444444444444"
KEY = bytes(range(32))
CREDENTIAL = "chn.SyntheticReceiverCredential"
BASE = "https://core.example.test"
NAME = "synthetic-receiver"
CHANNEL = "C0EXAMPLE1"
BOT = "U0BOT"
SENDER = "U0EXAMPLE1"


def api() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-19: fail during execution, never collection."""
    name = "curie_dispatcher.http_receiver"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "selected HTTP receiver is not implemented"
        raise
    for symbol in (
        "HttpReceiverScopeV1",
        "HttpReceiverBindingV1",
        "ReceiverSubmission",
        "build_http_receiver_runtime",
    ):
        assert hasattr(module, symbol), f"selected HTTP receiver missing {symbol}"
    return module


def scope_body(**changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-19: exact independently authored operator pins."""
    return {
        "schema_version": 1,
        "receiver_id": RECEIVER,
        "receiver_generation": 2,
        "channel_identity_id": IDENTITY,
        "channel_identity_name": NAME,
        "provider_installation_id": INSTALLATION,
        "namespace": {"id": NAMESPACE, "authority": "", "kind": "slack_workspace", "key": "T1"},
        "installation_account": "T1",
        "bot_user_id": BOT,
        **changes,
    }


def binding_body(**changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-19: pinned opaque credential, no token minting oracle."""
    return {
        "schema_version": 1,
        "native_channel_id": CHANNEL,
        "binding_id": BINDING,
        "binding_generation": 7,
        "channel_credential": CREDENTIAL,
        **changes,
    }


def native() -> tuple[dict[str, Any], dict[str, Any]]:
    """@spec HTTP-INVOCATION-EXECUTION-19: native shape already exercised by dispatcher fixtures."""
    event = {
        "type": "app_mention",
        "channel": CHANNEL,
        "user": SENDER,
        "text": "<@U0BOT> Hello synthetic receiver",
        "ts": "1700.0002",
        "thread_ts": "1700.0001",
        "user_team": "T1",
        "files": [{"id": "F0EXAMPLE1", "name": "sample.txt", "mimetype": "text/plain", "size": 17}],
    }
    return {
        "type": "event_callback",
        "event_id": "EvNativeSynthetic",
        "team_id": "T1",
        "is_ext_shared_channel": False,
        "event": event,
    }, event


def receipt(*, pending: bool = False, **changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-19: the API's existing three-field TurnAccepted shape."""
    return {
        "event_id": "chn-synthetic-event",
        "stream_id": None if pending else "1700000000000-0",
        "duplicate": pending,
        **changes,
    }


class HttpRig:
    """@spec HTTP-INVOCATION-EXECUTION-19: fresh client factory; external API only."""

    def __init__(self, handler: Any = None, *, auth: Any = None) -> None:
        self.requests: list[httpx.Request] = []
        self.clients: list[httpx.AsyncClient] = []
        self.handler = handler
        self.web = WebClient(token="xoxb-synthetic-receiver")
        self.web.auth_test = MagicMock(
            return_value=auth
            if auth is not None
            else {
                "ok": True,
                "team_id": "T1",
                "user_id": BOT,
                "bot_id": "B1",
                "is_enterprise_install": False,
            }
        )

    def factory(self) -> httpx.AsyncClient:
        """@spec HTTP-INVOCATION-EXECUTION-19: ambient client authority must be stripped."""

        async def handle(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            if self.handler is not None:
                return await self.handler(request)
            return httpx.Response(200, json=receipt())

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(handle),
            follow_redirects=True,
            timeout=None,
            auth=httpx.BasicAuth("synthetic-user", "synthetic-password"),
            headers={
                "Authorization": "Bearer SyntheticAmbientCredential",
                "Cookie": "ambient=1",
                "X-API-Key": "SyntheticAmbientPlatformKey",
            },
            cookies={"ambient-cookie": "1"},
        )
        self.clients.append(client)
        return client

    def runtime(
        self, module: Any, *, scope: Any = None, bindings: Any = None, key: Any = KEY
    ) -> Any:
        """@spec HTTP-INVOCATION-EXECUTION-19: the initializer calls the configured WebClient."""
        return module.build_http_receiver_runtime(
            module.HttpReceiverScopeV1.model_validate(scope_body()) if scope is None else scope,
            (module.HttpReceiverBindingV1.model_validate(binding_body()),)
            if bindings is None
            else bindings,
            receiver_key=key,
            web_client=self.web,
            api_base_url=BASE,
            http_client_factory=self.factory,
        )

    def submit(self, runtime: Any, **changes: Any) -> Any:
        """@spec HTTP-INVOCATION-EXECUTION-19."""
        body, event = native()
        return runtime.submit(
            **{"body": body, "event": event, "slack_identity": NAME, "bot_user_id": BOT, **changes}
        )

    def closed(self) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-19: owned clients must not survive submissions."""
        assert self.clients and all(client.is_closed for client in self.clients)


def test_operator_models_are_strict_frozen_and_runtime_calls_actual_auth_test() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-1/19: startup is independent of platform reports."""
    module = api()
    rig = HttpRig()
    scope = module.HttpReceiverScopeV1.model_validate(scope_body())
    binding = module.HttpReceiverBindingV1.model_validate(binding_body())
    assert CREDENTIAL not in repr(binding)
    runtime = rig.runtime(module, scope=scope, bindings=(binding,))
    rig.web.auth_test.assert_called_once_with()
    assert rig.clients == []
    for target, field, value in (
        (scope, "receiver_generation", 3),
        (binding, "binding_generation", 8),
        (runtime, "extra", "changed"),
    ):
        with pytest.raises((TypeError, AttributeError, ValueError)):
            setattr(target, field, value)
    with pytest.raises(TypeError):
        bool(runtime)
    with pytest.raises(TypeError):
        pickle.dumps(runtime)
    assert CREDENTIAL not in repr(runtime) and str(KEY) not in repr(runtime)
    for cls, bad in (
        (module.HttpReceiverScopeV1, scope_body(schema_version=True)),
        (module.HttpReceiverBindingV1, binding_body(binding_generation=True)),
        (module.HttpReceiverBindingV1, binding_body(binding_generation=-1)),
        (module.HttpReceiverBindingV1, binding_body(channel_credential="é")),
        (module.HttpReceiverBindingV1, binding_body(channel_credential="x" * 16385)),
    ):
        with pytest.raises(ValueError):
            cls.model_validate(bad)
    # @spec HTTP-INVOCATION-EXECUTION-19: a never-rebound binding is generation 0.
    never_rebound = module.HttpReceiverBindingV1.model_validate(binding_body(binding_generation=0))
    assert type(never_rebound.binding_generation) is int and never_rebound.binding_generation == 0


@pytest.mark.parametrize(
    "auth",
    [
        {"ok": True, "team_id": "T2", "user_id": BOT, "is_enterprise_install": False},
        {"ok": True, "team_id": "T1", "user_id": "U0OTHER", "is_enterprise_install": False},
        {"ok": True, "team_id": "T1", "user_id": BOT},
        {"ok": True, "team_id": "T1", "user_id": BOT, "is_enterprise_install": True},
        {
            "ok": True,
            "team_id": "T1",
            "user_id": BOT,
            "is_enterprise_install": False,
            "enterprise_id": "E0EXAMPLE1",
        },
    ],
)
def test_startup_refuses_unqualified_connection_observation(auth: dict[str, Any]) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-2/15/19."""
    module = api()
    rig = HttpRig(auth=auth)
    with pytest.raises(ValueError, match="^invalid_http_receiver_runtime$") as error:
        rig.runtime(module)
    assert error.value.__cause__ is None
    rig.web.auth_test.assert_called_once_with()
    assert rig.requests == []


@pytest.mark.parametrize("damage", ["duplicate_channel", "duplicate_binding", "list", "bad_key"])
def test_invalid_immutable_registration_collection_never_builds_partial_runtime(
    damage: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-1/19."""
    module = api()
    rig = HttpRig()
    one = module.HttpReceiverBindingV1.model_validate(binding_body())
    second = module.HttpReceiverBindingV1.model_validate(
        binding_body(
            native_channel_id=CHANNEL if damage == "duplicate_channel" else "C0EXAMPLE2",
            binding_id="55555555-5555-4555-8555-555555555555"
            if damage == "duplicate_channel"
            else BINDING,
        )
    )
    bindings: Any = [one] if damage == "list" else (one, second)
    if damage == "bad_key":
        bindings = (one,)
    with pytest.raises(ValueError, match="^invalid_http_receiver_runtime$"):
        rig.runtime(module, bindings=bindings, key=b"short" if damage == "bad_key" else KEY)
    assert rig.requests == []


def test_exact_native_turn_bytes_and_receiver_proof_are_bound_without_core_keys() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-2/3/19: inspect actual HTTP wire and public codec."""
    module = api()
    rig = HttpRig()
    source_scope = module.HttpReceiverScopeV1.model_validate(scope_body())
    source_binding = module.HttpReceiverBindingV1.model_validate(binding_body())
    runtime = rig.runtime(module, scope=source_scope, bindings=(source_binding,))
    object.__setattr__(source_scope, "receiver_generation", 99)
    object.__setattr__(source_binding, "channel_credential", "SyntheticMutatedCredential")
    result = rig.submit(runtime)
    assert isinstance(result, module.ReceiverSubmission) and result.status == "accepted"
    assert result.stream_id == "1700000000000-0"
    with pytest.raises((AttributeError, TypeError, ValueError)):
        result.status = "refused"
    assert len(rig.requests) == len(rig.clients) == 1
    request = rig.requests[0]
    assert request.method == "POST" and str(request.url) == BASE + "/channels/turns"
    assert request.headers["X-API-Key"] == CREDENTIAL
    assert not any(name in request.headers for name in ("Authorization", "Cookie"))
    assert json.loads(request.content) == {
        "kind": "slack",
        "address": CHANNEL,
        "adapter": NAME,
        "delivery_id": "EvNativeSynthetic",
        "conversation_id": "1700.0001",
        "author": SENDER,
        "text": "Hello synthetic receiver",
        "reply_ref": "",
        "attachments": [
            {"id": "F0EXAMPLE1", "name": "sample.txt", "mime_type": "text/plain", "size_bytes": 17}
        ],
    }
    ring = codec.build_identity_keyring("receiver", ((RECEIVER, 2, KEY),))
    proof = codec.verify_receiver_proof(request.headers["X-Curie-Receiver-Proof"], ring)
    assert isinstance(proof, codec.ReceiverProofV1)
    assert proof.model_dump(mode="json") == {
        "schema_version": 1,
        "receiver_id": RECEIVER,
        "receiver_generation": 2,
        "channel_identity_id": IDENTITY,
        "provider_installation_id": INSTALLATION,
        "binding_id": BINDING,
        "binding_generation": 7,
        "delivery_id": "EvNativeSynthetic",
        "native_channel_id": CHANNEL,
        "conversation_id": "1700.0001",
        "body_sha256": hashlib.sha256(request.content).hexdigest(),
        "slack": {
            "delivery": "app_mention",
            "user_id": SENDER,
            "event_user_team": "T1",
            "event_team": None,
            "interaction_user_team_id": None,
            "is_ext_shared_channel": False,
            "enterprise_ids": [],
        },
    }
    wrong = codec.build_identity_keyring("context", ((RECEIVER, 2, KEY),))
    assert isinstance(
        codec.verify_receiver_proof(request.headers["X-Curie-Receiver-Proof"], wrong),
        codec.ContextRefusal,
    )
    rig.closed()


@pytest.mark.parametrize(
    "damage", ["missing_event", "unknown_channel", "sender", "identity", "bot", "im"]
)
def test_invalid_or_unsupported_delivery_refuses_before_http(damage: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-2/19."""
    module = api()
    rig = HttpRig()
    runtime = rig.runtime(module)
    body, event = native()
    kwargs = {"body": body, "event": event, "slack_identity": NAME, "bot_user_id": BOT}
    if damage == "missing_event":
        body.pop("event_id")
    elif damage == "unknown_channel":
        event["channel"] = "C0EXAMPLE2"
    elif damage == "sender":
        event.pop("user")
    elif damage in {"identity", "bot"}:
        kwargs["slack_identity" if damage == "identity" else "bot_user_id"] = "synthetic-other"
    else:
        event.update(type="message", channel_type="im")
    result = runtime.submit(**kwargs)
    assert result.status == "refused" and result.stream_id is None
    assert rig.requests == []


@pytest.mark.parametrize(
    "failure",
    [
        httpx.ReadTimeout,
        httpx.ReadError,
        httpx.WriteTimeout,
        httpx.WriteError,
        httpx.RemoteProtocolError,
    ],
)
def test_only_ambiguous_transport_gets_one_identical_second_submission(failure: Any) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-3/19: do not refresh signed intent to salvage a retry."""
    module = api()
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise failure("SyntheticAmbiguousFailure", request=request)
        return httpx.Response(200, json=receipt())

    rig = HttpRig(handler)
    assert rig.submit(rig.runtime(module)).status == "accepted"
    assert len(rig.requests) == 2 and len(rig.clients) == 1
    first, second = rig.requests
    assert first.content == second.content
    assert first.headers["X-Curie-Receiver-Proof"] == second.headers["X-Curie-Receiver-Proof"]
    assert first.headers["X-API-Key"] == second.headers["X-API-Key"] == CREDENTIAL
    rig.closed()


@pytest.mark.parametrize(
    "status,expected",
    [
        (200, "accepted"),
        (202, "pending"),
        (401, "refused"),
        (403, "refused"),
        (409, "refused"),
        (413, "refused"),
        (422, "refused"),
        (404, "unavailable"),
        (429, "unavailable"),
        (503, "unavailable"),
    ],
)
def test_complete_http_outcomes_never_retry_or_fall_through(status: int, expected: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-3/19: a complete response ends this submission."""
    module = api()

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status,
            json=receipt(pending=status == 202),
            headers={"Location": "https://other.example.test/collect"},
        )

    rig = HttpRig(handler)
    result = rig.submit(rig.runtime(module))
    assert result.status == expected
    assert result.stream_id == ("1700000000000-0" if status == 200 else None)
    assert len(rig.requests) == 1
    rig.closed()


@pytest.mark.parametrize("failure", [httpx.ConnectError, httpx.PoolTimeout])
def test_nonambiguous_transport_does_not_retry(failure: Any) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19."""
    module = api()

    async def handler(request: httpx.Request) -> httpx.Response:
        raise failure("SyntheticConnectionFailure", request=request)

    rig = HttpRig(handler)
    result = rig.submit(rig.runtime(module))
    assert result.status == "unavailable" and result.stream_id is None
    assert len(rig.requests) == 1
    rig.closed()


@pytest.mark.parametrize(
    "damage", ["duplicate", "nonfinite", "boolean", "extra", "oversize", "missing"]
)
def test_receipt_strictness_and_bounds_fail_unavailable_without_retry(damage: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19: strict receipts fail closed."""
    module = api()

    async def handler(_request: httpx.Request) -> httpx.Response:
        value = receipt()
        if damage == "boolean":
            value["duplicate"] = 1
        elif damage == "extra":
            value["untrusted"] = "SyntheticResponseMarker"
        elif damage == "missing":
            del value["event_id"]
        raw = json.dumps(value).encode()
        if damage == "duplicate":
            raw = raw[:-1] + b',"duplicate":false}'
        elif damage == "nonfinite":
            raw = raw.replace(b'"duplicate": false', b'"duplicate":NaN')
        elif damage == "oversize":
            raw = b" " * 65537 + raw
        return httpx.Response(200, content=raw)

    rig = HttpRig(handler)
    result = rig.submit(rig.runtime(module))
    assert result.status == "unavailable" and result.stream_id is None
    assert len(rig.requests) == 1
    rig.closed()


class ReceiptStream(httpx.AsyncByteStream):
    """@spec HTTP-INVOCATION-EXECUTION-19: controlled response streaming and closure."""

    def __init__(self, chunks: list[bytes], *, block: bool = False) -> None:
        self.chunks, self.block, self.closed, self.read = chunks, block, False, 0
        self.entered = False
        self.release = threading.Event()

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for part in self.chunks:
            self.read += 1
            yield part
        self.entered = True
        if self.block:
            while not self.release.is_set():
                await asyncio.sleep(0.01)

    async def aclose(self) -> None:
        self.closed = True


def test_stream_bound_is_inclusive_and_overflow_stops_before_next_chunk() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19: bound bytes before buffering a complete response."""
    module = api()
    raw = json.dumps(receipt()).encode()
    exact = ReceiptStream([raw, b" " * (65536 - len(raw))])
    overflow = ReceiptStream([b" " * 32768, b" " * 32768, b"x", b"never-read"])
    streams = iter((exact, overflow))

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=next(streams))

    rig = HttpRig(handler)
    runtime = rig.runtime(module)
    assert rig.submit(runtime).status == "accepted"
    assert rig.submit(runtime).status == "unavailable"
    assert exact.closed and overflow.closed and overflow.read == 3
    assert len(rig.clients) == 2 and len(rig.requests) == 2
    rig.closed()


def test_total_deadline_covers_both_attempts_headers_and_blocked_receipt() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-3/19: one actual deadline observation, no patched clock."""
    module = api()
    stream = ReceiptStream([b"{"], block=True)
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        await asyncio.sleep(2)
        if attempts == 1:
            raise httpx.ReadTimeout("SyntheticAmbiguousFailure", request=request)
        return httpx.Response(200, stream=stream)

    rig = HttpRig(handler)
    runtime = rig.runtime(module)
    results, errors = [], []

    def submit() -> None:
        try:
            results.append(rig.submit(runtime))
        except BaseException as error:  # noqa: BLE001 - propagate owned thread failures.
            errors.append(error)

    thread = threading.Thread(target=submit, name="synthetic-http-receiver-deadline")
    started = time.monotonic()
    try:
        thread.start()
        thread.join(timeout=6.5)
        assert not thread.is_alive(), "total receiver deadline did not bound both attempts"
        assert not errors and len(results) == 1
        assert results[0].status == "unavailable" and 4.5 <= time.monotonic() - started < 6.5
        assert attempts == 2 and stream.entered and stream.closed
        rig.closed()
    finally:
        stream.release.set()
        thread.join(timeout=3)
        assert not thread.is_alive(), "owned synthetic receiver submit thread survived cleanup"


def test_caller_cancellation_propagates_and_owned_client_closes() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19."""
    module = api()

    async def handler(_request: httpx.Request) -> httpx.Response:
        raise asyncio.CancelledError

    rig = HttpRig(handler)
    with pytest.raises(asyncio.CancelledError):
        rig.submit(rig.runtime(module))
    assert len(rig.requests) == 1
    rig.closed()


@pytest.mark.parametrize("change", ["authority", "kind", "account"])
def test_namespace_registration_refuses_scope_outside_initial_ordinary_workspace(
    change: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-15/19: exact registered workspace authority."""
    module = api()
    value = scope_body()
    value["namespace"][{"authority": "authority", "kind": "kind", "account": "key"}[change]] = {
        "authority": "synthetic-enterprise",
        "kind": "enterprise",
        "account": "T2",
    }[change]
    rig = HttpRig()
    with pytest.raises(ValueError):
        rig.runtime(module, scope=module.HttpReceiverScopeV1.model_validate(value))
    assert rig.requests == []


GRID_ORG = "E0EXAMPLEORG"
GRID_WORKSPACE = "T0EXAMPLEHOME"
GRID_CHANNEL = "C0EXAMPLEGRIDCHANNEL"
GRID_SENDER = "W0EXAMPLEPERSON"


def grid_scope_body(**changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-21: a synthetic Grid workspace registration."""
    return scope_body(
        namespace={"id": NAMESPACE, "authority": "", "kind": "slack_enterprise", "key": GRID_ORG},
        installation_account=GRID_WORKSPACE,
        **changes,
    )


def grid_auth(**changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-21: synthetic workspace-install auth.test answer."""
    return {
        "ok": True, "team_id": GRID_WORKSPACE, "user_id": BOT, "bot_id": "B1",
        "enterprise_id": GRID_ORG, "is_enterprise_install": False, **changes,
    }


def grid_native() -> tuple[dict[str, Any], dict[str, Any]]:
    """@spec HTTP-INVOCATION-EXECUTION-21: synthetic Grid mention envelope."""
    event = {
        "type": "app_mention", "channel": GRID_CHANNEL, "user": GRID_SENDER,
        "text": "<@U0BOT> Hello synthetic organisation", "ts": "1700.0003",
        "user_team": "T0EXAMPLESIBLING", "team": GRID_WORKSPACE,
    }
    return {
        "type": "event_callback", "event_id": "EvNativeGridSynthetic",
        "team_id": GRID_WORKSPACE, "enterprise_id": GRID_ORG,
        "authorizations": [{
            "enterprise_id": GRID_ORG, "team_id": GRID_WORKSPACE, "user_id": BOT,
            "is_bot": True, "is_enterprise_install": False,
        }],
        "is_ext_shared_channel": False, "event": event,
    }, event


def test_grid_scope_accepts_only_the_workspace_inside_organisation_shape() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19/21."""
    module = api()
    scope = module.HttpReceiverScopeV1.model_validate(grid_scope_body())
    assert scope.namespace.kind == "slack_enterprise" and scope.namespace.key == GRID_ORG
    for namespace in (
        {"id": NAMESPACE, "authority": "", "kind": "slack_enterprise", "key": GRID_WORKSPACE},
        {"id": NAMESPACE, "authority": "https://slack.example.test", "kind": "slack_enterprise",
         "key": GRID_ORG},
        {"id": NAMESPACE, "authority": "", "kind": "slack_workspace", "key": GRID_ORG},
        {"id": NAMESPACE, "authority": "", "kind": "slack_organisation", "key": GRID_ORG},
    ):
        with pytest.raises(ValueError):
            module.HttpReceiverScopeV1.model_validate(grid_scope_body(namespace=namespace))


@pytest.mark.parametrize(
    "auth",
    [
        grid_auth(enterprise_id=None),
        grid_auth(enterprise_id="E0EXAMPLEPARTNER"),
        grid_auth(is_enterprise_install=True),
        grid_auth(is_enterprise_install=None),
        {key: value for key, value in grid_auth().items() if key != "is_enterprise_install"},
        {key: value for key, value in grid_auth().items() if key != "enterprise_id"},
        grid_auth(team_id="T0EXAMPLESIBLING"),
        grid_auth(user_id="U0OTHER"),
    ],
)
def test_grid_startup_requires_positive_workspace_install_evidence(auth: dict[str, Any]) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19/21: organisation-wide installs stay refused."""
    module = api()
    rig = HttpRig(auth=auth)
    scope = module.HttpReceiverScopeV1.model_validate(grid_scope_body())
    with pytest.raises(ValueError, match="^invalid_http_receiver_runtime$") as error:
        rig.runtime(module, scope=scope)
    assert error.value.__cause__ is None
    rig.web.auth_test.assert_called_once_with()
    assert rig.requests == []


def test_grid_mention_signs_every_enterprise_signal_for_the_api_to_check() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-2/19/21: the copier is reused, not reinterpreted."""
    module = api()
    rig = HttpRig(auth=grid_auth())
    runtime = rig.runtime(
        module,
        scope=module.HttpReceiverScopeV1.model_validate(grid_scope_body()),
        bindings=(module.HttpReceiverBindingV1.model_validate(
            binding_body(native_channel_id=GRID_CHANNEL)
        ),),
    )
    rig.web.auth_test.assert_called_once_with()
    body, event = grid_native()
    result = runtime.submit(body=body, event=event, slack_identity=NAME, bot_user_id=BOT)
    assert result.status == "accepted"
    ring = codec.build_identity_keyring("receiver", ((RECEIVER, 2, KEY),))
    proof = codec.verify_receiver_proof(rig.requests[0].headers["X-Curie-Receiver-Proof"], ring)
    assert isinstance(proof, codec.ReceiverProofV1)
    assert proof.slack.model_dump(mode="json") == {
        "delivery": "app_mention", "user_id": GRID_SENDER, "event_user_team": "T0EXAMPLESIBLING",
        "event_team": GRID_WORKSPACE, "interaction_user_team_id": None,
        "is_ext_shared_channel": False, "enterprise_ids": [GRID_ORG],
    }
    assert json.loads(rig.requests[0].content)["author"] == GRID_SENDER
    rig.closed()
    ordinary = HttpRig(auth=grid_auth())
    with pytest.raises(ValueError, match="^invalid_http_receiver_runtime$"):
        ordinary.runtime(module)
