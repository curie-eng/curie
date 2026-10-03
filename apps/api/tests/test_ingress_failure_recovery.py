"""Failed ingress attempts release owned claims and refund only their quota slot.

Both public ingress routes run against the real Postgres and Valkey fixtures.
Fault wrappers keep the real operations: a lost result is injected only after
the operation applied, and request cancellation comes from cancelling its task.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from types import ModuleType
from typing import Any

import httpx
import pytest
import redis
from aci_protocol import QueuedTurn
from curie_api import hook_signing
from curie_api.config import get_settings
from curie_api.crud import workspaces as crud_workspaces
from curie_api.routers import channels as channels_router
from curie_api.routers import hooks as hooks_router
from fastapi.testclient import TestClient
from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import create_async_engine


@dataclass
class Ingress:
    client: TestClient
    router: ModuleType
    agent_id: str
    channel_id: str
    address: str
    kind: str
    delivery_id: str
    stream: str
    valkey: redis.Redis

    @property
    def claim_key(self) -> str:
        digest = hashlib.sha256(self.delivery_id.encode()).hexdigest()[:16]
        if self.kind == "hook":
            return f"curie:hook:delivery:{self.agent_id}:recovery:{digest}"
        return f"curie:channel:delivery:{self.channel_id}:{digest}"

    @property
    def backlog_prefix(self) -> str:
        if self.kind == "hook":
            return f"curie:hook:backlog:{self.agent_id}"
        return f"curie:channel:backlog:{self.channel_id}"

    def request(self) -> tuple[str, dict[str, Any]]:
        if self.kind == "channel":
            return "/channels/turns", {
                "headers": {"X-API-Key": get_settings().api_key},
                "json": {
                    "kind": "email",
                    "address": self.address,
                    "delivery_id": self.delivery_id,
                    "conversation_id": "recovery-thread",
                    "author": "sender@example.test",
                    "text": "recover this delivery",
                    "reply_ref": "recovery-ref",
                },
            }
        body = json.dumps({"commonLabels": {"curie_workload": "acme-api"}}).encode()
        stamp = str(int(time.time()))
        secret = hook_signing.derive(get_settings().api_key, agent_id=self.agent_id, generation=0)
        return f"/hooks/{self.agent_id}/recovery", {
            "content": body,
            "headers": {
                "Content-Type": "application/json",
                "X-Curie-Timestamp": stamp,
                "X-Curie-Delivery-Id": self.delivery_id,
                "X-Curie-Signature-256": hook_signing.sign(
                    secret,
                    timestamp=stamp,
                    delivery_id=self.delivery_id,
                    hook="recovery",
                    tool_access=None,
                    body=body,
                ),
            },
        }

    def post(self) -> Any:
        path, kwargs = self.request()
        return self.client.post(path, **kwargs)

    def charges(self) -> int:
        # Sum real counter buckets rather than requiring two requests to share
        # one wall clock window. Per attempt marker keys are not counters.
        keys = [
            key
            for key in self.valkey.scan_iter(match=f"{self.backlog_prefix}:*")
            if str(key).removeprefix(f"{self.backlog_prefix}:").isdigit()
        ]
        return sum(int(self.valkey.get(key) or 0) for key in keys)

    def entries(self) -> list[tuple[str, QueuedTurn]]:
        return [
            (stream_id, QueuedTurn.model_validate_json(fields["payload"]))
            for stream_id, fields in self.valkey.xrange(self.stream)
        ]


def _channel_id(agent_id: str) -> str:
    async def read() -> str:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as connection:
                result = await connection.execute(
                    sql_text("SELECT id FROM curie.agent_channels WHERE agent_id = :agent_id"),
                    {"agent_id": uuid.UUID(agent_id)},
                )
                return str(result.scalar_one())
        finally:
            await engine.dispose()

    return asyncio.run(read())


@pytest.fixture(params=["hook", "channel"])
def ingress(
    request: pytest.FixtureRequest,
    hooks_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Ingress]:
    for prefix in ("HOOK", "CHANNEL_BINDING"):
        monkeypatch.setenv(f"{prefix}_BACKLOG_LIMIT", "64")
        monkeypatch.setenv(f"{prefix}_BACKLOG_WINDOW_S", "3600")
    get_settings.cache_clear()
    name = f"acme-recovery-{uuid.uuid4().hex[:12]}"
    address = f"{name}@example.test"
    created = hooks_client.post(
        "/agents",
        headers=auth_headers,
        json={
            "name": name,
            "channel": {
                "kind": "email",
                "address": address,
                "endpoint": "http://mail-adapter.example.com/",
                "adapter": "acme-mail",
            },
        },
    )
    assert created.status_code == 201, created.text
    agent_id = str(created.json()["id"])
    bound = Ingress(
        hooks_client,
        hooks_router if request.param == "hook" else channels_router,
        agent_id,
        _channel_id(agent_id),
        address,
        request.param,
        f"delivery-{uuid.uuid4().hex}",
        runs_stream,
        valkey,
    )
    try:
        yield bound
    finally:
        for scope in (bound.agent_id, bound.channel_id):
            for key in list(valkey.scan_iter(match=f"curie:*:{scope}:*")):
                valkey.delete(key)
        get_settings.cache_clear()


def _assert_retry_enqueues_once(ingress: Ingress) -> None:
    accepted = ingress.post()
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["duplicate"] is False
    assert accepted.json()["stream_id"]
    entries = ingress.entries()
    assert len(entries) == 1
    assert entries[0][0] == accepted.json()["stream_id"]
    assert ingress.valkey.get(ingress.claim_key) == accepted.json()["stream_id"]
    assert ingress.charges() == 1
    replay = ingress.post()
    assert replay.status_code == 200, replay.text
    assert replay.json()["duplicate"] is True
    assert replay.json()["stream_id"] == accepted.json()["stream_id"]
    assert len(ingress.entries()) == 1
    assert ingress.charges() == 1


def test_quota_refusal_keeps_charges_without_retaining_attempt_markers(
    ingress: Ingress,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = get_settings().model_copy(
        update={"hook_backlog_limit": 1, "channel_binding_backlog_limit": 1}
    )
    monkeypatch.setattr(ingress.router, "get_settings", lambda: settings)
    accepted_delivery = ingress.delivery_id
    accepted_claim = ingress.claim_key
    accepted = ingress.post()
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["duplicate"] is False
    assert ingress.charges() == 1
    marker_pattern = f"{ingress.backlog_prefix}:*:attempt:*"
    accepted_markers = {
        key: ingress.valkey.get(key) for key in ingress.valkey.scan_iter(match=marker_pattern)
    }
    assert len(accepted_markers) == 1

    ingress.delivery_id = f"refused_{uuid.uuid4().hex}"
    for expected_charges in (2, 3):
        refused = ingress.post()
        assert refused.status_code == 429, refused.text
        assert refused.headers["Retry-After"] == "3600"
        assert not ingress.valkey.exists(ingress.claim_key)
        assert ingress.charges() == expected_charges
        assert ingress.valkey.get(accepted_claim) == accepted.json()["stream_id"]
        assert len(ingress.entries()) == 1
        assert {
            key: ingress.valkey.get(key)
            for key in ingress.valkey.scan_iter(match=marker_pattern)
        } == accepted_markers

    ingress.delivery_id = accepted_delivery
    replay = ingress.post()
    assert replay.status_code == 200, replay.text
    assert replay.json()["duplicate"] is True
    assert replay.json()["stream_id"] == accepted.json()["stream_id"]
    assert ingress.charges() == 3
    assert len(ingress.entries()) == 1
    assert {
        key: ingress.valkey.get(key) for key in ingress.valkey.scan_iter(match=marker_pattern)
    } == accepted_markers


@pytest.mark.parametrize("failure", ["quota", "mint", "enqueue_wrongtype"])
def test_failure_refunds_the_slot_and_retry_enqueues_once(
    ingress: Ingress,
    failure: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_error = RuntimeError(f"injected {failure} failure")
    with monkeypatch.context() as fault:
        if failure == "quota":
            real_take = ingress.router.take_backlog_slot

            async def take_then_lose_result(*args: Any, **kwargs: Any) -> Any:
                await real_take(*args, **kwargs)
                assert ingress.charges() == 1
                raise original_error

            fault.setattr(ingress.router, "take_backlog_slot", take_then_lose_result)
        elif failure == "mint":
            real_mint = ingress.router._mint_turn

            def mint_then_fail(*args: Any, **kwargs: Any) -> Any:
                real_mint(*args, **kwargs)
                raise original_error

            fault.setattr(ingress.router, "_mint_turn", mint_then_fail)
        else:
            # XADD fails inside the actual enqueue Lua script. Its owner arm
            # has not written yet; the reclaim arm writes before XADD, and
            # Valkey does not roll back those writes on WRONGTYPE.
            ingress.valkey.set(ingress.stream, "not a stream")
        if failure == "enqueue_wrongtype":
            with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
                ingress.post()
            ingress.valkey.delete(ingress.stream)
        else:
            with pytest.raises(RuntimeError) as caught:
                ingress.post()
            assert caught.value is original_error

    if failure == "enqueue_wrongtype":
        # This is the issue's negative proof: the base answers this retry 202
        # with no stream id despite the failed attempt never enqueuing.
        _assert_retry_enqueues_once(ingress)
        return
    assert not ingress.valkey.exists(ingress.claim_key)
    assert ingress.entries() == []
    assert ingress.charges() == 0
    _assert_retry_enqueues_once(ingress)


def test_quota_failure_before_application_preserves_another_delivery_charge(
    ingress: Ingress,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed_delivery = ingress.delivery_id
    ingress.delivery_id = f"other-{uuid.uuid4().hex}"
    accepted = ingress.post()
    assert accepted.status_code == 200, accepted.text
    other_claim = ingress.claim_key
    ingress.delivery_id = failed_delivery
    assert ingress.charges() == 1
    client = ingress.client.app.state.valkey
    real_take = ingress.router.take_backlog_slot
    real_eval = client.eval
    quota_running = False
    injected = False

    async def faulted_eval(*args: Any, **kwargs: Any) -> Any:
        nonlocal injected
        if not quota_running or injected:
            return await real_eval(*args, **kwargs)
        injected = True
        counter = str(args[2])
        assert counter.startswith(f"{ingress.backlog_prefix}:")
        saved_count = ingress.valkey.get(counter)
        saved_ttl = ingress.valkey.ttl(counter)
        ingress.valkey.delete(counter)
        ingress.valkey.rpush(counter, "not a counter")
        try:
            # The real quota Lua script fails before INCR can charge anything.
            return await real_eval(*args, **kwargs)
        finally:
            ingress.valkey.delete(counter)
            if saved_count is not None:
                ingress.valkey.set(counter, saved_count)
                if saved_ttl > 0:
                    ingress.valkey.expire(counter, saved_ttl)

    async def take_with_wrongtype_counter(*args: Any, **kwargs: Any) -> Any:
        nonlocal quota_running
        quota_running = True
        try:
            return await real_take(*args, **kwargs)
        finally:
            quota_running = False

    with monkeypatch.context() as fault:
        fault.setattr(client, "eval", faulted_eval)
        fault.setattr(ingress.router, "take_backlog_slot", take_with_wrongtype_counter)
        with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
            ingress.post()
    assert injected
    assert not ingress.valkey.exists(ingress.claim_key)
    assert ingress.valkey.get(other_claim) == accepted.json()["stream_id"]
    assert ingress.charges() == 1
    assert len(ingress.entries()) == 1
    retried = ingress.post()
    assert retried.status_code == 200, retried.text
    assert retried.json()["duplicate"] is False
    assert len(ingress.entries()) == 2
    assert ingress.charges() == 2


@pytest.mark.parametrize("ingress", ["hook"], indirect=True)
def test_workspace_selection_failure_releases_hook_claim_and_refunds_slot(
    ingress: Ingress,
    auth_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    get_settings.cache_clear()
    configured = ingress.client.patch(
        f"/agents/{ingress.agent_id}",
        headers=auth_headers,
        json={
            "source_bindings": {
                "recovery": {
                    "workload_pointer": "/commonLabels/curie_workload",
                    "map": {
                        "acme-api": {
                            "repository": "acme-corp/acme-bot",
                            "revision": "0123456789abcdef0123456789abcdef01234567",
                        }
                    },
                }
            }
        },
    )
    assert configured.status_code == 200, configured.text
    original_error = RuntimeError("workspace selection result was lost")
    real_select = crud_workspaces.select_thread_workspace

    async def select_then_fail(*args: Any, **kwargs: Any) -> Any:
        await real_select(*args, **kwargs)
        raise original_error

    with monkeypatch.context() as fault:
        fault.setattr(crud_workspaces, "select_thread_workspace", select_then_fail)
        with pytest.raises(RuntimeError) as caught:
            ingress.post()
        assert caught.value is original_error
    assert not ingress.valkey.exists(ingress.claim_key)
    assert ingress.charges() == 0
    assert ingress.entries() == []
    _assert_retry_enqueues_once(ingress)


def _cancel_request_at(
    ingress: Ingress,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    *,
    apply: bool,
) -> asyncio.CancelledError:
    """Cancel the HTTP task while the chosen real operation's result is pending."""

    real_operation = getattr(ingress.router, operation)

    async def drive() -> asyncio.CancelledError:
        entered = asyncio.Event()
        held = asyncio.Event()

        async def pause(*args: Any, **kwargs: Any) -> Any:
            result = await real_operation(*args, **kwargs) if apply else None
            entered.set()
            await held.wait()
            return result if apply else await real_operation(*args, **kwargs)

        monkeypatch.setattr(ingress.router, operation, pause)
        path, kwargs = ingress.request()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=ingress.client.app),
            base_url="http://api.example.com",
        ) as client:
            task = asyncio.create_task(client.post(path, **kwargs))
            try:
                await asyncio.wait_for(entered.wait(), timeout=15)
                task.cancel("injected ingress request cancellation")
                with pytest.raises(asyncio.CancelledError) as caught:
                    await task
                return caught.value
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    assert ingress.client.portal is not None
    return ingress.client.portal.call(drive)


@pytest.mark.parametrize("operation", ["take_backlog_slot", "enqueue_owned"])
def test_cancelled_request_refunds_counter_directly(
    ingress: Ingress,
    operation: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as fault:
        error = _cancel_request_at(
            ingress, fault, operation, apply=operation == "take_backlog_slot"
        )
    assert isinstance(error, asyncio.CancelledError)
    assert error.args == ("injected ingress request cancellation",)
    assert not ingress.valkey.exists(ingress.claim_key)
    assert ingress.entries() == []
    # No second delivery or retry is needed to prove a refund. The quota
    # applied before either cancellation, including the unobserved take result.
    assert ingress.charges() == 0


def test_cancellation_before_quota_application_preserves_another_delivery_charge(
    ingress: Ingress,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed_delivery = ingress.delivery_id
    ingress.delivery_id = f"other-{uuid.uuid4().hex}"
    accepted = ingress.post()
    assert accepted.status_code == 200, accepted.text
    other_claim = ingress.claim_key
    ingress.delivery_id = failed_delivery
    with monkeypatch.context() as fault:
        _cancel_request_at(ingress, fault, "take_backlog_slot", apply=False)
    assert not ingress.valkey.exists(ingress.claim_key)
    assert ingress.valkey.get(other_claim) == accepted.json()["stream_id"]
    assert ingress.charges() == 1
    assert len(ingress.entries()) == 1


def test_failure_does_not_release_successor_or_refund_another_delivery(
    ingress: Ingress,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed_delivery = ingress.delivery_id
    ingress.delivery_id = f"other-{uuid.uuid4().hex}"
    accepted = ingress.post()
    assert accepted.status_code == 200, accepted.text
    other_claim = ingress.claim_key
    ingress.delivery_id = failed_delivery
    successor = f"pending:{uuid.uuid4().hex}"
    original_error = RuntimeError("the old owner failed after replacement")
    real_mint = ingress.router._mint_turn

    def replace_owner_then_fail(*args: Any, **kwargs: Any) -> Any:
        real_mint(*args, **kwargs)
        assert ingress.charges() == 2
        ingress.valkey.delete(ingress.claim_key)
        assert ingress.valkey.set(ingress.claim_key, successor, nx=True, ex=300)
        raise original_error

    with monkeypatch.context() as fault:
        fault.setattr(ingress.router, "_mint_turn", replace_owner_then_fail)
        with pytest.raises(RuntimeError) as caught:
            ingress.post()
        assert caught.value is original_error
    assert ingress.valkey.get(ingress.claim_key) == successor
    assert ingress.valkey.get(other_claim) == accepted.json()["stream_id"]
    assert ingress.charges() == 1
    assert len(ingress.entries()) == 1
    pending = ingress.post()
    assert pending.status_code == 202, pending.text
    assert pending.json()["duplicate"] is True
    assert pending.json()["stream_id"] is None
    assert ingress.charges() == 1


def test_cleanup_failure_is_logged_and_preserves_original_error(
    ingress: Ingress,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    original_error = RuntimeError("original ingress failure")
    cleanup_error = redis.ConnectionError("injected cleanup result loss")
    real_mint = ingress.router._mint_turn
    client = ingress.client.app.state.valkey
    real_eval = client.eval
    cleanup_started = False
    cleanup_calls = 0

    def mint_then_fail(*args: Any, **kwargs: Any) -> Any:
        nonlocal cleanup_started
        real_mint(*args, **kwargs)
        cleanup_started = True
        raise original_error

    async def eval_then_lose_cleanup_result(*args: Any, **kwargs: Any) -> Any:
        nonlocal cleanup_calls
        result = await real_eval(*args, **kwargs)
        if cleanup_started:
            cleanup_calls += 1
            raise cleanup_error
        return result

    with monkeypatch.context() as fault:
        fault.setattr(ingress.router, "_mint_turn", mint_then_fail)
        fault.setattr(client, "eval", eval_then_lose_cleanup_result)
        with pytest.raises(RuntimeError) as caught:
            ingress.post()
        assert caught.value is original_error
    assert cleanup_calls > 0
    assert str(cleanup_error) in caplog.text
    assert ingress.entries() == []


@pytest.mark.parametrize("lost_result", ["raise", "cancel"])
def test_committed_enqueue_keeps_receipt_entry_and_quota_charge(
    ingress: Ingress,
    lost_result: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_error = RuntimeError("the committed enqueue result was lost")
    real_enqueue = ingress.router.enqueue_owned
    with monkeypatch.context() as fault:
        if lost_result == "raise":

            async def enqueue_then_raise(*args: Any, **kwargs: Any) -> Any:
                result = await real_enqueue(*args, **kwargs)
                assert result[0] is True
                raise original_error

            fault.setattr(ingress.router, "enqueue_owned", enqueue_then_raise)
            with pytest.raises(RuntimeError) as caught:
                ingress.post()
            assert caught.value is original_error
        else:
            _cancel_request_at(ingress, fault, "enqueue_owned", apply=True)

    entries = ingress.entries()
    assert len(entries) == 1
    stream_id = entries[0][0]
    assert ingress.valkey.get(ingress.claim_key) == stream_id
    assert ingress.valkey.ttl(ingress.claim_key) == -1
    assert ingress.charges() == 1
    replay = ingress.post()
    assert replay.status_code == 200, replay.text
    assert replay.json()["duplicate"] is True
    assert replay.json()["stream_id"] == stream_id
    assert len(ingress.entries()) == 1
    assert ingress.charges() == 1
