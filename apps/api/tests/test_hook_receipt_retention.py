"""ADR 0237 receipt evidence survives production settled stream retention."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Iterator
from typing import Any

import pytest
import redis
import redis.asyncio as async_redis
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.routers import hooks as hooks_router
from curie_worker.stream_retention import trim_settled
from fastapi.testclient import TestClient
from test_hook_tool_access import _backlog, _claim, _post, _signed_headers
from test_hook_tool_access import hook_agent as _hook_agent

hook_agent = _hook_agent


@pytest.fixture
def rival_client(_disposable_db: Any, runs_stream: str) -> Iterator[TestClient]:
    with TestClient(create_app()) as client:
        yield client


def _receipt_key(agent_id: str) -> str:
    return _claim(agent_id) + ":receipt"


def _proof(first: Any) -> dict[str, Any]:
    receipt = first.json()
    return {
        "version": 1,
        "stream_id": receipt["stream_id"],
        "conversation_id": receipt["conversation_id"],
        "tool_access": receipt["tool_access"],
    }


def _snapshot(valkey: redis.Redis, agent_id: str, stream: str) -> tuple[Any, ...]:
    # DUMP observes corrupt bytes and non-string types without decoding them.
    return (
        valkey.dump(_claim(agent_id)),
        valkey.dump(_receipt_key(agent_id)),
        valkey.dump(stream),
        _backlog(valkey, agent_id),
    )


def _settle_and_trim(valkey: redis.Redis, stream: str, stream_id: str) -> None:
    """@spec HOOK-RECEIPT-6: exercise the actual worker retention algorithm."""
    group = "receipt-consumer"
    valkey.xgroup_create(stream, group, id="0-0")
    rows = valkey.xreadgroup(group, "worker", {stream: ">"}, count=1)
    assert rows[0][1][0][0] == stream_id
    assert valkey.xack(stream, group, stream_id) == 1

    async def retain() -> int:
        async with async_redis.from_url(get_settings().valkey_dsn()) as client:
            return await trim_settled(client, stream, min_age_s=0)

    # TIME's age floor is exclusive; allow the entry's creation millisecond to pass.
    time.sleep(0.002)
    assert asyncio.run(retain()) == 1
    assert valkey.xlen(stream) == 0


@pytest.mark.parametrize("access", [None, "read-only"])
def test_faithful_retry_after_settled_retention_retains_receipt(
    hooks_client: TestClient, hook_agent: str, valkey: redis.Redis,
    runs_stream: str, access: str | None,
) -> None:
    """@spec HOOK-RECEIPT-1, HOOK-RECEIPT-2, HOOK-RECEIPT-3, HOOK-RECEIPT-6."""
    first = _post(hooks_client, hook_agent, access=access, body=b'{"synthetic":"first"}')
    assert first.status_code == 200, first.text
    raw = valkey.get(_receipt_key(hook_agent))
    assert raw is not None
    assert json.loads(raw) == _proof(first)
    assert len(raw.encode("utf-8")) <= 16384
    assert valkey.ttl(_receipt_key(hook_agent)) == -1
    assert valkey.ttl(_claim(hook_agent)) == -1
    assert valkey.xlen(runs_stream) == 1  # Producers never trim.
    _settle_and_trim(valkey, runs_stream, first.json()["stream_id"])
    saved = _snapshot(valkey, hook_agent, runs_stream)
    retry = _post(hooks_client, hook_agent, access=access, body=b'{"synthetic":"retry"}')
    assert retry.status_code == 200, retry.text
    assert retry.json() == {**first.json(), "duplicate": True}
    assert _snapshot(valkey, hook_agent, runs_stream) == saved


@pytest.mark.parametrize("original,retry", [(None, "read-only"), ("read-only", None)])
@pytest.mark.parametrize("trim", [False, True])
def test_retained_policy_cannot_be_relabelled(
    hooks_client: TestClient, hook_agent: str, valkey: redis.Redis,
    runs_stream: str, original: str | None, retry: str | None, trim: bool,
) -> None:
    """@spec HOOK-RECEIPT-3: both mismatch directions preserve all stored state."""
    first = _post(hooks_client, hook_agent, access=original)
    assert first.status_code == 200, first.text
    if trim:
        _settle_and_trim(valkey, runs_stream, first.json()["stream_id"])
    saved = _snapshot(valkey, hook_agent, runs_stream)
    refused = _post(hooks_client, hook_agent, access=retry)
    assert refused.status_code == 409, refused.text
    assert _snapshot(valkey, hook_agent, runs_stream) == saved
    faithful = _post(hooks_client, hook_agent, access=original)
    assert faithful.status_code == 200, faithful.text
    assert faithful.json() == {**first.json(), "duplicate": True}


def _explicit(client: TestClient, agent_id: str, conversation: str) -> Any:
    body = b"{}"
    return client.post(
        f"/hooks/{agent_id}/issues",
        params={"conversation_id": conversation, "placeholder": "synthetic-reply"},
        content=body,
        headers=_signed_headers(agent_id, hook="issues", tool_access=None, body=body),
    )


def test_retained_receipt_preserves_original_explicit_conversation(
    hooks_client: TestClient, hook_agent: str, valkey: redis.Redis, runs_stream: str,
) -> None:
    """@spec HOOK-RECEIPT-3: retry reply selection cannot replace original proof."""
    first = _explicit(hooks_client, hook_agent, "synthetic-original-thread")
    assert first.status_code == 200, first.text
    assert first.json()["conversation_id"] == "synthetic-original-thread"
    _settle_and_trim(valkey, runs_stream, first.json()["stream_id"])
    retry = _explicit(hooks_client, hook_agent, "synthetic-different-thread")
    assert retry.status_code == 200, retry.text
    assert retry.json() == {**first.json(), "duplicate": True}


_BAD_METADATA = [
    "invalid-json", "array", "unknown-field", "duplicate-field", "missing-field",
    "bool-version", "float-version", "wrong-version", "nonfinite", "infinity", "invalid-utf8",
    "wrongtype", "oversize", "empty-conversation", "null-conversation",
    "bad-policy", "noncanonical-stream", "invalid-stream", "claim-disagreement", "lone-surrogate",
]


@pytest.mark.parametrize("bad", _BAD_METADATA)
def test_existing_bad_metadata_refuses_without_queue_fallback_or_mutation(
    hooks_client: TestClient, hook_agent: str, valkey: redis.Redis,
    runs_stream: str, bad: str,
) -> None:
    """@spec HOOK-RECEIPT-2, HOOK-RECEIPT-3: even a valid queue cannot repair proof."""
    first = _post(hooks_client, hook_agent)
    assert first.status_code == 200, first.text
    proof = _proof(first)
    raw: str | bytes
    if bad == "invalid-json":
        raw = "{"
    elif bad == "array":
        raw = "[]"
    elif bad == "duplicate-field":
        raw = json.dumps(proof)[:-1] + ',"version":1}'
    elif bad == "invalid-utf8":
        raw = b'\xff'
    elif bad == "oversize":
        proof["conversation_id"] = "x" * 16384
        raw = json.dumps(proof)
    elif bad == "wrongtype":
        valkey.delete(_receipt_key(hook_agent))
        valkey.rpush(_receipt_key(hook_agent), "not-a-string")
        raw = ""  # Already installed below's deliberately non-string value.
    else:
        if bad == "unknown-field":
            proof["payload"] = "must-not-be-retained"
        elif bad == "missing-field":
            del proof["tool_access"]
        elif bad == "bool-version":
            proof["version"] = True
        elif bad == "float-version":
            proof["version"] = 1.0
        elif bad == "wrong-version":
            proof["version"] = 2
        elif bad == "nonfinite":
            proof["version"] = float("nan")
        elif bad == "infinity":
            proof["version"] = float("inf")
        elif bad == "empty-conversation":
            proof["conversation_id"] = ""
        elif bad == "null-conversation":
            proof["conversation_id"] = None
        elif bad == "lone-surrogate":
            proof["conversation_id"] = "\ud800"
        elif bad == "bad-policy":
            proof["tool_access"] = "read-write"
        elif bad == "noncanonical-stream":
            proof["stream_id"] = "0" + proof["stream_id"]
        elif bad == "invalid-stream":
            proof["stream_id"] = "not-a-stream-id"
        elif bad == "claim-disagreement":
            proof["stream_id"] = "1-0"
        raw = json.dumps(proof)
    if bad != "wrongtype":
        valkey.set(_receipt_key(hook_agent), raw)
    saved = _snapshot(valkey, hook_agent, runs_stream)
    refused = _post(hooks_client, hook_agent)
    assert refused.status_code == 409, refused.text
    assert _snapshot(valkey, hook_agent, runs_stream) == saved


@pytest.mark.parametrize("wrongtype", [False, True])
def test_nonmatching_or_nonstring_claim_cannot_attest_metadata(
    hooks_client: TestClient, hook_agent: str, valkey: redis.Redis,
    runs_stream: str, wrongtype: bool,
) -> None:
    """@spec HOOK-RECEIPT-3: metadata is proof only beside its matching claim."""
    first = _post(hooks_client, hook_agent)
    assert first.status_code == 200, first.text
    valkey.delete(_claim(hook_agent))
    if wrongtype:
        valkey.rpush(_claim(hook_agent), "not-a-string")
    else:
        valkey.set(_claim(hook_agent), "1-0")
    saved = _snapshot(valkey, hook_agent, runs_stream)
    refused = _post(hooks_client, hook_agent)
    assert refused.status_code == 409, refused.text
    assert _snapshot(valkey, hook_agent, runs_stream) == saved


@pytest.mark.parametrize("access", [None, "read-only"])
def test_legacy_metadata_absence_uses_queue_without_reconstructing_receipt(
    hooks_client: TestClient, hook_agent: str, valkey: redis.Redis,
    runs_stream: str, access: str | None,
) -> None:
    """@spec HOOK-RECEIPT-4: old producer succeeds only while original evidence lives."""
    first = _post(hooks_client, hook_agent, access=access)
    assert first.status_code == 200, first.text
    valkey.delete(_receipt_key(hook_agent))  # Emulate the pre-metadata producer.
    retry = _post(hooks_client, hook_agent, access=access)
    assert retry.status_code == 200, retry.text
    assert retry.json() == {**first.json(), "duplicate": True}
    assert not valkey.exists(_receipt_key(hook_agent))
    _settle_and_trim(valkey, runs_stream, first.json()["stream_id"])
    saved = _snapshot(valkey, hook_agent, runs_stream)
    refused = _post(hooks_client, hook_agent, access=access)
    assert refused.status_code == 409, refused.text
    assert _snapshot(valkey, hook_agent, runs_stream) == saved


@pytest.mark.parametrize("pending", [False, True])
def test_orphan_metadata_never_admits_fresh_work(
    hooks_client: TestClient, hook_agent: str, valkey: redis.Redis,
    runs_stream: str, pending: bool,
) -> None:
    """@spec HOOK-RECEIPT-4: neither missing nor pending claim legitimizes an orphan."""
    proof = {"version": 1, "stream_id": "1-0", "conversation_id": "orphan", "tool_access": None}
    valkey.set(_receipt_key(hook_agent), json.dumps(proof))
    if pending:
        valkey.set(_claim(hook_agent), "pending:foreign-owner", ex=60)
    saved = _snapshot(valkey, hook_agent, runs_stream)
    refused = _post(hooks_client, hook_agent)
    assert refused.status_code == 409, refused.text
    assert _snapshot(valkey, hook_agent, runs_stream) == saved


def test_invalid_signature_is_refused_before_reading_bad_metadata(
    hooks_client: TestClient, hook_agent: str, valkey: redis.Redis, runs_stream: str,
) -> None:
    """@spec HOOK-RECEIPT-6: retained evidence cannot bypass authentication."""
    valkey.set(_claim(hook_agent), "1-0")
    valkey.rpush(_receipt_key(hook_agent), "bad-type")
    saved = _snapshot(valkey, hook_agent, runs_stream)
    refused = _post(hooks_client, hook_agent, signature="sha256=bad")
    assert refused.status_code == 401, refused.text
    assert _snapshot(valkey, hook_agent, runs_stream) == saved


def test_oversized_reply_conversation_refuses_before_claim_quota_and_enqueue(
    hooks_client: TestClient, hook_agent: str, valkey: redis.Redis, runs_stream: str,
) -> None:
    """@spec HOOK-RECEIPT-2: account for UTF-8 and JSON encoding before any writes."""
    # Escaped JSON exceeds 16KiB while the URL stays below HTTPx's URL cap.
    refused = _explicit(hooks_client, hook_agent, "\u2603" * 3000)
    assert refused.status_code == 422, refused.text
    assert not valkey.exists(_claim(hook_agent), _receipt_key(hook_agent))
    assert _backlog(valkey, hook_agent) == {}
    assert valkey.xlen(runs_stream) == 0


@pytest.mark.parametrize("access", [None, "read-only"])
def test_parallel_signed_deliveries_serialize_to_one_immutable_receipt(
    hooks_client: TestClient, rival_client: TestClient, hook_agent: str,
    valkey: redis.Redis, runs_stream: str, access: str | None,
) -> None:
    """@spec HOOK-RECEIPT-5: independent HTTP requests retain the SQL source gate."""
    async def both() -> list[Any]:
        return list(await asyncio.gather(
            asyncio.to_thread(_post, hooks_client, hook_agent, access=access),
            asyncio.to_thread(_post, rival_client, hook_agent, access=access),
        ))

    responses = asyncio.run(both())
    assert [response.status_code for response in responses] == [200, 200]
    winner, = [response for response in responses if not response.json()["duplicate"]]
    duplicate, = [response for response in responses if response.json()["duplicate"]]
    assert duplicate.json() == {**winner.json(), "duplicate": True}
    assert valkey.xlen(runs_stream) == 1
    assert json.loads(valkey.get(_receipt_key(hook_agent))) == _proof(winner)


def test_expired_lease_without_successor_is_reclaimed_with_metadata(
    hooks_client: TestClient, hook_agent: str, valkey: redis.Redis, runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HOOK-RECEIPT-5: an actual expired lease can enqueue with its proof."""
    real_claim = hooks_router.claim_delivery

    async def expire(*args: Any, **kwargs: Any) -> Any:
        result = await real_claim(*args, **kwargs)
        valkey.pexpire(_claim(hook_agent), 1)
        deadline = time.monotonic() + 2
        while valkey.exists(_claim(hook_agent)) and time.monotonic() < deadline:
            await asyncio.sleep(0.002)
        assert not valkey.exists(_claim(hook_agent))
        return result

    monkeypatch.setattr(hooks_router, "claim_delivery", expire)
    winner = _post(hooks_client, hook_agent)
    assert winner.status_code == 200, winner.text
    assert valkey.xlen(runs_stream) == 1
    assert valkey.get(_claim(hook_agent)) == winner.json()["stream_id"]
    assert json.loads(valkey.get(_receipt_key(hook_agent))) == _proof(winner)


def test_expired_owner_cannot_replace_real_successor_receipt(
    hook_agent: str, valkey: redis.Redis, runs_stream: str,
) -> None:
    """@spec HOOK-RECEIPT-5: real shared Lua rejects stale owners and policies."""
    from curie_api.delivery import claim_delivery, enqueue_owned

    key = _claim(hook_agent)
    metadata_key = _receipt_key(hook_agent)

    async def attempt(client: Any, owner: str, access: str | None) -> tuple[bool, str]:
        proof = {"version": 1, "conversation_id": "synthetic-successor", "tool_access": access}
        prefix = json.dumps(proof, separators=(",", ":"))[:-1] + ',"stream_id":"'
        return await enqueue_owned(
            client, key=key, stream=runs_stream, owner=owner, payload="synthetic-payload",
            payload_field="payload", lease_s=60, receipt_key=metadata_key, receipt_prefix=prefix,
        )

    async def race() -> tuple[str, tuple[Any, ...]]:
        async with async_redis.from_url(get_settings().valkey_dsn()) as client:
            assert await claim_delivery(client, key, "pending:old-owner", 60)
            valkey.pexpire(key, 1)
            deadline = time.monotonic() + 2
            while valkey.exists(key) and time.monotonic() < deadline:
                await asyncio.sleep(0.002)
            assert not valkey.exists(key)
            assert await claim_delivery(client, key, "pending:new-owner", 60)
            enqueued, stream_id = await attempt(client, "pending:new-owner", "read-only")
            assert enqueued
            saved = _snapshot(valkey, hook_agent, runs_stream)
            assert await attempt(client, "pending:old-owner", None) == (False, stream_id)
            return stream_id, saved

    stream_id, saved = asyncio.run(race())
    assert _snapshot(valkey, hook_agent, runs_stream) == saved
    assert valkey.xlen(runs_stream) == 1
    assert valkey.get(key) == stream_id
    assert json.loads(valkey.get(metadata_key)) == {
        "version": 1, "stream_id": stream_id,
        "conversation_id": "synthetic-successor", "tool_access": "read-only",
    }


def test_foreign_pending_successor_gets_no_orphan_success_receipt(
    hooks_client: TestClient, hook_agent: str, valkey: redis.Redis, runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HOOK-RECEIPT-5: a slow owner cannot publish metadata for its successor."""
    real_claim = hooks_router.claim_delivery

    async def hand_over(*args: Any, **kwargs: Any) -> Any:
        result = await real_claim(*args, **kwargs)
        valkey.set(_claim(hook_agent), "pending:foreign-successor", ex=60)
        return result

    monkeypatch.setattr(hooks_router, "claim_delivery", hand_over)
    pending = _post(hooks_client, hook_agent)
    assert pending.status_code == 202, pending.text
    assert pending.json()["stream_id"] is None
    assert valkey.get(_claim(hook_agent)) == "pending:foreign-successor"
    assert not valkey.exists(_receipt_key(hook_agent))
    assert valkey.xlen(runs_stream) == 0


def test_lost_successful_enqueue_response_preserves_atomic_metadata_and_charge(
    hooks_client: TestClient, hook_agent: str, valkey: redis.Redis, runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HOOK-RECEIPT-1, HOOK-RECEIPT-5: failure after real Lua success is durable."""
    real_enqueue = hooks_router.enqueue_owned
    applied: dict[str, Any] = {}
    failure = RuntimeError("synthetic lost enqueue response")

    async def enqueue_and_lose_result(*args: Any, **kwargs: Any) -> Any:
        result = await real_enqueue(*args, **kwargs)
        assert result[0] is True
        assert valkey.get(_claim(hook_agent)) == result[1]
        applied["proof"] = json.loads(valkey.get(_receipt_key(hook_agent)))
        applied["backlog"] = _backlog(valkey, hook_agent)
        raise failure

    with monkeypatch.context() as fault:
        fault.setattr(hooks_router, "enqueue_owned", enqueue_and_lose_result)
        with pytest.raises(RuntimeError) as caught:
            _post(hooks_client, hook_agent, access="read-only")
        assert caught.value is failure
    assert valkey.xlen(runs_stream) == 1
    assert _backlog(valkey, hook_agent) == applied["backlog"]
    _settle_and_trim(valkey, runs_stream, applied["proof"]["stream_id"])
    retry = _post(hooks_client, hook_agent, access="read-only")
    assert retry.status_code == 200, retry.text
    assert retry.json()["duplicate"] is True
    assert _proof(retry) == applied["proof"]
    assert _backlog(valkey, hook_agent) == applied["backlog"]
    assert valkey.xlen(runs_stream) == 0


def test_shared_enqueue_without_metadata_ignores_existing_sidekey(
    hook_agent: str, valkey: redis.Redis, runs_stream: str,
) -> None:
    """@spec HOOK-RECEIPT-1, HOOK-RECEIPT-6: preserve non-hook shared callers."""
    from curie_api.delivery import claim_delivery, enqueue_owned

    # A distinct synthetic namespace exercises the shared function signature
    # used by channel ingress and GitHub review admission, with metadata omitted.
    key = _claim(hook_agent) + ":shared-caller"
    sidekey = key + ":receipt"
    valkey.rpush(sidekey, "unrelated-non-string-value")
    saved = valkey.dump(sidekey)

    async def enqueue() -> tuple[bool, str]:
        async with async_redis.from_url(get_settings().valkey_dsn()) as client:
            assert await claim_delivery(client, key, "pending:shared-caller", 60)
            return await enqueue_owned(
                client, key=key, stream=runs_stream, owner="pending:shared-caller",
                payload='{"synthetic":"shared-caller"}', payload_field="payload", lease_s=60,
            )

    try:
        enqueued, stream_id = asyncio.run(enqueue())
        assert enqueued is True
        assert valkey.get(key) == stream_id
        assert valkey.xlen(runs_stream) == 1
        assert valkey.dump(sidekey) == saved
    finally:
        valkey.delete(key, sidekey)


def test_changed_claim_between_observation_and_receipt_read_refuses(
    hooks_client: TestClient, hook_agent: str, valkey: redis.Redis, runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HOOK-RECEIPT-3: old observed evidence cannot attest a replaced claim."""
    first = _post(hooks_client, hook_agent)
    assert first.status_code == 200, first.text
    client = hooks_client.app.state.valkey
    real_get = client.get
    replaced = False
    saved: tuple[Any, ...] = ()

    async def get_then_replace(name: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal replaced, saved
        held = await real_get(name, *args, **kwargs)
        if name == _claim(hook_agent) and not replaced:
            replaced = True
            valkey.set(_claim(hook_agent), "1-0")
            saved = _snapshot(valkey, hook_agent, runs_stream)
        return held

    monkeypatch.setattr(client, "get", get_then_replace)
    refused = _post(hooks_client, hook_agent)
    assert replaced
    assert refused.status_code == 409, refused.text
    assert _snapshot(valkey, hook_agent, runs_stream) == saved


@pytest.mark.parametrize("bad_payload", ["malformed-json", "invalid-turn", "invalid-utf8"])
def test_legacy_unverifiable_queue_payload_refuses_without_mutation(
    hooks_client: TestClient, hook_agent: str, valkey: redis.Redis, runs_stream: str,
    bad_payload: str,
) -> None:
    """@spec HOOK-RECEIPT-4: corrupt legacy evidence refuses instead of raising 500."""
    first = _post(hooks_client, hook_agent)
    assert first.status_code == 200, first.text
    stream_id = first.json()["stream_id"]
    ((original_id, fields),) = valkey.xrange(runs_stream)
    assert original_id == stream_id
    original_claim = valkey.get(_claim(hook_agent))
    payloads: dict[str, str | bytes] = {
        "malformed-json": "{",
        "invalid-turn": '{"synthetic":"not-a-QueuedTurn"}',
        "invalid-utf8": b"\xff",
    }
    fields["payload"] = payloads[bad_payload]
    valkey.delete(_receipt_key(hook_agent))  # Emulate a legacy producer.
    # Recreate this isolated, single-entry fixture stream at the identical ID.
    # All other fields and the permanent delivery claim remain unchanged.
    valkey.delete(runs_stream)
    assert valkey.xadd(runs_stream, fields, id=stream_id) == stream_id
    assert valkey.get(_claim(hook_agent)) == original_claim
    assert valkey.ttl(_claim(hook_agent)) == -1
    saved = _snapshot(valkey, hook_agent, runs_stream)
    refused = _post(hooks_client, hook_agent)
    assert refused.status_code == 409, refused.text
    assert _snapshot(valkey, hook_agent, runs_stream) == saved
