"""Atomic context carriage; synthetic capsules prove transport grammar, never a MAC.

@spec HTTP-INVOCATION-EXECUTION-7 HTTP-INVOCATION-EXECUTION-14.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import FrozenInstanceError
from typing import Any

import pytest
import redis.asyncio as redis
from curie_api import delivery
from curie_api.config import get_settings
from redis.backoff import NoBackoff
from redis.retry import Retry

CAPSULE = "curie-context-v1.e30." + "A" * 43
LATER_CAPSULE = "curie-context-v1.eyJuZXciOjF9." + "B" * 43
INTENT = "a" * 64
OWNER = "pending:" + "b" * 32
FOREIGN = "pending:" + "c" * 32
STABLE = "stable:AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"
PAYLOAD = '{"synthetic":"queued-payload"}'
TRACE = "00-" + "1" * 32 + "-" + "2" * 16 + "-01"


def context_type() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-14: missing feature fails in the body."""
    cls = getattr(delivery, "DeliveryIdentityContext", None)
    assert cls is not None, "DeliveryIdentityContext is not implemented"
    return cls


def receipt_reader() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-14."""
    reader = getattr(delivery, "read_identity_delivery_receipt", None)
    assert callable(reader), "read_identity_delivery_receipt is not implemented"
    return reader


class Scope:
    """@spec HTTP-INVOCATION-EXECUTION-14: isolate every datastore key."""

    def __init__(self) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-14."""
        self.prefix = f"test:http-context-delivery:{uuid.uuid4().hex}:"
        self.claim = self.prefix + "claim"
        self.stream = self.prefix + "stream"
        self.fence = self.prefix + "fence"
        self.receipt = self.prefix + "identity-receipt"
        self.reservation = delivery.backlog_reservation(
            key_prefix=self.prefix + "quota", window_s=60,
        )
        self.guard = delivery.DeliveryEnqueueGuard(
            fence_key=self.fence, expected_stable=STABLE,
            validity=delivery.DeliveryValidityWindow(-62135596800, 0, 253402300799, 999999),
            reservation=self.reservation,
        )

    def context(self, **changes: Any) -> Any:
        """@spec HTTP-INVOCATION-EXECUTION-14: no claim of keyless integrity proof."""
        values = dict(capsule=CAPSULE, intent_digest=INTENT, receipt_key=self.receipt)
        values.update(changes)
        return context_type()(**values)

    @property
    def keys(self) -> tuple[str, ...]:
        """@spec HTTP-INVOCATION-EXECUTION-14."""
        return (self.claim, self.stream, self.fence, self.reservation.token_key,
                self.reservation.counter_key, self.receipt)


@pytest.fixture(scope="module", autouse=True)
def required_context_delivery_valkey() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-14: an unavailable backend is setup ERROR."""
    async def check() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-14."""
        client = redis.from_url(
            get_settings().valkey_dsn(), socket_connect_timeout=2, socket_timeout=2,
            retry=Retry(NoBackoff(), 0),
        )
        try:
            if await client.ping() is not True:
                raise RuntimeError("context delivery Valkey prerequisite unavailable")
        finally:
            await client.aclose()
    asyncio.run(check())


@asynccontextmanager
async def clients(scope: Scope) -> AsyncIterator[tuple[Any, Any]]:
    """@spec HTTP-INVOCATION-EXECUTION-14: cleanup only after all consumers join."""
    async with (
        redis.from_url(get_settings().valkey_dsn()) as client,
        redis.from_url(get_settings().valkey_dsn()) as observer,
    ):
        try:
            yield client, observer
        finally:
            keys = [key async for key in observer.scan_iter(match=scope.prefix + "*")]
            if keys:
                await observer.delete(*keys)
            assert not [key async for key in observer.scan_iter(match=scope.prefix + "*")]


async def prepare(client: Any, scope: Scope, *, claim: bool = True) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-14: actual claim and original quota marker."""
    if claim:
        assert await delivery.claim_delivery(client, scope.claim, OWNER, 60)
    assert await delivery.take_backlog_slot(client, reservation=scope.reservation, limit=5)
    assert await client.set(scope.fence, STABLE)


async def append(client: Any, scope: Scope, **changes: Any) -> tuple[bool, str]:
    """@spec HTTP-INVOCATION-EXECUTION-7 HTTP-INVOCATION-EXECUTION-14."""
    arguments = dict(
        key=scope.claim, stream=scope.stream, owner=OWNER, payload=PAYLOAD,
        payload_field="payload", lease_s=60, transport_field="traceparent",
        transport_value=TRACE, guard=scope.guard, identity_context=scope.context(),
    )
    arguments.update(changes)
    return await delivery.enqueue_owned(client, **arguments)


async def snapshot(client: Any, scope: Scope) -> tuple[Any, ...]:
    """@spec HTTP-INVOCATION-EXECUTION-14: DUMP does not mask wrong stored types."""
    return tuple([await client.dump(key) for key in scope.keys])


async def read_receipt(client: Any, scope: Scope, **changes: Any) -> str | None:
    """@spec HTTP-INVOCATION-EXECUTION-14."""
    values = dict(key=scope.claim, receipt_key=scope.receipt, intent_digest=INTENT)
    values.update(changes)
    return await receipt_reader()(client, **values)


@pytest.mark.parametrize("owned", [True, False], ids=["owned", "reclaimed"])
def test_v2_atomically_retains_payload_trace_context_and_permanent_receipts(
    owned: bool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-7 HTTP-INVOCATION-EXECUTION-14."""
    context_type()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-14."""
        scope = Scope()
        async with clients(scope) as (client, observer):
            await prepare(client, scope, claim=owned)
            calls = []
            original = client.eval

            async def observed(script: str, count: int, *values: Any) -> Any:
                """@spec HTTP-INVOCATION-EXECUTION-7: delegate unchanged real EVAL."""
                calls.append((count, values))
                return await original(script, count, *values)

            monkeypatch.setattr(client, "eval", observed)
            success, stream_id = await append(client, scope)
            digest = hashlib.sha256(CAPSULE.encode("ascii")).hexdigest()
            assert calls == [(5, (
                scope.claim, scope.stream, scope.fence, scope.reservation.token_key,
                scope.receipt, OWNER, PAYLOAD, "payload", "60", "traceparent", TRACE, "",
                "identity_guard_v2", STABLE, scope.reservation.token,
                "-62135596800", "0", "253402300799", "999999", CAPSULE, INTENT, digest,
            ))]
            assert success is True
            assert await observer.xrange(scope.stream) == [(stream_id.encode(), {
                b"payload": PAYLOAD.encode(), b"traceparent": TRACE.encode(),
                b"curie_identity_context_v1": CAPSULE.encode(),
            })]
            assert await observer.get(scope.claim) == stream_id.encode()
            metadata = await observer.get(scope.receipt)
            assert json.loads(metadata) == {
                "schema_version": 1, "intent_digest": INTENT,
                "context_digest": digest, "stream_id": stream_id,
            }
            assert metadata == json.dumps(
                json.loads(metadata), sort_keys=True, separators=(",", ":"),
            ).encode()
            assert await observer.ttl(scope.claim) == await observer.ttl(scope.receipt) == -1
            assert await observer.get(scope.reservation.counter_key) == b"1"
            assert (
                await observer.get(scope.reservation.token_key) == scope.reservation.token.encode()
            )
    asyncio.run(run())


def test_duplicate_with_new_capsule_keeps_the_original_context_and_charge() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-7 HTTP-INVOCATION-EXECUTION-14."""
    context_type()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-14."""
        scope = Scope()
        async with clients(scope) as (client, observer):
            await prepare(client, scope)
            _, first = await append(client, scope)
            before = await snapshot(observer, scope)
            assert await append(
                client, scope, identity_context=scope.context(capsule=LATER_CAPSULE),
                payload="different retransmission must not replace the first payload",
            ) == (False, first)
            assert await snapshot(observer, scope) == before
            assert await read_receipt(client, scope) == first
            await delivery.settle_failed_delivery(
                client, key=scope.claim, owner=OWNER, reservation=scope.reservation,
                preserve_quota=False,
            )
            assert await snapshot(observer, scope) == before
    asyncio.run(run())


@pytest.mark.parametrize("damage", [
    "missing", "malformed", "intent", "stream", "digest", "orphan", "claim", "pending",
])
def test_corrupt_or_conflicting_history_never_appends_or_returns_a_receipt(
    damage: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-7 HTTP-INVOCATION-EXECUTION-14."""
    context_type()
    receipt_reader()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-14."""
        scope = Scope()
        async with clients(scope) as (client, observer):
            await prepare(client, scope)
            _, first = await append(client, scope)
            metadata = json.loads(await observer.get(scope.receipt))
            if damage == "missing":
                await observer.delete(scope.receipt)
            elif damage == "malformed":
                await observer.set(scope.receipt, '{"schema_version":1}')
            elif damage == "orphan":
                await observer.delete(scope.claim)
            elif damage == "claim":
                await observer.set(scope.claim, "unexpected-sensitive-value")
            elif damage == "pending":
                await observer.set(scope.claim, FOREIGN)
            else:
                field, value = {
                    "intent": ("intent_digest", "d" * 64),
                    "stream": ("stream_id", first + "0"),
                    "digest": ("context_digest", "D" * 64),
                }[damage]
                metadata[field] = value
                await observer.set(
                    scope.receipt, json.dumps(metadata, sort_keys=True, separators=(",", ":")),
                )
            before = await snapshot(observer, scope)
            original = client.eval

            async def observed(script: str, count: int, *values: Any) -> Any:
                """@spec HTTP-INVOCATION-EXECUTION-14: observe the actual refusal status."""
                result = await original(script, count, *values)
                if count == 5:
                    assert result[0] == -6
                return result

            monkeypatch.setattr(client, "eval", observed)
            for operation in (append, read_receipt):
                with pytest.raises(delivery.DeliveryGuardRefusal) as caught:
                    await operation(client, scope)
                assert caught.value.code == str(caught.value) == "intent_conflict"
                assert caught.value.__cause__ is None
                assert "sensitive" not in str(caught.value)
                assert await snapshot(observer, scope) == before
    asyncio.run(run())


def test_retained_metadata_requires_one_canonical_complete_spelling() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-7 HTTP-INVOCATION-EXECUTION-14."""
    context_type()
    receipt_reader()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-14."""
        scope = Scope()
        async with clients(scope) as (client, observer):
            await prepare(client, scope)
            await append(client, scope)
            canonical = (await observer.get(scope.receipt)).decode()
            metadata = json.loads(canonical)
            alternatives = (
                json.dumps(metadata),
                json.dumps(dict(reversed(list(metadata.items()))), separators=(",", ":")),
                canonical[:-1] + ',"schema_version":1}',
                canonical[:-1] + ',"extra":null}',
                canonical.replace('"schema_version":1', '"schema_version":true'),
            )
            for alternative in alternatives:
                assert alternative != canonical
                await observer.set(scope.receipt, alternative)
                before = await snapshot(observer, scope)
                for operation in (append, read_receipt):
                    with pytest.raises(delivery.DeliveryGuardRefusal) as caught:
                        await operation(client, scope)
                    assert caught.value.code == "intent_conflict"
                    assert await snapshot(observer, scope) == before
    asyncio.run(run())


def test_foreign_pending_is_in_flight_without_reacquiring_or_creating_metadata() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-14."""
    context_type()
    receipt_reader()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-14."""
        scope = Scope()
        async with clients(scope) as (client, observer):
            await prepare(client, scope, claim=False)
            await observer.set(scope.claim, FOREIGN, ex=60)
            before = await snapshot(observer, scope)
            assert await append(client, scope) == (False, FOREIGN)
            assert await read_receipt(client, scope) == FOREIGN
            assert await snapshot(observer, scope) == before
            assert await observer.exists(scope.receipt, scope.stream) == 0
    asyncio.run(run())


@pytest.mark.parametrize("refund_first", [True, False], ids=["refund-first", "append-first"])
def test_refund_and_append_have_only_one_winning_order(
    refund_first: bool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-14: command barrier, no server clock mutation."""
    context_type()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-14."""
        scope = Scope()
        async with clients(scope) as (client, observer):
            await prepare(client, scope)
            entered, release = asyncio.Event(), asyncio.Event()
            original = client.eval

            async def delayed(script: str, count: int, *values: Any) -> Any:
                """@spec HTTP-INVOCATION-EXECUTION-14: real append before/after real refund."""
                if refund_first:
                    entered.set()
                    await release.wait()
                    return await original(script, count, *values)
                result = await original(script, count, *values)
                entered.set()
                await release.wait()
                return result

            monkeypatch.setattr(client, "eval", delayed)
            task = asyncio.create_task(append(client, scope))
            try:
                await asyncio.wait_for(entered.wait(), timeout=2)
                await delivery.settle_failed_delivery(
                    observer, key=scope.claim, owner=OWNER, reservation=scope.reservation,
                    preserve_quota=False,
                )
            except BaseException:
                release.set()
                await asyncio.gather(task, return_exceptions=True)
                raise
            finally:
                release.set()
            if refund_first:
                with pytest.raises(delivery.DeliveryGuardRefusal) as caught:
                    await task
                assert caught.value.code == "reservation_lost"
                assert await observer.exists(scope.claim, scope.receipt, scope.stream,
                                             scope.reservation.token_key) == 0
                assert await observer.get(scope.reservation.counter_key) == b"0"
            else:
                assert (await task)[0] is True
                assert await observer.xlen(scope.stream) == 1
                assert await observer.get(scope.reservation.counter_key) == b"1"
                assert await read_receipt(observer, scope) == (await task)[1]
    asyncio.run(run())


def test_context_is_validated_frozen_redacted_and_hashes_exact_capsule_bytes() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-14: transport grammar is not MAC validation."""
    cls = context_type()
    scope = Scope()
    value = scope.context()
    assert value.context_digest == hashlib.sha256(CAPSULE.encode("ascii")).hexdigest()
    assert not any(field in repr(value) for field in (CAPSULE, INTENT, scope.receipt))
    with pytest.raises((FrozenInstanceError, AttributeError)):
        value.capsule = LATER_CAPSULE
    largest = "curie-context-v1." + "A" * (16384 - 16 - 1 - 43) + "." + "A" * 43
    assert len(largest) == 16384
    assert (
        scope.context(capsule=largest).context_digest
        == hashlib.sha256(largest.encode()).hexdigest()
    )
    for fields in (
        {"capsule": b"not-a-string"}, {"capsule": CAPSULE + "="},
        {"capsule": "curie-context-v1.." + "A" * 43},
        {"capsule": "curie-context-v1.e30." + "A" * 42},
        {"capsule": "curie-context-v1." + "A" * 16384 + "." + "A" * 43},
        {"capsule": "curie-context-v1.é." + "A" * 43},
        {"intent_digest": "A" * 64}, {"intent_digest": "a" * 63},
        {"receipt_key": ""}, {"receipt_key": "\ud800"},
    ):
        with pytest.raises(ValueError, match="^invalid_delivery_context$") as caught:
            scope.context(**fields)
        assert caught.value.__cause__ is None
    assert isinstance(value, cls)


def test_invalid_context_and_layouts_refuse_before_any_datastore_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-14."""
    context_type()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-14."""
        scope = Scope()
        async with clients(scope) as (client, observer):
            before = await snapshot(observer, scope)
            calls = []
            original = client.execute_command

            async def observed(*args: Any, **kwargs: Any) -> Any:
                """@spec HTTP-INVOCATION-EXECUTION-14: observe, never fabricate a reply."""
                calls.append(args)
                return await original(*args, **kwargs)

            monkeypatch.setattr(client, "execute_command", observed)
            corrupt = copy.copy(scope.context())
            object.__setattr__(corrupt, "capsule", "unchecked-sensitive-capsule")
            variants = [
                {"identity_context": object()}, {"identity_context": corrupt},
                {"guard": None},
                {"receipt_key": scope.prefix + "hook", "receipt_prefix": "hook:"},
                *[{"identity_context": scope.context(receipt_key=key)} for key in scope.keys[:-1]],
            ]
            for changes in variants:
                with pytest.raises(ValueError, match="^invalid_delivery_context$") as caught:
                    await append(client, scope, **changes)
                assert caught.value.__cause__ is None
                assert calls == []
                assert await snapshot(observer, scope) == before
    asyncio.run(run())


def test_server_rejects_malformed_v2_layout_before_any_mutation() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-7 HTTP-INVOCATION-EXECUTION-14."""
    context_type()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-14: actual script, not copied validation."""
        scope = Scope()
        async with clients(scope) as (client, observer):
            await prepare(client, scope)
            keys = [scope.claim, scope.stream, scope.fence,
                    scope.reservation.token_key, scope.receipt]
            args = [OWNER, PAYLOAD, "payload", "60", "traceparent", TRACE, "",
                    "identity_guard_v2", STABLE, scope.reservation.token,
                    "-62135596800", "0", "253402300799", "999999", CAPSULE, INTENT,
                    hashlib.sha256(CAPSULE.encode()).hexdigest()]
            layouts = [
                (keys, args + ["extra"]), (keys[:-1], args),
                (keys[:-1] + [scope.stream], args),
            ]
            for index, value in ((6, "hook:"), (14, "malformed"), (15, "A" * 64)):
                changed = args.copy()
                changed[index] = value
                layouts.append((keys, changed))
            before = await snapshot(observer, scope)
            for actual_keys, actual_args in layouts:
                result = await client.eval(
                    delivery._ENQUEUE_SCRIPT, len(actual_keys), *actual_keys, *actual_args,
                )
                assert result[0] < 0
                assert await snapshot(observer, scope) == before
    asyncio.run(run())


@pytest.mark.parametrize("state", ["absent", "pending", "permanent"])
def test_receipt_owner_is_read_only_for_all_valid_states(
    state: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-14: EVAL_RO makes zero-write observation enforceable."""
    context_type()
    receipt_reader()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-14."""
        scope = Scope()
        async with clients(scope) as (client, observer):
            expected = None
            if state == "pending":
                await observer.set(scope.claim, FOREIGN)
                expected = FOREIGN
            elif state == "permanent":
                await prepare(client, scope)
                _, expected = await append(client, scope)
            before = await snapshot(observer, scope)
            calls = []
            original = client.execute_command

            # The actual server refuses a writing script under EVAL_RO. No reply substitution.
            async def read_only(command: str, *args: Any, **kwargs: Any) -> Any:
                """@spec HTTP-INVOCATION-EXECUTION-14."""
                calls.append(command)
                selected = "EVAL_RO" if command.upper() == "EVAL" else command
                return await original(selected, *args, **kwargs)

            monkeypatch.setattr(client, "execute_command", read_only)
            assert await read_receipt(client, scope) == expected
            assert len(calls) == 1
            assert calls[0].upper() in {"EVAL", "EVAL_RO"}
            assert await snapshot(observer, scope) == before
    asyncio.run(run())


def test_receipt_owner_invalid_inputs_have_fixed_errors_before_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-14."""
    receipt_reader()
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-14."""
        scope = Scope()
        async with clients(scope) as (client, _):
            calls = []
            original = client.execute_command

            async def observed(*args: Any, **kwargs: Any) -> Any:
                """@spec HTTP-INVOCATION-EXECUTION-14."""
                calls.append(args)
                return await original(*args, **kwargs)

            monkeypatch.setattr(client, "execute_command", observed)
            for changes in (
                {"key": ""}, {"key": b"claim"}, {"receipt_key": scope.claim},
                {"receipt_key": "\ud800"}, {"intent_digest": "A" * 64},
                {"intent_digest": b"a" * 64},
            ):
                with pytest.raises(ValueError, match="^invalid_delivery_context$") as caught:
                    await read_receipt(client, scope, **changes)
                assert caught.value.__cause__ is None
                assert calls == []
    asyncio.run(run())


@pytest.mark.parametrize("mode", ["legacy", "v1"])
def test_ordinary_enqueue_controls_do_not_acquire_context_metadata(mode: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-7 HTTP-INVOCATION-EXECUTION-14."""
    async def run() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-14."""
        scope = Scope()
        async with clients(scope) as (client, observer):
            await prepare(client, scope)
            success, receipt = await delivery.enqueue_owned(
                client, key=scope.claim, stream=scope.stream, owner=OWNER,
                payload=PAYLOAD, payload_field="payload", lease_s=60,
                guard=scope.guard if mode == "v1" else None,
            )
            assert success is True
            assert await observer.xrange(scope.stream) == [
                (receipt.encode(), {b"payload": PAYLOAD.encode()}),
            ]
            assert await observer.get(scope.receipt) is None
    asyncio.run(run())
