"""Real delivery/Valkey boundaries for an optional, unwired enqueue guard.

@spec GUARDED-DELIVERY-V1-1 GUARDED-DELIVERY-V1-2 GUARDED-DELIVERY-V1-4
@spec GUARDED-DELIVERY-V1-5 GUARDED-DELIVERY-V1-6 GUARDED-DELIVERY-V1-7
@spec GUARDED-DELIVERY-V1-8.
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import FrozenInstanceError
from typing import Any

import pytest
import redis.asyncio as redis
from curie_api import delivery
from curie_api.config import get_settings
from redis.exceptions import ResponseError

MIN_S = -62135596800
MAX_S = 253402300799
STABLE = "stable:AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"
OWNER = "pending:" + "b" * 32


def api() -> Any:
    """@spec GUARDED-DELIVERY-V1-1: absent symbols are body assertions."""
    for name in ("DeliveryValidityWindow", "DeliveryEnqueueGuard", "DeliveryGuardRefusal"):
        assert hasattr(delivery, name), f"guarded delivery feature missing: {name}"
    return delivery


class Scope:
    """@spec GUARDED-DELIVERY-V1-8: every key is task-instance owned."""

    def __init__(self) -> None:
        """@spec GUARDED-DELIVERY-V1-8."""
        self.prefix = f"test:guarded-delivery:{uuid.uuid4().hex}:"
        self.claim = self.prefix + "claim"
        self.stream = self.prefix + "stream"
        self.fence = self.prefix + "fence"
        self.reservation = delivery.backlog_reservation(
            key_prefix=self.prefix + "counter", window_s=60,
        )

    @property
    def keys(self) -> tuple[str, ...]:
        """@spec GUARDED-DELIVERY-V1-8."""
        return (self.claim, self.stream, self.fence,
                self.reservation.token_key, self.reservation.counter_key)


@asynccontextmanager
async def clients(scope: Scope) -> AsyncIterator[tuple[Any, Any]]:
    """@spec GUARDED-DELIVERY-V1-8: no mock datastore, no cross-test cleanup."""
    async with (
        redis.from_url(get_settings().valkey_dsn()) as first,
        redis.from_url(get_settings().valkey_dsn()) as second,
    ):
        assert await first.ping() is True
        try:
            yield first, second
        finally:
            # All commands/tasks using these clients have joined before exit.
            owned = [key async for key in second.scan_iter(match=scope.prefix + "*")]
            if owned:
                await second.delete(*owned)
            assert not [key async for key in second.scan_iter(match=scope.prefix + "*")]


def window(*values: int) -> Any:
    """@spec GUARDED-DELIVERY-V1-2."""
    return api().DeliveryValidityWindow(
        lower_seconds=values[0], lower_microseconds=values[1],
        upper_seconds=values[2], upper_microseconds=values[3],
    )


def guard(scope: Scope, validity: Any = None) -> Any:
    """@spec GUARDED-DELIVERY-V1-2."""
    return api().DeliveryEnqueueGuard(
        fence_key=scope.fence, expected_stable=STABLE,
        validity=validity if validity is not None else window(MIN_S, 0, MAX_S, 999999),
        reservation=scope.reservation,
    )


async def prepare(client: Any, scope: Scope, *, owned: bool = True) -> None:
    """@spec GUARDED-DELIVERY-V1-5: reservation is fixed before the first await."""
    if owned:
        assert await delivery.claim_delivery(client, scope.claim, OWNER, 60)
    assert await delivery.take_backlog_slot(client, reservation=scope.reservation, limit=5)
    assert await client.set(scope.fence, STABLE)


async def append(client: Any, scope: Scope, **changes: Any) -> Any:
    """@spec GUARDED-DELIVERY-V1-1 GUARDED-DELIVERY-V1-4."""
    arguments = dict(
        key=scope.claim, stream=scope.stream, owner=OWNER, payload='{"synthetic":"payload"}',
        payload_field="payload", lease_s=60, transport_field="transport",
        transport_value="synthetic-transport",
    )
    arguments.update(changes)
    return await delivery.enqueue_owned(client, **arguments)


async def snapshot(client: Any, scope: Scope) -> tuple[Any, ...]:
    """@spec GUARDED-DELIVERY-V1-5: exact stored values, including non-string types."""
    return tuple([await client.dump(key) for key in scope.keys])


async def refuse(client: Any, scope: Scope, value: Any, code: str) -> None:
    """@spec GUARDED-DELIVERY-V1-6."""
    before = await snapshot(client, scope)
    ttl = await client.pttl(scope.reservation.token_key)
    with pytest.raises(api().DeliveryGuardRefusal) as caught:
        await append(client, scope, guard=value)
    assert caught.value.code == code
    assert str(caught.value) == code
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None or caught.value.__suppress_context__
    assert await snapshot(client, scope) == before
    assert await client.pttl(scope.reservation.token_key) <= ttl


@pytest.mark.parametrize("owned", [True, False], ids=["owned-pending", "absent"])
def test_guarded_append_has_one_permanent_receipt_and_original_transport(
    owned: bool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec GUARDED-DELIVERY-V1-4 GUARDED-DELIVERY-V1-5."""
    async def run() -> None:
        """@spec GUARDED-DELIVERY-V1-5."""
        scope = Scope()
        value = guard(scope)
        async with clients(scope) as (client, _):
            await prepare(client, scope, owned=owned)
            original = client.eval
            calls = []

            async def observed(script: str, number: int, *arguments: Any) -> Any:
                """@spec GUARDED-DELIVERY-V1-4."""
                calls.append((number, arguments))
                return await original(script, number, *arguments)

            monkeypatch.setattr(client, "eval", observed)
            success, receipt = await append(client, scope, guard=value)
            assert len(calls) == 1
            assert calls[0] == (4, (
                scope.claim, scope.stream, scope.fence, scope.reservation.token_key,
                OWNER, '{"synthetic":"payload"}', "payload", "60", "transport",
                "synthetic-transport", "", "identity_guard_v1", STABLE, scope.reservation.token,
                str(MIN_S), "0", str(MAX_S), "999999",
            ))
            assert success is True
            assert await client.get(scope.claim) == receipt.encode()
            assert await client.ttl(scope.claim) == -1
            assert await client.xrange(scope.stream) == [(receipt.encode(), {
                b"payload": b'{"synthetic":"payload"}', b"transport": b"synthetic-transport",
            })]
            assert await client.get(scope.reservation.counter_key) == b"1"
            assert await client.get(scope.reservation.token_key) == scope.reservation.token.encode()
    asyncio.run(run())


def test_refusal_precedence_is_fence_then_original_marker_then_server_window() -> None:
    """@spec GUARDED-DELIVERY-V1-5: later checks cannot hide a prior refusal."""
    async def run() -> None:
        """@spec GUARDED-DELIVERY-V1-5."""
        scope = Scope()
        value = guard(scope, window(MIN_S, 0, MIN_S, 0))
        async with clients(scope) as (client, _):
            await prepare(client, scope)
            await client.set(scope.fence, OWNER)
            await client.delete(scope.reservation.token_key)
            await refuse(client, scope, value, "fence_changed")
            await client.set(scope.fence, STABLE)
            await refuse(client, scope, value, "reservation_lost")
            await client.set(scope.reservation.token_key, scope.reservation.token)
            await refuse(client, scope, value, "outside_validity")
    asyncio.run(run())


def decoder() -> Any:
    """@spec GUARDED-DELIVERY-V1-6: pure protocol seam, no synthetic Valkey reply."""
    function = getattr(delivery, "_guarded_enqueue_result", None)
    assert callable(function), "guarded enqueue RESP decoder is not implemented"
    return function


@pytest.mark.parametrize(("status", "code"), [
    (-2, "fence_changed"), (-3, "outside_validity"),
    (-4, "reservation_lost"), (-5, "clock_unavailable"),
])
def test_pure_decoder_maps_only_documented_refusals(status: int, code: str) -> None:
    """@spec GUARDED-DELIVERY-V1-6: invalid-clock *protocol*, not clock qualification."""
    decode = decoder()
    with pytest.raises(api().DeliveryGuardRefusal) as caught:
        decode([status, b"synthetic-sensitive-server-reply"])
    assert caught.value.code == code
    assert str(caught.value) == code
    assert "synthetic-sensitive" not in repr(caught.value)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None or caught.value.__suppress_context__


@pytest.mark.parametrize("reply", [
    None, [], [1], [1, b"1-0", "extra"], [True, b"1-0"], [False, b"1-0"],
    [1.0, b"1-0"], ["1", b"1-0"], [-1, b"sensitive-hook-conflict"],
    [2, b"sensitive-unknown"], [-6, b"sensitive-unknown"], {"status": 1, "id": "1-0"},
])
def test_pure_decoder_refuses_unexpected_protocol_without_disclosure(reply: Any) -> None:
    """@spec GUARDED-DELIVERY-V1-6."""
    with pytest.raises(RuntimeError) as caught:
        decoder()(reply)
    assert str(caught.value) == "invalid_guarded_delivery_result"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None or caught.value.__suppress_context__


@pytest.mark.parametrize(("reply", "expected"), [
    ([1, b"123-4"], (True, "123-4")), ((1, "123-4"), (True, "123-4")),
    ([0, b"pending:" + b"c" * 32], (False, "pending:" + "c" * 32)),
    ((0, "123-4"), (False, "123-4")),
])
def test_pure_decoder_retains_original_success_and_current_owner(reply: Any, expected: Any) -> None:
    """@spec GUARDED-DELIVERY-V1-6."""
    assert decoder()(reply) == expected



@pytest.mark.parametrize("owned", [True, False], ids=["owned-pending", "absent"])
@pytest.mark.parametrize(
    ("changed", "code"),
    [("missing-fence", "fence_changed"), ("pending-fence", "fence_changed"),
     ("rotated-fence", "fence_changed"), ("equivalent-different-bytes", "fence_changed"),
     ("missing-marker", "reservation_lost"), ("wrong-marker", "reservation_lost"),
     ("expired-marker", "reservation_lost"), ("past", "outside_validity"),
     ("future", "outside_validity")],
)
def test_guard_refusals_leave_all_five_values_unchanged(
    owned: bool, changed: str, code: str,
) -> None:
    """@spec GUARDED-DELIVERY-V1-5 GUARDED-DELIVERY-V1-6."""
    async def run() -> None:
        """@spec GUARDED-DELIVERY-V1-5."""
        scope = Scope()
        value = guard(scope)
        async with clients(scope) as (client, _):
            await prepare(client, scope, owned=owned)
            if changed == "missing-fence":
                await client.delete(scope.fence)
            elif changed == "pending-fence":
                await client.set(scope.fence, OWNER)
            elif changed == "rotated-fence":
                await client.set(scope.fence, "stable:" + str(uuid.uuid4()))
            elif changed == "equivalent-different-bytes":
                await client.set(scope.fence, STABLE.lower())
            elif changed == "missing-marker":
                await client.delete(scope.reservation.token_key)
            elif changed == "wrong-marker":
                await client.set(scope.reservation.token_key, "c" * 32)
            elif changed == "expired-marker":
                await client.pexpire(scope.reservation.token_key, 1)
                for _ in range(50):
                    if await client.exists(scope.reservation.token_key) == 0:
                        break
                    await asyncio.sleep(0.01)
                assert await client.exists(scope.reservation.token_key) == 0
            else:
                seconds, _ = await client.time()
                values = ((seconds - 100, 0, seconds - 50, 999999)
                          if changed == "past" else (seconds + 50, 0, seconds + 100, 999999))
                value = guard(scope, window(*values))
            await refuse(client, scope, value, code)
    asyncio.run(run())


def test_actual_server_time_accepts_a_bounded_current_window() -> None:
    """@spec GUARDED-DELIVERY-V1-5 GUARDED-DELIVERY-V1-7."""
    async def run() -> None:
        """@spec GUARDED-DELIVERY-V1-7."""
        scope = Scope()
        api()
        async with clients(scope) as (client, _):
            await prepare(client, scope)
            seconds, _ = await client.time()
            assert (await append(client, scope, guard=guard(
                scope, window(seconds - 30, 0, seconds + 30, 999999),
            )))[0] is True
            assert await client.xlen(scope.stream) == 1
    asyncio.run(run())


@pytest.mark.parametrize("current", ["receipt", "pending"])
def test_foreign_owner_or_receipt_precedes_stale_guard_checks(current: str) -> None:
    """@spec GUARDED-DELIVERY-V1-4: duplicate disclosure remains caller-owned."""
    async def run() -> None:
        """@spec GUARDED-DELIVERY-V1-4."""
        scope = Scope()
        value = guard(scope, window(MIN_S, 0, MIN_S, 1))
        async with clients(scope) as (client, _):
            await prepare(client, scope)
            if current == "receipt":
                success, receipt = await append(client, scope)  # Existing append owner.
                assert success
                expected = receipt
            else:
                expected = "pending:" + "d" * 32
                await client.set(scope.claim, expected, ex=60)
            await client.delete(scope.reservation.token_key)
            await client.set(scope.fence, "stable:" + str(uuid.uuid4()))
            before = await snapshot(client, scope)
            assert await append(client, scope, guard=value) == (False, expected)
            assert await snapshot(client, scope) == before
    asyncio.run(run())


@pytest.mark.parametrize("stable", [
    "stable:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "stable:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "stable:{aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa}",
])
def test_reader_accepted_uuid_spelling_is_preserved_exactly(stable: str) -> None:
    """@spec GUARDED-DELIVERY-V1-2 GUARDED-DELIVERY-V1-5."""
    async def run() -> None:
        """@spec GUARDED-DELIVERY-V1-2."""
        scope = Scope()
        value = guard(scope)
        object.__setattr__(value, "expected_stable", stable)
        async with clients(scope) as (client, _):
            await prepare(client, scope)
            await client.set(scope.fence, stable)
            assert (await append(client, scope, guard=value))[0] is True
    asyncio.run(run())


@pytest.mark.parametrize("field", [
    "lower_seconds", "lower_microseconds", "upper_seconds", "upper_microseconds",
])
@pytest.mark.parametrize("bad", [True, 0.0, "0", None])
def test_window_fields_do_not_coerce_nonintegers(field: str, bad: Any) -> None:
    """@spec GUARDED-DELIVERY-V1-2."""
    values = dict(lower_seconds=0, lower_microseconds=0, upper_seconds=1, upper_microseconds=0)
    values[field] = bad
    with pytest.raises(ValueError):
        api().DeliveryValidityWindow(**values)


@pytest.mark.parametrize("values", [
    (MIN_S - 1, 0, 0, 0), (0, 0, MAX_S + 1, 0), (0, -1, 1, 0),
    (0, 0, 1, 1000000), (1, 1, 1, 0),
])
def test_window_ranges_and_pair_order_are_strict(values: tuple[int, ...]) -> None:
    """@spec GUARDED-DELIVERY-V1-2."""
    with pytest.raises(ValueError):
        window(*values)


def test_frozen_guard_representation_does_not_disclose_nested_authority() -> None:
    """@spec GUARDED-DELIVERY-V1-2."""
    scope = Scope()
    value = guard(scope)
    for target, field in [(value, "fence_key"), (value.validity, "lower_seconds")]:
        with pytest.raises((FrozenInstanceError, AttributeError, ValueError, TypeError)):
            setattr(target, field, 42)
    rendered = repr(value)
    for secret in (*scope.keys, STABLE, scope.reservation.token, OWNER):
        assert secret not in rendered


@pytest.mark.parametrize("fault", [
    "fence-empty", "fence-nonstr", "stable", "stable-pending", "validity",
    "window-mutated", "reservation-token", "reservation-token-upper", "reservation-shape",
    "reservation-window-zero", "reservation-window-negative", "guard-shape",
    "reservation-window", "counter-empty", "owner", "owner-receipt", "claim-empty",
    "stream-empty", "claim-stream", "claim-fence", "stream-fence", "claim-marker",
    "stream-marker", "fence-marker", "counter-claim", "counter-stream", "counter-fence",
    "hook-key", "hook-prefix", "hook-both",
])
def test_constructed_or_mutated_input_is_rejected_before_any_command(
    fault: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec GUARDED-DELIVERY-V1-2: validation cannot trust constructor provenance."""
    async def run() -> None:
        """@spec GUARDED-DELIVERY-V1-2."""
        scope = Scope()
        value = guard(scope)
        arguments: dict[str, Any] = {"guard": value}
        if fault == "guard-shape":
            arguments["guard"] = {}
        elif fault in {"fence-empty", "fence-nonstr"}:
            object.__setattr__(value, "fence_key", "" if fault == "fence-empty" else 1)
        elif fault == "stable-pending":
            object.__setattr__(value, "expected_stable", OWNER)
        elif fault == "reservation-shape":
            object.__setattr__(value, "reservation", {})
        elif fault == "stable":
            object.__setattr__(value, "expected_stable", "stable:sensitive-invalid")
        elif fault == "validity":
            object.__setattr__(value, "validity", {})
        elif fault == "window-mutated":
            object.__setattr__(value.validity, "lower_microseconds", True)
        elif fault.startswith("reservation-") or fault == "counter-empty":
            replacement = copy.copy(scope.reservation)
            field, bad = {
                "reservation-token": ("token", "sensitive-invalid"),
                "reservation-window": ("window_s", True),
                "reservation-window-zero": ("window_s", 0),
                "reservation-window-negative": ("window_s", -1),
                "reservation-token-upper": ("token", "A" * 32),
                "counter-empty": ("counter_key", ""),
            }[fault]
            object.__setattr__(replacement, field, bad)
            object.__setattr__(value, "reservation", replacement)
        elif fault in {"owner", "owner-receipt", "claim-empty", "stream-empty"}:
            field, bad = {
                "owner": ("owner", "pending:" + "B" * 32), "owner-receipt": ("owner", "1-0"),
                "claim-empty": ("key", ""), "stream-empty": ("stream", ""),
            }[fault]
            arguments[field] = bad
        elif fault.startswith("hook"):
            if fault != "hook-prefix":
                arguments["receipt_key"] = scope.prefix + "hook"
            if fault != "hook-key":
                arguments["receipt_prefix"] = '{"synthetic":"receipt"}'
        else:
            target = {
                "claim": "key", "stream": "stream",
                "fence": "fence_key", "counter": "counter_key",
            }
            left, right = fault.split("-")
            replacement = {"claim": scope.claim, "stream": scope.stream,
                           "fence": scope.fence, "marker": scope.reservation.token_key}[right]
            if left in {"claim", "stream"}:
                arguments[target[left]] = replacement
            elif left == "fence":
                object.__setattr__(value, "fence_key", replacement)
            else:
                reservation = copy.copy(scope.reservation)
                object.__setattr__(reservation, "counter_key", replacement)
                object.__setattr__(value, "reservation", reservation)
        # A real client, faithfully delegated if a command is accidentally sent.
        async with clients(scope) as (client, _):
            commands = []
            actual = client.execute_command

            async def observed(*args: Any, **kwargs: Any) -> Any:
                """@spec GUARDED-DELIVERY-V1-2."""
                commands.append(args)
                return await actual(*args, **kwargs)

            monkeypatch.setattr(client, "execute_command", observed)
            with pytest.raises(ValueError) as caught:
                await append(client, scope, **arguments)
            assert commands == []
            assert not any(piece in str(caught.value) for piece in (
                scope.prefix, scope.reservation.token, "sensitive-invalid",
            ))
            assert caught.value.__cause__ is None
            assert caught.value.__context__ is None or caught.value.__suppress_context__
    asyncio.run(run())


def guarded_arguments(scope: Scope) -> list[str]:
    """@spec GUARDED-DELIVERY-V1-4: independent exact wire protocol."""
    return [
        OWNER, '{"synthetic":"direct"}', "payload", "60", "", "", "",
        "identity_guard_v1", STABLE, scope.reservation.token, str(MIN_S), "0", str(MAX_S), "999999",
    ]


def test_existing_append_script_checks_guard_before_absent_claim_reacquisition() -> None:
    """@spec GUARDED-DELIVERY-V1-4 GUARDED-DELIVERY-V1-5."""
    async def run() -> None:
        """@spec GUARDED-DELIVERY-V1-5: behavioral RED even before new Python types."""
        scope = Scope()
        async with clients(scope) as (client, _):
            await prepare(client, scope, owned=False)
            await client.set(scope.fence, "pending:" + "c" * 32)
            before = await snapshot(client, scope)
            result = await client.eval(
                delivery._ENQUEUE_SCRIPT, 4, scope.claim, scope.stream, scope.fence,
                scope.reservation.token_key, *guarded_arguments(scope),
            )
            assert result[0] == -2, "guarded script appended despite changed fence"
            assert await snapshot(client, scope) == before
    asyncio.run(run())


@pytest.mark.parametrize("fault", ["mode", "arity", "seconds", "micros", "ordering", "alias"])
def test_raw_guard_protocol_validation_precedes_any_mutation(fault: str) -> None:
    """@spec GUARDED-DELIVERY-V1-4."""
    async def run() -> None:
        """@spec GUARDED-DELIVERY-V1-4."""
        scope = Scope()
        async with clients(scope) as (client, _):
            await prepare(client, scope)
            arguments = guarded_arguments(scope)
            keys = [scope.claim, scope.stream, scope.fence, scope.reservation.token_key]
            if fault == "mode":
                arguments[7] = "wrong_mode"
            elif fault == "arity":
                arguments.pop()
            elif fault == "seconds":
                arguments[10] = "0e0"
            elif fault == "micros":
                arguments[11] = "1000000"
            elif fault == "ordering":
                arguments[10:14] = ["1", "1", "1", "0"]
            else:
                keys[2] = keys[3]
            before = await snapshot(client, scope)
            try:
                result = await client.eval(delivery._ENQUEUE_SCRIPT, 4, *keys, *arguments)
            except ResponseError:
                pass
            else:
                assert result[0] not in (0, 1), "invalid guarded protocol accepted"
            assert await snapshot(client, scope) == before
    asyncio.run(run())


@pytest.mark.parametrize("successor", [False, True])
def test_refund_before_delayed_eval_never_resurrects_old_charge_or_claim(
    successor: bool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec GUARDED-DELIVERY-V1-5: two real clients, refund wins ordering."""
    async def run() -> None:
        """@spec GUARDED-DELIVERY-V1-5."""
        scope = Scope()
        value = guard(scope)
        async with clients(scope) as (client, rival):
            await prepare(client, scope)
            entered, release = asyncio.Event(), asyncio.Event()
            actual = client.eval

            async def delayed(script: str, number: int, *arguments: Any) -> Any:
                """@spec GUARDED-DELIVERY-V1-5."""
                if number == 4:
                    entered.set()
                    await release.wait()
                return await actual(script, number, *arguments)

            monkeypatch.setattr(client, "eval", delayed)
            task = asyncio.create_task(append(client, scope, guard=value))
            try:
                await asyncio.wait_for(entered.wait(), 2)
                next_owner = "pending:" + "c" * 32
                if successor:
                    await rival.set(scope.claim, next_owner, ex=60)
                await delivery.settle_failed_delivery(
                    rival, key=scope.claim, owner=OWNER,
                    reservation=scope.reservation, preserve_quota=False,
                )
                assert await rival.get(scope.reservation.counter_key) == b"0"
                assert await rival.get(scope.reservation.token_key) is None
                release.set()
                if successor:
                    assert await task == (False, next_owner)
                    assert await rival.get(scope.claim) == next_owner.encode()
                else:
                    with pytest.raises(api().DeliveryGuardRefusal) as caught:
                        await task
                    assert caught.value.code == "reservation_lost"
                    assert await rival.get(scope.claim) is None
                assert await rival.xlen(scope.stream) == 0
                assert await rival.get(scope.reservation.counter_key) == b"0"
            finally:
                release.set()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    asyncio.run(run())


@pytest.mark.parametrize("loss", ["reply", "cancel"])
def test_enqueue_commit_before_lost_reply_or_cancellation_keeps_entry_and_charge(
    loss: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec GUARDED-DELIVERY-V1-5 GUARDED-DELIVERY-V1-6."""
    async def run() -> None:
        """@spec GUARDED-DELIVERY-V1-5."""
        scope = Scope()
        value = guard(scope)
        async with clients(scope) as (client, rival):
            await prepare(client, scope)
            applied, release = asyncio.Event(), asyncio.Event()
            actual = client.eval
            lost = RuntimeError("synthetic lost enqueue acknowledgement")
            captured = []

            async def uncertain(script: str, number: int, *arguments: Any) -> Any:
                """@spec GUARDED-DELIVERY-V1-5."""
                result = await actual(script, number, *arguments)
                if number == 4:
                    assert result[0] == 1
                    captured.append(result)
                    applied.set()
                    await release.wait()
                    raise lost
                return result

            async def owner_attempt() -> Any:
                """@spec GUARDED-DELIVERY-V1-5."""
                try:
                    return await append(client, scope, guard=value)
                finally:
                    await delivery.settle_failed_delivery(
                        rival, key=scope.claim, owner=OWNER,
                        reservation=scope.reservation, preserve_quota=False,
                    )

            monkeypatch.setattr(client, "eval", uncertain)
            task = asyncio.create_task(owner_attempt())
            try:
                await asyncio.wait_for(applied.wait(), 2)
                receipt = captured[0][1]
                assert await rival.get(scope.claim) == receipt
                assert await rival.xlen(scope.stream) == 1
                # Settlement while the first caller has not received any response.
                await delivery.settle_failed_delivery(
                    rival, key=scope.claim, owner=OWNER,
                    reservation=scope.reservation, preserve_quota=False,
                )
                if loss == "cancel":
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                else:
                    release.set()
                    with pytest.raises(RuntimeError) as caught:
                        await task
                    assert caught.value is lost
                assert await rival.get(scope.claim) == receipt
                assert await rival.ttl(scope.claim) == -1
                assert await rival.xlen(scope.stream) == 1
                assert await rival.get(scope.reservation.counter_key) == b"1"
                assert await rival.get(scope.reservation.token_key) == (
                    scope.reservation.token.encode()
                )
            finally:
                release.set()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    asyncio.run(run())


def test_normal_quota_refusal_settlement_does_not_create_guard_authority() -> None:
    """@spec GUARDED-DELIVERY-V1-1 GUARDED-DELIVERY-V1-5."""
    async def run() -> None:
        """@spec GUARDED-DELIVERY-V1-5."""
        scope = Scope()
        value = guard(scope)
        async with clients(scope) as (client, _):
            assert await delivery.claim_delivery(client, scope.claim, OWNER, 60)
            assert not await delivery.take_backlog_slot(
                client, reservation=scope.reservation, limit=0,
            )
            await client.set(scope.fence, STABLE)
            await delivery.settle_failed_delivery(
                client, key=scope.claim, owner=OWNER,
                reservation=scope.reservation, preserve_quota=True,
            )
            assert await client.get(scope.claim) is None
            assert await client.get(scope.reservation.token_key) is None
            assert await client.get(scope.reservation.counter_key) == b"1"
            await refuse(client, scope, value, "reservation_lost")
    asyncio.run(run())


def test_guardless_layout_and_hook_receipt_layout_keep_their_exact_arity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec GUARDED-DELIVERY-V1-1 GUARDED-DELIVERY-V1-4 GUARDED-DELIVERY-V1-6."""
    async def run() -> None:
        """@spec GUARDED-DELIVERY-V1-4."""
        scope = Scope()
        async with clients(scope) as (client, _):
            original = client.eval
            calls = []

            async def observe(script: str, number: int, *arguments: Any) -> Any:
                """@spec GUARDED-DELIVERY-V1-4."""
                calls.append((number, arguments))
                return await original(script, number, *arguments)

            monkeypatch.setattr(client, "eval", observe)
            for index, metadata in enumerate([False, True]):
                key = scope.claim + str(index)
                receipt_key = key + ":receipt"
                assert await delivery.claim_delivery(client, key, "pending:legacy", 60)
                options = (
                    {"receipt_key": receipt_key, "receipt_prefix": '{"id":"'}
                    if metadata else {}
                )
                success, receipt = await append(client, scope, key=key, owner="pending:legacy",
                                                **options)
                assert success
                assert await client.ttl(key) == -1
                assert calls[-1][0] == (3 if metadata else 2)
                assert len(calls[-1][1]) == calls[-1][0] + 7
                if metadata:
                    assert await client.get(receipt_key) == ('{"id":"' + receipt + '"}').encode()
            assert await client.xlen(scope.stream) == 2
    asyncio.run(run())


@pytest.mark.parametrize("guarded", [False, True])
def test_actual_datastore_error_keeps_exception_identity(
    guarded: bool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec GUARDED-DELIVERY-V1-6."""
    async def run() -> None:
        """@spec GUARDED-DELIVERY-V1-6."""
        scope = Scope()
        value = guard(scope) if guarded else None
        async with clients(scope) as (client, _):
            await prepare(client, scope)
            await client.set(scope.stream, "wrong-type")
            original = client.eval
            errors = []

            async def observed(*args: Any, **kwargs: Any) -> Any:
                """@spec GUARDED-DELIVERY-V1-6."""
                try:
                    return await original(*args, **kwargs)
                except ResponseError as error:
                    errors.append(error)
                    raise

            monkeypatch.setattr(client, "eval", observed)
            options = {"guard": value} if guarded else {}
            with pytest.raises(ResponseError) as caught:
                await append(client, scope, **options)
            assert len(errors) == 1
            assert caught.value is errors[0]
    asyncio.run(run())


# Synthetic arithmetic operands test the *production-shared* comparator only.
# They never replace TIME in the production enqueue script.
@pytest.mark.parametrize(
    ("bounds", "instant", "expected"),
    [
        (["-1", "500000", "0", "0"], ["-1", "500000"], 1),
        (["-1", "500000", "0", "0"], ["-1", "499999"], -3),
        (["-1", "500000", "0", "0"], ["0", "0"], 1),
        (["-1", "500000", "0", "0"], ["0", "1"], -3),
        ([str(MIN_S), "0", str(MAX_S), "999999"], [str(MIN_S), "0"], 1),
        ([str(MIN_S), "0", str(MAX_S), "999999"], [str(MAX_S), "999999"], 1),
        ([str(MAX_S), "999998", str(MAX_S), "999999"], [str(MAX_S), "999997"], -3),
        (["0", "0", "0", "0"], ["0", "0"], 1),
        (["0", "0", "1", "0"], [str(MIN_S - 1), "0"], -5),
        (["0", "0", "1", "0"], [str(MAX_S + 1), "0"], -5),
        (["0", "0", "1", "0"], ["0", "-1"], -5),
        (["0", "0", "1", "0"], ["0", "1000000"], -5),
        (["0", "0", "1", "0"], ["1e0", "0"], -5),
        (["0", "0", "1", "0"], ["0", "NaN"], -5),
        (["00", "0", "1", "0"], ["0", "0"], -6),
        (["-0", "0", "1", "0"], ["0", "0"], -6),
        (["+0", "0", "1", "0"], ["0", "0"], -6),
        (["0.0", "0", "1", "0"], ["0", "0"], -6),
        (["0", "00", "1", "0"], ["0", "0"], -6),
        (["0", "0", "0", "-1"], ["0", "0"], -6),
        (["1", "1", "1", "0"], ["1", "0"], -6),
        ([str(MIN_S - 1), "0", "1", "0"], ["0", "0"], -6),
        (["0", "0", str(MAX_S + 1), "0"], ["0", "0"], -6),
    ],
)
def test_shared_lua_full_domain_arithmetic_without_substituting_server_time(
    bounds: list[str], instant: list[str], expected: int,
) -> None:
    """@spec GUARDED-DELIVERY-V1-4 GUARDED-DELIVERY-V1-7."""
    fragment = getattr(delivery, "_DELIVERY_WINDOW_LUA", None)
    assert isinstance(fragment, str), "production-shared window arithmetic is not implemented"
    wrapper = """
local b = delivery_window_bounds(ARGV[1],ARGV[2],ARGV[3],ARGV[4])
if not b then return -6 end
return delivery_window_contains(b,ARGV[5],ARGV[6])
"""
    async def run() -> None:
        """@spec GUARDED-DELIVERY-V1-7."""
        scope = Scope()
        async with clients(scope) as (client, _):
            assert await client.eval(fragment + wrapper, 0, *bounds, *instant) == expected
    asyncio.run(run())



def test_existing_enqueue_surface_adds_only_an_optional_guard() -> None:
    """@spec GUARDED-DELIVERY-V1-1."""
    parameter = inspect.signature(delivery.enqueue_owned).parameters.get("guard")
    assert parameter is not None, "enqueue_owned has no optional delivery guard"
    assert parameter.default is None
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY


@pytest.fixture(scope="module", autouse=True)
def required_guarded_delivery_valkey() -> None:
    """@spec GUARDED-DELIVERY-V1-8: unavailable prerequisites are setup errors."""
    from redis.backoff import NoBackoff
    from redis.retry import Retry

    async def check() -> None:
        """@spec GUARDED-DELIVERY-V1-8: no keys, fallback, or command retries."""
        client = redis.from_url(
            get_settings().valkey_dsn(), socket_connect_timeout=2, socket_timeout=2,
            retry=Retry(NoBackoff(), 0),
        )
        try:
            if await client.ping() is not True:
                raise RuntimeError("guarded delivery Valkey prerequisite unavailable")
        finally:
            await client.aclose()

    asyncio.run(check())
