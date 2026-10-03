"""Real Valkey primitive proof for @spec PROTECTED-HOOK-SOURCE-6/7.

These tests exercise source authority CAS only. They do not qualify an
independent protected broker, its ACL boundary, or protected activation.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import Any
from uuid import uuid4

import pytest
from curie_test_support.valkey import connect_or_skip


def _source_fence_module() -> Any:
    """Load the proposed primitive, @spec PROTECTED-HOOK-SOURCE-6."""
    try:
        available = importlib.util.find_spec("curie_protected_hooks.source_fence")
    except ModuleNotFoundError:
        available = None
    assert available is not None, "SOURCE-6 source authority primitive is missing"
    return importlib.import_module("curie_protected_hooks.source_fence")


@pytest.fixture
def source_store() -> Any:
    """Own random source keys only, @spec PROTECTED-HOOK-SOURCE-6."""
    admin = connect_or_skip(decode_responses=True)
    agents = (str(uuid4()), str(uuid4()))
    try:
        yield admin, agents
    finally:
        for agent_id in agents:
            keys = list(admin.scan_iter(match=f"protected:source:{agent_id}:*"))
            if keys:
                admin.delete(*keys)
            assert not list(admin.scan_iter(match=f"protected:source:{agent_id}:*"))
        admin.close()


def test_reserve_revokes_active_and_allocates_above_floor_and_sql_generation(
    source_store: Any,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6; PROTECTED-HOOK-SOURCE-7."""
    client, agents = source_store
    source_fence_module = _source_fence_module()
    fence = source_fence_module.SourceFence(client)
    agent_id = agents[0]
    first_op = str(uuid4())
    first = fence.reserve_and_revoke(agent_id, "probe", 0, first_op, 11)
    assert first > 11
    assert fence.publish_ordinary(agent_id, "probe", first, first_op, "a" * 64)
    second_op = str(uuid4())
    second = fence.reserve_and_revoke(agent_id, "probe", first, second_op, first + 20)
    assert second > first + 20
    assert fence.read(agent_id, "probe") == {
        "floor": second,
        "operation_id": second_op,
        "active": None,
    }


def test_current_reservation_retry_preserves_generation_and_published_state(
    source_store: Any,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6; PROTECTED-HOOK-SOURCE-7."""
    client, agents = source_store
    source_fence_module = _source_fence_module()
    fence = source_fence_module.SourceFence(client)
    agent_id, operation = agents[0], str(uuid4())
    generation = fence.reserve_and_revoke(agent_id, "probe", 0, operation, 7)
    closed = fence.read(agent_id, "probe")
    assert fence.reserve_and_revoke(agent_id, "probe", 0, operation, 7) == generation
    assert fence.read(agent_id, "probe") == closed
    assert fence.publish_ordinary(agent_id, "probe", generation, operation, "b" * 64)
    published = fence.read(agent_id, "probe")
    assert fence.reserve_and_revoke(agent_id, "probe", 0, operation, 7) == generation
    assert fence.read(agent_id, "probe") == published


def test_stale_expected_floor_refuses_without_revoking_current_state(source_store: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6; PROTECTED-HOOK-SOURCE-7."""
    client, agents = source_store
    source_fence_module = _source_fence_module()
    fence = source_fence_module.SourceFence(client)
    agent_id, operation = agents[0], str(uuid4())
    generation = fence.reserve_and_revoke(agent_id, "probe", 0, operation, 0)
    assert fence.publish_ordinary(agent_id, "probe", generation, operation, "c" * 64)
    before = fence.read(agent_id, "probe")
    with pytest.raises(source_fence_module.SourceFenceConflict):
        fence.reserve_and_revoke(agent_id, "probe", 0, str(uuid4()), generation + 99)
    assert fence.read(agent_id, "probe") == before


@pytest.mark.parametrize("mismatch", ["generation", "operation_id"])
def test_ordinary_publication_requires_exact_reserved_revision(
    source_store: Any, mismatch: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    client, agents = source_store
    source_fence_module = _source_fence_module()
    fence = source_fence_module.SourceFence(client)
    agent_id, operation = agents[0], str(uuid4())
    generation = fence.reserve_and_revoke(agent_id, "probe", 0, operation, 0)
    closed = fence.read(agent_id, "probe")
    wrong_generation = generation + 1 if mismatch == "generation" else generation
    wrong_operation = str(uuid4()) if mismatch == "operation_id" else operation
    assert not fence.publish_ordinary(
        agent_id, "probe", wrong_generation, wrong_operation, "d" * 64
    )
    assert fence.read(agent_id, "probe") == closed
    assert fence.publish_ordinary(agent_id, "probe", generation, operation, "d" * 64)
    published = fence.read(agent_id, "probe")
    assert fence.publish_ordinary(agent_id, "probe", generation, operation, "d" * 64)
    assert fence.read(agent_id, "probe") == published
    assert published == {
        "floor": generation,
        "operation_id": operation,
        "active": {
            "generation": generation,
            "operation_id": operation,
            "mode": "ordinary",
            "policy_fingerprint": "d" * 64,
        },
    }


def test_delayed_previous_publisher_and_superseded_retry_cannot_reopen_source(
    source_store: Any,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-7."""
    client, agents = source_store
    source_fence_module = _source_fence_module()
    fence = source_fence_module.SourceFence(client)
    agent_id, first_op, second_op = agents[0], str(uuid4()), str(uuid4())
    first = fence.reserve_and_revoke(agent_id, "probe", 0, first_op, 0)
    second = fence.reserve_and_revoke(agent_id, "probe", first, second_op, first)
    closed = fence.read(agent_id, "probe")
    assert not fence.publish_ordinary(agent_id, "probe", first, first_op, "e" * 64)
    with pytest.raises(source_fence_module.SourceFenceConflict):
        fence.reserve_and_revoke(agent_id, "probe", 0, first_op, 0)
    assert fence.read(agent_id, "probe") == closed
    assert fence.publish_ordinary(agent_id, "probe", second, second_op, "f" * 64)
    current = fence.read(agent_id, "probe")
    assert not fence.publish_ordinary(agent_id, "probe", first, first_op, "e" * 64)
    assert fence.read(agent_id, "probe") == current


def test_concurrent_reservations_have_one_cas_winner(source_store: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6; PROTECTED-HOOK-SOURCE-7."""
    client, agents = source_store
    source_fence_module = _source_fence_module()
    agent_id = agents[0]
    barrier = Barrier(2)
    operations = (str(uuid4()), str(uuid4()))

    def reserve(operation: str) -> tuple[str, int | None]:
        """Race independent connections, @spec PROTECTED-HOOK-SOURCE-6."""
        connection = connect_or_skip(decode_responses=True)
        try:
            fence = source_fence_module.SourceFence(connection)
            barrier.wait(timeout=10)
            try:
                return operation, fence.reserve_and_revoke(agent_id, "probe", 0, operation, 0)
            except source_fence_module.SourceFenceConflict:
                return operation, None
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(reserve, operations))
    winners = [
        (operation, generation) for operation, generation in results if generation is not None
    ]
    assert len(winners) == 1
    operation, generation = winners[0]
    assert generation is not None and generation > 0
    assert source_fence_module.SourceFence(client).read(agent_id, "probe") == {
        "floor": generation,
        "operation_id": operation,
        "active": None,
    }


def test_source_keys_isolate_agent_and_hook(source_store: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-6."""
    client, agents = source_store
    source_fence_module = _source_fence_module()
    fence = source_fence_module.SourceFence(client)
    first_op, second_op, sibling_op = str(uuid4()), str(uuid4()), str(uuid4())
    first = fence.reserve_and_revoke(agents[0], "probe", 0, first_op, 40)
    second = fence.reserve_and_revoke(agents[1], "probe", 0, second_op, 0)
    sibling = fence.reserve_and_revoke(agents[0], "sibling", 0, sibling_op, 0)
    assert second < first and sibling < first
    assert fence.publish_ordinary(agents[1], "probe", second, second_op, "1" * 64)
    assert fence.publish_ordinary(agents[0], "sibling", sibling, sibling_op, "2" * 64)
    second_state = fence.read(agents[1], "probe")
    sibling_state = fence.read(agents[0], "sibling")
    fence.reserve_and_revoke(agents[0], "probe", first, str(uuid4()), first)
    assert fence.read(agents[1], "probe") == second_state
    assert fence.read(agents[0], "sibling") == sibling_state


def test_reservation_preserves_exact_integer_generation_above_lua_float_precision(
    source_store: Any,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-1; PROTECTED-HOOK-SOURCE-6."""
    client, agents = source_store
    source_fence_module = _source_fence_module()
    fence = source_fence_module.SourceFence(client)
    operation = str(uuid4())
    minimum = (2**53) + 1
    generation = fence.reserve_and_revoke(agents[0], "probe", 0, operation, minimum)
    assert generation == minimum + 1
    next_operation = str(uuid4())
    following = fence.reserve_and_revoke(agents[0], "probe", generation, next_operation, generation)
    assert following == generation + 1
    assert fence.read(agents[0], "probe") == {
        "floor": following,
        "operation_id": next_operation,
        "active": None,
    }


def test_exhausted_generation_refuses_without_mutating_published_authority(
    source_store: Any,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-1; PROTECTED-HOOK-SOURCE-6."""
    client, agents = source_store
    source_fence_module = _source_fence_module()
    fence = source_fence_module.SourceFence(client)
    operation = str(uuid4())
    maximum = (2**63) - 1
    generation = fence.reserve_and_revoke(agents[0], "probe", 0, operation, maximum - 1)
    assert generation == maximum
    assert fence.publish_ordinary(agents[0], "probe", generation, operation, "3" * 64)
    before = fence.read(agents[0], "probe")
    with pytest.raises(source_fence_module.SourceFenceExhausted):
        fence.reserve_and_revoke(agents[0], "probe", maximum, str(uuid4()), maximum)
    assert fence.read(agents[0], "probe") == before


@pytest.mark.parametrize("field", ["expected_floor", "min_generation"])
def test_negative_generation_input_refuses_without_mutation(source_store: Any, field: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-1; PROTECTED-HOOK-SOURCE-6."""
    client, agents = source_store
    source_fence_module = _source_fence_module()
    fence = source_fence_module.SourceFence(client)
    operation = str(uuid4())
    generation = fence.reserve_and_revoke(agents[0], "probe", 0, operation, 0)
    assert fence.publish_ordinary(agents[0], "probe", generation, operation, "4" * 64)
    before = fence.read(agents[0], "probe")
    expected = -1 if field == "expected_floor" else generation
    minimum = -1 if field == "min_generation" else generation
    with pytest.raises(ValueError):
        fence.reserve_and_revoke(agents[0], "probe", expected, str(uuid4()), minimum)
    assert fence.read(agents[0], "probe") == before


@pytest.mark.parametrize("field", ["expected_floor", "min_generation"])
def test_generation_input_above_bigint_refuses_without_mutation(
    source_store: Any, field: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-1; PROTECTED-HOOK-SOURCE-6."""
    client, agents = source_store
    source_fence_module = _source_fence_module()
    fence = source_fence_module.SourceFence(client)
    operation = str(uuid4())
    generation = fence.reserve_and_revoke(agents[0], "probe", 0, operation, 0)
    assert fence.publish_ordinary(agents[0], "probe", generation, operation, "5" * 64)
    before = fence.read(agents[0], "probe")
    expected = 2**63 if field == "expected_floor" else generation
    minimum = 2**63 if field == "min_generation" else generation
    with pytest.raises(source_fence_module.SourceFenceExhausted):
        fence.reserve_and_revoke(agents[0], "probe", expected, str(uuid4()), minimum)
    assert fence.read(agents[0], "probe") == before


def test_same_revision_cannot_replace_published_policy_fingerprint(source_store: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-3; PROTECTED-HOOK-SOURCE-6."""
    client, agents = source_store
    source_fence_module = _source_fence_module()
    fence = source_fence_module.SourceFence(client)
    operation = str(uuid4())
    generation = fence.reserve_and_revoke(agents[0], "probe", 0, operation, 0)
    assert fence.publish_ordinary(agents[0], "probe", generation, operation, "6" * 64)
    before = fence.read(agents[0], "probe")
    assert not fence.publish_ordinary(agents[0], "probe", generation, operation, "7" * 64)
    assert fence.read(agents[0], "probe") == before
    assert fence.publish_ordinary(agents[0], "probe", generation, operation, "6" * 64)
    assert fence.read(agents[0], "probe") == before


@pytest.mark.parametrize("method", ["read", "reserve"])
@pytest.mark.parametrize("floor", [1.9, True, "-1", "9223372036854775808", "01"])
def test_corrupt_stored_floor_refuses_before_read_or_idempotent_reservation(
    source_store: Any, method: str, floor: Any
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-1; PROTECTED-HOOK-SOURCE-6."""
    client, agents = source_store
    source_fence_module = _source_fence_module()
    fence = source_fence_module.SourceFence(client)
    operation = str(uuid4())
    key = f"protected:source:{agents[0]}:probe"
    raw = json.dumps({"floor": floor, "operation_id": operation, "active": None})
    client.set(key, raw)
    try:
        with pytest.raises(ValueError) as rejected:
            if method == "read":
                fence.read(agents[0], "probe")
            else:
                fence.reserve_and_revoke(agents[0], "probe", 1, operation, 1)
        assert rejected.type.__name__ == "SourceFenceInvalid"
    finally:
        assert client.get(key) == raw


@pytest.mark.parametrize("method", ["read", "reserve"])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("generation", 1.9),
        ("generation", True),
        ("generation", "2"),
        ("generation", "-1"),
        ("generation", "01"),
        ("operation_id", "00000000-0000-0000-0000-000000000000"),
        ("policy_fingerprint", "invalid"),
        ("mode", "unsupported"),
    ],
)
def test_corrupt_active_binding_refuses_before_read_or_idempotent_reservation(
    source_store: Any, method: str, field: str, value: Any
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-1; PROTECTED-HOOK-SOURCE-6."""
    client, agents = source_store
    source_fence_module = _source_fence_module()
    fence = source_fence_module.SourceFence(client)
    operation = str(uuid4())
    active = {
        "generation": "1",
        "operation_id": operation,
        "mode": "ordinary",
        "policy_fingerprint": "8" * 64,
    }
    active[field] = value
    key = f"protected:source:{agents[0]}:probe"
    raw = json.dumps({"floor": "1", "operation_id": operation, "active": active})
    client.set(key, raw)
    try:
        with pytest.raises(ValueError) as rejected:
            if method == "read":
                fence.read(agents[0], "probe")
            else:
                fence.reserve_and_revoke(agents[0], "probe", 1, operation, 1)
        assert rejected.type.__name__ == "SourceFenceInvalid"
    finally:
        assert client.get(key) == raw
