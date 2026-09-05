"""Per-behavior contract tests for the pre-upgrade drain gate (issue #2010).

Against **real Valkey**, never a mock, for the same reason
``test_delivery_lease.py`` states: what the gate reads IS Valkey semantics --
``XPENDING`` ownership, key expiry, and the presence of a lease key another
process wrote. A mocked client would assert only that we called the verbs we
called.

Clocks are compressed by CONFIGURING short values, never by patching time. Every
ratio the ``WorkerConfig`` validators enforce is preserved, including the new
one: the quiesce TTL must strictly outlast the drain wait.

The API this file pins:

    UpgradeDrainGate(redis: Redis, config: WorkerConfig)
      .request_quiesce(*, ttl_s=None)     -> None   (always with an expiry)
      .clear_quiesce()                    -> None
      .is_quiescing()                     -> bool
      .unsettled_deliveries()             -> tuple[str, ...]  ("stream/group/entry")
      .await_drained(*, timeout_s=None, poll_interval_s=None) -> DrainOutcome

    DrainOutcome: .drained .remaining .waited_s
    run_gate(config, *, mode="drain"|"release") -> int   (the hook's exit code)
"""

from __future__ import annotations

import ast
import asyncio
import contextlib
import importlib
import re
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
import redis.asyncio
from curie_test_support.valkey import (
    VALKEY_HOST as _VALKEY_HOST,
)
from curie_test_support.valkey import (
    VALKEY_PORT as _VALKEY_PORT,
)
from curie_test_support.valkey import (
    VALKEY_PW as _VALKEY_PW,
)
from curie_worker.config import WorkerConfig
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.upgrade_drain import UpgradeDrainGate, _client, main, run_gate
from redis.asyncio import Redis as AsyncRedis
from redis.exceptions import ResponseError

# Compressed lease clocks, same shape as test_delivery_lease.py.
_TTL_S = 1.0
_HEARTBEAT_S = 0.3
_BUDGET_S = 60.0
# The drain wait, and a quiesce TTL that strictly outlasts it (the validator).
_DRAIN_TIMEOUT_S = 0.5
_QUIESCE_TTL_S = 2.0


def _config(names: dict[str, str], **overrides: object) -> WorkerConfig:
    base: dict[str, object] = {
        "valkey_host": _VALKEY_HOST,
        "valkey_port": _VALKEY_PORT,
        "valkey_password": _VALKEY_PW,
        "stream": names["stream"],
        "consumer_group": names["group"],
        "key_prefix": names["prefix"],
        # Per-test eval lane names too. The gate reads BOTH lanes, and leaving
        # the eval defaults in place would point it at a shared production-named
        # group on the shared test Valkey -- a cross-test coupling that reads as
        # a flake rather than as the wiring mistake it is.
        "eval_stream": f"{names['stream']}:evals",
        "eval_consumer_group": f"{names['group']}-evals",
        "delivery_budget_s": _BUDGET_S,
        "delivery_lease_ttl_s": _TTL_S,
        "delivery_lease_heartbeat_s": _HEARTBEAT_S,
        "reclaim_interval_s": 0.5,
        "runner_total_timeout_s": 30.0,
        "upgrade_drain_timeout_s": _DRAIN_TIMEOUT_S,
        "upgrade_drain_poll_interval_s": 0.05,
        "upgrade_quiesce_ttl_s": _QUIESCE_TTL_S,
    }
    base.update(overrides)
    return WorkerConfig(**base)


@contextlib.asynccontextmanager
async def _gate(
    names: dict[str, str], **overrides: object
) -> AsyncIterator[tuple[UpgradeDrainGate, WorkerConfig, AsyncRedis]]:
    config = _config(names, **overrides)
    client: AsyncRedis = AsyncRedis(
        host=_VALKEY_HOST,
        port=_VALKEY_PORT,
        password=_VALKEY_PW or None,
        decode_responses=True,
    )
    try:
        yield UpgradeDrainGate(client, config), config, client
    finally:
        with contextlib.suppress(Exception):
            await client.delete(config.upgrade_quiesce_key())
        with contextlib.suppress(Exception):
            await client.aclose()


async def _pending(client: AsyncRedis, config: WorkerConfig, consumer: str) -> str:
    """One entry, read into ``consumer``'s PEL. The lease's own precondition."""
    with contextlib.suppress(Exception):
        await client.xgroup_create(
            config.stream, config.consumer_group, id="0", mkstream=True
        )
    entry_id = await client.xadd(config.stream, {"payload": "p"})
    read: Any = await client.xreadgroup(
        config.consumer_group, consumer, {config.stream: ">"}, count=1
    )
    delivered = [eid for _s, entries in read for eid, _f in entries]
    assert delivered == [entry_id], f"expected {entry_id} pending, got {delivered}"
    return str(entry_id)


# --- the quiesce flag ---------------------------------------------------------


def test_quiesce_is_always_written_with_an_expiry(names) -> None:  # noqa: ANN001
    """A permanent flag turns a killed upgrade into a fleet that has silently
    stopped answering. Red if ``request_quiesce`` ever writes without a TTL."""

    async def go() -> None:
        async with _gate(names) as (gate, config, client):
            await gate.request_quiesce()
            assert await gate.is_quiescing() is True
            ttl = await client.ttl(config.upgrade_quiesce_key())
            assert ttl > 0, "the quiesce flag has no expiry; a dead upgrade wedges the fleet"
            assert ttl <= int(_QUIESCE_TTL_S)

    asyncio.run(go())


def test_quiesce_lapses_on_its_own_so_an_abandoned_upgrade_self_heals(names) -> None:  # noqa: ANN001
    """The TTL is the fail-safe for a hook that is killed between the gate and
    the post-upgrade release: nobody clears the flag, and the fleet recovers."""

    async def go() -> None:
        async with _gate(names, upgrade_quiesce_ttl_s=1.0, upgrade_drain_timeout_s=0.5) as (
            gate,
            _config,
            _client,
        ):
            await gate.request_quiesce()
            assert await gate.is_quiescing() is True
            await asyncio.sleep(1.4)
            assert await gate.is_quiescing() is False

    asyncio.run(go())


def test_clear_quiesce_is_idempotent(names) -> None:  # noqa: ANN001
    """The post-upgrade release runs on a fleet that may already be claiming
    (the TTL lapsed first); clearing twice must not be an error."""

    async def go() -> None:
        async with _gate(names) as (gate, _config, _client):
            await gate.clear_quiesce()
            await gate.request_quiesce()
            await gate.clear_quiesce()
            await gate.clear_quiesce()
            assert await gate.is_quiescing() is False

    asyncio.run(go())


# --- what counts as unsafe in-flight work ------------------------------------


def test_a_live_leased_delivery_is_reported_as_unsettled(names) -> None:  # noqa: ANN001
    """The whole delivery triple, not a bare entry id: an operator reading a
    refusal has to know which lane is still busy."""

    async def go() -> None:
        async with _gate(names) as (gate, config, client):
            entry_id = await _pending(client, config, "replica-a")
            store = DeliveryLeaseStore(client, config)
            await store.acquire(
                config.stream, config.consumer_group, entry_id, consumer="replica-a"
            )
            assert await gate.unsettled_deliveries() == (
                f"{config.stream}/{config.consumer_group}/{entry_id}",
            )

    asyncio.run(go())


def test_a_pending_entry_with_no_live_lease_is_not_unsettled(names) -> None:  # noqa: ANN001
    """Nobody is working it, so rolling interrupts nothing and the existing
    reclaim machinery picks it up afterwards.

    Red on a gate that reads pending-ness instead of liveness: every upgrade
    would then block behind whatever backlog happened to be un-acked, which is a
    gate that gets switched off in its first week.
    """

    async def go() -> None:
        async with _gate(names) as (gate, config, client):
            await _pending(client, config, "replica-a")
            assert await gate.unsettled_deliveries() == ()

    asyncio.run(go())


def test_a_lapsed_lease_stops_counting_as_unsettled(names) -> None:  # noqa: ANN001
    """The owner died. Its lease expires, the delivery becomes recoverable, and
    the gate must stop holding the upgrade for a process that is gone."""

    async def go() -> None:
        async with _gate(names) as (gate, config, client):
            entry_id = await _pending(client, config, "replica-a")
            store = DeliveryLeaseStore(client, config)
            await store.acquire(
                config.stream, config.consumer_group, entry_id, consumer="replica-a"
            )
            assert len(await gate.unsettled_deliveries()) == 1
            await asyncio.sleep(_TTL_S + 0.3)
            assert await gate.unsettled_deliveries() == ()

    asyncio.run(go())


def test_the_eval_lane_is_gated_too(names) -> None:  # noqa: ANN001
    """ADR-0131: "runs and evals must share the lease implementation by
    construction. A fix on only one consumer lane is incomplete." The same holds
    for the gate -- an eval delivery holds a sandbox and a lease exactly as a
    turn does."""

    async def go() -> None:
        async with _gate(names) as (gate, config, client):
            with contextlib.suppress(Exception):
                await client.xgroup_create(
                    config.eval_stream, config.eval_consumer_group, id="0", mkstream=True
                )
            entry_id = await client.xadd(config.eval_stream, {"payload": "suite"})
            await client.xreadgroup(
                config.eval_consumer_group,
                "eval-replica-a",
                {config.eval_stream: ">"},
                count=1,
            )
            store = DeliveryLeaseStore(client, config)
            await store.acquire(
                config.eval_stream,
                config.eval_consumer_group,
                str(entry_id),
                consumer="eval-replica-a",
            )
            assert await gate.unsettled_deliveries() == (
                f"{config.eval_stream}/{config.eval_consumer_group}/{entry_id}",
            )

    asyncio.run(go())


def test_a_lane_that_has_never_been_used_is_not_unsafe_work(names) -> None:  # noqa: ANN001
    """A release whose eval group does not exist yet must not be unable to
    upgrade. No group is no work, not an unreadable lane to refuse over."""

    async def go() -> None:
        async with _gate(names) as (gate, _config, _client):
            assert await gate.unsettled_deliveries() == ()

    asyncio.run(go())


def test_a_lane_that_cannot_be_read_refuses_the_upgrade(names) -> None:  # noqa: ANN001
    """Fail closed on an unreadable lane.

    Red on widening the NOGROUP guard back to a bare ``except``: a gate that
    answers "nothing in flight" about a lane it could not look at is worse than
    no gate, because it clears an upgrade over deliveries it never saw. Driven
    with a real WRONGTYPE (a plain string sitting where the stream should be),
    so the refusal is proven against Valkey's own error rather than a patched
    client.
    """

    async def go() -> None:
        async with _gate(names) as (gate, config, client):
            await client.set(config.stream, "not-a-stream")
            try:
                with pytest.raises(ResponseError):
                    await gate.unsettled_deliveries()
            finally:
                await client.delete(config.stream)

    asyncio.run(go())


# --- the gate itself ----------------------------------------------------------


def test_await_drained_quiesces_before_it_waits(names) -> None:  # noqa: ANN001
    """A wait that kept admitting new work could never terminate under load, so
    the flag goes up first and unconditionally."""

    async def go() -> None:
        async with _gate(names) as (gate, config, client):
            entry_id = await _pending(client, config, "replica-a")
            store = DeliveryLeaseStore(client, config)
            await store.acquire(
                config.stream, config.consumer_group, entry_id, consumer="replica-a"
            )
            outcome = await gate.await_drained()
            assert outcome.drained is False
            assert await gate.is_quiescing() is True, (
                "await_drained waited without quiescing first"
            )

    asyncio.run(go())


def test_await_drained_refuses_and_names_the_deliveries_holding_it_back(names) -> None:  # noqa: ANN001
    """"The gate refused" with no names is a message that gets the gate turned
    off. The refusal carries the delivery ids."""

    async def go() -> None:
        async with _gate(names) as (gate, config, client):
            entry_id = await _pending(client, config, "replica-a")
            store = DeliveryLeaseStore(client, config)
            await store.acquire(
                config.stream, config.consumer_group, entry_id, consumer="replica-a"
            )
            outcome = await gate.await_drained()
            assert outcome.drained is False
            assert outcome.remaining == (
                f"{config.stream}/{config.consumer_group}/{entry_id}",
            )
            assert outcome.waited_s >= _DRAIN_TIMEOUT_S

    asyncio.run(go())


def test_await_drained_returns_as_soon_as_the_owner_settles(names) -> None:  # noqa: ANN001
    """The success path, and it must not sit out the whole timeout: the gate
    polls, so a delivery that settles early lets the upgrade proceed early."""

    async def go() -> None:
        async with _gate(names, upgrade_drain_timeout_s=5.0, upgrade_quiesce_ttl_s=10.0) as (
            gate,
            config,
            client,
        ):
            entry_id = await _pending(client, config, "replica-a")
            store = DeliveryLeaseStore(client, config)
            lease = await store.acquire(
                config.stream, config.consumer_group, entry_id, consumer="replica-a"
            )

            async def settle_soon() -> None:
                await asyncio.sleep(0.2)
                await store.settle(config.stream, config.consumer_group, entry_id)

            settler = asyncio.create_task(settle_soon())
            outcome = await gate.await_drained(poll_interval_s=0.05)
            await settler
            assert lease.owner
            assert outcome.drained is True
            assert outcome.remaining == ()
            assert outcome.waited_s < 5.0, "the gate sat out the whole timeout"

    asyncio.run(go())


def test_an_empty_release_drains_immediately(names) -> None:  # noqa: ANN001
    """The overwhelmingly common upgrade: nothing in flight, no waiting."""

    async def go() -> None:
        async with _gate(names) as (gate, _config, _client):
            outcome = await gate.await_drained()
            assert outcome.drained is True
            assert outcome.waited_s < _DRAIN_TIMEOUT_S

    asyncio.run(go())


# --- the hook's exit contract -------------------------------------------------


def test_the_hook_exits_zero_and_leaves_the_fleet_quiesced_on_a_clean_drain(names) -> None:  # noqa: ANN001
    """Helm reads 0 as "roll". The flag STAYS set across the roll so the
    replacement pods that come up mid-upgrade do not reclaim the deliveries a
    still-draining replica is settling; the post-upgrade release clears it."""

    async def go() -> None:
        config = _config(names)
        code = await run_gate(config, mode="drain")
        assert code == 0
        client: AsyncRedis = AsyncRedis(
            host=_VALKEY_HOST,
            port=_VALKEY_PORT,
            password=_VALKEY_PW or None,
            decode_responses=True,
        )
        try:
            assert await client.exists(config.upgrade_quiesce_key())
            assert await run_gate(config, mode="release") == 0
            assert not await client.exists(config.upgrade_quiesce_key())
        finally:
            await client.delete(config.upgrade_quiesce_key())
            await client.aclose()

    asyncio.run(go())


def test_a_refused_upgrade_leaves_the_fleet_claiming_again(names) -> None:  # noqa: ANN001
    """Postponing must put the cluster back exactly as it was found.

    Red on a refusal path that leaves the flag set: a refused upgrade -- one
    where nothing was rolled and nothing changed -- would stop every replica in
    the release from claiming until the TTL lapsed, turning the refusal into the
    outage the refusal exists to avoid.
    """

    async def go() -> None:
        config = _config(names)
        client: AsyncRedis = AsyncRedis(
            host=_VALKEY_HOST,
            port=_VALKEY_PORT,
            password=_VALKEY_PW or None,
            decode_responses=True,
        )
        try:
            entry_id = await _pending(client, config, "replica-a")
            store = DeliveryLeaseStore(client, config)
            await store.acquire(
                config.stream, config.consumer_group, entry_id, consumer="replica-a"
            )
            assert await run_gate(config, mode="drain") == 1
            assert not await client.exists(config.upgrade_quiesce_key()), (
                "a refused upgrade left the fleet quiesced"
            )
        finally:
            await client.delete(config.upgrade_quiesce_key())
            await client.aclose()

    asyncio.run(go())


# --- the chart's cross-artifact coupling --------------------------------------

_CHART_HOOK = (
    Path(__file__).resolve().parents[3]
    / "charts"
    / "curie"
    / "templates"
    / "worker-upgrade-drain.yaml"
)


def test_the_chart_hooks_invoke_a_module_and_modes_this_package_actually_has() -> None:
    """The Jobs run `python -m curie_worker.upgrade_drain --mode ...` out of the
    worker image, and nothing else checks that string.

    The chart CI assertions render the command but cannot import it, and the
    helm-ci path filter does not include ``apps/worker/**`` -- so a rename or a
    dropped ``--mode`` value here would leave a chart that templates perfectly
    and fails at the one moment it runs, in the middle of an operator's upgrade.
    This assertion lives on the worker side deliberately: it is the side where
    that rename happens.
    """
    if not _CHART_HOOK.exists():  # a released wheel has no chart checkout
        return
    commands = [
        ast.literal_eval(match)
        for match in re.findall(r"command: (\[[^\]]*\])", _CHART_HOOK.read_text())
    ]
    assert commands, f"no container command found in {_CHART_HOOK}"
    for command in commands:
        interpreter, dash_m, module, mode_flag, mode = command
        assert (interpreter, dash_m, mode_flag) == ("python", "-m", "--mode"), command
        importlib.import_module(module)
        # The mode reaches argparse, whose `choices` is the real contract; an
        # unknown one exits 2 before the gate does anything.
        assert mode in ("drain", "release"), f"the chart asks for --mode {mode}"
    modes = {command[4] for command in commands}
    assert modes == {"drain", "release"}, (
        f"the chart wires {sorted(modes)}; both hooks are required -- without the "
        "release the fleet waits out the whole quiesce TTL after every upgrade"
    )


def test_an_unknown_mode_is_refused_rather_than_silently_draining() -> None:
    """``main`` is the process entry point the chart calls. A typo in the hook
    must fail loudly, not fall through to a default that quiesces the fleet."""
    try:
        main(["--mode", "quiesce-forever"])
    except SystemExit as exit_code:
        assert exit_code.code == 2
    else:
        raise AssertionError("an unknown --mode was accepted")


# --- _client TLS selection (#2315) -------------------------------------------
#
# Construction performs no I/O (no assertion here touches real Valkey), so
# this is hermetic: the seam under test is redis-py's own pool selection, the
# same shape run.py's _valkey_kwargs tests use.


def _drain_config(**overrides: object) -> WorkerConfig:
    base: dict[str, object] = {
        "valkey_host": _VALKEY_HOST,
        "valkey_port": _VALKEY_PORT,
        "valkey_password": _VALKEY_PW,
    }
    base.update(overrides)
    return WorkerConfig(**base)


def test_client_selects_the_plain_connection_by_default() -> None:
    client = _client(_drain_config())
    assert (
        client.connection_pool.connection_class is redis.asyncio.connection.Connection
    )


def test_client_selects_ssl_connection_when_tls_is_set() -> None:
    client = _client(_drain_config(valkey_tls=True))
    assert (
        client.connection_pool.connection_class
        is redis.asyncio.connection.SSLConnection
    )
