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
      .request_quiesce(*, ttl_s=None)     -> None   (always with an expiry;
                                                     raises if the fenced write
                                                     is refused)
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
import json
import os
import re
import socket
import subprocess
import sys
from collections.abc import AsyncIterator
from datetime import datetime
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


def _legacy_quiesce_key(config: WorkerConfig) -> str:
    """The pre-#2374 key, named explicitly for mixed-version assertions."""
    return f"{config.key_prefix}:upgrade:quiesce"


def _scoped_quiesce_key(config: WorkerConfig, installation_id: str) -> str:
    return f"{config.key_prefix}:upgrade:quiesce:{installation_id}"


def _module_env(config: WorkerConfig) -> dict[str, str]:
    """Only public connection/config values needed by the hook subprocess."""
    installation_id = str(getattr(config, "installation_id", "install-status"))
    revision = getattr(config, "upgrade_revision", 10)
    legacy = bool(getattr(config, "upgrade_legacy_quiesce", False))
    return {
        **os.environ,
        "VALKEY_HOST": config.valkey_host,
        "VALKEY_PORT": str(config.valkey_port),
        "VALKEY_PASSWORD": config.valkey_password,
        "VALKEY_DB": str(config.valkey_db),
        "VALKEY_TLS": "true" if config.valkey_tls else "false",
        "KEY_PREFIX": config.key_prefix,
        "CURIE_STREAM": config.stream,
        "CURIE_CONSUMER_GROUP": config.consumer_group,
        "CURIE_EVAL_STREAM": config.eval_stream,
        "CURIE_EVAL_CONSUMER_GROUP": config.eval_consumer_group,
        "CURIE_INSTALLATION_ID": installation_id,
        "CURIE_UPGRADE_REVISION": "" if revision is None else str(revision),
        "CURIE_UPGRADE_LEGACY_QUIESCE": "true" if legacy else "false",
        "CURIE_UPGRADE_DRAIN_TIMEOUT_S": str(config.upgrade_drain_timeout_s),
        "CURIE_UPGRADE_DRAIN_POLL_INTERVAL_S": str(
            config.upgrade_drain_poll_interval_s
        ),
        "CURIE_UPGRADE_QUIESCE_TTL_S": str(config.upgrade_quiesce_ttl_s),
    }


def _configure_module_env(
    monkeypatch: pytest.MonkeyPatch, config: WorkerConfig
) -> None:
    for name, value in _module_env(config).items():
        if name in {
            "VALKEY_HOST",
            "VALKEY_PORT",
            "VALKEY_PASSWORD",
            "VALKEY_DB",
            "VALKEY_TLS",
            "KEY_PREFIX",
            "CURIE_STREAM",
            "CURIE_CONSUMER_GROUP",
            "CURIE_EVAL_STREAM",
            "CURIE_EVAL_CONSUMER_GROUP",
            "CURIE_INSTALLATION_ID",
            "CURIE_UPGRADE_REVISION",
            "CURIE_UPGRADE_LEGACY_QUIESCE",
            "CURIE_UPGRADE_DRAIN_TIMEOUT_S",
            "CURIE_UPGRADE_DRAIN_POLL_INTERVAL_S",
            "CURIE_UPGRADE_QUIESCE_TTL_S",
        }:
            monkeypatch.setenv(name, value)


def _status_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    config: WorkerConfig,
) -> tuple[dict[str, object], str]:
    _configure_module_env(monkeypatch, config)
    assert main(["--mode", "status", "--json"]) == 0
    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    assert len(lines) == 1, f"status must write one JSON object, got {lines!r}"
    return json.loads(lines[0]), captured.err


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


def test_scoped_marker_isolates_installations_and_has_a_finite_ttl(
    names,
) -> None:  # noqa: ANN001
    """A leftover hook can pause only the installation whose ID it carries."""

    async def go() -> None:
        first = _config(
            names,
            installation_id="install-first",
            upgrade_revision=9,
            upgrade_legacy_quiesce=False,
        )
        second = _config(
            names,
            installation_id="install-second",
            upgrade_revision=9,
            upgrade_legacy_quiesce=False,
        )
        client: AsyncRedis = AsyncRedis(
            host=_VALKEY_HOST,
            port=_VALKEY_PORT,
            password=_VALKEY_PW or None,
            decode_responses=True,
        )
        first_key = _scoped_quiesce_key(first, "install-first")
        second_key = _scoped_quiesce_key(second, "install-second")
        legacy_key = _legacy_quiesce_key(first)
        try:
            await client.delete(first_key, second_key, legacy_key)
            await UpgradeDrainGate(client, first).request_quiesce()

            assert await UpgradeDrainGate(client, first).is_quiescing() is True
            assert await UpgradeDrainGate(client, second).is_quiescing() is False
            assert await client.exists(first_key)
            assert not await client.exists(second_key)
            assert not await client.exists(legacy_key), (
                "a current install touched the global compatibility key"
            )
            ttl_ms = await client.pttl(first_key)
            assert 0 < ttl_ms <= int(_QUIESCE_TTL_S * 1000)
        finally:
            await client.delete(first_key, second_key, legacy_key)
            await client.aclose()

    asyncio.run(go())


def test_marker_json_fences_numeric_revisions_and_retains_same_revision_since(
    names,
) -> None:  # noqa: ANN001
    """Revision 10 outranks 9; retries by one owner preserve authorship time."""

    async def go() -> None:
        revision_9 = _config(
            names,
            installation_id="install-fenced",
            upgrade_revision=9,
            upgrade_legacy_quiesce=False,
        )
        revision_10 = _config(
            names,
            installation_id="install-fenced",
            upgrade_revision=10,
            upgrade_legacy_quiesce=False,
        )
        client: AsyncRedis = AsyncRedis(
            host=_VALKEY_HOST,
            port=_VALKEY_PORT,
            password=_VALKEY_PW or None,
            decode_responses=True,
        )
        key = _scoped_quiesce_key(revision_9, "install-fenced")
        gate_9 = UpgradeDrainGate(client, revision_9)
        gate_10 = UpgradeDrainGate(client, revision_10)
        try:
            await client.delete(key, _legacy_quiesce_key(revision_9))
            await gate_9.request_quiesce()
            first_raw = await client.get(key)
            assert first_raw is not None
            first = json.loads(first_raw)
            assert first["revision"] == 9
            assert isinstance(first["revision"], int)
            parsed_since = datetime.fromisoformat(first["since"])
            assert parsed_since.tzinfo is not None

            await asyncio.sleep(0.02)
            await gate_9.request_quiesce()
            assert await client.get(key) == first_raw, (
                "a same-revision retry rewrote the original since timestamp"
            )

            await gate_10.request_quiesce()
            revision_10_raw = await client.get(key)
            assert revision_10_raw is not None
            assert json.loads(revision_10_raw)["revision"] == 10

            with pytest.raises(Exception, match="(?i)higher revision"):
                await gate_9.request_quiesce()
            assert await client.get(key) == revision_10_raw, (
                "numeric revision 9 replaced the newer revision 10 marker"
            )
            await gate_9.clear_quiesce()
            assert await client.get(key) == revision_10_raw, (
                "a delayed revision 9 release cleared revision 10"
            )
            await gate_10.clear_quiesce()
            assert not await client.exists(key)
        finally:
            await client.delete(key, _legacy_quiesce_key(revision_9))
            await client.aclose()

    asyncio.run(go())


def test_current_marker_never_reads_writes_or_clears_the_global_key(
    names,
) -> None:  # noqa: ANN001
    async def go() -> None:
        config = _config(
            names,
            installation_id="install-current",
            upgrade_revision=10,
            upgrade_legacy_quiesce=False,
        )
        client: AsyncRedis = AsyncRedis(
            host=_VALKEY_HOST,
            port=_VALKEY_PORT,
            password=_VALKEY_PW or None,
            decode_responses=True,
        )
        gate = UpgradeDrainGate(client, config)
        legacy_key = _legacy_quiesce_key(config)
        scoped_key = _scoped_quiesce_key(config, "install-current")
        legacy_value = json.dumps(
            {"since": "2026-09-08T00:00:00+00:00", "revision": 77},
            separators=(",", ":"),
        )
        try:
            await client.delete(legacy_key, scoped_key)
            await client.set(legacy_key, legacy_value, ex=30)

            assert await gate.is_quiescing() is False
            await gate.request_quiesce()
            assert await client.get(legacy_key) == legacy_value
            assert await client.exists(scoped_key)
            await gate.clear_quiesce()
            assert await client.get(legacy_key) == legacy_value
            assert not await client.exists(scoped_key)
        finally:
            await client.delete(legacy_key, scoped_key)
            await client.aclose()

    asyncio.run(go())


def test_first_mixed_version_upgrade_atomically_writes_and_clears_both_keys(
    names,
) -> None:  # noqa: ANN001
    async def go() -> None:
        config = _config(
            names,
            installation_id="install-adopted",
            upgrade_revision=9,
            upgrade_legacy_quiesce=True,
        )
        client: AsyncRedis = AsyncRedis(
            host=_VALKEY_HOST,
            port=_VALKEY_PORT,
            password=_VALKEY_PW or None,
            decode_responses=True,
        )
        gate = UpgradeDrainGate(client, config)
        legacy_key = _legacy_quiesce_key(config)
        scoped_key = _scoped_quiesce_key(config, "install-adopted")
        try:
            await client.delete(legacy_key, scoped_key)
            await gate.request_quiesce()
            legacy_raw, scoped_raw = await client.mget(legacy_key, scoped_key)
            assert legacy_raw is not None
            assert scoped_raw is not None
            assert scoped_raw == legacy_raw, (
                "the compatibility bridge did not author one marker on both keys"
            )
            assert json.loads(scoped_raw)["revision"] == 9
            assert await client.ttl(legacy_key) > 0
            assert await client.ttl(scoped_key) > 0

            await gate.clear_quiesce()
            assert await client.mget(legacy_key, scoped_key) == [None, None]
        finally:
            await client.delete(legacy_key, scoped_key)
            await client.aclose()

    asyncio.run(go())


def test_run_gate_refuses_when_a_higher_revision_fences_the_quiesce_write(
    names,
) -> None:  # noqa: ANN001
    """A fenced Lua write must fail the hook, not report a drain that never happened.

    Chief-of-staff review 2026-09-09 finding 4 (follow-up to #2471):
    ``_WRITE_OWNED_MARKER_LUA`` returns 0 and writes nothing when an applicable
    key already holds a higher revision, but ``request_quiesce`` discarded that
    result and ``await_drained`` proceeded into the settle loop. Reachable on
    the legacy-bridge upgrade, where the shared unscoped key is in KEYS, and
    on the standalone/Compose path, where ``_revision()`` is 0.
    """

    async def go() -> None:
        bridge = _config(
            names,
            installation_id="install-fenced-write",
            upgrade_revision=9,
            upgrade_legacy_quiesce=True,
        )
        standalone = _config(names, installation_id="")
        client: AsyncRedis = AsyncRedis(
            host=_VALKEY_HOST,
            port=_VALKEY_PORT,
            password=_VALKEY_PW or None,
            decode_responses=True,
        )
        legacy_key = _legacy_quiesce_key(bridge)
        scoped_key = _scoped_quiesce_key(bridge, "install-fenced-write")
        foreign = json.dumps(
            {"since": "2026-09-08T00:00:00+00:00", "revision": 11},
            separators=(",", ":"),
        )
        try:
            await client.delete(legacy_key, scoped_key)
            await client.set(legacy_key, foreign, ex=30)

            gate = UpgradeDrainGate(client, bridge)
            # Match the refusal text rather than importing QuiesceWriteRefused:
            # verify-fix-pin reverses only product files, so a new exception
            # class would fail collection instead of failing the selected test.
            with pytest.raises(Exception, match="(?i)higher revision"):
                await gate.request_quiesce()
            assert not await client.exists(scoped_key), (
                "a fenced mixed-version write still authored the scoped marker"
            )
            assert await client.get(legacy_key) == foreign
            assert await gate.is_quiescing() is False, (
                "this installation's workers were quiesced even though the write "
                "was refused"
            )

            with pytest.raises(Exception, match="(?i)higher revision"):
                await gate.await_drained()

            assert await run_gate(bridge, mode="drain") == 1
            assert not await client.exists(scoped_key)
            assert await client.get(legacy_key) == foreign

            await client.delete(legacy_key, scoped_key)
            await client.set(legacy_key, foreign, ex=30)
            assert await run_gate(standalone, mode="drain") == 1
            assert json.loads(await client.get(legacy_key))["revision"] == 11
        finally:
            await client.delete(legacy_key, scoped_key)
            await client.aclose()

    asyncio.run(go())


def test_mixed_version_release_clears_owned_scoped_marker_when_shared_key_is_foreign(
    names,
) -> None:  # noqa: ANN001
    """A foreign marker on the shared key must not strand this install's scoped one.

    Chief-of-staff review 2026-09-09 finding 4b: ``_CLEAR_OWNED_MARKER_LUA``
    returned 0 on the first non-matching key before deleting anything, so a
    legacy-bridge release left the installation's own scoped marker for the
    full ``upgrade_quiesce_ttl_s``.
    """

    async def go() -> None:
        config = _config(
            names,
            installation_id="install-clear-owned",
            upgrade_revision=9,
            upgrade_legacy_quiesce=True,
        )
        client: AsyncRedis = AsyncRedis(
            host=_VALKEY_HOST,
            port=_VALKEY_PORT,
            password=_VALKEY_PW or None,
            decode_responses=True,
        )
        gate = UpgradeDrainGate(client, config)
        legacy_key = _legacy_quiesce_key(config)
        scoped_key = _scoped_quiesce_key(config, "install-clear-owned")
        foreign = json.dumps(
            {"since": "2026-09-08T00:00:00+00:00", "revision": 11},
            separators=(",", ":"),
        )
        try:
            await client.delete(legacy_key, scoped_key)
            await gate.request_quiesce()
            assert await client.exists(scoped_key)
            await client.set(legacy_key, foreign, ex=30)

            await gate.clear_quiesce()
            assert not await client.exists(scoped_key), (
                "a foreign shared-key marker stranded this installation's "
                "owned scoped quiesce flag"
            )
            assert await client.get(legacy_key) == foreign, (
                "clear_quiesce deleted a foreign higher-revision shared marker"
            )
        finally:
            await client.delete(legacy_key, scoped_key)
            await client.aclose()

    asyncio.run(go())


def test_mixed_version_release_does_not_clear_legacy_when_scoped_holds_newer_revision(
    names,
) -> None:  # noqa: ANN001
    """A delayed lower-revision release must not resume legacy workers.

    The owned scoped marker is safe to delete unilaterally, but the shared
    key is one half of the mixed-version pair. If the scoped key already
    holds a newer revision, clearing a matching legacy key would let
    pre-scoped replicas claim during the newer drain.
    """

    async def go() -> None:
        config = _config(
            names,
            installation_id="install-clear-asymmetric",
            upgrade_revision=9,
            upgrade_legacy_quiesce=True,
        )
        client: AsyncRedis = AsyncRedis(
            host=_VALKEY_HOST,
            port=_VALKEY_PORT,
            password=_VALKEY_PW or None,
            decode_responses=True,
        )
        gate = UpgradeDrainGate(client, config)
        legacy_key = _legacy_quiesce_key(config)
        scoped_key = _scoped_quiesce_key(config, "install-clear-asymmetric")
        newer = json.dumps(
            {"since": "2026-09-08T00:00:00+00:00", "revision": 10},
            separators=(",", ":"),
        )
        older = json.dumps(
            {"since": "2026-09-08T00:00:00+00:00", "revision": 9},
            separators=(",", ":"),
        )
        try:
            await client.delete(legacy_key, scoped_key)
            await client.set(scoped_key, newer, ex=30)
            await client.set(legacy_key, older, ex=30)

            await gate.clear_quiesce()
            assert await client.get(scoped_key) == newer, (
                "a delayed revision 9 release cleared the newer scoped marker"
            )
            assert await client.get(legacy_key) == older, (
                "a delayed revision 9 release cleared the shared key while "
                "scoped held a newer revision"
            )
        finally:
            await client.delete(legacy_key, scoped_key)
            await client.aclose()

    asyncio.run(go())


def test_refused_mixed_version_upgrade_clears_both_owned_markers(
    names,
) -> None:  # noqa: ANN001
    """A pre-fix worker sees claims enabled again when the new hook refuses."""

    async def go() -> None:
        config = _config(
            names,
            installation_id="install-adopted",
            upgrade_revision=9,
            upgrade_legacy_quiesce=True,
        )
        legacy_config = _config(names, installation_id="")
        client: AsyncRedis = AsyncRedis(
            host=_VALKEY_HOST,
            port=_VALKEY_PORT,
            password=_VALKEY_PW or None,
            decode_responses=True,
        )
        legacy_key = _legacy_quiesce_key(config)
        scoped_key = _scoped_quiesce_key(config, "install-adopted")
        try:
            await client.delete(legacy_key, scoped_key)
            entry_id = await _pending(client, config, "replica-pre-fix")
            store = DeliveryLeaseStore(client, config)
            await store.acquire(
                config.stream,
                config.consumer_group,
                entry_id,
                consumer="replica-pre-fix",
            )

            assert await run_gate(config, mode="drain") == 1
            assert await client.mget(legacy_key, scoped_key) == [None, None]
            assert await UpgradeDrainGate(client, legacy_config).is_quiescing() is False
        finally:
            await client.delete(legacy_key, scoped_key)
            await client.aclose()

    asyncio.run(go())


def test_status_writes_one_safe_json_object_for_absent_owned_and_malformed_markers(
    names,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:  # noqa: ANN001
    """Existence is pause authority even when marker metadata is unreadable."""

    config = _config(
        names,
        installation_id="install-status",
        upgrade_revision=10,
        upgrade_legacy_quiesce=False,
    )
    key = _scoped_quiesce_key(config, "install-status")

    async def arrange(value: str | None) -> None:
        client: AsyncRedis = AsyncRedis(
            host=_VALKEY_HOST,
            port=_VALKEY_PORT,
            password=_VALKEY_PW or None,
            decode_responses=True,
        )
        try:
            await client.delete(key, _legacy_quiesce_key(config))
            if value is None:
                return
            if value == "owned":
                await UpgradeDrainGate(client, config).request_quiesce()
            else:
                await client.set(key, value, ex=30)
                assert await UpgradeDrainGate(client, config).is_quiescing() is True
        finally:
            await client.aclose()

    try:
        asyncio.run(arrange(None))
        status, stderr = _status_json(monkeypatch, capsys, config)
        assert status == {
            "state": "claims_enabled",
            "since": None,
            "revision": None,
        }
        assert "install-status" not in stderr

        asyncio.run(arrange("owned"))
        status, stderr = _status_json(monkeypatch, capsys, config)
        assert status["state"] == "quiescing"
        assert status["revision"] == 10
        assert isinstance(status["since"], str)
        since = status["since"]
        assert isinstance(since, str)
        assert datetime.fromisoformat(since).tzinfo is not None
        assert "install-status" not in json.dumps(status)
        assert "install-status" not in stderr

        asyncio.run(arrange("not-json"))
        status, stderr = _status_json(monkeypatch, capsys, config)
        assert status == {"state": "quiescing", "since": None, "revision": None}
        assert "install-status" not in stderr
    finally:
        async def cleanup() -> None:
            client: AsyncRedis = AsyncRedis(
                host=_VALKEY_HOST,
                port=_VALKEY_PORT,
                password=_VALKEY_PW or None,
                decode_responses=True,
            )
            try:
                await client.delete(key, _legacy_quiesce_key(config))
            finally:
                await client.aclose()

        asyncio.run(cleanup())


def test_status_reports_unknown_when_the_marker_cannot_be_read(
    names,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:  # noqa: ANN001
    config = _config(
        names,
        installation_id="install-status",
        upgrade_revision=10,
        upgrade_legacy_quiesce=False,
    )
    with socket.socket() as unreachable:
        unreachable.bind(("127.0.0.1", 0))
        port = int(unreachable.getsockname()[1])
        unreadable = config.model_copy(
            update={"valkey_host": "127.0.0.1", "valkey_port": port}
        )
        status, stderr = _status_json(monkeypatch, capsys, unreadable)

    assert status == {"state": "unknown", "since": None, "revision": None}
    assert "install-status" not in stderr
    if _VALKEY_PW:
        assert _VALKEY_PW not in stderr


@pytest.mark.parametrize("mode", ["drain", "release"])
def test_unobserved_installation_refuses_mutating_modes_before_connecting(
    names,
    mode: str,
) -> None:  # noqa: ANN001
    """Client-only upgrade lookup failure must not mutate either hook path.

    The bound-but-not-listening port makes Valkey deliberately unreachable. A
    return code of 1 plus the authored refusal proves the identity guard ran;
    argparse's pre-fix unknown-option exit is 2 and cannot satisfy this test.
    """
    config = _config(
        names,
        installation_id="install-unobserved",
        upgrade_revision=10,
        upgrade_legacy_quiesce=True,
    )
    with socket.socket() as unreachable:
        unreachable.bind(("127.0.0.1", 0))
        env = _module_env(
            config.model_copy(
                update={
                    "valkey_host": "127.0.0.1",
                    "valkey_port": int(unreachable.getsockname()[1]),
                }
            )
        )
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "curie_worker.upgrade_drain",
                "--mode",
                mode,
                "--installation-id-observed=false",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
            env=env,
        )

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert "installation ID was not observed" in completed.stderr
    assert "refus" in completed.stderr.lower()
    assert "usage:" not in completed.stderr.lower()
    assert "connection" not in completed.stderr.lower()
    assert "traceback" not in completed.stderr.lower()
    if _VALKEY_PW:
        assert _VALKEY_PW not in completed.stderr


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
    the flag goes up first. A fenced write that does not take effect must not
    proceed into that wait."""

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
        interpreter, dash_m, module, mode_flag, mode, *_hook_args = command
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
