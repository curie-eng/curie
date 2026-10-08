"""The protected worker's broker role, measured on the pinned Valkey,
@spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-6 PROTECTED-HOOK-LANE-7.

``curie_protected_hooks.worker_acl.worker_acl_rules("worker")`` is the LANE-3 protected
worker row: read control/evidence and envelope bindings, consume and reclaim the
protected runs stream, read/write the worker's own execution key families, and nothing
else. The worker's own families live under the dedicated ``protected:lane:`` prefix (the
maintainer decision), never under a ``curie:*`` wildcard.

The exact key selectors (as ``ACL GETUSER`` normalizes them; ``~`` is read-write):

    %R~protected:control:*                       control and evidence reads
    %R~protected:admission:binding:*             immutable envelope binding reads
    ~protected:lane:*                            locks, markers, leases, capacity wait,
                                                 affinity routes, graveyard, SDK routes
    ~curie:runs                                  the admission-written runs stream
                                                 (read, group, ack, claim, retry XADD)
    ~curie:runs:consumer-heartbeat:*             liveness keys derived from the stream
    ~curie:runs:consumer-heartbeat-capable:*       name by the unchanged consumer
    ~curie:runs:consumer-reclaim-lock:*
    ~                                            the empty key only: the unchanged
                                                 delivery-lease release script declares
                                                 "" as its second key when a delivery
                                                 has no resume marker

The command inventory is the one the real Consumer/Kernel/Lua scripts were measured to
issue (see ``.projects/plans/s1.tests.md``). The ``curie:thread-reset-*`` keys are not
granted: the consumer's pre-turn drain is wrapped, so the denial is logged and the turn
continues.

Every behavioral case runs against the owned disposable TLS Valkey of
``worker_broker.py`` (default user disabled, exact container ownership) with a principal
installed from the product recipe; the real worker components run unchanged over it.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid

import pytest
from curie_protected_hooks.admission_acl import admission_acl_rules
from curie_protected_hooks.broker_metadata import metadata_acl_rules
from redis.exceptions import NoPermissionError, ResponseError

from . import worker_broker as wb

pworker_service = wb.pworker_service
owned_worker = wb.owned_worker

HANDSHAKE = {"auth", "hello", "ping", "client|setname", "client|setinfo"}
# Measured by running the real Consumer, Kernel, delivery lease, capacity wait, markers,
# thread lock, liveness and affinity store over a restricted principal.
REQUIRED = HANDSHAKE | {
    "info|server",
    "time",
    "get",
    "set",
    "del",
    "exists",
    "expire",
    "pexpire",
    "pexpireat",
    "pexpiretime",
    "pttl",
    "ttl",
    "incr",
    "hget",
    "hset",
    "hsetnx",
    "hdel",
    "hgetall",
    "hmget",
    "hexists",
    "hincrby",
    "sadd",
    "srem",
    "scard",
    "smembers",
    "sismember",
    "srandmember",
    "zadd",
    "zrem",
    "zscore",
    "zcount",
    "zrangebyscore",
    "multi",
    "exec",
    "eval",
    "evalsha",
    "script|load",
    "scan",
    "xack",
    "xadd",
    "xautoclaim",
    "xclaim",
    "xdel",
    "xgroup|create",
    "xgroup|delconsumer",
    "xinfo|consumers",
    "xinfo|groups",
    "xlen",
    "xpending",
    "xrange",
    "xreadgroup",
    "xrevrange",
}
OPTIONAL = {"type", "unlink", "xinfo|stream", "watch", "unwatch", "discard"}
FORBIDDEN = {
    "config|get",
    "config|set",
    "config|rewrite",
    "config|resetstat",
    "acl|setuser",
    "acl|deluser",
    "acl|list",
    "acl|log",
    "acl|getuser",
    "flushall",
    "flushdb",
    "keys",
    "shutdown",
    "debug",
    "client|kill",
    "client|list",
    "client|pause",
    "script|flush",
    "script|kill",
    "function",
    "replicaof",
    "slaveof",
    "module",
    "publish",
    "subscribe",
    "psubscribe",
    "pubsub",
    "monitor",
    "save",
    "bgsave",
    "swapdb",
    "rename",
    "dump",
    "restore",
    "migrate",
    "mset",
    "spop",
    "info",
    "info|commandstats",
}
KEYS_EXPECTED = {
    "~",
    "~protected:lane:*",
    "~curie:runs",
    "~curie:runs:consumer-heartbeat:*",
    "~curie:runs:consumer-heartbeat-capable:*",
    "~curie:runs:consumer-reclaim-lock:*",
    "%R~protected:control:*",
    "%R~protected:admission:binding:*",
}

# Keys the worker must never write, read, or hold (evidence is a control family).
NO_WRITE = [
    "protected:control:selection:33333333-3333-4333-8333-333333333333",
    "protected:control:readiness:33333333-3333-4333-8333-333333333333:1",
    "protected:source:11111111-1111-4111-8111-111111111111:incident",
    wb.BINDING,
    "protected:admission:intent:digest-example",
    "protected:admission:state:digest-example",
    "protected:admission:commit:digest-example",
    "protected:admission:recovery:digest-example",
    "protected:admission:quota",
    "protected:other:example",
    "protected:lanes:example",
    "protected:lane",
    "curie:worker:example",
    "curie:sandbox:example",
    "curie:runs:dead",
    "curie:runs:other",
    "curie:thread-reset-requests",
    "curie:thread-reset-inflight",
    "curie:thread-reset-result:thread-example",
    "curie:kill:11111111-1111-4111-8111-111111111111",
    "curie:admission:example",
    "curie:dedupe:example",
]
NO_READ = [
    key for key in NO_WRITE if not key.startswith("protected:control:") and key != wb.BINDING
]
WRITERS = [
    ("SET", "x"),
    ("DEL",),
    ("EXPIRE", 10),
    ("HSET", "f", "v"),
    ("INCR",),
    ("SADD", "m"),
    ("ZADD", 1, "m"),
    ("XADD", "*", "f", "v"),
]


def recipe():
    """Missing product is an assertion in the test body, @spec PROTECTED-HOOK-LANE-3."""
    return wb.worker_acl().worker_acl_rules


def granted(broker, username):
    """Normalized ``ACL GETUSER`` as (key selectors, commands, channels, enabled).

    Root and parenthesized selectors are unioned: the recipe may use either form.
    """

    def text(value):
        return value.decode() if isinstance(value, bytes) else str(value)

    user = {text(k): v for k, v in broker.command("ACL", "GETUSER", username).items()}
    parts = [user]
    for selector in user.get("selectors", []):
        parts.append({text(k): v for k, v in selector.items()})
    keys, commands, channels = set(), set(), []
    for part in parts:
        keys.update(text(part.get("keys", "")).split())
        channels.append(text(part.get("channels", "")).strip())
        for token in text(part.get("commands", "")).split():
            commands.add(token)
    return keys, commands, [c for c in channels if c], "on" in [text(f) for f in user["flags"]]


def command_names(tokens):
    """Allowed command names, refusing categories and non-reset negations."""
    assert not [t for t in tokens if t.startswith("+@")], "category grants are not allowed"
    assert not [t for t in tokens if t.startswith("-") and t != "-@all"], "negations beyond -@all"
    return {t[1:] for t in tokens if t.startswith("+")}


# -- recipe shape --------------------------------------------------------------------


def test_the_recipe_is_a_closed_role_with_no_credential_or_enabled_flag():
    """One role name, tuple of strings, resets first, no principal data,
    @spec PROTECTED-HOOK-LANE-3."""
    rules = recipe()("worker")
    assert type(rules) is tuple and all(type(rule) is str for rule in rules)
    assert rules[:4] == ("-@all", "resetkeys", "resetchannels", "clearselectors")
    for token in rules:
        assert token not in {
            "on",
            "off",
            "reset",
            "nopass",
            "allkeys",
            "allcommands",
            "allchannels",
        }
        assert not token.startswith((">", "<", "#", "!", "&"))
        assert "+@" not in token
    assert recipe()("worker") == rules
    for role in ("enqueue", "verifier", "WORKER", "worker ", "", None, b"worker", ("worker",)):
        with pytest.raises(ValueError, match="unknown protected worker role"):
            recipe()(role)


def test_no_other_recipe_gains_the_worker_role_or_the_lane_family():
    """The worker row exists only in the worker module, @spec PROTECTED-HOOK-LANE-3."""
    for rules in (admission_acl_rules, metadata_acl_rules):
        with pytest.raises(ValueError):
            rules("worker")
    for role in ("enqueue", "verifier"):
        assert not [r for r in admission_acl_rules(role) if "protected:lane" in r]
    for role in ("source_writer", "control_reader"):
        assert not [r for r in metadata_acl_rules(role) if "protected:lane" in r]


def test_installed_selectors_and_commands_are_exactly_the_measured_set(owned_worker):
    """The exact key selectors and command inventory, no wildcard and no category,
    @spec PROTECTED-HOOK-LANE-3."""
    keys, tokens, channels, enabled = granted(owned_worker.broker, owned_worker.username)
    assert enabled is True
    assert keys == KEYS_EXPECTED
    assert channels == []
    commands = command_names(tokens)
    assert REQUIRED <= commands <= REQUIRED | OPTIONAL
    assert not commands & FORBIDDEN


def test_installing_the_recipe_twice_is_idempotent_and_keeps_the_password(owned_worker):
    """Reset of permissions preserves password and enabled state, @spec PROTECTED-HOOK-LANE-3."""
    before = granted(owned_worker.broker, owned_worker.username)
    owned_worker.broker.command(
        "ACL", "SETUSER", owned_worker.username, *wb.worker_acl().worker_acl_rules("worker")
    )
    assert granted(owned_worker.broker, owned_worker.username) == before
    client = owned_worker.raw()
    try:
        assert client.ping()
    finally:
        client.close()


# -- allowed authority ---------------------------------------------------------------


def test_the_worker_reads_control_evidence_and_bindings_and_the_live_identity(owned_worker):
    """Control, evidence and binding reads plus INFO server and TIME,
    @spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-6."""
    broker = owned_worker.broker
    broker.command("SET", wb.CONTROL, "control-bytes")
    broker.command("SET", "protected:control:readiness:example:1", "evidence-bytes")
    broker.command("SET", wb.BINDING, "binding-bytes")
    client = owned_worker.raw()
    try:
        assert client.get(wb.CONTROL) == b"control-bytes"
        assert client.get("protected:control:readiness:example:1") == b"evidence-bytes"
        assert client.get(wb.BINDING) == b"binding-bytes"
        assert client.get("protected:control:absent") is None
        assert client.info("server")["run_id"] == broker.command("INFO", "server")["run_id"]
        assert len(client.time()) == 2
        assert owned_worker.denials() == []
    finally:
        client.close()


def test_the_worker_writes_exactly_its_own_families(owned_worker):
    """Lane keys, the runs stream and the three stream-derived liveness families,
    @spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-7."""
    client = owned_worker.raw()
    try:
        for key in (
            wb.WORKER_PREFIX + ":lock:thread-example",
            wb.SANDBOX_PREFIX + ":route:thread-example",
            wb.DEAD_LETTER,
            "curie:runs:consumer-heartbeat:g:c",
            "curie:runs:consumer-heartbeat-capable:g:c",
            "curie:runs:consumer-reclaim-lock:g:c",
        ):
            assert client.set(key, "x") is True
            assert client.get(key) == b"x"
            assert client.delete(key) == 1
        entry = client.xadd(wb.STREAM, {"payload": "p"})
        client.xgroup_create(wb.STREAM, wb.GROUP, id="0")
        read = client.xreadgroup(wb.GROUP, wb.CONSUMER, {wb.STREAM: ">"}, count=1)
        assert read[0][1][0][0].decode() == entry.decode()
        assert client.xack(wb.STREAM, wb.GROUP, entry) == 1
        assert client.eval("return 1", 1, "") == 1
        assert owned_worker.denials() == []
    finally:
        client.close()


# -- refused authority ---------------------------------------------------------------


@pytest.mark.parametrize("key", NO_WRITE)
@pytest.mark.parametrize("writer", WRITERS, ids=lambda w: w[0])
def test_the_worker_refuses_control_evidence_source_admission_and_foreign_writes(
    owned_worker, key, writer
):
    """No write, including SET on any ``protected:*`` key outside the lane family and
    the immutable binding, leaves a trace, @spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-6."""
    broker = owned_worker.broker
    broker.command("SET", key, "original")
    client = owned_worker.raw()
    try:
        with pytest.raises(NoPermissionError):
            client.execute_command(writer[0], key, *writer[1:])
    finally:
        client.close()
    assert broker.command("GET", key) == b"original"
    assert broker.command("PTTL", key) == -1
    assert broker.command("TYPE", key) == b"string"


@pytest.mark.parametrize("key", NO_READ)
def test_the_worker_reads_nothing_outside_control_bindings_and_its_own_families(owned_worker, key):
    """Source, admission intent/state/quota and every other ``curie:*`` key are unreadable,
    @spec PROTECTED-HOOK-LANE-3."""
    owned_worker.broker.command("SET", key, "private")
    client = owned_worker.raw()
    try:
        with pytest.raises(NoPermissionError):
            client.get(key)
    finally:
        client.close()


@pytest.mark.parametrize(
    "command",
    [
        ("CONFIG", "GET", "*"),
        ("CONFIG", "SET", "maxmemory", "0"),
        ("CONFIG", "REWRITE"),
        ("ACL", "SETUSER", "intruder", "on", "+@all", "~*"),
        ("ACL", "DELUSER", "provisioner"),
        ("ACL", "LIST"),
        ("ACL", "LOG"),
        ("FLUSHDB",),
        ("FLUSHALL",),
        ("KEYS", "*"),
        ("SCRIPT", "FLUSH"),
        ("FUNCTION", "LIST"),
        ("MODULE", "LIST"),
        ("CLIENT", "KILL", "SKIPME", "yes"),
        ("CLIENT", "LIST"),
        ("PUBLISH", "curie:kill-events", "{}"),
        ("PUBSUB", "CHANNELS"),
        ("INFO",),
        ("INFO", "commandstats"),
        ("INFO", "clients"),
        ("MSET", "protected:lane:a", "1", "protected:lane:b", "2"),
        ("SPOP", "protected:lane:set"),
        ("RENAME", "protected:lane:a", "protected:lane:b"),
    ],
    ids=lambda c: "-".join(c[:2]),
)
def test_administration_channels_and_unmeasured_commands_are_refused(owned_worker, command):
    """CONFIG, ACL, flush, enumeration beyond SCAN, pub/sub and any command the real
    components were not measured to issue, @spec PROTECTED-HOOK-LANE-3."""
    broker = owned_worker.broker
    users_before = sorted(broker.command("ACL", "USERS"))
    client = owned_worker.raw()
    try:
        broker.command("SET", "protected:lane:a", "keep")
        with pytest.raises(NoPermissionError):
            client.execute_command(*command)
    finally:
        client.close()
    assert sorted(broker.command("ACL", "USERS")) == users_before
    assert broker.command("GET", "protected:lane:a") == b"keep"


def test_a_script_cannot_write_what_the_role_cannot_write(owned_worker):
    """The measured separation between declared-key authorization and the commands a
    script runs: neither declaring a protected key nor calling a command on one inside
    a script widens the role, and nothing is written, @spec PROTECTED-HOOK-LANE-3."""
    broker = owned_worker.broker
    broker.command("SET", wb.CONTROL, "original")
    broker.command("SET", wb.BINDING, "original")
    client = owned_worker.raw()
    attempts = [
        ("redis.call('SET', KEYS[1], 'x') return 1", [wb.CONTROL]),
        ("redis.call('DEL', KEYS[1]) return 1", [wb.BINDING]),
        ("return redis.call('GET', KEYS[1])", [wb.CONTROL]),
        ("redis.call('SET', '" + wb.CONTROL + "', 'x') return 1", []),
        ("redis.call('SET', '" + wb.CONTROL + "', 'x') return 1", ["protected:lane:declared"]),
        ("redis.call('CONFIG', 'SET', 'maxmemory', '0') return 1", []),
        ("redis.call('ACL', 'SETUSER', 'intruder', 'on') return 1", []),
        ("redis.call('FLUSHDB') return 1", []),
    ]
    try:
        for script, keys in attempts:
            with pytest.raises(ResponseError):
                client.eval(script, len(keys), *keys)
        assert (
            client.eval("return redis.call('GET', KEYS[1])", 1, "protected:lane:declared") is None
        )
    finally:
        client.close()
    assert broker.command("GET", wb.CONTROL) == b"original"
    assert broker.command("GET", wb.BINDING) == b"original"
    assert "intruder" not in [u.decode() for u in broker.command("ACL", "USERS")]


def test_scan_names_other_families_but_never_exposes_a_value(owned_worker):
    """Measured on the pinned Valkey: SCAN is not key-filtered, so a worker can list key
    names it cannot read (affinity pressure scans need SCAN). Values stay refused; this
    pins the measured exposure so a later change cannot widen it unnoticed,
    @spec PROTECTED-HOOK-LANE-3."""
    broker = owned_worker.broker
    broker.command("SET", "protected:admission:intent:digest-example", "private-bytes")
    broker.command("SET", wb.SANDBOX_PREFIX + ":route:thread-example", "route")
    client = owned_worker.raw()
    try:
        _, keys = client.scan(0, match=wb.SANDBOX_PREFIX + ":route:*", count=1000)
        assert [k.decode() for k in keys] == [wb.SANDBOX_PREFIX + ":route:thread-example"]
        _, keys = client.scan(0, count=1000)
        assert b"protected:admission:intent:digest-example" in keys
        with pytest.raises(NoPermissionError):
            client.get("protected:admission:intent:digest-example")
        with pytest.raises(NoPermissionError):
            client.type("protected:admission:intent:digest-example")
    finally:
        client.close()


@pytest.mark.parametrize("other", ["enqueue", "verifier", "source_writer", "control_reader"])
def test_no_other_role_reaches_the_worker_family(pworker_service, other):
    """Enqueue, verifier and the metadata roles cannot read, write or consume the lane
    family or the stream-derived liveness keys, @spec PROTECTED-HOOK-LANE-3."""
    pworker_service.command("FLUSHDB")
    rules = (
        admission_acl_rules(other)
        if other in ("enqueue", "verifier")
        else metadata_acl_rules(other)
    )
    principal = wb.WorkerPrincipal(pworker_service, role=other, rules=rules)
    principal.install()
    pworker_service.command("SET", wb.WORKER_PREFIX + ":lock:thread-example", "owned")
    client = principal.raw()
    try:
        for key in (
            wb.WORKER_PREFIX + ":lock:thread-example",
            "curie:runs:consumer-heartbeat:g:c",
            "curie:runs:consumer-reclaim-lock:g:c",
            wb.DEAD_LETTER,
        ):
            with pytest.raises(NoPermissionError):
                client.set(key, "x")
            with pytest.raises(NoPermissionError):
                client.get(key)
        with pytest.raises(NoPermissionError):
            client.xgroup_create(wb.STREAM, "other-group", id="0", mkstream=True)
    finally:
        client.close()
        principal.remove()
        pworker_service.command("FLUSHDB")


# -- the real worker components run unchanged under the role -------------------------


async def pending_entry(client, config):
    """One entry in this consumer's PEL, the standing a lease acquire verifies."""
    with contextlib.suppress(Exception):
        await client.xgroup_create(config.stream, config.consumer_group, id="0", mkstream=True)
    entry = await client.xadd(config.stream, {"payload": "p"})
    read = await client.xreadgroup(
        config.consumer_group, config.consumer_name, {config.stream: ">"}, count=1
    )
    assert [e for _s, entries in read for e, _f in entries] == [entry]
    return entry


def test_delivery_lease_scripts_run_including_release_without_a_resume_marker(owned_worker):
    """Acquire, heartbeat, release (empty second key and a real resume key) and settle,
    unchanged, @spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-7."""
    from curie_worker.delivery_lease import DeliveryLeaseStore, LeaseRefused

    async def go():
        client = owned_worker.araw(decode_responses=True)
        config = wb.lane_config(owned_worker.broker)
        store = DeliveryLeaseStore(client, config)
        try:
            entry = await pending_entry(client, config)
            lease = await store.acquire(
                config.stream, config.consumer_group, entry, consumer=config.consumer_name
            )
            with pytest.raises(LeaseRefused):
                await store.acquire(
                    config.stream, config.consumer_group, entry, consumer=config.consumer_name
                )
            budget = await store.heartbeat(
                config.stream,
                config.consumer_group,
                entry,
                consumer=config.consumer_name,
                owner=lease.owner,
                generation=lease.generation,
                resume_event_id=None,
            )
            assert budget is not None
            assert await store.is_live(config.stream, config.consumer_group, entry)
            assert await store.release(
                config.stream, config.consumer_group, entry, owner=lease.owner, resume_event_id=None
            )
            second = await store.acquire(
                config.stream, config.consumer_group, entry, consumer=config.consumer_name
            )
            assert await store.release(
                config.stream,
                config.consumer_group,
                entry,
                owner=second.owner,
                resume_event_id="event-example",
            )
            await store.settle(config.stream, config.consumer_group, entry)
            assert not await store.has_state(config.stream, config.consumer_group, entry)
        finally:
            await client.aclose()

    asyncio.run(go())
    assert owned_worker.denials() == []


def test_capacity_wait_markers_threadlock_and_liveness_run(owned_worker):
    """Capacity-wait park/snapshot, markers (side effect, done, terminal), thread lock
    acquire/renew/release and consumer liveness publish/renew, unchanged,
    @spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-7."""
    from curie_worker.capacity_wait import CapacityWaitStore
    from curie_worker.consumer_liveness import (
        ConsumerLivenessStore,
        consumer_heartbeat_capable_key,
        consumer_heartbeat_key,
        consumer_reclaim_lock_key,
    )
    from curie_worker.delivery_lease import DeliveryLeaseStore
    from curie_worker.markers import Markers
    from curie_worker.threadlock import LockAcquireTimeout, ThreadLock

    async def go():
        client = owned_worker.araw(decode_responses=True)
        config = wb.lane_config(owned_worker.broker)
        try:
            entry = await pending_entry(client, config)
            leases = DeliveryLeaseStore(client, config)
            lease = await leases.acquire(
                config.stream, config.consumer_group, entry, consumer=config.consumer_name
            )
            waits = CapacityWaitStore(client, config)
            record = await waits.park(entry, {"payload": "p"}, "event-example", lease)
            assert record.event_id == "event-example"
            assert (await waits.get("event-example")).state == record.state
            assert isinstance(await waits.snapshot(), dict)
            assert isinstance(await waits.wake_due(), int)

            markers = Markers(client, config)
            assert not await markers.saw_side_effect("event-marker")
            await markers.mark_side_effect("event-marker")
            assert await markers.saw_side_effect("event-marker")
            assert not await markers.is_terminal("event-marker")
            await markers.mark_done_without_completion("event-marker")
            assert await markers.is_terminal("event-marker")

            lock = ThreadLock(client, ttl_ms=2000, acquire_timeout_s=2.0, poll_interval_s=0.02)
            key = config.lock_key("thread-example")
            assert key.startswith(wb.WORKER_PREFIX + ":lock:")
            async with lock.hold(key) as held:
                await held.ensure_owned()
                with pytest.raises(LockAcquireTimeout):
                    await ThreadLock(
                        client, ttl_ms=2000, acquire_timeout_s=0.1, poll_interval_s=0.02
                    ).acquire(key)

            liveness = ConsumerLivenessStore(client)
            await liveness.publish(
                stream=config.stream,
                group=config.consumer_group,
                consumer=config.consumer_name,
                heartbeat_ttl_ms=2000,
                capability_ttl_ms=4000,
            )
            await liveness.renew(
                stream=config.stream,
                group=config.consumer_group,
                consumer=config.consumer_name,
                heartbeat_ttl_ms=2000,
                capability_ttl_ms=4000,
            )
            assert await liveness.is_alive(
                stream=config.stream, group=config.consumer_group, consumer=config.consumer_name
            )
            assert (
                await client.exists(
                    consumer_heartbeat_key(
                        config.stream, config.consumer_group, config.consumer_name
                    ),
                    consumer_heartbeat_capable_key(
                        config.stream, config.consumer_group, config.consumer_name
                    ),
                )
                == 2
            )
            assert await client.set(
                consumer_reclaim_lock_key(
                    config.stream, config.consumer_group, config.consumer_name
                ),
                "1",
                nx=True,
                px=1000,
            )
        finally:
            await client.aclose()

    asyncio.run(go())
    assert owned_worker.denials() == []


def test_affinity_store_routes_over_the_sync_and_pressure_connections(owned_worker):
    """Route claim, CAS, touch, guarded delete, claim credentials, route inventory on the
    sync connection and the bounded pressure scan/detach on the async one, unchanged,
    @spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-7."""
    import time as _time

    from curie_worker.sandbox import AffinityStore
    from curie_worker.sandbox.types import RouteRecord, SandboxHandle

    def handle(thread_key, claim):
        return SandboxHandle(
            thread_key=thread_key,
            claim_name=claim,
            sandbox_name="sandbox-example",
            namespace="namespace-example",
            service_fqdn="sandbox.example.invalid",
            port=8080,
            session_id="session-example",
        )

    async def go(store, lock_client):
        thread_key = "thread-example"
        record = RouteRecord(handle(thread_key, "claim-a"))
        assert store.put_if_absent(thread_key, record, 60) is True
        assert (
            store.put_if_absent(thread_key, RouteRecord(handle(thread_key, "claim-b")), 60) is False
        )
        assert store.get(thread_key).handle.claim_name == "claim-a"
        assert store.touch(thread_key, 60) is True
        assert store.touch_if_live_claim(thread_key, "claim-a", 60) is True
        assert store.touch_if_live_claim(thread_key, "claim-b", 60) is False
        assert store.live_claim_names() == {"claim-a"}
        assert (
            store.replace_if_generation(
                thread_key,
                expected_claim="claim-a",
                expected_generation=0,
                record=RouteRecord(handle(thread_key, "claim-a")),
                ttl_seconds=60,
            )
            is True
        )
        store.remember_claim_credential("claim-a", "agent-example", "credential-example", ttl_s=60)
        assert store.claim_credential("claim-a") == ("agent-example", "credential-example")
        assert store.iter_claim_credentials() == [
            ("claim-a", "agent-example", "credential-example")
        ]
        store.forget_claim_credential("claim-a")
        assert (await store.pressure_get(thread_key)).handle.claim_name == "claim-a"
        scan = await store.pressure_candidates(
            max_pages=8, max_records=256, deadline=_time.monotonic() + 5
        )
        assert scan.outcome == "complete" and [c.thread_key for c in scan.candidates] == [
            thread_key
        ]
        token = uuid.uuid4().hex
        lock_key = wb.WORKER_PREFIX + ":lock:" + thread_key
        await lock_client.set(lock_key, token)
        assert (
            await store.detach_if_unchanged(
                thread_key,
                expected_claim="claim-a",
                expected_generation=0,
                expected_expires_at_ms=scan.candidates[0].expires_at_ms,
                lock_key=lock_key,
                lock_token=token,
            )
            is True
        )
        assert store.get(thread_key) is None
        assert store.delete_if_claim(thread_key, "claim-a") is False

    sync = owned_worker.raw(decode_responses=True)
    pressure = owned_worker.araw(decode_responses=True)

    async def run():
        try:
            await go(
                AffinityStore(sync, pressure_client=pressure, key_prefix=wb.SANDBOX_PREFIX),
                pressure,
            )
        finally:
            await pressure.aclose()

    try:
        asyncio.run(run())
    finally:
        sync.close()
    assert owned_worker.denials() == []
    assert owned_worker.broker.command("KEYS", wb.SANDBOX_PREFIX + ":*") == []
