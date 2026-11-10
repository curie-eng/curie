"""The protected worker's pinned authenticated broker transport,
@spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-6 PROTECTED-HOOK-LANE-7.

``curie_protected_hooks.worker_transport`` realises the worker's half of the LANE-2/3
identity. ``AuthenticatedWorkerClient`` is the sibling of the metadata reader, source
writer and enqueue clients: the same input validation, TLS, CA, hostname, SPKI pin,
HELLO AUTH by name, two second timeouts, disabled retries, redaction, and the live
``INFO server`` run_id verified on every connection it opens. It never reconnects: a
lost connection leaves the whole client permanently unusable (fail closed), so a worker
that lost its private broker session restarts through a fresh manifest check.

Expected surface (see ``.projects/plans/s1.tests.md``):

    WorkerCredential(username, password)                      frozen, slotted, redacted
    await AuthenticatedWorkerClient.connect(manifest, credential, ca_pem)
        -> client holding ONE async connection (runs/locks/markers/leases, a pool that
           grows only before any loss, each connection pinned and run_id verified) and
           ONE sync connection (affinity), both opened by ``connect``
    await client.read_control(key)  -> bytes | None     control and evidence GET
    await client.read_binding(event_id) -> bytes | None envelope binding GET
    await client.observe()          -> BrokerObservation live run_id and broker TIME
    client.lane()                   -> LaneConnections(runs, affinity): the only way to
                                       obtain the unchanged-component handles
    await client.close()
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import socket
import time
import uuid

import pytest
from curie_protected_hooks.broker_metadata import BrokerMetadataUnavailable, BrokerObservation
from redis import Redis
from redis import asyncio as aioredis
from redis.connection import Connection
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import NoPermissionError

from . import worker_broker as wb

pworker_service = wb.pworker_service
owned_worker = wb.owned_worker
FixtureSecret = wb.FixtureSecret
connections = wb.connections
command_count = wb.command_count


def types():
    """Missing product names are assertions in the test body, @spec PROTECTED-HOOK-LANE-3."""
    module = wb.worker_transport()
    assert hasattr(module, "WorkerCredential"), "worker credential absent"
    assert hasattr(module, "AuthenticatedWorkerClient"), "authenticated worker client absent"
    return module.WorkerCredential, module.AuthenticatedWorkerClient


def assert_safe(error, owned):
    """Exact safe error, no credential, endpoint or certificate, @spec PROTECTED-HOOK-LANE-3."""
    assert type(error) is BrokerMetadataUnavailable
    assert str(error) == "Broker metadata unavailable"
    assert error.__cause__ is None
    assert error.__context__ is None or error.__suppress_context__
    broker = owned.broker
    for value in (
        owned.username,
        owned.password,
        broker.ca_pem.splitlines()[1],
        "127.0.0.1",
        str(broker.port),
        broker.pin,
    ):
        assert value not in repr(error) and value not in str(error)


def args(owned, manifest=None, ca=None, password=None):
    """The three public connect inputs, @spec PROTECTED-HOOK-LANE-3."""
    return (
        manifest or owned.broker.manifest(),
        owned.credential(password),
        ca or owned.broker.ca_pem,
    )


def run(coroutine):
    """One event loop per case, as the worker suite does, @spec PROTECTED-HOOK-LANE-3."""
    return asyncio.run(coroutine)


async def connected(owned, **kwargs):
    """Only the public factory, @spec PROTECTED-HOOK-LANE-3."""
    _, client_type = types()
    return await client_type.connect(*args(owned, **kwargs))


async def wait_for_no_sessions(owned):
    """@spec PROTECTED-HOOK-LANE-3."""
    deadline = time.monotonic() + 2
    while owned.sessions():
        assert time.monotonic() < deadline, "a refused or closed worker session stayed open"
        await asyncio.sleep(0.02)


# -- credential and closed surface ---------------------------------------------------


def test_worker_credential_is_frozen_slotted_and_redacted():
    """A sibling of the other roles' credentials, @spec PROTECTED-HOOK-LANE-3."""
    credential_type, _ = types()
    module = wb.worker_transport()
    credential = credential_type("unique-worker", "unique-password")
    assert (credential.username, credential.password) == ("unique-worker", "unique-password")
    for text in (repr(credential), str(credential)):
        assert "unique-worker" not in text and "unique-password" not in text
    assert not hasattr(credential, "__dict__")
    with pytest.raises((AttributeError, TypeError)):
        credential.password = "changed"
    broker_module = __import__(
        "curie_protected_hooks.broker_transport", fromlist=["EnqueueCredential"]
    )
    for other in (
        broker_module.MetadataReaderCredential,
        broker_module.SourceWriterCredential,
        broker_module.EnqueueCredential,
    ):
        assert credential_type is not other
    assert module.WorkerCredential is credential_type


@pytest.mark.parametrize(
    "username,password",
    [("", "x"), ("default", "x"), (None, "x"), (7, "x"), ("worker", ""), ("worker", None)],
)
def test_invalid_worker_credentials_are_safe(username, password):
    """@spec PROTECTED-HOOK-LANE-3."""
    credential_type, _ = types()
    with pytest.raises(BrokerMetadataUnavailable):
        credential_type(username, password)


ALLOWED_PUBLIC = {"connect", "lane", "read_control", "read_binding", "observe", "close"}


def test_closed_surface_exports_only_control_binding_observe_and_the_lane_handles():
    """Control and binding reads and observe; the raw handles only through ``lane()``,
    never a generic command, eval or execute, @spec PROTECTED-HOOK-LANE-3
    PROTECTED-HOOK-LANE-7."""
    _, client_type = types()
    public = {name for name in dir(client_type) if not name.startswith("_")}
    assert public == ALLOWED_PUBLIC
    assert list(inspect.signature(client_type.connect).parameters)[:3] == [
        "manifest",
        "credential",
        "ca_pem",
    ]
    assert inspect.iscoroutinefunction(client_type.connect)
    for name in ("read_control", "read_binding", "observe", "close"):
        assert inspect.iscoroutinefunction(getattr(client_type, name)), name
    assert "__slots__" in vars(client_type)
    with pytest.raises((TypeError, BrokerMetadataUnavailable)):
        client_type()


def test_a_connected_client_holds_nothing_public_that_is_a_raw_client(owned_worker):
    """No exported attribute of a live instance is a Redis client or connection, and the
    handles live behind ``lane()`` as exactly two named, typed members,
    @spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-7."""

    async def go():
        client = await connected(owned_worker)
        try:
            for name in dir(client):
                if name.startswith("_"):
                    continue
                value = getattr(client, name)
                assert not isinstance(value, (Redis, aioredis.Redis, Connection)), name
                assert callable(value), name
            assert not hasattr(client, "__dict__")
            text = repr(client) + str(client)
            for secret in (
                owned_worker.username,
                owned_worker.password,
                "127.0.0.1",
                str(owned_worker.broker.port),
            ):
                assert secret not in text
            lane = client.lane()
            assert {n for n in dir(lane) if not n.startswith("_")} == {"runs", "affinity"}
            assert isinstance(lane.runs, aioredis.Redis)
            assert isinstance(lane.affinity, Redis)
            assert lane.runs.get_encoder().decode_responses is True
            assert lane.affinity.get_encoder().decode_responses is True
            for representation in (repr(lane), repr(lane.runs), repr(lane.affinity)):
                for secret in (
                    owned_worker.username,
                    owned_worker.password,
                    "127.0.0.1",
                    str(owned_worker.broker.port),
                    owned_worker.broker.ca_pem.splitlines()[1],
                ):
                    assert secret not in representation
        finally:
            await client.close()

    run(go())


# -- connection identity -------------------------------------------------------------


def test_connect_opens_one_async_and_one_sync_session_and_reads_on_them(owned_worker):
    """Control, binding and observation share the retained async session; the affinity
    connection is the second; each authenticated with HELLO by name and run_id verified,
    no denied command ever sent, @spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-6."""
    broker = owned_worker.broker
    broker.command("SET", wb.CONTROL, "control-bytes")
    broker.command("SET", wb.BINDING, b'{"schema_version":1}')

    async def go():
        hellos, autos, infos = (command_count(broker, n) for n in ("hello", "auth", "info"))
        accepted = connections(broker)
        client = await connected(owned_worker)
        try:
            opened = connections(broker) - accepted
            assert opened == 2
            assert command_count(broker, "hello") - hellos == opened
            assert command_count(broker, "auth") == autos
            assert command_count(broker, "info") - infos >= opened
            sessions = owned_worker.sessions()
            assert len(sessions) == 2
            assert await client.read_control(wb.CONTROL) == b"control-bytes"
            assert await client.read_control("protected:control:absent") is None
            assert await client.read_binding(wb.BINDING_EVENT) == b'{"schema_version":1}'
            assert await client.read_binding("hook-absent-event") is None
            before = broker.command("TIME")
            observation = await client.observe()
            after = broker.command("TIME")
            assert isinstance(observation, BrokerObservation)
            assert observation.run_id == broker.command("INFO", "server")["run_id"]
            assert int(before[0]) * 1000 + int(before[1]) // 1000 <= observation.now_ms
            assert observation.now_ms <= int(after[0]) * 1000 + int(after[1]) // 1000
            assert {s["id"] for s in owned_worker.sessions()} == {s["id"] for s in sessions}
            assert owned_worker.denials() == []
            assert broker.command("GET", wb.CONTROL) == b"control-bytes"
            assert broker.command("PTTL", wb.BINDING) == -1
        finally:
            await client.close()

    run(go())


@pytest.mark.parametrize("case", ["ca", "hostname", "pin"])
def test_tls_refusals_precede_authentication(owned_worker, case):
    """Chain, name and SPKI pin are checked before HELLO AUTH on both connections,
    @spec PROTECTED-HOOK-LANE-3."""
    broker = owned_worker.broker
    manifest, ca = broker.manifest(), broker.ca_pem
    if case == "ca":
        ca = wb.wrong_ca()
    elif case == "hostname":
        manifest = broker.manifest(
            endpoint={"host": "localhost", "port": broker.port}, tls_server_name="localhost"
        )
    else:
        manifest = broker.manifest(tls_spki_sha256="0" * 64)
    bad = FixtureSecret("wrong-fixture-password")

    async def go():
        _, client_type = types()
        hellos = command_count(broker, "hello")
        with pytest.raises(BrokerMetadataUnavailable) as caught:
            await client_type.connect(*args(owned_worker, manifest, ca, bad))
        assert_safe(caught.value, owned_worker)
        assert broker.command("ACL", "LOG") == []
        assert command_count(broker, "hello") == hellos
        with pytest.raises(BrokerMetadataUnavailable):
            await connected(owned_worker, password=bad)
        entries = broker.command("ACL", "LOG")
        assert entries and all(entry[b"reason"] == b"auth" for entry in entries)
        await wait_for_no_sessions(owned_worker)

    run(go())


def test_named_authentication_never_falls_back_to_default(owned_worker):
    """A default-only password cannot authenticate the worker principal,
    @spec PROTECTED-HOOK-LANE-3."""
    broker = owned_worker.broker
    default_secret = FixtureSecret(uuid.uuid4().hex)
    broker.command("ACL", "SETUSER", "default", "on", ">" + default_secret, "~*", "+@all")

    async def go():
        before = {name: command_count(broker, name) for name in ("hello", "auth")}
        with pytest.raises(BrokerMetadataUnavailable) as caught:
            await connected(owned_worker, password=default_secret)
        assert command_count(broker, "auth") - before["auth"] == 0
        assert command_count(broker, "hello") - before["hello"] >= 1
        assert_safe(caught.value, owned_worker)
        entries = broker.command("ACL", "LOG")
        assert entries and all(
            entry[b"username"] == owned_worker.username.encode() for entry in entries
        )
        assert owned_worker.sessions() == []

    try:
        run(go())
    finally:
        broker.command("ACL", "SETUSER", "default", "reset", "off")


def test_a_wrong_run_id_refuses_before_any_application_command(owned_worker):
    """TLS and authentication cannot stand in for the live broker epoch; a refused
    connect leaves no session open, @spec PROTECTED-HOOK-LANE-2 PROTECTED-HOOK-LANE-3."""
    broker = owned_worker.broker
    broker.command("ACL", "SETUSER", owned_worker.username, "-get", "-eval", "-time", "-xadd")

    async def go():
        _, client_type = types()
        with pytest.raises(BrokerMetadataUnavailable) as caught:
            await client_type.connect(*args(owned_worker, broker.manifest(run_id="0" * 40)))
        assert_safe(caught.value, owned_worker)
        assert broker.command("ACL", "LOG") == []
        await wait_for_no_sessions(owned_worker)

    run(go())


def test_a_restarted_broker_is_refused_by_the_old_manifest_and_a_live_client_dies(owned_worker):
    """A replacement epoch is never silently adopted: the live client loses its sessions
    and cannot reopen them, and the old manifest's run_id no longer connects,
    @spec PROTECTED-HOOK-LANE-2 PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-6."""
    broker = owned_worker.broker
    old = broker.manifest()

    async def first():
        client = await connected(owned_worker)
        broker.restart()
        owned_worker.install()
        with pytest.raises(BrokerMetadataUnavailable):
            await client.observe()
        accepted = connections(broker)
        with pytest.raises(RedisConnectionError):
            await client.lane().runs.ping()
        with pytest.raises(RedisConnectionError):
            client.lane().affinity.ping()
        assert connections(broker) == accepted
        await client.close()

    async def second():
        _, client_type = types()
        with pytest.raises(BrokerMetadataUnavailable) as caught:
            await client_type.connect(*args(owned_worker, old))
        assert_safe(caught.value, owned_worker)
        await wait_for_no_sessions(owned_worker)
        fresh = await connected(owned_worker)
        try:
            assert (await fresh.observe()).run_id == broker.command("INFO", "server")["run_id"]
        finally:
            await fresh.close()

    run(first())
    run(second())


# -- no reconnect --------------------------------------------------------------------


def test_a_killed_connection_is_never_reopened_and_disables_the_whole_client(owned_worker):
    """Loss of the async session is permanent for both sessions and every method; no
    second connection is accepted, @spec PROTECTED-HOOK-LANE-3."""
    broker = owned_worker.broker
    broker.command("SET", wb.CONTROL, "control-bytes")

    async def go():
        client = await connected(owned_worker)
        lane = client.lane()
        try:
            assert await lane.runs.ping()
            assert lane.affinity.ping()
            assert (
                broker.command("CLIENT", "KILL", "USER", owned_worker.username, "SKIPME", "yes")
                == 2
            )
            accepted = connections(broker)
            for _ in range(3):
                for operation in (
                    lambda: client.read_control(wb.CONTROL),
                    lambda: client.read_binding(wb.BINDING_EVENT),
                    client.observe,
                ):
                    with pytest.raises(BrokerMetadataUnavailable) as caught:
                        await operation()
                    assert_safe(caught.value, owned_worker)
                with pytest.raises(RedisConnectionError):
                    await lane.runs.get(wb.WORKER_PREFIX + ":lock:x")
                with pytest.raises(RedisConnectionError):
                    lane.affinity.get(wb.SANDBOX_PREFIX + ":route:x")
            assert connections(broker) == accepted, "the worker client opened a second connection"
            assert owned_worker.sessions() == []
        finally:
            await client.close()

    run(go())


def test_losing_only_the_sync_session_also_disables_the_async_one(owned_worker):
    """Either session's loss is the client's loss (fail closed), @spec PROTECTED-HOOK-LANE-3."""
    broker = owned_worker.broker

    async def go():
        client = await connected(owned_worker)
        lane = client.lane()
        try:
            sessions = owned_worker.sessions()
            assert len(sessions) == 2
            # The sync connection is the one the first sync command named last.
            lane.affinity.client_setname("pworker-affinity")
            named = [s for s in owned_worker.sessions() if s.get("name") == "pworker-affinity"]
            assert len(named) == 1
            broker.command("CLIENT", "KILL", "ID", named[0]["id"])
            with pytest.raises(RedisConnectionError):
                lane.affinity.ping()
            accepted = connections(broker)
            with pytest.raises(BrokerMetadataUnavailable):
                await client.observe()
            with pytest.raises(RedisConnectionError):
                await lane.runs.ping()
            assert connections(broker) == accepted
        finally:
            await client.close()

    run(go())


def test_the_async_pool_grows_only_with_verified_pinned_connections(owned_worker):
    """Concurrent blocking reads need more than one connection; each is pinned,
    HELLO-authenticated and run_id verified before first use, @spec PROTECTED-HOOK-LANE-3
    PROTECTED-HOOK-LANE-7."""
    broker = owned_worker.broker

    async def go():
        client = await connected(owned_worker)
        runs = client.lane().runs
        try:
            await runs.xgroup_create(wb.STREAM, wb.GROUP, id="0", mkstream=True)
            hellos, infos = command_count(broker, "hello"), command_count(broker, "info")
            accepted = connections(broker)
            replies = await asyncio.gather(
                *[
                    runs.xreadgroup(wb.GROUP, f"c{n}", {wb.STREAM: ">"}, count=1, block=300)
                    for n in range(4)
                ]
            )
            assert all(not reply for reply in replies)
            grown = connections(broker) - accepted
            assert grown >= 3, "blocking reads cannot share one connection"
            assert command_count(broker, "hello") - hellos == grown
            assert command_count(broker, "info") - infos >= grown
            assert owned_worker.denials() == []
            assert len(owned_worker.sessions()) == 2 + grown
        finally:
            await client.close()

    run(go())


def test_close_is_terminal(owned_worker):
    """@spec PROTECTED-HOOK-LANE-3."""
    broker = owned_worker.broker

    async def go():
        client = await connected(owned_worker)
        lane = client.lane()
        await client.close()
        await client.close()
        accepted = connections(broker)
        for operation in (
            lambda: client.read_control(wb.CONTROL),
            lambda: client.read_binding(wb.BINDING_EVENT),
            client.observe,
        ):
            with pytest.raises(BrokerMetadataUnavailable):
                await operation()
        with pytest.raises(RedisConnectionError):
            await lane.runs.ping()
        with pytest.raises(RedisConnectionError):
            lane.affinity.ping()
        assert connections(broker) == accepted
        await wait_for_no_sessions(owned_worker)

    run(go())


def test_a_stalled_broker_is_bounded_by_the_connect_timeouts(owned_worker):
    """A listener never completing TLS cannot hold ``connect``, @spec PROTECTED-HOOK-LANE-3."""
    broker = owned_worker.broker

    async def go():
        with socket.socket() as stalled:
            stalled.bind(("127.0.0.1", 0))
            stalled.listen(4)
            manifest = broker.manifest(
                endpoint={"host": "127.0.0.1", "port": stalled.getsockname()[1]}
            )
            started = time.monotonic()
            with pytest.raises(BrokerMetadataUnavailable) as caught:
                await connected(owned_worker, manifest=manifest)
            elapsed = time.monotonic() - started
        assert_safe(caught.value, owned_worker)
        assert elapsed < 5, f"the worker client outlived its connect timeout: {elapsed:.2f}s"

    run(go())


@pytest.mark.parametrize(
    "kind", ["manifest", "reader-credential", "enqueue-credential", "empty-ca", "text-ca", "tuple"]
)
def test_invalid_inputs_precede_network(owned_worker, monkeypatch, kind):
    """@spec PROTECTED-HOOK-LANE-3."""
    broker = owned_worker.broker
    module = __import__(
        "curie_protected_hooks.broker_transport",
        fromlist=["MetadataReaderCredential", "EnqueueCredential"],
    )
    manifest, credential, ca = broker.manifest(), owned_worker.credential(), broker.ca_pem
    if kind == "manifest":
        manifest = manifest.as_dict()
    elif kind == "reader-credential":
        credential = module.MetadataReaderCredential(owned_worker.username, owned_worker.password)
    elif kind == "enqueue-credential":
        credential = module.EnqueueCredential(owned_worker.username, owned_worker.password)
    elif kind == "empty-ca":
        ca = ""
    elif kind == "text-ca":
        ca = FixtureSecret(broker.ca_pem + "trailing text")
    else:
        manifest = broker.manifest(tls_server_name="localhost")
    attempts = []

    def forbidden(*a, **k):
        """Network tripwire, @spec PROTECTED-HOOK-LANE-3."""
        attempts.append(True)
        raise AssertionError("invalid input attempted network I/O")

    _, client_type = types()
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)

    async def go():
        with pytest.raises(BrokerMetadataUnavailable) as caught:
            await client_type.connect(manifest, credential, ca)
        assert_safe(caught.value, owned_worker)

    run(go())
    assert attempts == []


def test_invalid_coordinates_refuse_before_any_command(owned_worker):
    """Malformed keys and event ids send nothing, @spec PROTECTED-HOOK-LANE-3."""
    broker = owned_worker.broker

    async def go():
        client = await connected(owned_worker)
        broker.command("ACL", "SETUSER", owned_worker.username, "-get")
        try:
            before = command_count(broker, "get")
            for key in (
                "",
                "protected:control:",
                "protected:control:with space",
                "protected:control:x\n",
                "protected:source:a:b",
                "protected:admission:binding:event",
                "curie:runs",
                wb.WORKER_PREFIX + ":lock:x",
                "protected:control:" + "a" * 257,
                None,
                7,
                b"protected:control:x",
            ):
                with pytest.raises(BrokerMetadataUnavailable) as caught:
                    await client.read_control(key)
                assert_safe(caught.value, owned_worker)
            for event in ("", " leading", "-dash", "a" * 257, "with space", "x\n", None, 7, b"x"):
                with pytest.raises(BrokerMetadataUnavailable) as caught:
                    await client.read_binding(event)
                assert_safe(caught.value, owned_worker)
            assert command_count(broker, "get") == before
            assert broker.command("ACL", "LOG") == []
        finally:
            await client.close()

    run(go())


def test_an_oversized_binding_is_refused(owned_worker):
    """The binding read is bounded like the enqueue client's, @spec PROTECTED-HOOK-LANE-3
    PROTECTED-HOOK-LANE-6."""
    broker = owned_worker.broker
    broker.command("SET", wb.BINDING, b"x" * 16385)

    async def go():
        client = await connected(owned_worker)
        try:
            with pytest.raises(BrokerMetadataUnavailable) as caught:
                await client.read_binding(wb.BINDING_EVENT)
            assert_safe(caught.value, owned_worker)
        finally:
            await client.close()

    run(go())


# -- the lane handles ----------------------------------------------------------------


def test_a_denied_command_is_an_error_reply_not_a_lost_session(owned_worker):
    """The role's refusals surface as ordinary Redis errors on the handles and do not
    poison the client, @spec PROTECTED-HOOK-LANE-3."""
    broker = owned_worker.broker
    broker.command("SET", wb.CONTROL, "original")

    async def go():
        client = await connected(owned_worker)
        lane = client.lane()
        try:
            with pytest.raises(NoPermissionError):
                await lane.runs.set(wb.CONTROL, "tampered")
            with pytest.raises(NoPermissionError):
                lane.affinity.set(wb.BINDING, "tampered")
            with pytest.raises(NoPermissionError):
                await lane.runs.execute_command("CONFIG", "GET", "*")
            assert await lane.runs.set(wb.WORKER_PREFIX + ":lock:x", "owner")
            assert lane.affinity.get(wb.WORKER_PREFIX + ":lock:x") == "owner"
            assert await client.read_control(wb.CONTROL) == b"original"
            assert len(owned_worker.sessions()) == 2
        finally:
            await client.close()

    run(go())
    assert broker.command("GET", wb.CONTROL) == b"original"


def test_the_unchanged_components_run_over_the_pinned_handles(owned_worker):
    """The real lease store over the async handle and the real affinity store over the
    sync handle plus the async pressure handle, with only the pinned sessions,
    @spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-7."""
    from curie_worker.delivery_lease import DeliveryLeaseStore
    from curie_worker.markers import Markers
    from curie_worker.sandbox import AffinityStore
    from curie_worker.sandbox.types import RouteRecord, SandboxHandle

    broker = owned_worker.broker

    async def go():
        client = await connected(owned_worker)
        lane = client.lane()
        config = wb.lane_config(broker)
        try:
            await lane.runs.xgroup_create(
                config.stream, config.consumer_group, id="0", mkstream=True
            )
            entry = await lane.runs.xadd(config.stream, {"payload": "p"})
            read = await lane.runs.xreadgroup(
                config.consumer_group, config.consumer_name, {config.stream: ">"}, count=1
            )
            assert [e for _s, entries in read for e, _f in entries] == [entry]
            leases = DeliveryLeaseStore(lane.runs, config)
            lease = await leases.acquire(
                config.stream, config.consumer_group, entry, consumer=config.consumer_name
            )
            assert await leases.release(
                config.stream, config.consumer_group, entry, owner=lease.owner, resume_event_id=None
            )
            await Markers(lane.runs, config).mark_done_without_completion("event-example")
            assert await Markers(lane.runs, config).is_terminal("event-example")
            store = AffinityStore(
                lane.affinity, pressure_client=lane.runs, key_prefix=wb.SANDBOX_PREFIX
            )
            handle = SandboxHandle(
                thread_key="thread-example",
                claim_name="claim-a",
                sandbox_name="sandbox-example",
                namespace="namespace-example",
                service_fqdn="sandbox.example.invalid",
                port=8080,
                session_id="session-example",
            )
            assert store.put_if_absent("thread-example", RouteRecord(handle), 60)
            assert (await store.pressure_get("thread-example")).handle.claim_name == "claim-a"
            assert store.delete_if_claim("thread-example", "claim-a")
            assert owned_worker.denials() == []
        finally:
            with contextlib.suppress(Exception):
                await client.close()

    run(go())


def test_metadata_reads_preserve_non_utf8_bytes(owned_worker):
    """Metadata identity is byte-exact despite decoded lane handles,
    @spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-6."""
    blob = b"\xff\x00\x80\r\n"
    owned_worker.broker.command("SET", wb.CONTROL, blob)
    owned_worker.broker.command("SET", wb.BINDING, blob)

    async def go():
        client = await connected(owned_worker)
        try:
            assert await client.read_control(wb.CONTROL) == blob
            assert await client.read_binding(wb.BINDING_EVENT) == blob
            assert await client.lane().runs.ping()
        finally:
            await client.close()

    run(go())


def test_cancelled_inflight_read_closes_all_sessions_without_swallowing_cancellation(owned_worker):
    """A cancelled five-second blocking read exceeds the two-second socket deadline;
    actual timeout closes all sessions without reconnect, @spec PROTECTED-HOOK-LANE-3."""
    broker = owned_worker.broker

    async def go():
        client = await connected(owned_worker)
        lane = client.lane()
        try:
            await lane.runs.xgroup_create(wb.STREAM, wb.GROUP, id="0", mkstream=True)
            read = asyncio.create_task(
                lane.runs.xreadgroup(wb.GROUP, "cancelled-reader", {wb.STREAM: ">"}, block=5000)
            )
            await asyncio.sleep(0.03)
            read.cancel()
            with pytest.raises(asyncio.CancelledError):
                await read
            accepted = connections(broker)
            with pytest.raises(RedisConnectionError):
                await lane.runs.ping()
            with pytest.raises(RedisConnectionError):
                lane.affinity.ping()
            assert connections(broker) == accepted
            await wait_for_no_sessions(owned_worker)
        finally:
            await client.close()

    run(go())


def test_cancelling_a_short_read_drains_reply_and_preserves_session_alignment(owned_worker):
    """Graceful consumer shutdown is cancellation, not loss, when the pending reply
    drains within the socket deadline, @spec PROTECTED-HOOK-LANE-3."""
    broker = owned_worker.broker

    async def go():
        client = await connected(owned_worker)
        lane = client.lane()
        try:
            await lane.runs.xgroup_create(wb.STREAM, wb.GROUP, id="0", mkstream=True)
            accepted = connections(broker)
            read = asyncio.create_task(
                lane.runs.xreadgroup(wb.GROUP, "stopping-reader", {wb.STREAM: ">"}, block=100)
            )
            await asyncio.sleep(0.03)
            read.cancel()
            with pytest.raises(asyncio.CancelledError):
                await read
            assert await lane.runs.set(wb.WORKER_PREFIX + ":aligned", "exact-answer")
            assert await lane.runs.get(wb.WORKER_PREFIX + ":aligned") == "exact-answer"
            assert lane.affinity.ping()
            assert (await client.observe()).run_id == broker.command("INFO", "server")["run_id"]
            assert connections(broker) == accepted
            assert len(owned_worker.sessions()) == 2
        finally:
            await client.close()

    run(go())


def test_sync_eof_disables_async_operations_before_any_sync_command(owned_worker):
    """The async lane passively observes affinity FIN before pool growth,
    @spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-7."""
    broker = owned_worker.broker

    async def go():
        client = await connected(owned_worker)
        lane = client.lane()
        try:
            lane.affinity.client_setname("pworker-affinity-fin")
            session = next(
                s for s in owned_worker.sessions() if s.get("name") == "pworker-affinity-fin"
            )
            broker.command("CLIENT", "KILL", "ID", session["id"])
            await asyncio.sleep(0.03)
            accepted = connections(broker)
            with pytest.raises(BrokerMetadataUnavailable):
                await client.observe()
            with pytest.raises(RedisConnectionError):
                await asyncio.gather(*[lane.runs.ping() for _ in range(4)])
            assert connections(broker) == accepted
            await wait_for_no_sessions(owned_worker)
        finally:
            await client.close()

    run(go())


def test_passive_sync_probe_does_not_race_a_thread_owned_reply_parser(owned_worker):
    """Passive EOF checks skip an in-use affinity session rather than sharing its
    parser with concurrent async commands, @spec PROTECTED-HOOK-LANE-3."""
    broker = owned_worker.broker

    async def go():
        client = await connected(owned_worker)
        lane = client.lane()
        accepted = connections(broker)
        try:

            def affinity_reads():
                for _ in range(100):
                    assert lane.affinity.ping()

            async def run_reads():
                for _ in range(100):
                    assert await lane.runs.ping()

            await asyncio.gather(asyncio.to_thread(affinity_reads), run_reads())
            assert (await client.observe()).run_id == broker.command("INFO", "server")["run_id"]
            assert connections(broker) == accepted
            assert owned_worker.denials() == []
        finally:
            await client.close()

    run(go())
