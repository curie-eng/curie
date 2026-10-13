"""Admission owner retained refund on pinned TLS broker, @spec PROTECTED-HOOK-LANE-6."""

import hashlib
import json

import pytest
from redis.exceptions import NoPermissionError, ResponseError

from . import admission_broker as helpers
from .admission_broker import canonical, client, facade, module, queued_payload, request, seed
from .test_settlement_records import context_value

admission_broker = helpers.admission_broker
admission_service = helpers.admission_service
PREFIX = "protected:admission:"


def setup(b):
    p = module("atomic_admission")
    records = module("admission_records")
    namespace = module("execution_namespace").derive_lane_namespace(b.manifest())
    b.command(
        "ACL",
        "SETUSER",
        "enqueue",
        *module("admission_acl").admission_acl_rules("enqueue", trusted_manifest=b.manifest()),
    )
    seed(b)
    a = facade(b, p, limit=1)
    req = request(records)
    result = a.admit(req)
    assert result.status == "accepted"
    digest = records.delivery_digest(req.identity)
    return a, req, digest, namespace


def evidence(b, digest, ns):
    i = json.loads(b.command("GET", PREFIX + "intent:" + digest))
    m = b.manifest().as_dict()
    value = context_value()
    value.update(
        event_id=i["event_id"],
        intent_digest=digest,
        manifest_digest=i["manifest_digest"],
        runtime_id=i["runtime_id"],
        runtime_generation=i["runtime_generation"],
        broker_run_id=m["broker_identity"]["run_id"],
        envelope_sha256=i["envelope_sha256"],
        payload_sha256=i["payload_sha256"],
        committed_stream_id=i["reserved_stream_id"],
    )
    context = canonical(value)
    witness = {k: v for k, v in value.items() if k != "completion_record_json"}
    witness.update(
        delivery_stream=ns.stream,
        delivery_group=ns.group,
        delivery_entry_id=i["reserved_stream_id"],
        delivery_owner="owner/example",
        delivery_generation="1",
        completion_record_sha256=hashlib.sha256(
            value["completion_record_json"].encode()
        ).hexdigest(),
        completion_context_sha256=hashlib.sha256(context).hexdigest(),
        marker_value="1",
        retirement_ref="retirement/example",
        retirement_proof_sha256="a" * 64,
    )
    b.command(
        "SET",
        ns.prefix + ":completion-context:" + i["event_id"] + ":" + value["completion_generation"],
        context,
    )
    b.command("SADD", ns.prefix + ":completion-repair:pending", i["event_id"])
    return i, value, witness


def test_index_written_before_commit_and_immutable(admission_broker):
    b = admission_broker
    a, req, digest, ns = setup(b)
    assert b.command("GET", PREFIX + "event:event/example") == canonical(
        dict(schema_version=1, event_id="event/example", intent_digest=digest)
    )
    assert a.recover(req.identity).status == "duplicate"
    b.command(
        "SET",
        PREFIX + "event:event/example",
        canonical(dict(schema_version=1, event_id="event/example", intent_digest="f" * 64)),
    )
    with pytest.raises(module("admission_records").AdmissionUnavailable):
        a.recover(req.identity)


def test_durable_owed_refund_retains_proofs_and_allows_next_event(admission_broker):
    b = admission_broker
    a, req, digest, ns = setup(b)
    original = {
        key: b.command("GET", PREFIX + key + ":" + digest) for key in ("intent", "state", "commit")
    }
    i, c, w = evidence(b, digest, ns)
    assert a.release_accepted(req.identity) == "retained"
    b.command("SET", ns.prefix + ":settlement:" + i["event_id"], canonical(w))
    assert a.release_accepted(req.identity) == "released"
    assert a.release_accepted(req.identity) == "already_released"
    assert b.command("ZCARD", PREFIX + "quota") == 0
    for key, raw in original.items():
        assert b.command("GET", PREFIX + key + ":" + digest) == raw
        assert b.command("PTTL", PREFIX + key + ":" + digest) == -1
    assert (
        a.admit(
            request(
                module("admission_records"),
                delivery="delivery/next",
                payload=queued_payload(event_id="event/next"),
            )
        ).status
        == "accepted"
    )
    assert b.command("GET", PREFIX + "event:event/example") is not None
    assert b.command("PTTL", PREFIX + "event:event/example") == -1


@pytest.mark.parametrize(
    "mutation",
    [
        "context",
        "repair",
        "binding",
        "commit",
        "witness_group",
        "epoch",
        "resolution_generation",
        "witness_digest",
        "context_digest",
        "retirement_missing",
    ],
)
def test_refund_requires_all_exact_proof(admission_broker, mutation):
    b = admission_broker
    a, req, digest, ns = setup(b)
    i, c, w = evidence(b, digest, ns)
    if mutation == "witness_group":
        w["delivery_group"] = "foreign"
    if mutation == "epoch":
        w["broker_run_id"] = "f" * 40
    if mutation == "witness_digest":
        w["intent_digest"] = "f" * 64
    if mutation == "context_digest":
        w["completion_context_sha256"] = "f" * 64
    if mutation == "retirement_missing":
        del w["retirement_ref"]
    b.command("SET", ns.prefix + ":settlement:" + i["event_id"], canonical(w))
    if mutation in ("binding", "commit"):
        b.command(
            "DEL", PREFIX + mutation + ":" + (i["event_id"] if mutation == "binding" else digest)
        )
    if mutation == "context":
        b.command(
            "DEL",
            ns.prefix + ":completion-context:" + i["event_id"] + ":" + c["completion_generation"],
        )
    if mutation == "repair":
        b.command("SREM", ns.prefix + ":completion-repair:pending", i["event_id"])
    if mutation == "resolution_generation":
        b.command(
            "DEL",
            ns.prefix + ":completion-context:" + i["event_id"] + ":" + c["completion_generation"],
        )
        b.command(
            "SET",
            ns.prefix
            + ":completion-resolution:"
            + i["event_id"]
            + ":"
            + c["completion_generation"],
            canonical(
                dict(
                    schema_version=1,
                    event_id=i["event_id"],
                    completion_generation="e" * 32,
                    resolution="emitted",
                )
            ),
        )
    try:
        assert a.release_accepted(req.identity) == "retained"
    except module("admission_records").AdmissionUnavailable:
        pass
    assert b.command("ZSCORE", PREFIX + "quota", digest) is not None


def test_exact_resolution_refunds_without_context(admission_broker):
    b = admission_broker
    a, req, digest, ns = setup(b)
    i, c, w = evidence(b, digest, ns)
    b.command("SET", ns.prefix + ":settlement:" + i["event_id"], canonical(w))
    b.command(
        "DEL", ns.prefix + ":completion-context:" + i["event_id"] + ":" + c["completion_generation"]
    )
    b.command("SREM", ns.prefix + ":completion-repair:pending", i["event_id"])
    b.command(
        "SET",
        ns.prefix + ":completion-resolution:" + i["event_id"] + ":" + c["completion_generation"],
        canonical(
            dict(
                schema_version=1,
                event_id=i["event_id"],
                completion_generation=c["completion_generation"],
                resolution="emitted",
            )
        ),
    )
    assert a.release_accepted(req.identity) == "released"


def test_enqueue_runtime_witness_reads_do_not_grant_mutations(admission_broker):
    b = admission_broker
    a, req, digest, ns = setup(b)
    conn = client(b)
    key = ns.prefix + ":settlement:event/example"
    foreign = "protected:lane:worker:foreign:settlement:event/example"
    try:
        assert conn.get(key) is None
        for command in [("SET", key, "bad"), ("DEL", key), ("GET", foreign)]:
            with pytest.raises(NoPermissionError):
                conn.execute_command(*command)
        with pytest.raises(ResponseError, match="ACL failure"):
            conn.eval("return redis.call('SET',KEYS[1],'bad')", 1, key)
    finally:
        conn.close()


def test_api_tick_releases_only_terminal_committed_member(admission_broker):
    """Actual enqueue session exercises the existing API tick owner."""
    from curie_api.protected_reconciler import _tick
    from curie_api.protected_runtime_files import IngressRuntime, RuntimeBootstrap
    from curie_protected_hooks.broker_transport import EnqueueCredential, MetadataReaderCredential

    b = admission_broker
    a, req, digest, ns = setup(b)
    i, c, w = evidence(b, digest, ns)
    b.command("SET", ns.prefix + ":settlement:" + i["event_id"], canonical(w))
    runtime = IngressRuntime(
        RuntimeBootstrap(
            b.manifest(), b.ca_pem, MetadataReaderCredential("unused", "synthetic"), 60000
        ),
        EnqueueCredential("enqueue", b.passwords["enqueue"]),
    )
    counts = _tick(runtime)
    assert counts.released == 1
    assert counts.quota == 1
    assert b.command("ZCARD", PREFIX + "quota") == 0
    assert a.release_accepted(req.identity) == "already_released"


def test_worker_constant_lookup_after_refund_and_broker_denials(admission_broker):
    """No census is required after capacity has been reused by a different event."""
    import asyncio

    from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient, WorkerCredential
    from curie_protected_hooks.worker_verification import (
        Dispatchable,
        restore_committed_event,
        verification_acl_rules,
        verify_delivery,
    )

    b = admission_broker
    a, req, digest, ns = setup(b)
    i, c, w = evidence(b, digest, ns)
    b.command("SET", ns.prefix + ":settlement:" + i["event_id"], canonical(w))
    assert a.release_accepted(req.identity) == "released"
    assert (
        a.admit(
            request(
                module("admission_records"),
                delivery="delivery/next",
                payload=queued_payload(event_id="event/next"),
            )
        ).status
        == "accepted"
    )
    b.passwords["worker"] = helpers.FixtureSecret("synthetic-test-worker-password")
    b.command(
        "ACL",
        "SETUSER",
        "worker",
        "reset",
        "on",
        ">" + b.passwords["worker"],
        *(r for r in verification_acl_rules("worker") if "+zrange" not in r),
    )
    conn = client(b, "worker")
    try:
        assert json.loads(conn.get(PREFIX + "event:event/example"))["intent_digest"] == digest
        assert conn.get(PREFIX + "intent:" + digest) is not None

        # Actual worker verifier and cold restoration both ignore refunded quota.
        async def lookup():
            worker = await AuthenticatedWorkerClient.connect(
                b.manifest(), WorkerCredential("worker", b.passwords["worker"]), b.ca_pem
            )
            try:
                proof = await restore_committed_event(
                    worker, "event/example", trusted_manifest=b.manifest()
                )
                assert proof is not None
                assert proof.intent.as_dict()["identity"] == req.identity.as_dict()
                assert len(proof.proof_snapshots) == 5
                original = b.command(
                    "XRANGE", "curie:runs", i["reserved_stream_id"], i["reserved_stream_id"]
                )[0][1]
                fields = {k.decode(): v.decode() for k, v in original.items()}
                verdict = await verify_delivery(worker, fields, trusted_manifest=b.manifest())
                assert isinstance(verdict, Dispatchable)
            finally:
                await worker.close()

        asyncio.run(lookup())
        # The constant path remains available with quota census denied.
        with pytest.raises(NoPermissionError):
            conn.zrange(PREFIX + "quota", 0, 64)
        assert (
            conn.eval("return redis.call('GET',KEYS[1])", 1, PREFIX + "event:event/example")
            is not None
        )
        for key in (
            PREFIX + "intent:" + digest,
            PREFIX + "event:event/example",
            "protected:control:example",
            helpers.SOURCE,
        ):
            with pytest.raises(NoPermissionError):
                conn.set(key, "forged")
            with pytest.raises(ResponseError, match="ACL failure"):
                conn.eval("return redis.call('SET',ARGV[1],'forged')", 0, key)
        with pytest.raises(NoPermissionError):
            conn.zrem(PREFIX + "quota", digest)
        with pytest.raises(ResponseError, match="ACL failure"):
            conn.eval("return redis.call('ZREM',ARGV[1],ARGV[2])", 0, PREFIX + "quota", digest)
    finally:
        conn.close()
        b.command("CLIENT", "KILL", "USER", "worker", "SKIPME", "yes")
        b.command("ACL", "DELUSER", "worker")
        del b.passwords["worker"]


@pytest.mark.parametrize("kind", ["string", "hash"])
def test_event_index_collision_or_wrong_type_does_not_begin_admission(admission_broker, kind):
    b = admission_broker
    seed(b)
    a = facade(b, module("atomic_admission"), limit=1)
    b.command("ACL", "SETUSER", "enqueue", *module("admission_acl").admission_acl_rules("enqueue"))
    key = PREFIX + "event:event/example"
    if kind == "string":
        b.command(
            "SET",
            key,
            canonical(dict(schema_version=1, event_id="event/example", intent_digest="f" * 64)),
        )
    else:
        b.command("HSET", key, "foreign", "value")
    with pytest.raises(module("admission_records").AdmissionUnavailable):
        a.admit(request(module("admission_records")))
    assert b.command("ZCARD", PREFIX + "quota") == 0
    assert b.command("EXISTS", "curie:runs") == 0


def test_owner_backfill_validates_exact_commit_and_refuses_census_overflow(admission_broker):
    b = admission_broker
    a, req, digest, ns = setup(b)
    b.command("DEL", PREFIX + "event:event/example")
    assert a.backfill_accepted(req.identity) is True
    b.command("ZADD", PREFIX + "quota", 1, "f" * 64)
    with pytest.raises(module("admission_records").AdmissionUnavailable):
        a.committed(1)


def test_concurrent_owner_refunds_remove_only_one_quota_member(admission_broker):
    from concurrent.futures import ThreadPoolExecutor

    b = admission_broker
    a, req, digest, ns = setup(b)
    i, c, w = evidence(b, digest, ns)
    b.command("SET", ns.prefix + ":settlement:" + i["event_id"], canonical(w))

    def release():
        conn = client(b)
        try:
            owner = module("atomic_admission").AtomicAdmission(
                conn, trusted_manifest=b.manifest(), trusted_max_readiness_ms=60000, backlog_limit=1
            )
            return owner.release_accepted(req.identity)
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _: release(), range(2)))
    assert sorted(outcomes) == ["already_released", "released"]
    assert b.command("GET", PREFIX + "commit:" + digest) is not None


def test_partial_index_before_final_commit_is_not_accepted(admission_broker):
    from .test_admission_recovery import interrupt
    from .test_atomic_admission import setup_product

    b = admission_broker
    records, a, _ = setup_product(b)
    req = request(records)
    owned = interrupt(b, records, a, req, "commit")
    assert b.command("GET", PREFIX + "event:event/example") is not None
    assert b.command("GET", owned["commit"]) is None
    assert a.recover(req.identity).status == "accepted"
    assert b.command("GET", owned["commit"]) is not None


def test_bare_done_ack_outbox_and_diagnostic_never_refund(admission_broker):
    b = admission_broker
    a, req, digest, ns = setup(b)
    i, c, w = evidence(b, digest, ns)
    b.command("SET", ns.prefix + ":done:" + i["event_id"], "1")
    b.command(
        "HSET",
        ns.prefix + ":completion:" + i["event_id"],
        "record",
        c["completion_record_json"],
        "done",
        "1",
        "gen",
        c["completion_generation"],
    )
    b.command("XGROUP", "CREATE", "curie:runs", ns.group, "0")
    entries = b.command(
        "XREADGROUP", "GROUP", ns.group, "owner/example", "STREAMS", "curie:runs", ">"
    )
    assert entries
    b.command("XACK", "curie:runs", ns.group, i["reserved_stream_id"])
    b.command("XADD", ns.dead, "*", "event_id", i["event_id"], "dl_reason", "delivery_exhausted")
    assert a.release_accepted(req.identity) == "retained"
    assert b.command("ZSCORE", PREFIX + "quota", digest) is not None


def test_lost_actual_refund_response_is_safe_to_repeat(admission_broker):
    from .admission_broker import ResponseDropRelay

    b = admission_broker
    a, req, digest, ns = setup(b)
    i, c, w = evidence(b, digest, ns)
    b.command("SET", ns.prefix + ":settlement:" + i["event_id"], canonical(w))
    with ResponseDropRelay(b, PREFIX + "commit:" + digest, released_digest=digest) as relay:
        connection = client(b, port=relay.port)
        try:
            owner = module("atomic_admission").AtomicAdmission(
                connection,
                trusted_manifest=b.manifest(),
                trusted_max_readiness_ms=60000,
                backlog_limit=1,
            )
            with pytest.raises(module("admission_records").AdmissionUnavailable) as caught:
                owner.release_accepted(req.identity)
            assert caught.value.__context__ is None
            assert caught.value.__cause__ is None
            assert relay.dropped is True
            assert b.command("ZCARD", PREFIX + "quota") == 0
        finally:
            connection.close()
    assert a.release_accepted(req.identity) == "already_released"


class _AcceptedCensusCorruptionRelay(helpers.ResponseDropRelay):
    """Forward real TLS replies; corrupt an owned proof at the accepted census boundary."""

    def __init__(self, broker, committed_intent):
        super().__init__(broker, "unused")
        self.committed_intent = committed_intent
        self.censuses = 0
        self.injected = False
        self.observed_error = False
        self.upstream_port = broker.port

    def run(self):
        import socket
        import ssl

        from curie_protected_hooks.admission_scripts import QUOTA_CENSUS
        from redis.exceptions import RedisError

        try:
            server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server.minimum_version = ssl.TLSVersion.TLSv1_2
            server.load_cert_chain(
                str(self.broker.private / "server.crt"), str(self.broker.private / "server.key")
            )
            peer, _ = self.listener.accept()
            peer.settimeout(5)
            self.downstream = server.wrap_socket(peer, server_side=True)
            trusted = ssl.create_default_context(cafile=str(self.broker.private / "ca.crt"))
            trusted.minimum_version = ssl.TLSVersion.TLSv1_2
            self.upstream = trusted.wrap_socket(
                socket.create_connection(("127.0.0.1", self.upstream_port), timeout=5),
                server_hostname="127.0.0.1",
            )
            with (
                self.downstream.makefile("rb") as commands,
                self.upstream.makefile("rb") as replies,
            ):
                while True:
                    command = self.frame(commands)
                    if QUOTA_CENSUS.encode() in command:
                        self.censuses += 1
                        if self.censuses == 2:
                            # Preparing listing and the existing occupancy counts read
                            # the original valid proof before this independent corruption.
                            self.broker.command("DEL", self.committed_intent)
                            self.broker.command("HSET", self.committed_intent, "corrupt", "proof")
                            self.injected = True
                    self.upstream.sendall(command)
                    reply = self.frame(replies)
                    self.downstream.sendall(reply)
                    if self.injected and reply.startswith(b"-WRONGTYPE"):
                        self.observed_error = True
                        break
        except (OSError, ValueError, RedisError):
            self.error = True
        finally:
            for connection in (self.downstream, self.upstream):
                if connection is not None:
                    connection.close()


def test_preparing_recovery_precedes_failing_accepted_census(admission_broker):
    """A newly corrupted accepted proof cannot block an already-listed preparing intent."""
    from curie_api.protected_reconciler import _tick
    from curie_api.protected_runtime_files import IngressRuntime, RuntimeBootstrap
    from curie_protected_hooks.broker_transport import EnqueueCredential, MetadataReaderCredential

    from .test_admission_recovery import interrupt

    b = admission_broker
    records = module("admission_records")
    atomic = module("atomic_admission")
    committed_req = request(
        records, delivery="delivery/committed", payload=queued_payload(event_id="event/committed")
    )
    committed_digest = records.delivery_digest(committed_req.identity)
    preparing_req = request(
        records, delivery="delivery/preparing", payload=queued_payload(event_id="event/preparing")
    )
    preparing_digest = records.delivery_digest(preparing_req.identity)
    upstream_port = b.port
    with _AcceptedCensusCorruptionRelay(b, PREFIX + "intent:" + committed_digest) as relay:
        try:
            # Authority is genuinely bound to the relay's TLS endpoint; the
            # relay forwards to the same independently observed broker epoch.
            b.port = relay.port
            manifest = b.manifest()
            seed(b)
            owner = atomic.AtomicAdmission(
                b.admin, trusted_manifest=manifest, trusted_max_readiness_ms=60000, backlog_limit=64
            )
            assert owner.admit(committed_req).status == "accepted"
            connection = client(b, port=upstream_port)
            try:
                preparing_owner = atomic.AtomicAdmission(
                    connection,
                    trusted_manifest=manifest,
                    trusted_max_readiness_ms=60000,
                    backlog_limit=64,
                )
                interrupt(b, records, preparing_owner, preparing_req, "runs")
            finally:
                connection.close()
            b.command(
                "ACL",
                "SETUSER",
                "enqueue",
                *module("admission_acl").admission_acl_rules("enqueue", trusted_manifest=manifest),
            )
            assert b.command("GET", PREFIX + "commit:" + preparing_digest) is None
            runtime = IngressRuntime(
                RuntimeBootstrap(
                    manifest, b.ca_pem, MetadataReaderCredential("unused", "synthetic"), 60000
                ),
                EnqueueCredential("enqueue", b.passwords["enqueue"]),
            )
            with pytest.raises(records.AdmissionUnavailable):
                _tick(runtime)
            assert relay.injected and relay.observed_error
            committed_raw = b.command("GET", PREFIX + "commit:" + preparing_digest)
            assert committed_raw is not None
            assert records.parse_state(committed_raw).as_dict()["status"] == "committed"
            assert b.command("TYPE", PREFIX + "intent:" + committed_digest) == b"hash"
        finally:
            b.port = upstream_port
