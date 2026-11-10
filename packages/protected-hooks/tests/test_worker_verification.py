"""Accepted-copy proof on real TLS Valkey, @spec PROTECTED-HOOK-LANE-5/6.

The protected worker reads the bounded admission census without becoming an
enqueue principal. Binding existence alone does not attest final commit.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import FrozenInstanceError

import pytest
from redis.exceptions import NoPermissionError

from . import worker_broker as wb
from .admission_broker import (
    AGENT,
    HOOK,
    canonical,
    module,
    queued_payload,
    request,
    seed,
    snapshot,
)

pworker_service = wb.pworker_service
owned_worker = wb.owned_worker
PREFIX = "protected:admission:"


def verification():
    """Absence is an assertion in the body, @spec PROTECTED-HOOK-LANE-6."""
    product = module("worker_verification")
    for name in ("verify_delivery", "verification_acl_rules", "Dispatchable", "Parked", "Refused"):
        assert hasattr(product, name), f"worker verification surface absent: {name}"
    return product


def admitted(broker, *, payload=None):
    """Actual atomic admission under the fixture provisioner, @spec PROTECTED-HOOK-ADMISSION-4."""
    records = module("admission_records")
    atomic = module("atomic_admission")
    controls = seed(broker)
    req = request(records, payload=payload)
    admission = atomic.AtomicAdmission(
        broker.admin,
        trusted_manifest=broker.manifest(),
        trusted_max_readiness_ms=60000,
        backlog_limit=64,
    )
    result = admission.admit(req).as_dict()
    assert result["status"] == "accepted"
    digest = records.delivery_digest(req.identity)
    entry = broker.command("XRANGE", wb.STREAM, "-", "+")[0]
    fields = {k.decode(): v.decode() for k, v in entry[1].items()}
    return digest, fields, controls


@asynccontextmanager
async def connected(product, owned):
    """Install only the supplemental product recipe, @spec PROTECTED-HOOK-LANE-3/6."""
    owned.rules = product.verification_acl_rules("worker")
    owned.install()
    client = await wb.worker_transport().AuthenticatedWorkerClient.connect(
        owned.broker.manifest(),
        owned.credential(),
        owned.broker.ca_pem,
    )
    try:
        yield client
    finally:
        await client.close()


def run_verify(product, owned, fields, *, limit=64, trusted=None):
    """One pinned session lifetime per proof, @spec PROTECTED-HOOK-LANE-6."""

    async def perform():
        async with connected(product, owned) as client:
            return await product.verify_delivery(
                client,
                fields,
                trusted_manifest=trusted or owned.broker.manifest(),
                backlog_limit=limit,
            )

    return asyncio.run(perform())


def rewrite(broker, key, transform):
    """Provisioner corruption of one real record, @spec PROTECTED-HOOK-LANE-6."""
    value = json.loads(broker.command("GET", key))
    transform(value)
    broker.command("SET", key, canonical(value))


def test_committed_copy_dispatches_without_mutating_proof_or_requiring_original_row(owned_worker):
    """Copied IDs and trimmed originals preserve acceptance, @spec PROTECTED-HOOK-LANE-6."""
    product = verification()
    b = owned_worker.broker
    digest, fields, _ = admitted(b)
    original = json.loads(b.command("GET", PREFIX + "intent:" + digest))
    b.command("XTRIM", wb.STREAM, "MAXLEN", "=", 0)
    copied_id = b.command("XADD", wb.STREAM, "*", *sum(([k, v] for k, v in fields.items()), []))
    assert copied_id.decode() != original["reserved_stream_id"]
    before = snapshot(b)
    verdict = run_verify(product, owned_worker, fields)
    assert isinstance(verdict, product.Dispatchable)
    assert verdict.turn.event_id == "event/example"
    assert verdict.envelope.as_dict()["event_id"] == verdict.turn.event_id
    assert verdict.intent.as_dict() == original
    assert snapshot(b) == before
    assert "anonymous prompt" not in repr(verdict)
    with pytest.raises((FrozenInstanceError, AttributeError, TypeError)):
        verdict.turn = None
    assert not owned_worker.denials()


@pytest.mark.parametrize("whitespace", [False, True])
def test_dispatchable_retains_immutable_exact_source_fields(owned_worker, whitespace):
    """Reverification needs original bytes, not serialized models, @spec PROTECTED-HOOK-LANE-6/7."""
    product = verification()
    payload = queued_payload(text="anonymous source prompt \u03bb")
    if whitespace:
        formatted = json.dumps(json.loads(payload), indent=2, ensure_ascii=False)
        payload = (" \n" + formatted + "\n ").encode()
    _, fields, _ = admitted(owned_worker.broker, payload=payload)
    original = dict(fields)
    before = snapshot(owned_worker.broker)
    verdict = run_verify(product, owned_worker, fields)
    assert isinstance(verdict, product.Dispatchable)
    assert hasattr(verdict, "source_fields"), "accepted delivery must retain original source fields"
    assert type(verdict.source_fields) is tuple
    assert all(type(pair) is tuple for pair in verdict.source_fields)
    assert dict(verdict.source_fields) == original
    assert dict(verdict.source_fields)["payload"].encode("utf-8") == payload
    assert fields == original
    with pytest.raises(TypeError):
        verdict.source_fields[0][1] = "substituted source"
    with pytest.raises((FrozenInstanceError, AttributeError, TypeError)):
        verdict.source_fields = ()
    fields.clear()
    assert dict(verdict.source_fields) == original
    assert repr(verdict) == "Dispatchable()"
    legacy = product.Dispatchable(verdict.turn, verdict.envelope, verdict.intent)
    assert legacy.source_fields == ()
    assert repr(legacy) == "Dispatchable()"
    assert snapshot(owned_worker.broker) == before
    assert not owned_worker.denials()


def test_source_fields_snapshot_survives_caller_mutation_during_await(owned_worker):
    """Caller mutation during proof I/O cannot replace bytes, @spec PROTECTED-HOOK-LANE-6/7."""
    product = verification()
    _, fields, _ = admitted(owned_worker.broker)
    original = dict(fields)
    before = snapshot(owned_worker.broker)

    async def perform():
        async with connected(product, owned_worker) as client:
            # The callback runs when verification first yields for real TLS I/O.
            asyncio.get_running_loop().call_soon(
                fields.update,
                {"payload": "substituted payload", "protected_envelope": "substituted envelope"},
            )
            return await product.verify_delivery(
                client, fields, trusted_manifest=owned_worker.broker.manifest()
            )

    verdict = asyncio.run(perform())
    assert fields == {
        "payload": "substituted payload",
        "protected_envelope": "substituted envelope",
    }
    assert isinstance(verdict, product.Dispatchable)
    assert hasattr(verdict, "source_fields"), "accepted delivery must retain original source fields"
    assert dict(verdict.source_fields) == original
    assert verdict.turn.text == "anonymous prompt"
    assert repr(verdict) == "Dispatchable()"
    assert snapshot(owned_worker.broker) == before
    assert not owned_worker.denials()


@pytest.mark.parametrize("mutable_state", ["preparing", "absent"])
def test_missing_final_commit_parks_without_any_write(owned_worker, mutable_state):
    """Post-XADD interruption does not authorize execution, @spec PROTECTED-HOOK-ADMISSION-5."""
    product = verification()
    b = owned_worker.broker
    digest, fields, _ = admitted(b)
    b.command("DEL", PREFIX + "commit:" + digest)
    if mutable_state == "absent":
        b.command("DEL", PREFIX + "state:" + digest)
    before = snapshot(b)
    verdict = run_verify(product, owned_worker, fields)
    assert isinstance(verdict, product.Parked)
    assert not hasattr(verdict, "source_fields")
    assert snapshot(b) == before


def test_failed_preparation_has_distinct_terminal_verdict(owned_worker):
    """Terminal acknowledgement needs no model, @spec PROTECTED-HOOK-ADMISSION-5."""
    product = verification()
    b = owned_worker.broker
    digest, fields, _ = admitted(b)
    b.command("DEL", PREFIX + "commit:" + digest)
    b.command(
        "SET",
        PREFIX + "state:" + digest,
        canonical(
            dict(
                schema_version=1,
                status="failed",
                recovery_attempts=10,
                reason="attempts_exhausted",
                receipt=None,
            )
        ),
    )
    before = snapshot(b)
    verdict = run_verify(product, owned_worker, fields)
    assert isinstance(verdict, product.Refused)
    assert verdict.reason == "admission_failed"
    assert not hasattr(verdict, "source_fields")
    assert snapshot(b) == before


@pytest.mark.parametrize("kind", ["binding", "intent", "commit", "quota"])
def test_binding_or_orphan_proof_never_implies_dispatch(owned_worker, kind):
    """Only missing commit with retained intent is parkable, @spec PROTECTED-HOOK-LANE-6."""
    product = verification()
    b = owned_worker.broker
    digest, fields, _ = admitted(b)
    if kind == "binding":
        b.command("DEL", PREFIX + "binding:event/example")
    elif kind == "quota":
        # Legacy unindexed admission still needs its bounded current census.
        b.command("DEL", PREFIX + "event:event/example")
        b.command("ZREM", PREFIX + "quota", digest)
    elif kind == "commit":
        b.command("SET", PREFIX + "commit:" + digest, b"{}")
    else:
        b.command("DEL", PREFIX + "intent:" + digest)
    before = snapshot(b)
    assert isinstance(run_verify(product, owned_worker, fields), product.Refused)
    assert snapshot(b) == before


@pytest.mark.parametrize(
    "field",
    [
        "event_id",
        "source_revision",
        "runtime_id",
        "runtime_generation",
        "manifest_digest",
        "qualification_id",
        "runner_image_digest",
        "bundle_digest",
        "execution_config_digest",
        "logical_conversation_key",
        "execution_session_key",
        "payload_sha256",
        "remediation_generation",
    ],
)
def test_substituted_envelope_even_with_equal_binding_is_refused(owned_worker, field):
    """Exact original Intent and trusted tuple bind each field, @spec PROTECTED-HOOK-LANE-6."""
    product = verification()
    b = owned_worker.broker
    _, fields, _ = admitted(b)
    envelope = json.loads(fields["protected_envelope"])
    replacements = {
        "runtime_id": "99999999-9999-4999-8999-999999999999",
        "qualification_id": "99999999-9999-4999-8999-999999999999",
        "source_revision": "2",
        "runtime_generation": "2",
        "remediation_generation": "2",
        "runner_image_digest": "sha256:" + "9" * 64,
        "manifest_digest": "9" * 64,
        "bundle_digest": "9" * 64,
        "execution_config_digest": "9" * 64,
        "payload_sha256": "9" * 64,
    }
    envelope[field] = replacements.get(field, "substituted/example")
    raw = canonical(envelope)
    fields["protected_envelope"] = raw.decode()
    b.command("SET", PREFIX + "binding:" + envelope["event_id"], raw)
    assert isinstance(run_verify(product, owned_worker, fields), product.Refused)


@pytest.mark.parametrize(
    "violation",
    [
        "extra_field",
        "missing_envelope",
        "missing_payload",
        "payload_changed",
        "intent_digest",
        "receipt_stream",
        "receipt_payload",
        "failed_with_commit",
        "committed_mutable",
        "duplicate_event",
        "quota_overflow",
        "member_digest",
    ],
)
def test_ambiguous_or_contradictory_admission_proof_refuses(owned_worker, violation):
    """Bounded exact proof rather than best-effort lookup, @spec PROTECTED-HOOK-LANE-6."""
    product = verification()
    b = owned_worker.broker
    digest, fields, _ = admitted(b)
    if violation in {"duplicate_event", "quota_overflow", "member_digest"}:
        # Exercise legacy fallback specifically; indexed proof is quota independent.
        b.command("DEL", PREFIX + "event:event/example")
    if violation == "extra_field":
        fields["untrusted"] = "extra"
    elif violation.startswith("missing_"):
        fields.pop("protected_envelope" if violation == "missing_envelope" else "payload")
    elif violation == "payload_changed":
        fields["payload"] = queued_payload(text="changed prompt").decode()
    elif violation == "intent_digest":
        rewrite(
            b,
            PREFIX + "intent:" + digest,
            lambda d: d["identity"].update(delivery_id="another/delivery"),
        )
    elif violation.startswith("receipt_"):
        field = "stream_id" if violation == "receipt_stream" else "payload_sha256"
        rewrite(
            b,
            PREFIX + "commit:" + digest,
            lambda d: d["receipt"].update({field: "1-0" if field == "stream_id" else "9" * 64}),
        )
    elif violation == "failed_with_commit":
        b.command(
            "SET",
            PREFIX + "state:" + digest,
            canonical(
                dict(
                    schema_version=1,
                    status="failed",
                    recovery_attempts=10,
                    reason="deadline",
                    receipt=None,
                )
            ),
        )
    elif violation == "committed_mutable":
        b.command("SET", PREFIX + "state:" + digest, b.command("GET", PREFIX + "commit:" + digest))
    elif violation == "member_digest":
        b.command("ZADD", PREFIX + "quota", 0, "invalid-digest")
    elif violation == "quota_overflow":
        for i in range(64):
            b.command("ZADD", PREFIX + "quota", i, hashlib.sha256(str(i).encode()).hexdigest())
        assert b.command("ZCARD", PREFIX + "quota") == 65
    else:
        records = module("admission_records")
        intent = json.loads(b.command("GET", PREFIX + "intent:" + digest))
        intent["identity"]["delivery_id"] = "different/example"
        other = records.delivery_digest(records.DeliveryIdentity(**intent["identity"]))
        b.command("SET", PREFIX + "intent:" + other, canonical(intent))
        b.command("ZADD", PREFIX + "quota", 0, other)
    before = snapshot(b)
    verdict = run_verify(product, owned_worker, fields)
    assert isinstance(verdict, product.Refused)
    assert snapshot(b) == before


@pytest.mark.parametrize("change", ["admission_closed", "readiness_expired", "selection_rotated"])
def test_accepted_work_retains_compatible_consumption_authority(owned_worker, change):
    """Admission expiry/rotation alone cannot delete accepted work, @spec PROTECTED-HOOK-LANE-5."""
    product = verification()
    b = owned_worker.broker
    _, fields, controls = admitted(b)
    if change == "readiness_expired":
        key = next(k for k in controls if ":readiness:" in k)
        rewrite(b, key, lambda d: d.update(issued_at_ms="1", expires_at_ms="2"))
    else:
        key = next(k for k in controls if ":selection:" in k)
        rewrite(
            b,
            key,
            lambda d: d.update(
                admission_open=False,
                **(
                    {"runtime_generation": "2", "manifest_digest": "9" * 64}
                    if change == "selection_rotated"
                    else {}
                ),
            ),
        )
    before = snapshot(b)
    assert isinstance(run_verify(product, owned_worker, fields), product.Dispatchable)
    assert snapshot(b) == before


@pytest.mark.parametrize("kind", ["manifest", "qualification"])
def test_missing_or_mismatched_retained_runtime_cannot_execute(owned_worker, kind):
    """No substitution of authority for old accepted tuple, @spec PROTECTED-HOOK-LANE-5/6."""
    product = verification()
    b = owned_worker.broker
    _, fields, controls = admitted(b)
    key = next(k for k in controls if ":" + kind + ":" in k)
    b.command("DEL", key)
    verdict = run_verify(product, owned_worker, fields)
    assert not isinstance(verdict, product.Dispatchable)


@pytest.mark.parametrize(
    "changes",
    [
        {"source": "slack"},
        {"source": "cron"},
        {"tool_access": None},
        {"event_id": "different/example"},
        {"conversation_id": "different/example"},
    ],
)
def test_queued_identity_and_effective_policy_are_checked_after_matching_hashes(
    owned_worker, changes
):
    """Coherent hashes cannot authorize an ordinary turn, @spec PROTECTED-HOOK-LANE-6/7."""
    product = verification()
    b = owned_worker.broker
    digest, fields, _ = admitted(b)
    payload = queued_payload(**changes)
    fields["payload"] = payload.decode()
    payload_hash = hashlib.sha256(payload).hexdigest()
    envelope = json.loads(fields["protected_envelope"])
    envelope["payload_sha256"] = payload_hash
    raw = canonical(envelope)
    fields["protected_envelope"] = raw.decode()
    b.command("SET", PREFIX + "binding:event/example", raw)
    rewrite(
        b,
        PREFIX + "intent:" + digest,
        lambda d: d.update(
            payload_sha256=payload_hash, envelope_sha256=hashlib.sha256(raw).hexdigest()
        ),
    )
    rewrite(
        b, PREFIX + "commit:" + digest, lambda d: d["receipt"].update(payload_sha256=payload_hash)
    )
    assert isinstance(run_verify(product, owned_worker, fields), product.Refused)


def test_verification_permissions_are_read_only_on_admission_families(owned_worker):
    """A worker still cannot write its proof, @spec PROTECTED-HOOK-LANE-3/6."""
    product = verification()
    b = owned_worker.broker
    digest, _, _ = admitted(b)

    async def perform():
        async with connected(product, owned_worker) as client:
            lane = client.lane().runs
            assert await lane.zrange(PREFIX + "quota", 0, 64) == [digest]
            for kind in ("intent", "state", "commit"):
                key = PREFIX + kind + ":" + digest
                assert await lane.get(key) is not None
                with pytest.raises(NoPermissionError):
                    await lane.set(key, "forged")
            with pytest.raises(NoPermissionError):
                await lane.zrem(PREFIX + "quota", digest)

    before = snapshot(b)
    asyncio.run(perform())
    assert snapshot(b) == before


def test_a_protected_scheduled_hook_with_exact_identity_is_dispatchable(owned_worker):
    """Scheduled admission shares the lane, @spec PROTECTED-HOOK-ADMISSION-2/4."""
    product = verification()
    _, fields, _ = admitted(
        owned_worker.broker,
        payload=queued_payload(
            source="cron",
            hook_run=dict(agent_id=AGENT, name=HOOK, slot_utc="2026-10-03T00:00:00Z"),
        ),
    )
    verdict = run_verify(product, owned_worker, fields)
    assert isinstance(verdict, product.Dispatchable)
    assert verdict.turn.hook_run.name == HOOK


def test_restored_proofs_do_not_authorize_a_new_broker_epoch(owned_worker):
    """Restoring every proof cannot restore live authority, @spec PROTECTED-HOOK-LANE-6."""
    product = verification()
    b = owned_worker.broker
    digest, fields, controls = admitted(b)
    trusted = b.manifest()
    names = (
        list(controls)
        + [PREFIX + kind + ":" + digest for kind in ("intent", "state", "commit")]
        + [PREFIX + "binding:event/example"]
    )
    retained = {key: b.command("GET", key) for key in names}
    b.restart()
    assert b.command("INFO", "server")["run_id"] != trusted.as_dict()["broker_identity"]["run_id"]
    for key, value in retained.items():
        b.command("SET", key, value)
    b.command("ZADD", PREFIX + "quota", 0, digest)
    owned_worker.install()
    before = snapshot(b)
    verdict = run_verify(product, owned_worker, fields, trusted=trusted)
    assert isinstance(verdict, product.Refused)
    assert snapshot(b) == before


def test_real_terminal_admission_failure_after_xadd_is_refused_when_quota_is_refunded(
    owned_worker,
):
    """Actual Lua failure refunds census membership but keeps the reserved row;
    every Refused is terminal, regardless of diagnostic reason,
    @spec PROTECTED-HOOK-ADMISSION-5 PROTECTED-HOOK-LANE-6."""
    from .admission_broker import SOURCE
    from .test_admission_recovery import interrupt
    from .test_atomic_admission import setup_product

    product = verification()
    b = owned_worker.broker
    # Recovery harness uses the fixed enqueue test principal; provision it only
    # inside this owned broker, with a disposable credential.
    import secrets

    b.passwords["enqueue"] = wb.FixtureSecret(secrets.token_hex(24))
    b.command("ACL", "SETUSER", "enqueue", "reset", "on", ">" + b.passwords["enqueue"])
    records, admission, _ = setup_product(b)
    try:
        req = request(records)
        owned = interrupt(b, records, admission, req, "commit")
        assert b.command("EXISTS", owned["runs"])
        row = b.command("XRANGE", wb.STREAM, "-", "+")[0]
        fields = {k.decode(): v.decode() for k, v in row[1].items()}
        assert isinstance(run_verify(product, owned_worker, fields), product.Parked)
        b.command("DEL", SOURCE)
        for attempt in range(1, 11):
            outcome = admission.recover(req.identity).as_dict()
            assert outcome["status"] == ("preparing" if attempt < 10 else "failed")
        assert outcome["reason"] == "attempts_exhausted"
        assert b.command("ZCARD", owned["quota"]) == 0
        assert records.parse_state(b.command("GET", owned["state"])).as_dict()["status"] == "failed"
        assert b.command("GET", owned["intent"]) is not None
        assert b.command("GET", owned["binding"]) is not None
        assert b.command("GET", owned["commit"]) is None
        assert b.command("XRANGE", wb.STREAM, "-", "+") == [row]
        before = snapshot(b)
        verdict = run_verify(product, owned_worker, fields)
        assert isinstance(verdict, product.Refused)
        assert not isinstance(verdict, (product.Parked, product.Dispatchable))
        assert snapshot(b) == before
    finally:
        admission._client.close()
        b.command("ACL", "DELUSER", "enqueue", "verifier")
        b.passwords.pop("enqueue")
