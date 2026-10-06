"""Facade amendments for ingress wiring, @spec PROTECTED-HOOK-ADMISSION-1/4/5/7.

Every case runs on the owned disposable TLS Valkey of ``admission_broker.py``
under the product enqueue recipe. The facade receives the provisioner's
trusted manifest; a preparing original recovers from an authenticated retry
whose payload bytes differ; only byte identical payload restores missing
recovery bytes; a reconciler ``recover`` racing a retry leaves one entry and
the original receipt; and ``preparing`` lists exactly the outstanding intents
holding capacity, in quota score order, writing nothing.
"""

from __future__ import annotations

import copy
import hashlib
import threading

import pytest

from . import admission_broker as broker_helpers
from .admission_broker import (
    AGENT,
    HOOK,
    SOURCE,
    canonical,
    client,
    module,
    queued_payload,
    request,
    snapshot,
)
from .test_admission_recovery import interrupt, keys
from .test_atomic_admission import setup_product

admission_service = broker_helpers.admission_service
admission_broker = broker_helpers.admission_broker

# A retried turn differs only in API receive time or in reply coordinates the original fixed.
RETRY_CHANGES = {
    "received_at": dict(received_at="2026-10-03T00:00:07Z"),
    "reply": dict(
        reply_handle=dict(
            kind="slack",
            channel="channel/other",
            placeholder=None,
            adapter="adapter/example",
            endpoint=None,
        )
    ),
}


def control_key(values, kind):
    """@spec PROTECTED-HOOK-ADMISSION-4."""
    return next(key for key in values if key.startswith("protected:control:" + kind + ":"))


def set_admission_open(broker, values, opened):
    """Owned selection change, @spec PROTECTED-HOOK-ADMISSION-4 PROTECTED-HOOK-SOURCE-9."""
    key = control_key(values, "selection")
    selection = copy.deepcopy(values[key])
    selection["admission_open"] = opened
    broker.command("SET", key, canonical(selection))


def stream(broker):
    """@spec PROTECTED-HOOK-ADMISSION-3/5."""
    return broker.raw_command("XRANGE", "curie:runs", "-", "+")


def assert_original_receipt(broker, records, receipt, original, intent):
    """The original's tuple, never the retry's, @spec PROTECTED-HOOK-ADMISSION-3/4
    PROTECTED-HOOK-SOURCE-8."""
    assert receipt["stream_id"] == intent["reserved_stream_id"]
    assert receipt["payload_sha256"] == hashlib.sha256(original.queued_payload).hexdigest()
    assert receipt["requested_tool_access"] == original.requested_tool_access
    assert receipt["effective_tool_access"] == "read-only"
    assert receipt["request_body_sha256"] == original.request_body_sha256
    assert receipt["source_generation"] == original.source_policy["generation"]
    entries = stream(broker)
    assert len(entries) == 1
    assert entries[0][0].decode() == intent["reserved_stream_id"]
    assert entries[0][1][1] == original.queued_payload


@pytest.mark.parametrize("change", sorted(RETRY_CHANGES))
def test_preparing_retry_with_other_turn_bytes_recovers_the_original(admission_broker, change):
    """A preparing original whose authenticated retry keeps the signed tuple but carries
    other payload bytes is a recovery, not a conflict; it returns the original receipt once
    authority opens. @spec PROTECTED-HOOK-ADMISSION-4 @spec PROTECTED-HOOK-ADMISSION-7."""
    b = admission_broker
    r, f, values = setup_product(b)
    original = request(r)
    owned = interrupt(b, r, f, original, "runs")
    original_intent = b.command("GET", owned["intent"])
    intent = r.parse_intent(original_intent).as_dict()
    retry = request(r, payload=queued_payload(**RETRY_CHANGES[change]))
    assert retry.queued_payload != original.queued_payload

    set_admission_open(b, values, False)
    closed = f.admit(retry).as_dict()
    assert closed == dict(status="preparing", reason=None, receipt=None), (
        "a retry differing only in turn bytes was not treated as recovery"
    )
    assert b.command("GET", owned["intent"]) == original_intent
    assert b.command("GET", owned["recovery"]) == original.queued_payload
    assert not stream(b)

    set_admission_open(b, values, True)
    opened = f.admit(retry).as_dict()
    assert opened["status"] == "accepted" and opened["reason"] is None
    assert_original_receipt(b, r, opened["receipt"], original, intent)
    assert b.command("GET", owned["intent"]) == original_intent
    assert not b.command("EXISTS", owned["recovery"])
    before = snapshot(b)
    for again in (original, retry):
        assert f.admit(again).as_dict() == dict(
            status="duplicate", reason=None, receipt=opened["receipt"]
        )
    assert snapshot(b) == before


def test_only_byte_identical_retry_restores_deleted_recovery_bytes(admission_broker):
    """A freshly signed retry recovers without restoring; resending the same signed
    request (identical bytes) restores and commits. @spec PROTECTED-HOOK-ADMISSION-4
    @spec PROTECTED-HOOK-ADMISSION-7."""
    b = admission_broker
    r, f, _ = setup_product(b)
    original = request(r)
    owned = interrupt(b, r, f, original, "runs")
    intent = r.parse_intent(b.command("GET", owned["intent"])).as_dict()
    b.command("DEL", owned["recovery"])

    fresh = request(r, payload=queued_payload(**RETRY_CHANGES["received_at"]))
    out = f.admit(fresh).as_dict()
    assert out == dict(status="preparing", reason=None, receipt=None), (
        "a freshly signed retry conflicted instead of recovering"
    )
    assert not b.command("EXISTS", owned["recovery"]), "other bytes restored recovery"
    assert not stream(b)
    assert f.recover(original.identity).as_dict() == dict(
        status="preparing", reason=None, receipt=None
    )
    assert not b.command("EXISTS", owned["recovery"])

    resent = request(r)
    assert resent.queued_payload == original.queued_payload
    out = f.admit(resent).as_dict()
    assert out["status"] == "accepted"
    assert_original_receipt(b, r, out["receipt"], original, intent)
    assert not b.command("EXISTS", owned["recovery"])
    assert b.command("ZCARD", owned["quota"]) == 1


def test_reconciler_recover_racing_a_signed_retry_leaves_one_entry_and_receipt(
    admission_broker,
):
    """Gate free reconcilers race ingress retries on other connections; every answer is the
    original receipt and one entry exists. @spec PROTECTED-HOOK-ADMISSION-4/5
    @spec PROTECTED-HOOK-ADMISSION-7 @spec PROTECTED-HOOK-LANE-4."""
    b = admission_broker
    r, f, _ = setup_product(b)
    atomic = module("atomic_admission")
    rounds = 12
    prepared = []
    for index in range(rounds):
        original = request(
            r,
            delivery=f"delivery/race-{index}",
            payload=queued_payload(event_id=f"event/race-{index}"),
        )
        owned = interrupt(b, r, f, original, "runs")
        intent = r.parse_intent(b.command("GET", owned["intent"])).as_dict()
        retry = request(
            r,
            delivery=f"delivery/race-{index}",
            payload=queued_payload(
                event_id=f"event/race-{index}", received_at="2026-10-03T00:00:09Z"
            ),
        )
        prepared.append((original, retry, owned, intent))
    connections = [client(b) for _ in range(3)]
    try:
        facades = [
            atomic.AtomicAdmission(
                connection,
                trusted_manifest=b.manifest(),
                trusted_max_readiness_ms=60000,
                backlog_limit=100,
            )
            for connection in connections
        ]
        for original, retry, owned, intent in prepared:
            barrier = threading.Barrier(3)
            results: list[object] = [None, None, None]
            calls = (
                lambda original=original: facades[0].recover(original.identity),
                lambda original=original: facades[1].recover(original.identity),
                lambda retry=retry: facades[2].admit(retry),
            )

            def run(slot, barrier=barrier, results=results, calls=calls):
                """@spec PROTECTED-HOOK-ADMISSION-7."""
                barrier.wait(timeout=10)
                try:
                    results[slot] = calls[slot]().as_dict()
                except Exception as error:  # noqa: BLE001  Recorded as an outcome.
                    results[slot] = type(error).__name__

            threads = [threading.Thread(target=run, args=(slot,)) for slot in range(3)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=30)
            assert all(type(result) is dict for result in results), (
                "a racing recover or retry answered unavailable: "
                + ", ".join(str(result) for result in results if type(result) is not dict)
            )
            assert sorted(result["status"] for result in results) == [
                "accepted",
                "duplicate",
                "duplicate",
            ]
            receipts = {canonical(result["receipt"]) for result in results}
            assert len(receipts) == 1
            commit = r.parse_state(b.command("GET", owned["commit"])).as_dict()
            assert canonical(commit["receipt"]) in receipts
            entries = b.raw_command(
                "XRANGE", "curie:runs", intent["reserved_stream_id"], intent["reserved_stream_id"]
            )
            assert len(entries) == 1 and entries[0][1][1] == original.queued_payload
        assert b.command("XLEN", "curie:runs") == rounds
    finally:
        for connection in connections:
            connection.close()


def test_preparing_lists_exactly_outstanding_intents_in_score_order(admission_broker):
    """Committed members outnumber the preparing ones; committed, failed and orphan members
    are skipped, intents without a quota member are not found, nothing is authorized and
    nothing is written. @spec PROTECTED-HOOK-ADMISSION-5 @spec PROTECTED-HOOK-ADMISSION-6
    @spec PROTECTED-HOOK-ADMISSION-7 @spec PROTECTED-HOOK-LANE-4."""
    b = admission_broker
    limit = 10
    r, f, values = setup_product(b, limit)
    quota = "protected:admission:quota"

    def make(name):
        """@spec PROTECTED-HOOK-ADMISSION-5."""
        return request(
            r, delivery="delivery/" + name, payload=queued_payload(event_id="event/" + name)
        )

    for index in range(6):
        assert f.admit(make(f"committed-{index}")).as_dict()["status"] == "accepted"
    first, second = make("preparing-first"), make("preparing-second")
    owned_first = interrupt(b, r, f, first, "runs")
    owned_second = interrupt(b, r, f, second, "runs")
    # One preparing intent has lost its recovery bytes; it still holds capacity.
    b.command("DEL", owned_second["recovery"])
    unfound = make("no-member")
    interrupt(b, r, f, unfound, "quota")
    failed = make("failed")
    owned_failed = interrupt(b, r, f, failed, "runs")
    sec, usec = b.command("TIME")
    now = int(sec) * 1000 + int(usec) // 1000
    intent = r.parse_intent(b.command("GET", owned_failed["intent"])).as_dict()
    intent.update(created_at_ms=str(now - 300001), deadline_ms=str(now - 1))
    b.command("SET", owned_failed["intent"], canonical(intent))
    assert f.recover(failed.identity).as_dict()["status"] == "failed"
    # Owned faults: a failed intent's member restored, and an orphan member with no intent.
    b.command("ZADD", quota, 1, owned_failed["d"])
    b.command("ZADD", quota, 2, "0" * 64)
    # Score order, not creation or member order: the second preparing intent comes first.
    b.command("ZADD", quota, "XX", now - 5000, owned_second["d"])
    b.command("ZADD", quota, "XX", now - 4000, owned_first["d"])
    assert b.command("ZCARD", quota) == limit
    # Authorizes nothing: closed source and admission do not hide outstanding intents.
    b.command("DEL", SOURCE)
    set_admission_open(b, values, False)

    b.command("ACL", "LOG", "RESET")
    before = snapshot(b)
    listed = f.preparing(limit)
    assert snapshot(b) == before, "preparing wrote to the broker"
    assert b.command("ACL", "LOG") == [], "preparing needed more than the enqueue recipe"
    assert type(listed) is tuple
    assert all(type(item) is r.DeliveryIdentity for item in listed)
    assert [item.as_dict() for item in listed] == [
        second.identity.as_dict(),
        first.identity.as_dict(),
    ]
    assert [(item.agent_id, item.hook) for item in listed] == [(AGENT, HOOK)] * 2


def test_preparing_is_empty_with_only_committed_members_and_writes_nothing(admission_broker):
    """@spec PROTECTED-HOOK-ADMISSION-5 @spec PROTECTED-HOOK-ADMISSION-7."""
    b = admission_broker
    r, f, _ = setup_product(b, 3)
    for index in range(3):
        f.admit(
            request(
                r,
                delivery=f"delivery/c-{index}",
                payload=queued_payload(event_id=f"event/c-{index}"),
            )
        )
    before = snapshot(b)
    assert f.preparing(3) == ()
    assert snapshot(b) == before
    b.command("FLUSHDB")
    assert f.preparing(3) == ()
    assert b.command("KEYS", "*") == []


def alternate_authority(broker, values):
    """A coherent selected authority for another manifest than the trusted one.

    @spec PROTECTED-HOOK-ADMISSION-1 @spec PROTECTED-HOOK-ADMISSION-4.
    """
    manifest = copy.deepcopy(values[control_key(values, "manifest")])
    manifest["worker_image_digest"] = "sha256:" + "9" * 64
    digest = hashlib.sha256(canonical(manifest)).hexdigest()
    changes = {"protected:control:manifest:" + digest: manifest}
    for kind in ("qualification", "readiness", "selection"):
        key = control_key(values, kind)
        record = copy.deepcopy(values[key])
        record["manifest_digest"] = digest
        changes[key] = record
    for key, value in changes.items():
        broker.command("SET", key, canonical(value))


def differing_bytes(broker, values):
    """The trusted digest's control key holds other bytes, @spec PROTECTED-HOOK-ADMISSION-1."""
    key = control_key(values, "manifest")
    manifest = copy.deepcopy(values[key])
    manifest["worker_image_digest"] = "sha256:" + "9" * 64
    broker.command("SET", key, canonical(manifest))


@pytest.mark.parametrize(
    "differ", [alternate_authority, differing_bytes], ids=["selected", "bytes"]
)
def test_control_manifest_differing_from_trusted_refuses_with_no_write(admission_broker, differ):
    """@spec PROTECTED-HOOK-ADMISSION-1 @spec PROTECTED-HOOK-ADMISSION-4
    @spec PROTECTED-HOOK-ADMISSION-7."""
    b = admission_broker
    r, f, values = setup_product(b)
    differ(b, values)
    before = snapshot(b)
    out = f.admit(request(r)).as_dict()
    assert out == dict(status="refused", reason="runtime_unavailable", receipt=None)
    assert snapshot(b) == before


def test_recovery_under_a_manifest_differing_from_trusted_never_appends(admission_broker):
    """@spec PROTECTED-HOOK-ADMISSION-1 @spec PROTECTED-HOOK-ADMISSION-4/5."""
    b = admission_broker
    r, f, values = setup_product(b)
    original = request(r)
    owned = interrupt(b, r, f, original, "runs")
    alternate_authority(b, values)
    for operation in (lambda: f.recover(original.identity), lambda: f.admit(original)):
        out = operation().as_dict()
        assert out["status"] in {"preparing", "refused"}, "recovered under another manifest"
        assert out["receipt"] is None
    assert not stream(b)
    assert not b.command("EXISTS", owned["commit"])
    assert b.command("GET", owned["recovery"]) == original.queued_payload


def test_trusted_manifest_facade_accepts_with_the_product_recipe(admission_broker):
    """The facade derives the broker identity from the trusted manifest and accepts a
    valid tuple under the enqueue recipe alone. @spec PROTECTED-HOOK-ADMISSION-1/4."""
    b = admission_broker
    r, f, _ = setup_product(b)
    req = request(r)
    b.command("ACL", "LOG", "RESET")
    out = f.admit(req).as_dict()
    assert out["status"] == "accepted"
    assert b.command("ACL", "LOG") == []
    assert b.command("ZSCORE", "protected:admission:quota", keys(r, req)["d"]) is not None
