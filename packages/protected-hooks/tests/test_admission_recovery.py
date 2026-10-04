"""Actual partial Lua writes and exact recovery, @spec PROTECTED-HOOK-ADMISSION-4/5/7."""

import hashlib
import json

import pytest

from . import admission_broker as broker_helpers
from .admission_broker import (
    SOURCE,
    baseline_rules,
    canonical,
    install,
    module,
    queued_payload,
    request,
    safe_error,
    snapshot,
)
from .test_atomic_admission import setup_product

admission_service = broker_helpers.admission_service
admission_broker = broker_helpers.admission_broker


def keys(records, req):
    """Exact independently derived owned keys, @spec PROTECTED-HOOK-ADMISSION-2/4."""
    d = records.delivery_digest(req.identity)
    return {
        name: "protected:admission:" + name + ":" + d
        for name in ("intent", "state", "commit", "recovery")
    } | {
        "quota": "protected:admission:quota",
        "binding": "protected:admission:binding:" + json.loads(req.queued_payload)["event_id"],
        "runs": "curie:runs",
        "d": d,
    }


def deny_next(broker, owned, command, key=None):
    """Fixture-only exact-key selectors, @spec PROTECTED-HOOK-ADMISSION-4/5/6/7."""
    # Main rights still cover declared EVAL keys; inner write authority is separate.
    rules = tuple(
        t
        for t in baseline_rules("enqueue")
        if t not in {"+set", "+del", "+zadd", "+zrem", "+xadd", "+" + command}
    )
    setters = [
        owned[n]
        for n in ("intent", "state", "binding", "recovery", "commit")
        if not (command == "set" and owned[n] == key)
    ]
    rules += ("(+set " + " ".join("%W~" + name for name in setters) + ")",)
    for write, name in (
        ("del", "recovery"),
        ("zadd", "quota"),
        ("zrem", "quota"),
        ("xadd", "runs"),
    ):
        if write != command:
            rules += ("(+" + write + " %W~" + owned[name] + ")",)
    broker.command("ACL", "SETUSER", "enqueue", *rules)


def interrupt(broker, records, facade, req, stop):
    """Real broker command denial retains successful writes, @spec PROTECTED-HOOK-ADMISSION-4/7."""
    owned = keys(records, req)
    command = {
        "state": "set",
        "quota": "zadd",
        "binding": "set",
        "recovery": "set",
        "runs": "xadd",
        "delete": "del",
        "commit": "set",
    }[stop]
    deny_next(broker, owned, command, owned.get(stop))
    with pytest.raises(records.AdmissionUnavailable) as caught:
        facade.admit(req)
    safe_error(caught.value, broker)
    install(broker, module("admission_acl"))
    return owned


@pytest.mark.parametrize(
    "stop,retained",
    [
        ("state", {"intent"}),
        ("quota", {"intent", "state"}),
        ("binding", {"intent", "state", "quota"}),
        ("recovery", {"intent", "state", "quota", "binding"}),
        ("runs", {"intent", "state", "quota", "binding", "recovery"}),
        ("delete", {"intent", "state", "quota", "binding", "recovery", "runs"}),
        ("commit", {"intent", "state", "quota", "binding", "runs"}),
    ],
)
def test_real_failure_after_every_successful_write_has_no_false_acceptance(
    admission_broker, stop, retained
):
    """@spec PROTECTED-HOOK-ADMISSION-4/5/7 PROTECTED-HOOK-LANE-4."""
    b = admission_broker
    r, f, _ = setup_product(b)
    req = request(r)
    owned = interrupt(b, r, f, req, stop)
    assert {
        name
        for name in ("intent", "state", "quota", "binding", "recovery", "runs", "commit")
        if b.command("EXISTS", owned[name])
    } == retained
    original_intent = b.command("GET", owned["intent"])
    intent = r.parse_intent(original_intent).as_dict()
    if stop in {"state", "quota", "binding", "recovery"}:
        # Only authenticated retry possesses original bytes that were never retained.
        before = snapshot(b)
        out = f.recover(req.identity).as_dict()
        assert out["status"] == "preparing" and out["receipt"] is None
        assert b.command("GET", owned["intent"]) == original_intent
        assert not b.command("EXISTS", owned["runs"])
        assert snapshot(b) != before
        out = f.admit(req).as_dict()
    else:
        out = f.recover(req.identity).as_dict()
    assert out["status"] == "accepted" and out["reason"] is None
    assert out["receipt"]["stream_id"] == intent["reserved_stream_id"]
    assert b.command("GET", owned["intent"]) == original_intent
    assert b.command("XLEN", "curie:runs") == 1
    assert b.command("ZCARD", owned["quota"]) == 1
    assert not b.command("EXISTS", owned["recovery"])
    commit = r.parse_state(b.command("GET", owned["commit"])).as_dict()
    assert commit["status"] == "committed" and commit["receipt"] == out["receipt"]
    before = snapshot(b)
    retry = f.admit(req).as_dict()
    assert retry == dict(status="duplicate", reason=None, receipt=out["receipt"])
    assert snapshot(b) == before


def test_preparing_retry_cannot_replace_original_payload_or_event(admission_broker):
    """@spec PROTECTED-HOOK-ADMISSION-4/5."""
    b = admission_broker
    r, f, _ = setup_product(b)
    req = request(r)
    owned = interrupt(b, r, f, req, "recovery")
    before = snapshot(b)
    out = f.admit(
        request(r, payload=queued_payload(event_id="event/changed", text="changed"))
    ).as_dict()
    assert out == dict(status="conflict", reason="delivery_conflict", receipt=None)
    assert snapshot(b) == before
    assert not b.command("EXISTS", owned["recovery"])


@pytest.mark.parametrize("closed", ["source", "readiness", "selection"])
def test_closed_recovery_never_appends_and_tenth_attempt_fails_refunds(admission_broker, closed):
    """@spec PROTECTED-HOOK-ADMISSION-5 PROTECTED-HOOK-SOURCE-6 PROTECTED-HOOK-LANE-4."""
    b = admission_broker
    r, f, values = setup_product(b)
    req = request(r)
    owned = interrupt(b, r, f, req, "runs")
    original = b.command("GET", owned["intent"])
    key = SOURCE if closed == "source" else next(k for k in values if ":" + closed + ":" in k)
    b.command("DEL", key)
    for attempt in range(1, 11):
        out = f.recover(req.identity).as_dict()
        assert out["status"] == ("preparing" if attempt < 10 else "failed")
        state = r.parse_state(b.command("GET", owned["state"])).as_dict()
        assert state["recovery_attempts"] == attempt
        assert not b.command("EXISTS", "curie:runs")
        assert b.command("GET", owned["intent"]) == original
    assert out["reason"] == "attempts_exhausted"
    assert b.command("ZCARD", owned["quota"]) == 0
    assert not b.command("EXISTS", owned["recovery"])
    assert b.command("EXISTS", owned["binding"])
    before = snapshot(b)
    assert f.recover(req.identity).as_dict() == out
    assert snapshot(b) == before


def test_deadline_uses_actual_broker_time_and_terminal_cleanup_needs_no_source(admission_broker):
    """@spec PROTECTED-HOOK-ADMISSION-5."""
    b = admission_broker
    r, f, _ = setup_product(b)
    req = request(r)
    owned = interrupt(b, r, f, req, "runs")
    sec, usec = b.command("TIME")
    now = int(sec) * 1000 + int(usec) // 1000
    intent = r.parse_intent(b.command("GET", owned["intent"])).as_dict()
    intent.update(created_at_ms=str(now - 300001), deadline_ms=str(now - 1))
    b.command("SET", owned["intent"], canonical(intent))
    b.command("DEL", SOURCE)
    out = f.recover(req.identity).as_dict()
    assert out == dict(status="failed", reason="deadline", receipt=None)
    assert not b.command("EXISTS", owned["recovery"])
    assert b.command("ZCARD", owned["quota"]) == 0
    assert b.command("GET", owned["intent"]) == canonical(intent)
    assert b.command("EXISTS", owned["binding"])
    before = snapshot(b)
    assert f.recover(req.identity).as_dict() == out
    assert snapshot(b) == before


def test_absent_reserved_id_after_stream_advance_fails_without_new_identity(admission_broker):
    """@spec PROTECTED-HOOK-ADMISSION-5."""
    b = admission_broker
    r, f, _ = setup_product(b)
    req = request(r)
    owned = interrupt(b, r, f, req, "runs")
    intent = r.parse_intent(b.command("GET", owned["intent"])).as_dict()
    milliseconds, sequence = map(int, intent["reserved_stream_id"].split("-"))
    b.command("XADD", "curie:runs", f"{milliseconds}-{sequence + 1}", "payload", "unrelated")
    out = f.recover(req.identity).as_dict()
    assert out == dict(status="failed", reason="stream_id_unappendable", receipt=None)
    assert b.command("XLEN", "curie:runs") == 1
    assert b.command("ZCARD", owned["quota"]) == 0
    assert not b.command("EXISTS", owned["recovery"])


@pytest.mark.parametrize(
    "field,raw",
    [
        ("payload", b"corrupted payload"),
        ("protected_envelope", b"corrupted envelope"),
        ("extra", b"unowned field"),
    ],
)
def test_wrong_existing_reserved_entry_never_commits_or_overwrites(admission_broker, field, raw):
    """SHA256 in Python, exact XRANGE snapshot in Lua, @spec PROTECTED-HOOK-ADMISSION-5."""
    b = admission_broker
    r, f, _ = setup_product(b)
    req = request(r)
    owned = interrupt(b, r, f, req, "runs")
    intent = r.parse_intent(b.command("GET", owned["intent"])).as_dict()
    envelope = b.command("GET", owned["binding"])
    fields = {"payload": req.queued_payload, "protected_envelope": envelope, field: raw}
    args = [part for item in fields.items() for part in item]
    b.command("XADD", "curie:runs", intent["reserved_stream_id"], *args)
    before = snapshot(b)
    with pytest.raises(r.AdmissionUnavailable) as caught:
        f.recover(req.identity)
    safe_error(caught.value, b)
    assert snapshot(b) == before
    assert not b.command("EXISTS", owned["commit"])


def test_missing_quota_repair_cannot_overfill_after_another_delivery(admission_broker):
    """@spec PROTECTED-HOOK-ADMISSION-5."""
    b = admission_broker
    r, f, _ = setup_product(b, 1)
    req = request(r)
    owned = interrupt(b, r, f, req, "runs")
    b.command("ZREM", owned["quota"], owned["d"])
    second = request(r, delivery="delivery/second", payload=queued_payload(event_id="event/second"))
    interrupt(b, r, f, second, "runs")
    out = f.recover(req.identity).as_dict()
    assert out["status"] == "preparing" and out["receipt"] is None
    assert b.command("ZCARD", owned["quota"]) == 1
    assert b.command("ZSCORE", owned["quota"], owned["d"]) is None
    assert not b.command("EXISTS", "curie:runs")


def test_exact_existing_entry_after_deleted_recovery_commits_original_receipt(admission_broker):
    """@spec PROTECTED-HOOK-ADMISSION-4/5."""
    b = admission_broker
    r, f, _ = setup_product(b)
    req = request(r)
    owned = interrupt(b, r, f, req, "commit")
    intent = r.parse_intent(b.command("GET", owned["intent"])).as_dict()
    entry = b.raw_command(
        "XRANGE", "curie:runs", intent["reserved_stream_id"], intent["reserved_stream_id"]
    )[0]
    assert hashlib.sha256(entry[1][1]).hexdigest() == intent["payload_sha256"]
    assert hashlib.sha256(entry[1][3]).hexdigest() == intent["envelope_sha256"]
    assert not b.command("EXISTS", owned["recovery"])
    out = f.recover(req.identity).as_dict()
    assert out["status"] == "accepted" and out["receipt"]["stream_id"] == entry[0].decode()
    assert b.raw_command("XRANGE", "curie:runs", "-", "+") == [entry]


@pytest.mark.parametrize("stop", ["quota", "state"])
def test_terminal_cleanup_real_partial_failure_is_idempotent(admission_broker, stop):
    """@spec PROTECTED-HOOK-ADMISSION-5/7."""
    b = admission_broker
    r, f, _ = setup_product(b)
    req = request(r)
    owned = interrupt(b, r, f, req, "runs")
    sec, usec = b.command("TIME")
    now = int(sec) * 1000 + int(usec) // 1000
    intent = r.parse_intent(b.command("GET", owned["intent"])).as_dict()
    intent.update(created_at_ms=str(now - 300001), deadline_ms=str(now - 1))
    b.command("SET", owned["intent"], canonical(intent))
    if stop == "quota":
        deny_next(b, owned, "zrem")
    else:
        deny_next(b, owned, "set", owned["state"])
    with pytest.raises(r.AdmissionUnavailable) as caught:
        f.recover(req.identity)
    safe_error(caught.value, b)
    assert not b.command("EXISTS", owned["recovery"])
    assert (b.command("ZSCORE", owned["quota"], owned["d"]) is not None) == (stop == "quota")
    assert not b.command("EXISTS", owned["commit"])
    install(b, module("admission_acl"))
    out = f.recover(req.identity).as_dict()
    assert out == dict(status="failed", reason="deadline", receipt=None)
    assert b.command("ZCARD", owned["quota"]) == 0
    before = snapshot(b)
    assert f.recover(req.identity).as_dict() == out
    assert snapshot(b) == before


def test_recovery_restart_same_port_retained_preparation_refuses_epoch_change(admission_broker):
    """@spec PROTECTED-HOOK-ADMISSION-5/7 PROTECTED-HOOK-LANE-2."""
    b = admission_broker
    r, f, _ = setup_product(b)
    req = request(r)
    owned = interrupt(b, r, f, req, "runs")
    old_run = b.command("INFO", "server")["run_id"]
    retained = {k: b.command("DUMP", k) for k in b.command("KEYS", "*")}
    b.restart()
    for key, raw in retained.items():
        b.command("RESTORE", key, 0, raw, "REPLACE")
    assert b.command("INFO", "server")["run_id"] != old_run
    before = snapshot(b)
    try:
        out = f.recover(req.identity).as_dict()
        assert out["status"] == "refused" and out["reason"] == "broker_identity_mismatch"
    except r.AdmissionUnavailable as error:
        safe_error(error, b)
    assert snapshot(b) == before
    assert not b.command("EXISTS", owned["commit"])


def test_terminal_state_and_committed_record_contradiction_never_dispatchable(admission_broker):
    """@spec PROTECTED-HOOK-ADMISSION-3/5."""
    b = admission_broker
    r, f, _ = setup_product(b)
    req = request(r)
    assert f.admit(req).as_dict()["status"] == "accepted"
    owned = keys(r, req)
    b.command(
        "SET",
        owned["state"],
        canonical(
            dict(
                schema_version=1,
                status="failed",
                recovery_attempts=1,
                reason="deadline",
                receipt=None,
            )
        ),
    )
    before = snapshot(b)
    for operation in (lambda: f.admit(req), lambda: f.recover(req.identity)):
        with pytest.raises(r.AdmissionUnavailable) as caught:
            operation()
        safe_error(caught.value, b)
    assert snapshot(b) == before


def test_real_committed_response_loss_is_safe_and_retry_never_duplicates(admission_broker):
    """@spec PROTECTED-HOOK-ADMISSION-1/4/5/7 PROTECTED-HOOK-LANE-4."""
    from .admission_broker import ResponseDropRelay, client

    b = admission_broker
    r, direct, _ = setup_product(b)
    req = request(r)
    owned = keys(r, req)
    atomic = module("atomic_admission")
    c = client(b)
    with ResponseDropRelay(b, owned["commit"]) as relay:
        c.connection_pool.connection_kwargs["port"] = relay.port
        f = atomic.AtomicAdmission(
            c,
            broker_identity=b.manifest().as_dict()["broker_identity"],
            trusted_max_readiness_ms=60000,
            backlog_limit=100,
        )
        try:
            with pytest.raises(r.AdmissionUnavailable) as caught:
                f.admit(req)
            safe_error(caught.value, b)
            assert relay.dropped
        finally:
            c.close()
    state = r.parse_state(b.command("GET", owned["commit"])).as_dict()
    assert state["status"] == "committed"
    assert b.command("XLEN", "curie:runs") == b.command("ZCARD", owned["quota"]) == 1
    before = snapshot(b)
    assert direct.admit(req).as_dict() == dict(
        status="duplicate", reason=None, receipt=state["receipt"]
    )
    assert snapshot(b) == before


@pytest.mark.parametrize(
    "damage",
    [
        "malformed_commit",
        "wrong_type_commit",
        "orphan_commit",
        "missing_binding",
        "receipt_event",
        "receipt_stream",
        "receipt_digest",
    ],
)
def test_commit_requires_exact_original_evidence_and_binding(admission_broker, damage):
    """@spec PROTECTED-HOOK-ADMISSION-3/4/5."""
    b = admission_broker
    r, f, _ = setup_product(b)
    req = request(r)
    assert f.admit(req).as_dict()["status"] == "accepted"
    owned = keys(r, req)
    if damage == "malformed_commit":
        b.command("SET", owned["commit"], b"{}")
    elif damage == "wrong_type_commit":
        b.command("DEL", owned["commit"])
        b.command("LPUSH", owned["commit"], "wrong")
    elif damage == "orphan_commit":
        b.command("DEL", owned["intent"])
    elif damage == "missing_binding":
        b.command("DEL", owned["binding"])
    else:
        state = r.parse_state(b.command("GET", owned["commit"])).as_dict()
        field = {
            "receipt_event": "event_id",
            "receipt_stream": "stream_id",
            "receipt_digest": "payload_sha256",
        }[damage]
        state["receipt"][field] = {
            "receipt_event": "event/other",
            "receipt_stream": "1-0",
            "receipt_digest": "0" * 64,
        }[damage]
        b.command("SET", owned["commit"], canonical(state))
    before = snapshot(b)
    for operation in (lambda: f.admit(req), lambda: f.recover(req.identity)):
        with pytest.raises(r.AdmissionUnavailable) as caught:
            operation()
        safe_error(caught.value, b)
    assert snapshot(b) == before
