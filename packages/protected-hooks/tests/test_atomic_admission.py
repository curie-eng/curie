"""Real isolated atomic admissions, @spec PROTECTED-HOOK-ADMISSION-4/7."""

import hashlib
from concurrent.futures import ThreadPoolExecutor

import pytest

from . import admission_broker as broker_helpers
from .admission_broker import (
    SOURCE,
    canonical,
    facade,
    install,
    module,
    queued_payload,
    request,
    safe_error,
    seed,
    snapshot,
)

admission_broker = broker_helpers.admission_broker
admission_service = broker_helpers.admission_service


def setup_product(broker, limit=100):
    """Called after missing module assertion, @spec PROTECTED-HOOK-ADMISSION-1/7."""
    records = module("admission_records")
    atomic = module("atomic_admission")
    acl = module("admission_acl")
    install(broker, acl)
    authority = seed(broker)
    return records, facade(broker, atomic, limit), authority


def test_real_acceptance_exact_bytes_metadata_and_no_ttl(admission_broker):
    """@spec PROTECTED-HOOK-ADMISSION-3/4 PROTECTED-HOOK-SOURCE-6 PROTECTED-HOOK-LANE-4."""
    b = admission_broker
    r, f, _ = setup_product(b)
    req = request(r)
    before = b.command("TIME")
    out = f.admit(req).as_dict()
    after = b.command("TIME")
    assert set(out) == {"status", "reason", "receipt"}
    assert out["status"] == "accepted" and out["reason"] is None
    receipt = out["receipt"]
    d = r.delivery_digest(req.identity)
    intent = r.parse_intent(b.command("GET", "protected:admission:intent:" + d)).as_dict()
    assert (
        int(before[0]) * 1000 + int(before[1]) // 1000
        <= int(intent["created_at_ms"])
        <= int(after[0]) * 1000 + int(after[1]) // 1000
    )
    assert int(intent["deadline_ms"]) == int(intent["created_at_ms"]) + 300000
    assert b.command("ZCARD", "protected:admission:quota") == 1
    assert b.command("ZSCORE", "protected:admission:quota", d) == int(intent["created_at_ms"])
    stream = b.raw_command("XRANGE", "curie:runs", "-", "+")
    assert len(stream) == 1
    sid, fields = stream[0]
    assert sid.decode() == receipt["stream_id"] == intent["reserved_stream_id"]
    assert len(fields) == 4 and fields[::2] == [b"payload", b"protected_envelope"]
    assert fields[1] == req.queued_payload
    assert hashlib.sha256(fields[1]).hexdigest() == intent["payload_sha256"]
    assert hashlib.sha256(fields[3]).hexdigest() == intent["envelope_sha256"]
    assert b.command("GET", "protected:admission:binding:" + receipt["event_id"]) == fields[3]
    assert not b.command("EXISTS", "protected:admission:recovery:" + d)
    assert all(b.command("PTTL", k) == -1 for k in b.command("KEYS", "protected:admission:*"))
    old = snapshot(b)
    duplicate = f.admit(req).as_dict()
    assert duplicate == dict(status="duplicate", reason=None, receipt=receipt)
    assert snapshot(b) == old


def test_concurrent_delivery_has_one_intent_member_and_entry(admission_broker):
    """@spec PROTECTED-HOOK-ADMISSION-4/7 PROTECTED-HOOK-LANE-4."""
    b = admission_broker
    r, f, _ = setup_product(b, 1)
    req = request(r)
    with ThreadPoolExecutor(max_workers=8) as pool:
        outputs = list(pool.map(lambda _: f.admit(req).as_dict(), range(24)))
    assert sum(o["status"] == "accepted" for o in outputs) == 1
    assert all(o["status"] in {"accepted", "duplicate"} for o in outputs)
    assert all(o["receipt"] == outputs[0]["receipt"] for o in outputs)
    assert len(b.command("KEYS", "protected:admission:intent:*")) == 1
    assert b.command("ZCARD", "protected:admission:quota") == b.command("XLEN", "curie:runs") == 1


@pytest.mark.parametrize("closed", ["readiness", "selection", "manifest", "quota", "trim"])
def test_duplicate_precedes_closed_or_rotated_control_and_new_payload(admission_broker, closed):
    """Original authenticated receipt survives rollout, @spec PROTECTED-HOOK-ADMISSION-4
    PROTECTED-HOOK-SOURCE-8."""
    b = admission_broker
    r, f, values = setup_product(b, 1)
    original = f.admit(request(r)).as_dict()["receipt"]
    if closed == "trim":
        b.command("XTRIM", "curie:runs", "MAXLEN", "=", 0)
    elif closed != "quota":
        key = next(k for k in values if ":" + closed + ":" in k)
        b.command("DEL", key)
    before = snapshot(b)
    changed_payload = queued_payload(event_id="event/rollout", text="newly reconstructed payload")
    out = f.admit(request(r, payload=changed_payload)).as_dict()
    assert out == dict(status="duplicate", reason=None, receipt=original)
    assert snapshot(b) == before


@pytest.mark.parametrize("change", ["body", "requested", "generation", "operation", "fingerprint"])
def test_changed_authenticated_duplicate_conflicts_without_effect(admission_broker, change):
    """@spec PROTECTED-HOOK-ADMISSION-4 PROTECTED-HOOK-SOURCE-6/8."""
    from .admission_broker import source_policy

    b = admission_broker
    r, f, _ = setup_product(b)
    f.admit(request(r))
    p = source_policy()
    options = {}
    if change == "body":
        options["body"] = "b" * 64
    elif change == "requested":
        options["requested"] = "read-only"
    else:
        field = {
            "generation": "generation",
            "operation": "operation_id",
            "fingerprint": "legacy_generation",
        }[change]
        p[field] = "2" if field != "operation_id" else "55555555-5555-4555-8555-555555555555"
        from curie_protected_hooks.source_policy_records import policy_fingerprint

        b.command(
            "SET",
            SOURCE,
            canonical(
                dict(
                    floor=p["generation"],
                    operation_id=p["operation_id"],
                    active=dict(
                        generation=p["generation"],
                        operation_id=p["operation_id"],
                        mode="protected",
                        policy_fingerprint=policy_fingerprint(p),
                    ),
                )
            ),
        )
    before = snapshot(b)
    assert f.admit(request(r, policy=p, **options)).as_dict() == dict(
        status="conflict", reason="delivery_conflict", receipt=None
    )
    assert snapshot(b) == before


@pytest.mark.parametrize(
    "case",
    [
        "source_missing",
        "source_revoked",
        "source_floor",
        "selection_missing",
        "admission_closed",
        "manifest_missing",
        "qualification_missing",
        "readiness_missing",
        "readiness_expired",
        "readiness_future",
        "run_id",
        "quota",
    ],
)
def test_every_authority_time_capacity_refusal_writes_nothing(admission_broker, case):
    """@spec PROTECTED-HOOK-ADMISSION-4/7 PROTECTED-HOOK-LANE-2 PROTECTED-HOOK-SOURCE-6."""
    b = admission_broker
    r, f, values = setup_product(b, 1)
    name = case.split("_")[0]
    if case == "quota":
        b.command("ZADD", "protected:admission:quota", 1, "other-delivery")
    else:
        key = (
            SOURCE
            if name == "source"
            else next(
                k
                for k in values
                if ":" + ("selection" if case in {"admission_closed", "run_id"} else name) + ":"
                in k
            )
        )
        value = values[key]
        if case.endswith("missing"):
            b.command("DEL", key)
        else:
            if case == "source_revoked":
                value["active"] = None
            elif case == "source_floor":
                value["floor"] = "2"
            elif case == "admission_closed":
                value["admission_open"] = False
            elif case == "run_id":
                value["broker_run_id"] = "0" * 40
            else:
                sec, usec = b.command("TIME")
                now = int(sec) * 1000 + int(usec) // 1000
                value.update(
                    issued_at_ms=str(now - 60000 if case.endswith("expired") else now + 60000),
                    expires_at_ms=str(now if case.endswith("expired") else now + 120000),
                )
            b.command("SET", key, canonical(value))
    before = snapshot(b)
    try:
        output = f.admit(request(r)).as_dict()
        assert output["status"] == "refused" and output["receipt"] is None
    except r.AdmissionUnavailable as error:
        safe_error(error, b)
    assert snapshot(b) == before


@pytest.mark.parametrize(
    "family", ["intent", "state", "commit", "recovery", "binding", "quota", "runs"]
)
def test_wrong_key_type_preflight_has_no_writes(admission_broker, family):
    """@spec PROTECTED-HOOK-ADMISSION-4/7."""
    b = admission_broker
    r, f, _ = setup_product(b)
    req = request(r)
    d = r.delivery_digest(req.identity)
    key = (
        "curie:runs"
        if family == "runs"
        else "protected:admission:"
        + family
        + ("" if family == "quota" else ":" + ("event/example" if family == "binding" else d))
    )
    b.command("LPUSH", key, "wrong-type")
    before = snapshot(b)
    try:
        output = f.admit(req).as_dict()
        assert output["status"] == "refused"
    except r.AdmissionUnavailable as error:
        safe_error(error, b)
    assert snapshot(b) == before


@pytest.mark.parametrize(
    "last,expected",
    [
        ("9007199254740993-9007199254740993", "9007199254740993-9007199254740994"),
        ("9007199254740993-18446744073709551615", "9007199254740994-0"),
        ("18446744073709551615-18446744073709551614", "18446744073709551615-18446744073709551615"),
    ],
)
def test_uint64_reservation_increment_and_carry(admission_broker, last, expected):
    """@spec PROTECTED-HOOK-ADMISSION-4."""
    b = admission_broker
    r, f, _ = setup_product(b)
    b.command("XADD", "curie:runs", last, "payload", "unrelated")
    out = f.admit(request(r)).as_dict()
    assert out["status"] == "accepted" and out["receipt"]["stream_id"] == expected


def test_uint64_overflow_no_effect(admission_broker):
    """@spec PROTECTED-HOOK-ADMISSION-4."""
    b = admission_broker
    r, f, _ = setup_product(b)
    b.command(
        "XADD", "curie:runs", "18446744073709551615-18446744073709551615", "payload", "unrelated"
    )
    before = snapshot(b)
    try:
        assert f.admit(request(r)).as_dict()["status"] == "refused"
    except r.AdmissionUnavailable as error:
        safe_error(error, b)
    assert snapshot(b) == before


def test_live_restart_retained_keys_refuses_old_identity(admission_broker):
    """@spec PROTECTED-HOOK-ADMISSION-4/7 PROTECTED-HOOK-LANE-2."""
    b = admission_broker
    r, f, _ = setup_product(b)
    old = b.command("INFO", "server")["run_id"]
    retained = {key: b.command("GET", key) for key in b.command("KEYS", "*")}
    b.restart()
    for key, value in retained.items():
        b.command("SET", key, value)
    assert b.command("INFO", "server")["run_id"] != old
    before = snapshot(b)
    try:
        assert f.admit(request(r)).as_dict()["status"] == "refused"
    except r.AdmissionUnavailable as error:
        safe_error(error, b)
    assert snapshot(b) == before


@pytest.mark.parametrize(
    "kind,field,replacement",
    [
        ("selection", "runtime_generation", "2"),
        ("selection", "qualification_generation", "2"),
        ("selection", "manifest_digest", "0" * 64),
        ("selection", "runtime_id", "55555555-5555-4555-8555-555555555555"),
        ("selection", "qualification_id", "55555555-5555-4555-8555-555555555555"),
        ("manifest", "runtime_generation", "2"),
        ("manifest", "runtime_id", "55555555-5555-4555-8555-555555555555"),
        ("manifest", "qualification_id", "55555555-5555-4555-8555-555555555555"),
        ("manifest", "runner_image_digest", "sha256:" + "0" * 64),
        ("manifest", "worker_image_digest", "sha256:" + "0" * 64),
        ("manifest", "execution_config_digest", "0" * 64),
        ("qualification", "runtime_generation", "2"),
        ("qualification", "qualification_generation", "2"),
        ("qualification", "manifest_digest", "0" * 64),
        ("qualification", "runtime_id", "55555555-5555-4555-8555-555555555555"),
        ("qualification", "qualification_id", "55555555-5555-4555-8555-555555555555"),
        ("qualification", "execution_config_digest", "0" * 64),
        ("readiness", "runtime_generation", "2"),
        ("readiness", "qualification_generation", "2"),
        ("readiness", "manifest_digest", "0" * 64),
        ("readiness", "runtime_id", "55555555-5555-4555-8555-555555555555"),
        ("readiness", "qualification_id", "55555555-5555-4555-8555-555555555555"),
    ],
)
def test_each_independent_control_binding_mismatch_has_no_effect(
    admission_broker, kind, field, replacement
):
    """@spec PROTECTED-HOOK-ADMISSION-4 PROTECTED-HOOK-LANE-2."""
    b = admission_broker
    r, f, values = setup_product(b)
    key = next(k for k in values if ":" + kind + ":" in k)
    values[key][field] = replacement
    b.command("SET", key, canonical(values[key]))
    before = snapshot(b)
    try:
        assert f.admit(request(r)).as_dict()["status"] == "refused"
    except r.AdmissionUnavailable as error:
        safe_error(error, b)
    assert snapshot(b) == before


@pytest.mark.parametrize("kind", ["manifest", "qualification", "readiness"])
@pytest.mark.parametrize(
    "path,value",
    [
        (("broker_identity", "run_id"), "0" * 40),
        (("broker_identity", "instance_id"), "55555555-5555-4555-8555-555555555555"),
        (("broker_identity", "endpoint", "host"), "other.example.test"),
        (("broker_identity", "endpoint", "port"), 6380),
        (("broker_identity", "tls_server_name"), "other.example.test"),
        (("broker_identity", "tls_spki_sha256"), "0" * 64),
        (("guard_identity", "control_id"), "55555555-5555-4555-8555-555555555555"),
        (("guard_identity", "revision"), "2"),
        (("guard_identity", "config_sha256"), "0" * 64),
    ],
)
def test_every_nested_authority_identity_component_compared(admission_broker, kind, path, value):
    """@spec PROTECTED-HOOK-ADMISSION-4 PROTECTED-HOOK-LANE-2."""
    b = admission_broker
    r, f, values = setup_product(b)
    key = next(k for k in values if ":" + kind + ":" in k)
    node = values[key]
    for field in path[:-1]:
        node = node[field]
    node[path[-1]] = value
    b.command("SET", key, canonical(values[key]))
    before = snapshot(b)
    try:
        assert f.admit(request(r)).as_dict()["status"] == "refused"
    except r.AdmissionUnavailable as error:
        safe_error(error, b)
    assert snapshot(b) == before


@pytest.mark.parametrize("kind", ["source", "selection", "manifest", "qualification", "readiness"])
@pytest.mark.parametrize(
    "raw", [b"{}", b"\xff", b"null", b'{"schema_version":1,"schema_version":1}', b"x" * 16385]
)
def test_malformed_authority_is_safe_unavailable_no_effect(admission_broker, kind, raw):
    """@spec PROTECTED-HOOK-ADMISSION-1/2/4 PROTECTED-HOOK-SOURCE-6."""
    b = admission_broker
    r, f, values = setup_product(b)
    key = SOURCE if kind == "source" else next(k for k in values if ":" + kind + ":" in k)
    b.command("SET", key, raw)
    before = snapshot(b)
    with pytest.raises(r.AdmissionUnavailable) as caught:
        f.admit(request(r))
    safe_error(caught.value, b)
    assert snapshot(b) == before


def test_event_binding_collision_and_orphan_data_are_not_replaced(admission_broker):
    """@spec PROTECTED-HOOK-ADMISSION-4."""
    b = admission_broker
    r, f, _ = setup_product(b)
    b.command("SET", "protected:admission:binding:event/example", b"unrelated envelope")
    before = snapshot(b)
    try:
        assert f.admit(request(r)).as_dict()["status"] == "refused"
    except r.AdmissionUnavailable as error:
        safe_error(error, b)
    assert snapshot(b) == before
