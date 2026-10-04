"""Closed immutable admission records, @spec PROTECTED-HOOK-ADMISSION-1/2/3/7."""

import hashlib
import json

import pytest

from .admission_broker import AGENT, HOOK, canonical, module, queued_payload, request, source_policy


def examples():
    """Independent complete wire vectors, @spec PROTECTED-HOOK-ADMISSION-3."""
    p = source_policy()
    identity = dict(agent_id=AGENT, hook=HOOK, delivery_id="delivery/example")
    envelope = dict(
        schema_version=1,
        event_id="event/example",
        source_revision="1",
        runtime_id=p["runtime_id"],
        runtime_generation="1",
        manifest_digest="b" * 64,
        qualification_id=p["qualification_id"],
        runner_image_digest="sha256:" + "e" * 64,
        bundle_digest=p["bundle_digest"],
        execution_config_digest="1" * 64,
        logical_conversation_key="conversation/example",
        execution_session_key="protected:"
        + p["runtime_id"]
        + ":1:"
        + hashlib.sha256(canonical([p["runtime_id"], "1", "conversation/example"])).hexdigest(),
        payload_sha256=hashlib.sha256(queued_payload()).hexdigest(),
    )
    intent = dict(
        schema_version=1,
        identity=identity,
        requested_tool_access=None,
        effective_tool_access="read-only",
        request_body_sha256="a" * 64,
        source_generation="1",
        source_operation_id=p["operation_id"],
        policy_fingerprint=hashlib.sha256(canonical(p)).hexdigest(),
        manifest_digest=envelope["manifest_digest"],
        runtime_id=p["runtime_id"],
        runtime_generation="1",
        qualification_id=p["qualification_id"],
        event_id=envelope["event_id"],
        conversation_id="conversation/example",
        payload_sha256=envelope["payload_sha256"],
        envelope_sha256=hashlib.sha256(canonical(envelope)).hexdigest(),
        reserved_stream_id="1000-0",
        created_at_ms="1000",
        deadline_ms="301000",
    )
    receipt = {
        k: v
        for k, v in intent.items()
        if k not in {"created_at_ms", "deadline_ms", "reserved_stream_id", "envelope_sha256"}
    }
    receipt.update(stream_id="1000-0", acceptance_status="accepted", tool_access="read-only")
    return dict(
        envelope=envelope,
        intent=intent,
        receipt=receipt,
        state=dict(
            schema_version=1, status="committed", recovery_attempts=0, reason=None, receipt=receipt
        ),
    )


@pytest.mark.parametrize("kind", ["envelope", "intent", "receipt", "state"])
def test_canonical_records_are_closed_immutable_and_copy_out(kind):
    """@spec PROTECTED-HOOK-ADMISSION-2/3 PROTECTED-HOOK-LANE-2."""
    r = module("admission_records")
    data = examples()[kind]
    parsed = getattr(r, "parse_" + kind)(json.dumps(data, indent=2).encode())
    assert parsed.canonical_bytes == canonical(data)
    assert parsed.as_dict() == data
    output = parsed.as_dict()
    output["schema_version"] = 99
    assert parsed.as_dict() == data
    if kind == "state":
        output["receipt"]["identity"]["delivery_id"] = "changed"
        assert parsed.as_dict() == data
    with pytest.raises((AttributeError, TypeError)):
        parsed.canonical_bytes = b"{}"
    assert "anonymous prompt" not in repr(parsed)


@pytest.mark.parametrize("kind", ["envelope", "intent", "receipt", "state"])
@pytest.mark.parametrize(
    "violation", ["extra", "missing", "duplicate", "bool", "float", "utf8", "nonfinite", "size"]
)
def test_record_invalid_grammar_and_bounds(kind, violation):
    """@spec PROTECTED-HOOK-ADMISSION-2/3."""
    r = module("admission_records")
    data = examples()[kind]
    if violation == "extra":
        data["secret"] = "private marker"
    elif violation == "missing":
        del data["schema_version"]
    elif violation in ("bool", "float"):
        data["schema_version"] = True if violation == "bool" else 1.0
    raw = canonical(data)
    if violation == "duplicate":
        raw = raw[:-1] + b',"schema_version":1}'
    elif violation == "utf8":
        raw = b"\xff"
    elif violation == "nonfinite":
        raw = b'{"schema_version":NaN}'
    elif violation == "size":
        raw = raw + b" " * 16385
    with pytest.raises((ValueError, r.AdmissionUnavailable)) as caught:
        getattr(r, "parse_" + kind)(raw)
    assert "private marker" not in str(caught.value)


@pytest.mark.parametrize("kind", ["intent", "receipt", "state"])
def test_recursive_duplicate_fields_rejected(kind):
    """@spec PROTECTED-HOOK-ADMISSION-2/3."""
    r = module("admission_records")
    raw = canonical(examples()[kind])
    raw = raw.replace(
        b'"delivery_id":"delivery/example"',
        b'"delivery_id":"delivery/example","delivery_id":"delivery/example"',
    )
    with pytest.raises((ValueError, r.AdmissionUnavailable)):
        getattr(r, "parse_" + kind)(raw)


@pytest.mark.parametrize(
    "field,bad",
    [
        ("delivery_id", ""),
        ("delivery_id", "x" * 1025),
        ("delivery_id", "é" * 513),
        ("delivery_id", "x\n"),
        ("delivery_id", "x\x7f"),
        ("delivery_id", True),
        ("agent_id", "bad"),
        ("hook", "Upper"),
        ("hook", "x" * 64),
    ],
)
def test_identity_constructor_grammar(field, bad):
    """@spec PROTECTED-HOOK-ADMISSION-2 PROTECTED-HOOK-SOURCE-1."""
    r = module("admission_records")
    fields = dict(agent_id=AGENT, hook=HOOK, delivery_id="delivery/example")
    fields[field] = bad
    with pytest.raises((ValueError, r.AdmissionUnavailable)):
        r.DeliveryIdentity(**fields)


def test_identity_and_execution_domain_hash_independent_vectors():
    """Canonical array framing cannot collide, @spec PROTECTED-HOOK-ADMISSION-2/3."""
    r = module("admission_records")
    identity = r.DeliveryIdentity(agent_id=AGENT, hook=HOOK, delivery_id="a:b/é")
    assert (
        r.delivery_digest(identity) == hashlib.sha256(canonical([AGENT, HOOK, "a:b/é"])).hexdigest()
    )
    runtime = source_policy()["runtime_id"]
    assert (
        r.execution_session_key(runtime, "9007199254740993", "logical:é")
        == f"protected:{runtime}:9007199254740993:"
        + hashlib.sha256(canonical([runtime, "9007199254740993", "logical:é"])).hexdigest()
    )
    assert r.execution_session_key(runtime, "1", "ab:c") != r.execution_session_key(
        runtime, "1", "a:bc"
    )


@pytest.mark.parametrize("source", ["webhook", "cron"])
def test_actual_aci_sources_and_request_immutability(source):
    """@spec PROTECTED-HOOK-ADMISSION-2 PROTECTED-HOOK-SOURCE-8."""
    r = module("admission_records")
    p = source_policy()
    hook_run = (
        None
        if source == "webhook"
        else dict(agent_id=AGENT, name=HOOK, slot_utc="2026-10-03T00:00:00Z")
    )
    payload = queued_payload(source=source, hook_run=hook_run)
    req = request(r, policy=p, payload=payload)
    p["generation"] = "99"
    assert req.source_policy["generation"] == "1"
    assert req.queued_payload == payload
    with pytest.raises((AttributeError, TypeError)):
        req.queued_payload = b"changed"
    assert "anonymous prompt" not in repr(req)


@pytest.mark.parametrize(
    "change",
    [
        dict(source="slack"),
        dict(tool_access=None),
        dict(event_id="bad event"),
        dict(conversation_id=""),
        dict(conversation_id="x" * 1025),
        dict(
            source="cron",
            reply_handle=None,
            hook_run=dict(agent_id=AGENT, name=HOOK, slot_utc="2026-10-03T00:00:00Z"),
        ),
        dict(
            source="cron",
            hook_run=dict(agent_id=AGENT, name="wrong", slot_utc="2026-10-03T00:00:00Z"),
        ),
        dict(
            attachments=[
                dict(id="file/example", filename="file.txt", mime_type="text/plain", size_bytes=1)
            ]
        ),
    ],
)
def test_unsupported_protected_turns_refuse(change):
    """@spec PROTECTED-HOOK-ADMISSION-2 PROTECTED-HOOK-SOURCE-8."""
    r = module("admission_records")
    value = json.loads(queued_payload())
    value.update(change)
    with pytest.raises((ValueError, r.AdmissionUnavailable)):
        request(r, payload=canonical(value))


@pytest.mark.parametrize("bad", [True, "read-write", "approval-required", 1])
def test_requested_policy_is_strict(bad):
    """@spec PROTECTED-HOOK-ADMISSION-2."""
    r = module("admission_records")
    with pytest.raises((ValueError, r.AdmissionUnavailable)):
        request(r, requested=bad)


@pytest.mark.parametrize(
    "stream_id", ["9007199254740993-9007199254740993", "18446744073709551615-18446744073709551615"]
)
def test_uint64_ids_roundtrip_without_float_loss(stream_id):
    """@spec PROTECTED-HOOK-ADMISSION-3/4."""
    r = module("admission_records")
    data = examples()["intent"]
    data["reserved_stream_id"] = stream_id
    assert r.parse_intent(canonical(data)).as_dict()["reserved_stream_id"] == stream_id


@pytest.mark.parametrize(
    "stream_id",
    [
        "0-0",
        "01-0",
        "1-01",
        "-1-0",
        "1",
        "1-0-0",
        "18446744073709551616-0",
        "0-18446744073709551616",
    ],
)
def test_stream_id_noncanonical_and_overflow_refuse(stream_id):
    """@spec PROTECTED-HOOK-ADMISSION-3/4."""
    r = module("admission_records")
    data = examples()["intent"]
    data["reserved_stream_id"] = stream_id
    with pytest.raises((ValueError, r.AdmissionUnavailable)):
        r.parse_intent(canonical(data))


@pytest.mark.parametrize(
    "status,reason,receipt",
    [
        ("preparing", "deadline", None),
        ("preparing", None, "receipt"),
        ("committed", None, None),
        ("failed", None, None),
        ("failed", "quota_full", None),
    ],
)
def test_state_cross_field_shape(status, reason, receipt):
    """@spec PROTECTED-HOOK-ADMISSION-3."""
    r = module("admission_records")
    data = dict(
        schema_version=1,
        status=status,
        recovery_attempts=0,
        reason=reason,
        receipt=examples()["receipt"] if receipt else None,
    )
    with pytest.raises((ValueError, r.AdmissionUnavailable)):
        r.parse_state(canonical(data))


@pytest.mark.parametrize(
    "kind,field", [(kind, field) for kind, record in examples().items() for field in record]
)
def test_every_record_field_required(kind, field):
    """@spec PROTECTED-HOOK-ADMISSION-2/3."""
    r = module("admission_records")
    value = examples()[kind]
    del value[field]
    with pytest.raises((ValueError, r.AdmissionUnavailable)):
        getattr(r, "parse_" + kind)(canonical(value))


@pytest.mark.parametrize(
    "kind,field",
    [
        (kind, field)
        for kind, record in examples().items()
        for field, value in record.items()
        if type(value) is str
    ],
)
@pytest.mark.parametrize("bad", [True, 1, 1.5, None])
def test_all_string_fields_reject_scalar_coercion(kind, field, bad):
    """@spec PROTECTED-HOOK-ADMISSION-2/3."""
    r = module("admission_records")
    value = examples()[kind]
    value[field] = bad
    with pytest.raises((ValueError, r.AdmissionUnavailable)):
        getattr(r, "parse_" + kind)(canonical(value))


@pytest.mark.parametrize(
    "kind,field",
    [
        (kind, field)
        for kind, record in examples().items()
        for field in record
        if field in {"source_revision", "source_generation", "runtime_generation"}
    ],
)
@pytest.mark.parametrize("bad", ["0", "01", "+1", "-1", "1e2", "9223372036854775808"])
def test_generation_fields_canonical_bigint_bound(kind, field, bad):
    """@spec PROTECTED-HOOK-ADMISSION-2/3 PROTECTED-HOOK-LANE-2."""
    r = module("admission_records")
    value = examples()[kind]
    value[field] = bad
    with pytest.raises((ValueError, r.AdmissionUnavailable)):
        getattr(r, "parse_" + kind)(canonical(value))


@pytest.mark.parametrize("bad", [True, -1, 11, 1.0, "1"])
def test_attempt_counter_strict_zero_through_ten(bad):
    """@spec PROTECTED-HOOK-ADMISSION-3/5."""
    r = module("admission_records")
    value = examples()["state"]
    value["recovery_attempts"] = bad
    with pytest.raises((ValueError, r.AdmissionUnavailable)):
        r.parse_state(canonical(value))


@pytest.mark.parametrize(
    "field,bad",
    [
        ("deadline_ms", "300999"),
        ("created_at_ms", "9007199254740991"),
        ("deadline_ms", "9007199254740992"),
        ("created_at_ms", "-1"),
        ("created_at_ms", "01"),
    ],
)
def test_deadline_exact_interval_and_safe_millisecond_bound(field, bad):
    """@spec PROTECTED-HOOK-ADMISSION-3/4."""
    r = module("admission_records")
    value = examples()["intent"]
    value[field] = bad
    with pytest.raises((ValueError, r.AdmissionUnavailable)):
        r.parse_intent(canonical(value))


def test_payload_exact_byte_limit_and_invalid_encoding():
    """@spec PROTECTED-HOOK-ADMISSION-2."""
    r = module("admission_records")
    empty = queued_payload(text="")
    payload = queued_payload(text="x" * (262144 - len(empty)))
    assert len(payload) == 262144
    assert request(r, payload=payload).queued_payload == payload
    for raw in (
        queued_payload(text="x" * (262145 - len(empty))),
        b"\xff",
        b"{}",
        b"null",
        b'{"text":NaN}',
    ):
        with pytest.raises((ValueError, r.AdmissionUnavailable)):
            request(r, payload=raw)


@pytest.mark.parametrize(
    "case",
    [
        "ordinary",
        "other_agent",
        "other_hook",
        "audit",
        "bad_body",
        "text_payload",
        "duplicate_payload",
    ],
)
def test_request_constructor_closes_policy_identity_and_payload(case):
    """@spec PROTECTED-HOOK-ADMISSION-2 PROTECTED-HOOK-SOURCE-6."""
    r = module("admission_records")
    policy = source_policy()
    options = {}
    if case == "ordinary":
        policy.update(
            mode="ordinary",
            tool_access=None,
            runtime_id=None,
            qualification_id=None,
            bundle_digest=None,
        )
    elif case == "other_agent":
        policy["agent_id"] = "55555555-5555-4555-8555-555555555555"
    elif case == "other_hook":
        policy["hook"] = "other"
    elif case == "audit":
        policy["updated_at"] = "2026-10-03T00:00:00Z"
    elif case == "bad_body":
        options["body"] = "A" * 64
    elif case == "text_payload":
        options["payload"] = queued_payload().decode()
    else:
        options["payload"] = queued_payload()[:-1] + b',"source":"webhook"}'
    with pytest.raises((ValueError, r.AdmissionUnavailable)):
        request(r, policy=policy, **options)
