"""Closed retained settlement codecs, @spec PROTECTED-HOOK-LANE-6."""

import hashlib
import importlib.util
import json

import pytest

from .admission_broker import canonical, module


def product():
    assert importlib.util.find_spec("curie_protected_hooks.settlement_records") is not None
    return module("settlement_records")


def context_value():
    return dict(
        schema_version=1,
        event_id="event/example",
        intent_digest="a" * 64,
        manifest_digest="b" * 64,
        runtime_id="33333333-3333-4333-8333-333333333333",
        runtime_generation="1",
        broker_run_id="c" * 40,
        envelope_sha256="d" * 64,
        payload_sha256="e" * 64,
        committed_stream_id="1-0",
        completion_generation="f" * 32,
        completion_record_json=json.dumps(
            dict(
                event_id="event/example",
                event=dict(
                    version="1.0",
                    event="turn.completed",
                    event_id="event/example",
                    outcome="delivered",
                    target=dict(
                        kind="slack",
                        address="channel/example",
                        conversation_id=None,
                        reply_ref=None,
                    ),
                ),
                route=dict(endpoint=None, adapter="adapter/example"),
                created_at=1.5,
                done=True,
            )
        ),
    )


def test_event_index_is_closed_and_digest_bound():
    p = product()
    value = dict(schema_version=1, event_id="event/example", intent_digest="a" * 64)
    record = p.EventIndex(canonical(value))
    assert record.as_dict() == value
    assert record.canonical_bytes == canonical(value)
    for delta in ({"unexpected": 1}, {"intent_digest": "A" * 64}, {"schema_version": True}):
        with pytest.raises(ValueError):
            p.EventIndex(canonical(value | delta))


def test_context_preserves_exact_completion_and_rejects_mixed_event():
    p = product()
    value = context_value()
    record = p.CompletionContext(canonical(value))
    assert record.as_dict() == value
    assert record.digest == hashlib.sha256(canonical(value)).hexdigest()
    for delta in (
        {"event_id": "event/other"},
        {"completion_generation": "f" * 31},
        {"completion_record_json": "{}"},
        {"extra": 1},
    ):
        with pytest.raises(ValueError):
            p.CompletionContext(canonical(value | delta))


def test_resolution_never_accepts_diagnostic_as_success():
    p = product()
    value = dict(
        schema_version=1,
        event_id="event/example",
        completion_generation="f" * 32,
        resolution="emitted",
    )
    assert p.CompletionResolution(canonical(value)).as_dict() == value
    for resolution in ("acked", "done", "refused", "expired"):
        with pytest.raises(ValueError):
            p.CompletionResolution(canonical(value | dict(resolution=resolution)))


def test_codec_error_detaches_ambient_proof_content():
    p = product()
    try:
        raise RuntimeError("private ambient proof input")
    except RuntimeError:
        with pytest.raises(ValueError) as caught:
            p.EventIndex(b'{"private":"proof bytes"}')
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
    assert "private" not in repr(caught.value)


@pytest.mark.parametrize(
    "raw",
    [
        b'{"schema_version":1,"event_id":"event/example","intent_digest":"a","event_id":"event/other"}',
        b'{"schema_version":1.0,"event_id":"event/example","intent_digest":"a"}',
        b"[" * 10000,
        b"x" * 65537,
    ],
)
def test_closed_codec_rejects_duplicate_float_deep_and_oversized_input(raw):
    with pytest.raises(ValueError):
        product().EventIndex(raw)


def test_reconciler_reports_refunds_as_safe_counts():
    from curie_api.protected_reconciler import TickCounts

    assert hasattr(TickCounts(), "released")


def test_cold_proof_restoration_has_explicit_non_execution_surface():
    p = module("worker_verification")
    assert hasattr(p, "restore_committed_event")
    assert hasattr(p, "RetainedProof")


def test_namespace_uses_canonical_runtime_generation_and_is_immutable():
    from dataclasses import FrozenInstanceError

    from curie_protected_hooks.authority_records import parse_manifest
    from curie_protected_hooks.execution_namespace import derive_lane_namespace

    from .test_authority_records import example_records

    value = example_records()["manifest"]
    manifest = parse_manifest(canonical(value))
    ns = derive_lane_namespace(manifest)
    digest = hashlib.sha256(
        canonical([value["runtime_id"], value["runtime_generation"]])
    ).hexdigest()
    assert ns.prefix == "protected:lane:worker:" + digest
    assert ns.group == "protected-" + digest
    assert ns.stream == "curie:runs"
    assert ns.dead == ns.prefix + ":dead"
    with pytest.raises(FrozenInstanceError):
        ns.prefix = "foreign"
    other = derive_lane_namespace(parse_manifest(canonical(value | dict(runtime_generation="1"))))
    assert other.prefix != ns.prefix


def test_completion_validator_detaches_raw_json_and_ambient_input():
    p = product()
    try:
        raise RuntimeError("private ambient completion")
    except RuntimeError:
        with pytest.raises(ValueError) as caught:
            p.validate_completion_json('{"private":"content"}', "event/example")
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
    assert str(caught.value) == "invalid protected settlement record"
