"""Closed copy grammar alone grants no delivery or execution authority."""

import pytest
from curie_worker.capacity_wait import (
    WAIT_GENERATION_FIELD,
    WAIT_REPLY_REF_FIELD,
    WAIT_TERMINAL_ONLY_FIELD,
)
from curie_worker.protected_recovery import RecoveryUnavailable, parse_capacity_fields


def copy_fields(**extra):
    return {"payload": "original UTF-8 λ\n", "protected_envelope": " original envelope ",
            WAIT_GENERATION_FIELD: "2", **extra}


def test_copy_grammar_preserves_both_original_strings_without_issuing_execution():
    fields = copy_fields()
    before = dict(fields)
    parsed = parse_capacity_fields(fields)
    assert parsed.generation == 2
    assert parsed.source_fields == (("payload", fields["payload"]),
                                    ("protected_envelope", fields["protected_envelope"]))
    assert fields == before
    assert not hasattr(parsed, "turn")


@pytest.mark.parametrize("change", [
    {WAIT_GENERATION_FIELD: "0"}, {WAIT_GENERATION_FIELD: "02"},
    {WAIT_GENERATION_FIELD: "-1"}, {WAIT_GENERATION_FIELD: "9223372036854775808"},
    {WAIT_REPLY_REF_FIELD: ""}, {WAIT_TERMINAL_ONLY_FIELD: "0"},
    {"unknown": "derived"}, {"traceparent": "not-a-carrier"},
])
def test_unknown_noncanonical_or_malformed_copy_metadata_refuses(change):
    with pytest.raises(RecoveryUnavailable):
        parse_capacity_fields(copy_fields(**change))


def test_missing_required_wake_generation_refuses():
    fields = copy_fields()
    del fields[WAIT_GENERATION_FIELD]
    with pytest.raises(RecoveryUnavailable):
        parse_capacity_fields(fields)


def test_valid_trace_is_diagnostic_and_keeps_original_source_bytes():
    trace = "00-" + "1" * 32 + "-" + "2" * 16 + "-01"
    parsed = parse_capacity_fields(copy_fields(traceparent=trace))
    assert parsed.traceparent == trace
    assert parsed.source_fields == (("payload", "original UTF-8 λ\n"),
                                    ("protected_envelope", " original envelope "))


def test_delivery_graveyard_is_closed_diagnostic_preserving_original_strings():
    from curie_worker.protected_recovery import parse_graveyard_fields
    fields = {
        "payload": "original UTF-8 λ\n", "protected_envelope": " original envelope ",
        "dl_original_id": "1-0", "dl_delivery_count": "5",
        "dl_reason": "max-delivery-exceeded", "dl_dead_lettered_at": "2026-10-01T00:00:00Z",
    }
    diagnostic = parse_graveyard_fields(fields)
    assert diagnostic.source_fields == (("payload", fields["payload"]),
                                        ("protected_envelope", fields["protected_envelope"]))
    assert diagnostic.original_id == "1-0" and diagnostic.delivery_count == 5
    assert not hasattr(diagnostic, "turn")
    with pytest.raises(RecoveryUnavailable):
        parse_graveyard_fields({**fields, "dl_dl_authority": "unrecognized"})


def test_graveyard_without_complete_original_bytes_is_not_replayable():
    from curie_worker.protected_recovery import parse_graveyard_fields
    with pytest.raises(RecoveryUnavailable):
        parse_graveyard_fields({"dl_original_id": "1-0", "dl_delivery_count": "5",
                               "dl_reason": "max-delivery-exceeded",
                               "dl_dead_lettered_at": "2026-10-01T00:00:00Z"})
