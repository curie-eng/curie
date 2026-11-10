"""Provisioner/control protocol events and markers, ADR 0236.

@spec PROTECTED-COMMISSIONING-5. Events are reports, never authority: these
tests prove the closed codec only.
"""

from __future__ import annotations

import json

import pytest
from curie_protected_hooks.commissioning_operator_protocol import (
    EVENT_PREFIX,
    EVENTS,
    MARKERS,
    OperatorProtocolRefused,
    encode_event,
    guard_result,
    marker_document,
    parse_events,
)


def test_events_round_trip_and_other_log_lines_are_ignored():
    lines = [
        "ordinary log line",
        encode_event("SERVER_READY", {"pod_uid": "u-1"}),
        guard_result(call=1, outcome="pass", latency_ms=1250),
        encode_event("FINISHED", {}),
    ]
    assert parse_events("\n".join(lines)) == (
        ("SERVER_READY", {"pod_uid": "u-1"}),
        ("GUARD_RESULT", {"call": 1, "outcome": "pass", "latency_ms": 1250}),
        ("FINISHED", {}),
    )


@pytest.mark.parametrize(
    "line",
    [
        EVENT_PREFIX + "UNKNOWN {}",
        EVENT_PREFIX + "FINISHED",
        EVENT_PREFIX + "FINISHED []",
        EVENT_PREFIX + "FINISHED {bad",
        EVENT_PREFIX + 'FINISHED {"a":1,"a":2}',
        EVENT_PREFIX + 'FINISHED {"a": 1}',
        EVENT_PREFIX + 'FINISHED {"a":1.5}',
        EVENT_PREFIX + 'FINISHED {"a":NaN}',
        EVENT_PREFIX + "FINISHED {" + '"a":"' + "x" * 70000 + '"}',
        EVENT_PREFIX + 'GUARD_RESULT {"call":1,"latency_ms":3,"outcome":"maybe"}',
        EVENT_PREFIX + 'GUARD_RESULT {"call":0,"latency_ms":3,"outcome":"pass"}',
        EVENT_PREFIX + 'GUARD_RESULT {"call":1,"latency_ms":-1,"outcome":"pass"}',
        EVENT_PREFIX + 'GUARD_RESULT {"call":true,"latency_ms":3,"outcome":"pass"}',
        EVENT_PREFIX + 'GUARD_RESULT {"call":1,"outcome":"pass"}',
    ],
    ids=[
        "unknown",
        "no-body",
        "array",
        "invalid-json",
        "duplicate-member",
        "not-canonical",
        "float",
        "nan",
        "oversize",
        "guard-outcome",
        "guard-call",
        "guard-latency",
        "guard-bool",
        "guard-closed",
    ],
)
def test_a_malformed_event_line_refuses_the_whole_log(line):
    with pytest.raises(OperatorProtocolRefused):
        parse_events("ordinary\n" + line)


def test_encode_refuses_unknown_names_and_non_objects():
    with pytest.raises(OperatorProtocolRefused):
        encode_event("UNKNOWN", {})
    with pytest.raises(OperatorProtocolRefused):
        encode_event("FINISHED", [])  # type: ignore[arg-type]
    with pytest.raises(OperatorProtocolRefused):
        guard_result(call=1, outcome="pass", latency_ms=1.5)  # type: ignore[arg-type]


def test_events_carry_no_authority_word():
    # Reports only: nothing in the closed set names an acceptance or a record.
    for name in EVENTS:
        assert not any(word in name for word in ("ACCEPT", "QUALIF", "READINESS", "PERMIT"))


def test_markers_are_closed_immutable_and_carry_data_only_where_declared():
    document = marker_document("campaign-start", namespace="acme-control", data={"phase": "c"})
    assert document["immutable"] is True
    assert document["metadata"]["name"] == "commissioning-marker-campaign-start"
    assert document["data"] == {"phase": "c"}
    assert "data" not in marker_document("finish-guard", namespace="acme-control")
    for bad in (
        lambda: marker_document("unknown", namespace="acme-control"),
        lambda: marker_document("finish-guard", namespace="acme-control", data={"a": "b"}),
        lambda: marker_document("campaign-start", namespace="acme-control"),
        lambda: marker_document("campaign-start", namespace="Bad_NS", data={}),
        lambda: marker_document("campaign-start", namespace="acme-control", data={"a": 1}),
    ):
        with pytest.raises(OperatorProtocolRefused):
            bad()
    assert len(set(MARKERS)) == len(MARKERS)
    json.dumps(document)
