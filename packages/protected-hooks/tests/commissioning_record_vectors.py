"""Valid commissioning records bound to a production manifest, @spec PROTECTED-COMMISSIONING-4.

Shared by the caller-boundary refusal suites of the protected hooks package,
the API and the worker. Every record is accepted by its own commissioning codec
and copies every member it shares with the given production manifest (runtime,
broker, guard, execution configuration, qualification and credential
references), so a substitution differs from production only in its record kind.
Absolute imports only: the API suite loads this file by path.
"""

from __future__ import annotations

import copy
import hashlib
import json
from typing import Any

from curie_protected_hooks.commissioning_bootstrap import parse_commissioning_bootstrap
from curie_protected_hooks.commissioning_campaign_records import (
    parse_campaign_admission,
    parse_campaign_envelope,
    parse_campaign_launch_ownership,
    parse_campaign_launch_request,
    parse_commissioning_acceptance,
)
from curie_protected_hooks.commissioning_records import parse_commissioning_permit

KINDS = (
    "commissioning_permit",
    "commissioning_bootstrap",
    "commissioning_envelope",
    "commissioning_admission",
    "commissioning_launch_request",
    "commissioning_launch_ownership",
    "commissioning_acceptance",
)
PARSERS = {
    "commissioning_permit": parse_commissioning_permit,
    "commissioning_bootstrap": parse_commissioning_bootstrap,
    "commissioning_envelope": parse_campaign_envelope,
    "commissioning_admission": parse_campaign_admission,
    "commissioning_launch_request": parse_campaign_launch_request,
    "commissioning_launch_ownership": parse_campaign_launch_ownership,
    "commissioning_acceptance": parse_commissioning_acceptance,
}
# The commissioning journal prefix, which no production principal may read (PC-1).
COMMISSIONING_PREFIX = "protected:commissioning:"

_CAMPAIGN_ID = "11111111-1111-4111-8111-111111111111"
_UUID_A = "33333333-3333-4333-8333-333333333333"
_UUID_B = "44444444-4444-4444-8444-444444444444"
_UUID_C = "55555555-5555-4555-8555-555555555555"
_SOURCE = {
    "source_id": "77777777-7777-4777-8777-777777777777",
    "generation": "1",
    "policy_fingerprint": "8" * 64,
}
_TARGET = "fixture/example-readonly"


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _reference(name: str) -> dict[str, str]:
    return {"id": "commissioning/example-" + name, "generation": "1"}


def commissioning_records(manifest: dict[str, Any]) -> dict[str, bytes]:
    """Canonical bytes of one valid record of every commissioning kind.

    ``manifest`` is the production manifest value the substituted caller trusts.
    @spec PROTECTED-COMMISSIONING-1/2/4/7.
    """
    m = copy.deepcopy(manifest)
    manifest_digest = hashlib.sha256(_canonical(m)).hexdigest()
    bootstrap = {
        "schema_version": 1,
        "record_type": "commissioning_bootstrap",
        "runtime_id": m["runtime_id"],
        "runtime_generation": m["runtime_generation"],
        "execution_config_digest": m["execution_config_digest"],
        "entry_points": {
            "production": "composition/example-production",
            "commissioning": "composition/example-commissioning",
        },
        "production_credentials": copy.deepcopy(m["credential_refs"]),
        "candidate_credentials": {"enqueue": _reference("enqueue"), "worker": _reference("worker")},
        "issuer_identity": _reference("issuer"),
        "control_identity": _reference("control"),
        "qualification_publisher_identity": _reference("qualification-publisher"),
        "control_reader_identity": _reference("control-reader"),
        "mediator_identity": _reference("mediator"),
        "gateway_identity": _reference("gateway"),
        "source_scope": [copy.deepcopy(_SOURCE)],
        "target_scope": [_TARGET],
        "max_permit_ms": "60000",
    }
    bootstrap_digest = hashlib.sha256(_canonical(bootstrap)).hexdigest()
    permit = {
        "schema_version": 1,
        "record_type": "commissioning_permit",
        "campaign_id": _CAMPAIGN_ID,
        "campaign_generation": "1",
        "runtime_id": m["runtime_id"],
        "runtime_generation": m["runtime_generation"],
        "manifest_digest": manifest_digest,
        "bootstrap_digest": bootstrap_digest,
        "execution_config_digest": m["execution_config_digest"],
        "broker_identity": copy.deepcopy(m["broker_identity"]),
        "guard_identity": copy.deepcopy(m["guard_identity"]),
        "issuer_identity": _reference("issuer"),
        "control_identity": _reference("control"),
        "candidate_credentials": copy.deepcopy(bootstrap["candidate_credentials"]),
        "sources": [copy.deepcopy(_SOURCE)],
        "targets": [_TARGET],
        "issued_at_ms": "1000",
        "expires_at_ms": "2000",
        "limits": {
            key: "4"
            for key in ("deliveries", "launches", "model_calls", "output_tokens", "spend_microusd")
        },
    }
    permit_digest = hashlib.sha256(_canonical(permit)).hexdigest()
    campaign = {
        "campaign_id": _CAMPAIGN_ID,
        "campaign_generation": "1",
        "permit_digest": permit_digest,
        "bootstrap_digest": bootstrap_digest,
        "manifest_digest": manifest_digest,
        "runtime_id": m["runtime_id"],
        "runtime_generation": m["runtime_generation"],
        "broker_run_id": m["broker_identity"]["run_id"],
    }
    delivery = {
        "delivery_id": _UUID_A,
        "attempt_id": _UUID_B,
        "reservation_id": _UUID_C,
        "request_digest": "e" * 64,
        "payload_digest": "f" * 64,
        "payload_bytes": "512",
        "source": copy.deepcopy(_SOURCE),
        "target_ref": _TARGET,
    }
    common = {"schema_version": 1, "campaign": campaign}
    values: dict[str, dict[str, Any]] = {
        "commissioning_permit": permit,
        "commissioning_bootstrap": bootstrap,
        "commissioning_envelope": {**common, "delivery": delivery, "tool_access": "read-only"},
        "commissioning_admission": {
            **common,
            "delivery": delivery,
            "envelope_digest": "2" * 64,
            "stream_entry_id": "1700000000000-0",
            "accepted_at_ms": "1700000000001",
        },
        "commissioning_launch_request": {
            **common,
            "delivery": delivery,
            "admission_digest": "3" * 64,
            "launch_id": _UUID_A,
            "launch_reservation_id": _UUID_B,
            "projection_digest": "4" * 64,
        },
        "commissioning_launch_ownership": {
            **common,
            "delivery": delivery,
            "launch_id": _UUID_A,
            "launch_request_digest": "5" * 64,
            "projection_digest": "4" * 64,
            "workload": {
                "kind": "unbound",
                "namespace": "campaign-1",
                "pod_name": "runner-1",
                "create_operation_id": _UUID_B,
            },
            "state": "creating",
            "revision": "1",
        },
        "commissioning_acceptance": {
            **common,
            "evidence_digest": "6" * 64,
            "qualification_id": m["qualification_id"],
            "qualification_generation": "1",
            "accepted_at_ms": "1700000000002",
        },
    }
    records = {}
    for kind in KINDS:
        value = copy.deepcopy(values[kind])
        value["record_type"] = kind
        raw = _canonical(value)
        assert PARSERS[kind](raw).canonical_bytes == raw, kind + " vector is not canonical"
        records[kind] = raw
    return records
