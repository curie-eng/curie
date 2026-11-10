"""Commissioning campaign records never substitute for worker launch records.

The protected-hooks codecs stay free of worker imports, so the matrix against
the worker's launch record codecs and launch registry lives here.
@spec PROTECTED-COMMISSIONING-7.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
from curie_protected_hooks import commissioning_campaign_records as c
from curie_worker import protected_launch_records as launch
from curie_worker import protected_launch_registry as registry

from .protected_configuration_vectors import canonical
from .test_protected_launch_records import receipt_value, request_value

CAMPAIGN_ID = "11111111-1111-4111-8111-111111111111"
UUID_A = "33333333-3333-4333-8333-333333333333"
UUID_B = "44444444-4444-4444-8444-444444444444"
CAMPAIGN = dict(
    campaign_id=CAMPAIGN_ID,
    campaign_generation="2",
    permit_digest="a" * 64,
    bootstrap_digest="b" * 64,
    manifest_digest="c" * 64,
    runtime_id="22222222-2222-4222-8222-222222222222",
    runtime_generation="1",
    broker_run_id="d" * 40,
)
DELIVERY = dict(
    delivery_id=UUID_A,
    attempt_id=UUID_B,
    reservation_id=UUID_A,
    request_digest="e" * 64,
    payload_digest="f" * 64,
    payload_bytes="512",
    source=dict(source_id=UUID_A, generation="1", policy_fingerprint="1" * 64),
    target_ref="fixture:read-only/deployment",
)
EXTRAS: dict[str, dict[str, Any]] = {
    "commissioning_envelope": dict(tool_access="read-only"),
    "commissioning_admission": dict(
        envelope_digest="2" * 64, stream_entry_id="1000-0", accepted_at_ms="1000"
    ),
    "commissioning_launch_request": dict(
        admission_digest="3" * 64,
        launch_id=UUID_A,
        launch_reservation_id=UUID_B,
        projection_digest="4" * 64,
    ),
    "commissioning_launch_ownership": dict(
        launch_id=UUID_A,
        launch_request_digest="5" * 64,
        projection_digest="4" * 64,
        workload=dict(
            kind="unbound", namespace="campaign-1", pod_name="runner-1", create_operation_id=UUID_B
        ),
        state="creating",
        revision="1",
    ),
    "commissioning_acceptance": dict(
        evidence_digest="6" * 64,
        qualification_id="77777777-7777-4777-8777-777777777777",
        qualification_generation="1",
        accepted_at_ms="1000",
    ),
}
PARSERS = {
    "commissioning_envelope": c.parse_campaign_envelope,
    "commissioning_admission": c.parse_campaign_admission,
    "commissioning_launch_request": c.parse_campaign_launch_request,
    "commissioning_launch_ownership": c.parse_campaign_launch_ownership,
    "commissioning_acceptance": c.parse_commissioning_acceptance,
}
LAUNCH_PARSERS = (
    launch.parse_launch_request,
    launch.parse_launch_receipt,
    launch.parse_launch_success,
    launch.parse_launch_pending_success,
    launch.parse_launch_observation,
    launch.parse_start_request,
    launch.parse_admit_request,
    launch.parse_epoch_request,
    launch.parse_retire_request,
    launch.parse_applied_response,
    launch.parse_retire_response,
    launch.parse_launch_error_response,
)
REGISTRY_RECORDS = (
    registry.LaunchOwnership,
    registry.OwnedObjects,
    registry.AssignmentMetadata,
    registry.TerminationObservation,
    registry.CleanupReceipt,
    registry.LaunchSnapshot,
    registry.OperationRecord,
    registry.OperationReservation,
)


def record(kind: str) -> Any:
    value: dict[str, Any] = dict(schema_version=1, record_type=kind, campaign=CAMPAIGN)
    if kind != "commissioning_acceptance":
        value["delivery"] = DELIVERY
    return PARSERS[kind](json.dumps(copy.deepcopy(value | EXTRAS[kind])).encode())


@pytest.mark.parametrize("kind", list(PARSERS))
def test_campaign_bytes_refuse_every_worker_launch_codec(kind: str) -> None:
    data = record(kind).canonical_bytes
    for parse in LAUNCH_PARSERS:
        with pytest.raises(launch.LaunchRecordInvalid):
            parse(data)


@pytest.mark.parametrize("kind", list(PARSERS))
def test_campaign_values_refuse_every_launch_registry_record(kind: str) -> None:
    value = record(kind).as_dict()
    for cls in REGISTRY_RECORDS:
        with pytest.raises(registry.RegistryInvalid):
            cls(**value)


def test_campaign_ownership_never_enters_the_launch_registry(tmp_path: Path) -> None:
    root = tmp_path.resolve() / "owned"
    root.mkdir(mode=0o700)
    owner = registry.ProtectedLaunchRegistry.open(str(root), boot_id=CAMPAIGN_ID)
    try:
        with pytest.raises(registry.RegistryInvalid):
            owner.create(
                worker_principal_id="worker:campaign",
                request_id=UUID_A,
                request_digest="5" * 64,
                ownership=record("commissioning_launch_ownership"),
                creation_budget_ms=1000,
            )
    finally:
        owner.close()


@pytest.mark.parametrize("production", [request_value, receipt_value])
def test_worker_launch_bytes_refuse_every_campaign_parser(production: Any) -> None:
    value = production()
    for kind, parse in PARSERS.items():
        for data in (canonical(value), canonical(value | {"record_type": kind})):
            with pytest.raises(c.CampaignRecordInvalid):
                parse(data)
