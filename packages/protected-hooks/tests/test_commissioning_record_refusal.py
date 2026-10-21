"""Shared callers refuse commissioning records, @spec PROTECTED-COMMISSIONING-1/4.

Atomic admission and authenticated artifact selection read retained records on
the real isolated TLS broker. Each case stores one valid commissioning record
(``commissioning_record_vectors``) under a key the caller reads while every
other record stays genuine production, so each case is a mixed production and
commissioning reference set. The caller refuses before any positive claim or
enqueue and the broker is unchanged. No production principal recipe reaches a
``protected:commissioning:*`` key, and records stored there change no outcome.
"""

from __future__ import annotations

import asyncio

import pytest
from curie_protected_hooks import admission_acl, broker_metadata, worker_acl
from curie_protected_hooks.worker_verification import verification_acl_rules

from . import admission_broker as broker_helpers
from . import worker_broker as wb
from .admission_broker import request, safe_error, snapshot
from .commissioning_record_vectors import COMMISSIONING_PREFIX, KINDS, commissioning_records
from .test_atomic_admission import setup_product
from .test_worker_artifact_selection import (
    assert_detached,
    connected,
    run_selection,
    seeded,
    selection,
)

admission_broker = broker_helpers.admission_broker
admission_service = broker_helpers.admission_service
pworker_service = wb.pworker_service
owned_worker = wb.owned_worker

CONTROL_REASONS = {
    "selection": "runtime_unavailable",
    "manifest": "runtime_unavailable",
    "qualification": "qualification_unavailable",
    "readiness": "evidence_unavailable",
}


def records_for(broker) -> dict[str, bytes]:
    """Every commissioning kind bound to the broker's trusted production manifest."""
    return commissioning_records(broker.manifest().as_dict())


def refuses_without_effect(broker, records, call) -> None:
    """A refused or unavailable admission leaves no write and no stream entry."""
    before = snapshot(broker)
    try:
        output = call().as_dict()
        assert output["status"] == "refused" and output["receipt"] is None, output
    except records.AdmissionUnavailable as error:
        safe_error(error, broker)
    assert snapshot(broker) == before
    assert not broker.command("EXISTS", "curie:runs")


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("control", sorted(CONTROL_REASONS))
def test_atomic_admission_refuses_a_commissioning_control_record(admission_broker, control, kind):
    """A commissioning record under one selected control key never admits.

    The selection, manifest, qualification and readiness decoders stay closed:
    the record reads as absent at its own step and refuses with that step's
    reason, writing nothing. @spec PROTECTED-COMMISSIONING-1/4.
    """
    b = admission_broker
    r, f, values = setup_product(b)
    key = next(k for k in values if ":" + control + ":" in k)
    b.command("SET", key, records_for(b)[kind])
    before = snapshot(b)
    assert f.admit(request(r)).as_dict() == dict(
        status="refused", reason=CONTROL_REASONS[control], receipt=None
    )
    assert snapshot(b) == before
    assert not b.command("EXISTS", "curie:runs")


def test_atomic_admission_refuses_an_all_commissioning_control_set(admission_broker):
    """No combination of commissioning records stands in for the selected tuple.

    @spec PROTECTED-COMMISSIONING-4.
    """
    b = admission_broker
    r, f, values = setup_product(b)
    vectors = records_for(b)
    for control in CONTROL_REASONS:
        key = next(k for k in values if ":" + control + ":" in k)
        for kind in KINDS:
            b.command("SET", key, vectors[kind])
            refuses_without_effect(b, r, lambda: f.admit(request(r)))


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("family", ["intent", "state", "commit", "recovery", "binding", "event"])
def test_atomic_admission_never_adopts_a_commissioning_admission_record(
    admission_broker, family, kind
):
    """A commissioning record under an admission key is neither adopted nor replaced.

    Admission and recovery of the same delivery refuse or are unavailable and
    the commissioning bytes stay as they were. @spec PROTECTED-COMMISSIONING-4.
    """
    b = admission_broker
    r, f, _ = setup_product(b)
    req = request(r)
    suffix = "event/example" if family in {"binding", "event"} else r.delivery_digest(req.identity)
    key = "protected:admission:" + family + ":" + suffix
    raw = records_for(b)[kind]
    b.command("SET", key, raw)
    refuses_without_effect(b, r, lambda: f.admit(req))
    refuses_without_effect(b, r, lambda: f.recover(req.identity))
    assert b.command("GET", key) == raw


@pytest.mark.parametrize(
    "role,rules",
    [
        ("enqueue", lambda manifest: admission_acl.admission_acl_rules("enqueue")),
        (
            "enqueue-settlement",
            lambda manifest: admission_acl.admission_acl_rules(
                "enqueue", trusted_manifest=manifest
            ),
        ),
        ("verifier", lambda manifest: admission_acl.admission_acl_rules("verifier")),
        ("control_reader", lambda manifest: broker_metadata.metadata_acl_rules("control_reader")),
        ("source_writer", lambda manifest: broker_metadata.metadata_acl_rules("source_writer")),
        ("worker", lambda manifest: worker_acl.worker_acl_rules("worker")),
        ("verification-worker", lambda manifest: verification_acl_rules("worker")),
    ],
)
def test_no_production_principal_reaches_a_commissioning_key(admission_broker, role, rules):
    """Every production recipe is denied read, write and script access to the journal.

    @spec PROTECTED-COMMISSIONING-1.
    """
    b = admission_broker
    user = "refusal-" + role
    b.command("ACL", "SETUSER", user, "reset", "on", "nopass", *rules(b.manifest()))
    try:
        key = COMMISSIONING_PREFIX + "permit:11111111-1111-4111-8111-111111111111"
        # Every recipe keeps its handshake: the oracle tells a grant from a denial.
        allowed = b.command("ACL", "DRYRUN", user, "PING")
        for command in (
            ("GET", key),
            ("TYPE", key),
            ("SET", key, "x"),
            ("EVAL", "return 1", "1", key),
        ):
            outcome = b.command("ACL", "DRYRUN", user, *command)
            text = outcome.decode() if isinstance(outcome, bytes) else str(outcome)
            assert outcome != allowed and "no permissions" in text, (role, command[0])
    finally:
        b.command("ACL", "DELUSER", user)


def test_commissioning_journal_records_change_no_admission_outcome(admission_broker):
    """Records under the commissioning prefix neither open nor close production admission.

    @spec PROTECTED-COMMISSIONING-1.
    """
    b = admission_broker
    r, f, _ = setup_product(b)
    vectors = records_for(b)
    for kind, raw in vectors.items():
        b.command("SET", COMMISSIONING_PREFIX + kind, raw)
    out = f.admit(request(r)).as_dict()
    assert out["status"] == "accepted"
    for kind, raw in vectors.items():
        assert b.command("GET", COMMISSIONING_PREFIX + kind) == raw


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("control", ["manifest", "readiness", "qualification"])
def test_artifact_selection_refuses_a_commissioning_control_record(owned_worker, control, kind):
    """Authenticated selection never returns a pair over a commissioning record.

    @spec PROTECTED-COMMISSIONING-4.
    """
    product = selection()
    b = owned_worker.broker
    manifest, record, _, keys = seeded(b)
    b.command("SET", keys[control], commissioning_records(manifest.as_dict())[kind])
    before = snapshot(b)
    with pytest.raises(product.ArtifactSelectionUnavailable) as caught:
        run_selection(owned_worker, manifest, record)
    assert_detached(caught.value)
    assert snapshot(b) == before


def test_worker_control_reads_refuse_the_commissioning_prefix(owned_worker):
    """The pinned worker session cannot name a commissioning key, even one that exists.

    @spec PROTECTED-COMMISSIONING-1.
    """
    from curie_protected_hooks.broker_metadata import BrokerMetadataUnavailable

    b = owned_worker.broker
    manifest, _, _, _ = seeded(b)
    for kind, raw in commissioning_records(manifest.as_dict()).items():
        b.command("SET", COMMISSIONING_PREFIX + kind, raw)

    async def read_all():
        async with connected(owned_worker) as client:
            for kind in KINDS:
                with pytest.raises(BrokerMetadataUnavailable):
                    await client.read_control(COMMISSIONING_PREFIX + kind)

    asyncio.run(read_all())
