"""Primitive-probe ownership registry. @spec PROTECTED-COMMISSIONING-8.

The campaign ownership rules hold for probes, in a registry of their own that
grants no campaign or production authority: names are durable before any
create, UIDs come only from create responses, a restart never resumes a
creation, and retirement needs the recorded termination or never-started fact.
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import fields

import pytest
from curie_worker.commissioning_probe_registry import (
    MAX_PROBES,
    ProbeRegistry,
    ProbeSnapshot,
    probe_names,
)
from curie_worker.commissioning_registry import (
    CommissioningLaunchSnapshot,
    UnstartedObservation,
    object_names,
)
from curie_worker.protected_launch_registry import (
    CleanupReceipt,
    RegistryConflict,
    RegistryError,
    RegistryInvalid,
    RegistryRetired,
    RegistryUnauthorized,
    RegistryUnavailable,
    TerminationObservation,
)

BOOT = "b0000000-0000-4000-8000-000000000001"
LATER_BOOT = "b0000000-0000-4000-8000-000000000002"
SCOPE = "a" * 64
PROBE = "10000000-0000-4000-8000-000000000001"
OTHER = "10000000-0000-4000-8000-000000000002"
OPERATION = "20000000-0000-4000-8000-000000000001"
POD, SERVICE, SECRET = (
    "30000000-0000-4000-8000-000000000001",
    "30000000-0000-4000-8000-000000000002",
    "30000000-0000-4000-8000-000000000003",
)
NODE = "40000000-0000-4000-8000-000000000001"
CONTAINER = "containerd://" + "e" * 64
NAMESPACE = "acme-runtime"


def root(tmp_path, name="probes"):
    path = tmp_path / name
    path.mkdir(mode=0o700)
    return str(path)


def opened(tmp_path, *, boot=BOOT, scope=SCOPE, name="probes"):
    path = str(tmp_path / name)
    if not os.path.exists(path):
        os.mkdir(path, 0o700)
    return ProbeRegistry.open(path, boot_id=boot, scope_digest=scope)


def live(registry, probe_id=PROBE, operation=OPERATION):
    row = registry.create(probe_id, namespace=NAMESPACE, create_operation_id=operation)
    for kind, uid in (("secret", SECRET), ("service", SERVICE), ("pod", POD)):
        if probe_id != PROBE:
            uid = uid[:-1] + "9"
        row = registry.record_object_uid(probe_id, row.revision, kind, uid, "7")
    return registry.transition(probe_id, row.revision, "live")


def test_names_and_ownership_are_durable_before_any_effect(tmp_path):
    with opened(tmp_path) as registry:
        row = registry.create(PROBE, namespace=NAMESPACE, create_operation_id=OPERATION)
        assert row.state == "preparing" and row.boot_id == BOOT
        assert (row.objects.pod_name, row.objects.service_name, row.objects.secret_name) == (
            probe_names(PROBE)
        )
        # The guard admits only the campaign derivation, so probes share it.
        assert probe_names(PROBE) == object_names(PROBE)
        assert row.objects.pod_uid is None
        # An identical replay on the same boot returns the row; a change conflicts.
        assert registry.create(PROBE, namespace=NAMESPACE, create_operation_id=OPERATION) == row
        with pytest.raises(RegistryConflict):
            registry.create(PROBE, namespace=NAMESPACE, create_operation_id=PROBE)
        with pytest.raises(RegistryConflict):
            registry.create(PROBE, namespace="other", create_operation_id=OPERATION)
        # One create operation backs exactly one probe.
        with pytest.raises(RegistryConflict):
            registry.create(OTHER, namespace=NAMESPACE, create_operation_id=OPERATION)


def test_a_probe_row_carries_no_campaign_or_production_member():
    names = {field.name for field in fields(ProbeSnapshot)}
    campaign = {field.name for field in fields(CommissioningLaunchSnapshot)}
    assert not names & {
        "worker_principal_id",
        "binding",
        "assignment",
        "owned_epoch",
        "permit_digest",
        "launch_request_digest",
        "launch_reservation_id",
    }
    assert "binding" in campaign and "worker_principal_id" in campaign


@pytest.mark.parametrize("launch_file", ["registry.sqlite", "commissioning.sqlite"])
def test_a_root_with_a_launch_registry_refuses(tmp_path, launch_file):
    path = root(tmp_path)
    open(os.path.join(path, launch_file), "w").close()
    with pytest.raises(RegistryInvalid):
        ProbeRegistry.open(path, boot_id=BOOT, scope_digest=SCOPE)


def test_a_launch_registry_appearing_later_refuses_every_operation(tmp_path):
    with opened(tmp_path) as registry:
        registry.create(PROBE, namespace=NAMESPACE, create_operation_id=OPERATION)
        open(os.path.join(str(tmp_path / "probes"), "commissioning.sqlite"), "w").close()
        with pytest.raises(RegistryInvalid):
            registry.get(PROBE)


def test_scope_is_pinned_and_the_lock_is_exclusive(tmp_path):
    with opened(tmp_path):
        with pytest.raises(RegistryUnavailable):
            opened(tmp_path)
    with pytest.raises(RegistryConflict):
        opened(tmp_path, scope="b" * 64)
    with opened(tmp_path) as registry:
        assert registry.scope_digest == SCOPE


def test_unsafe_roots_refuse(tmp_path):
    loose = tmp_path / "loose"
    loose.mkdir(mode=0o755)
    loose.chmod(0o755)
    with pytest.raises(RegistryInvalid):
        ProbeRegistry.open(str(loose), boot_id=BOOT, scope_digest=SCOPE)
    with pytest.raises(RegistryInvalid):
        ProbeRegistry.open("relative", boot_id=BOOT, scope_digest=SCOPE)
    with pytest.raises(RegistryInvalid):
        ProbeRegistry()


def test_uids_come_only_from_this_boots_create_responses(tmp_path):
    with opened(tmp_path) as registry:
        row = registry.create(PROBE, namespace=NAMESPACE, create_operation_id=OPERATION)
        row = registry.record_object_uid(PROBE, row.revision, "secret", SECRET, "7")
        with pytest.raises(RegistryConflict):
            registry.record_object_uid(PROBE, row.revision, "secret", SERVICE, "8")
        with pytest.raises(RegistryConflict):
            # A stale revision never writes.
            registry.record_object_uid(PROBE, "1", "service", SERVICE, "7")
        with pytest.raises(RegistryConflict):
            # Not live before every UID is bound.
            registry.transition(PROBE, row.revision, "live")
    with opened(tmp_path, boot=LATER_BOOT) as registry:
        row = registry.get(PROBE)
        # A restart retires every live or preparing probe and never resumes creation.
        assert row.state == "retiring"
        with pytest.raises(RegistryConflict):
            registry.record_object_uid(PROBE, row.revision, "service", SERVICE, "7")
        with pytest.raises(RegistryRetired):
            registry.create(PROBE, namespace=NAMESPACE, create_operation_id=OPERATION)


def test_an_uncertain_probe_never_moves_on(tmp_path):
    with opened(tmp_path) as registry:
        row = registry.create(PROBE, namespace=NAMESPACE, create_operation_id=OPERATION)
        row = registry.mark_uncertain(PROBE, row.revision)
        assert row.state == "uncertain"
        for attempt in (
            lambda: registry.transition(PROBE, row.revision, "retiring"),
            lambda: registry.record_object_uid(PROBE, row.revision, "pod", POD, "7"),
            lambda: registry.mark_uncertain(PROBE, row.revision),
            lambda: registry.record_cleanup_receipt(
                PROBE, row.revision, CleanupReceipt(POD, SERVICE, SECRET)
            ),
        ):
            with pytest.raises(RegistryError):
                attempt()
    with opened(tmp_path, boot=LATER_BOOT) as registry:
        assert registry.get(PROBE).state == "uncertain"


def test_a_completion_needs_the_live_recorded_runner_and_is_recorded_once(tmp_path):
    with opened(tmp_path) as registry:
        row = live(registry)
        with pytest.raises(RegistryConflict):
            registry.record_completion(PROBE, row.revision, "c" * 64)
        row = registry.record_container_identity(PROBE, row.revision, CONTAINER, NODE)
        with pytest.raises(RegistryConflict):
            registry.record_container_identity(PROBE, row.revision, CONTAINER, NODE)
        row = registry.record_completion(PROBE, row.revision, "c" * 64)
        assert row.completion_digest == "c" * 64
        with pytest.raises(RegistryConflict):
            registry.record_completion(PROBE, row.revision, "d" * 64)
        with pytest.raises(RegistryInvalid):
            registry.record_completion(PROBE, row.revision, "not-a-digest")


def termination(container=CONTAINER, pod=POD):
    return TerminationObservation(pod, container, "9", 0, "2026-10-22T00:00:00Z")


def test_retirement_needs_the_recorded_termination_of_the_recorded_runner(tmp_path):
    with opened(tmp_path) as registry:
        row = live(registry)
        row = registry.record_container_identity(PROBE, row.revision, CONTAINER, NODE)
        with pytest.raises(RegistryConflict):
            # Not before retiring.
            registry.record_termination(PROBE, row.revision, termination())
        row = registry.transition(PROBE, row.revision, "retiring")
        receipt = CleanupReceipt(POD, SERVICE, SECRET)
        with pytest.raises(RegistryConflict):
            registry.record_cleanup_receipt(PROBE, row.revision, receipt)
        with pytest.raises(RegistryConflict):
            registry.record_termination(PROBE, row.revision, termination("cri-o://" + "f" * 64))
        with pytest.raises(RegistryConflict):
            registry.record_termination(PROBE, row.revision, termination(pod=SERVICE))
        with pytest.raises(RegistryConflict):
            # A recorded runner is proved by its termination, never by never-started.
            registry.record_unstarted(
                PROBE, row.revision, UnstartedObservation(POD, "9", "terminal")
            )
        row = registry.record_termination(PROBE, row.revision, termination())
        with pytest.raises(RegistryConflict):
            registry.record_cleanup_receipt(
                PROBE, row.revision, CleanupReceipt(POD, SECRET, SERVICE)
            )
        row = registry.record_cleanup_receipt(PROBE, row.revision, receipt)
        assert row.state == "retired" and row.retirement_outcome == "cleanup-recorded"
        assert row.termination == termination()


def test_a_runner_that_never_started_retires_over_the_campaign_fact(tmp_path):
    with opened(tmp_path) as registry:
        row = live(registry)
        row = registry.transition(PROBE, row.revision, "retiring")
        with pytest.raises(RegistryConflict):
            registry.record_unstarted(
                PROBE, row.revision, UnstartedObservation(SERVICE, "9", "unscheduled")
            )
        row = registry.record_unstarted(
            PROBE, row.revision, UnstartedObservation(POD, "9", "unscheduled")
        )
        with pytest.raises(RegistryConflict):
            registry.record_container_identity(PROBE, row.revision, CONTAINER, NODE)
        receipt = CleanupReceipt(POD, SERVICE, SECRET)
        row = registry.record_cleanup_receipt(PROBE, row.revision, receipt)
        assert row.state == "retired" and row.unstarted is not None and row.termination is None


def test_the_registry_is_finite_and_rows_are_ordered(tmp_path):
    with opened(tmp_path) as registry:
        for index in range(MAX_PROBES):
            probe_id = f"10000000-0000-4000-8000-{index:012d}"
            operation = f"20000000-0000-4000-8000-{index:012d}"
            registry.create(probe_id, namespace=NAMESPACE, create_operation_id=operation)
        with pytest.raises(RegistryConflict):
            registry.create(
                "10000000-0000-4000-8000-999999999999",
                namespace=NAMESPACE,
                create_operation_id="20000000-0000-4000-8000-999999999999",
            )
        ids = [row.probe_id for row in registry.probes()]
        assert ids == sorted(ids) and len(ids) == MAX_PROBES
        with pytest.raises(RegistryUnauthorized):
            registry.get("10000000-0000-4000-8000-999999999999")


def test_errors_are_fixed_and_detached(tmp_path):
    with opened(tmp_path) as registry:
        with pytest.raises(RegistryInvalid) as caught:
            registry.create("not-a-uuid", namespace=NAMESPACE, create_operation_id=OPERATION)
        assert caught.value.__cause__ is None and caught.value.__context__ is None
        assert str(caught.value) == "Protected launch registry refused operation"
    registry.close()
    with pytest.raises(RegistryUnavailable):
        registry.get(PROBE)


def test_storage_is_private_and_separate_from_the_launch_registries(tmp_path):
    with opened(tmp_path):
        files = sorted(os.listdir(tmp_path / "probes"))
        assert "probe.sqlite" in files and "probe.lock" in files
        assert not {"registry.sqlite", "commissioning.sqlite"} & set(files)
        for name in files:
            assert os.stat(tmp_path / "probes" / name).st_mode & 0o777 == 0o600
        db = sqlite3.connect(str(tmp_path / "probes" / "probe.sqlite"))
        try:
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master")}
        finally:
            db.close()
        assert {"launches", "assignments", "campaign"}.isdisjoint(tables)
