"""Commissioning launch bookkeeping, never launch authority. @spec PROTECTED-COMMISSIONING-8."""

from __future__ import annotations

import importlib
import importlib.util
import os
import sqlite3
from pathlib import Path
from typing import Any

import pytest

BOOT = "11111111-1111-4111-8111-111111111111"
NEXT_BOOT = "22222222-2222-4222-8222-222222222222"
LAUNCH = "33333333-3333-4333-8333-333333333333"
OTHER_LAUNCH = "44444444-4444-4444-8444-444444444444"
RESERVATION = "55555555-5555-4555-8555-555555555555"
OTHER_RESERVATION = "66666666-6666-4666-8666-666666666666"
POD_UID = "99999999-9999-4999-8999-999999999999"
SERVICE_UID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
SECRET_UID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
NODE_UID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
OWNER = "candidate-worker"
PERMIT = "a" * 64
OTHER_PERMIT = "b" * 64
DIGEST = "d" * 64
NAMESPACE = "protected-runtime"
CONTAINER = "containerd://" + "c" * 64


def api() -> Any:
    name = "curie_worker.commissioning_registry"
    assert importlib.util.find_spec(name) is not None, "commissioning launch registry is missing"
    return importlib.import_module(name)


def production() -> Any:
    return importlib.import_module("curie_worker.protected_launch_registry")


@pytest.fixture
def root(tmp_path: Path) -> Path:
    location = tmp_path.resolve() / "owned"
    location.mkdir(mode=0o700)
    return location


def open_registry(root: Path, boot: str = BOOT, permit: str = PERMIT) -> Any:
    return api().CommissioningLaunchRegistry.open(str(root), boot_id=boot, permit_digest=permit)


def binding(**changes: object) -> Any:
    values: dict[str, object] = {
        "permit_digest": PERMIT,
        "launch_request_digest": DIGEST,
        "projection_digest": DIGEST,
        "launch_reservation_id": RESERVATION,
    }
    values.update(changes)
    return api().CampaignLaunchBinding(**values)


def create(registry: Any, **changes: object) -> Any:
    values: dict[str, object] = {
        "worker_principal_id": OWNER,
        "launch_id": LAUNCH,
        "binding": binding(),
        "namespace": NAMESPACE,
        "creation_budget_ms": 60000,
    }
    values.update(changes)
    return registry.create(**values)


def assignment() -> Any:
    return production().AssignmentMetadata(
        "1" * 64, "2" * 64, "3" * 64, "history:acme", "1", "1700000000000", "model:acme", "1"
    )


def created(registry: Any) -> Any:
    """A launch whose three objects were created and their UIDs recorded."""
    row = create(registry)
    row = registry.record_assignment(OWNER, LAUNCH, row.revision, assignment())
    for kind, uid in (("secret", SECRET_UID), ("service", SERVICE_UID), ("pod", POD_UID)):
        row = registry.record_object_uid(OWNER, LAUNCH, row.revision, kind, uid, "7")
    return row


def termination() -> Any:
    return production().TerminationObservation(POD_UID, CONTAINER, "9", 0, "2026-10-21T00:00:00Z")


def test_names_are_deterministic_and_distinct_from_production() -> None:
    names = api().object_names(LAUNCH)
    hexid = LAUNCH.replace("-", "")
    assert names == ("pc-p-" + hexid, "pc-s-" + hexid, "pc-c-" + hexid)
    assert all(not name.startswith("pl-") for name in names)
    assert api().object_names(OTHER_LAUNCH) != names


def test_create_plans_the_deterministic_names_before_any_effect(root: Path) -> None:
    with open_registry(root) as registry:
        row = create(registry)
        pod, service, secret = api().object_names(LAUNCH)
        assert row.launch_id == LAUNCH and row.state == "preparing" and row.boot_id == BOOT
        assert (row.objects.namespace, row.objects.pod_name) == (NAMESPACE, pod)
        assert (row.objects.service_name, row.objects.secret_name) == (service, secret)
        assert row.objects.pod_uid is None and row.binding == binding()
        assert row.owned_epoch is None and row.termination is None


def test_create_replay_is_idempotent_and_any_change_conflicts(root: Path) -> None:
    errors = production()
    with open_registry(root) as registry:
        first = create(registry)
        assert create(registry) == first
        for changes in (
            {"binding": binding(launch_request_digest="e" * 64)},
            {"namespace": "other-namespace"},
            {"creation_budget_ms": 1000},
            {"worker_principal_id": "other-worker"},
        ):
            with pytest.raises((errors.RegistryConflict, errors.RegistryUnauthorized)):
                create(registry, **changes)


def test_one_launch_reservation_backs_one_launch(root: Path) -> None:
    with open_registry(root) as registry:
        create(registry)
        with pytest.raises(production().RegistryConflict):
            create(registry, launch_id=OTHER_LAUNCH)
        assert (
            create(
                registry,
                launch_id=OTHER_LAUNCH,
                binding=binding(launch_reservation_id=OTHER_RESERVATION),
            ).launch_id
            == OTHER_LAUNCH
        )


def test_registry_is_bound_to_one_campaign_permit(root: Path) -> None:
    errors = production()
    with open_registry(root) as registry, pytest.raises(errors.RegistryConflict):
        create(registry, binding=binding(permit_digest=OTHER_PERMIT))
    with pytest.raises(errors.RegistryError):
        open_registry(root, permit=OTHER_PERMIT)
    with open_registry(root) as registry:
        assert registry.permit_digest == PERMIT


def test_separate_files_from_the_production_registry(root: Path) -> None:
    with open_registry(root):
        pass
    production().ProtectedLaunchRegistry.open(str(root), boot_id=BOOT).close()
    assert {"commissioning.sqlite", "registry.sqlite"} <= set(os.listdir(root))
    with sqlite3.connect(root / "commissioning.sqlite") as db:
        columns = {row[1] for row in db.execute("PRAGMA table_info(launches)")}
    assert "qualification_id" not in columns and "permit_digest" in columns


def test_exclusive_owner_lock(root: Path) -> None:
    with open_registry(root), pytest.raises(production().RegistryUnavailable):
        open_registry(root)


def test_uid_is_recorded_only_from_the_create_response(root: Path) -> None:
    """A same-name object found by name during retirement is never adopted."""
    errors = production()
    with open_registry(root) as registry:
        row = create(registry)
        row = registry.record_object_uid(OWNER, LAUNCH, row.revision, "secret", SECRET_UID, "7")
        row = registry.transition(OWNER, LAUNCH, row.revision, "retiring")
        with pytest.raises(errors.RegistryConflict):
            registry.record_object_uid(OWNER, LAUNCH, row.revision, "pod", POD_UID, "7")
        assert registry.get(OWNER, LAUNCH).objects.pod_uid is None


def test_uid_is_never_replaced(root: Path) -> None:
    with open_registry(root) as registry:
        row = created(registry)
        with pytest.raises(production().RegistryConflict):
            registry.record_object_uid(OWNER, LAUNCH, row.revision, "pod", NODE_UID, "8")


def test_witnessed_resolution_only_for_an_unknown_uid_while_retiring(root: Path) -> None:
    errors = production()
    with open_registry(root) as registry:
        row = create(registry)
        with pytest.raises(errors.RegistryConflict):
            registry.record_witnessed_uid(OWNER, LAUNCH, row.revision, "pod", POD_UID, "7")
        row = registry.transition(OWNER, LAUNCH, row.revision, "retiring")
        row = registry.record_witnessed_uid(OWNER, LAUNCH, row.revision, "pod", POD_UID, "7")
        assert row.objects.pod_uid == POD_UID
        with pytest.raises(errors.RegistryConflict):
            registry.record_witnessed_uid(OWNER, LAUNCH, row.revision, "pod", NODE_UID, "7")


def test_lifecycle_to_retired_needs_termination_and_exact_cleanup(root: Path) -> None:
    errors = production()
    with open_registry(root) as registry:
        row = created(registry)
        row = registry.record_container_identity(OWNER, LAUNCH, row.revision, CONTAINER, NODE_UID)
        row = registry.transition(OWNER, LAUNCH, row.revision, "idle")
        assert row.state == "idle"
        with pytest.raises(errors.RegistryConflict):
            registry.transition(OWNER, LAUNCH, row.revision, "retired")
        row = registry.transition(OWNER, LAUNCH, row.revision, "retiring")
        receipt = production().CleanupReceipt(POD_UID, SERVICE_UID, SECRET_UID)
        with pytest.raises(errors.RegistryConflict):
            registry.record_cleanup_receipt(OWNER, LAUNCH, row.revision, receipt)
        row = registry.record_termination_observation(OWNER, LAUNCH, row.revision, termination())
        with pytest.raises(errors.RegistryConflict):
            registry.record_cleanup_receipt(
                OWNER,
                LAUNCH,
                row.revision,
                production().CleanupReceipt(POD_UID, SERVICE_UID, NODE_UID),
            )
        row = registry.record_cleanup_receipt(OWNER, LAUNCH, row.revision, receipt)
        assert row.state == "retired" and row.retirement_outcome == "cleanup-recorded"
        with pytest.raises(errors.RegistryRetired):
            registry.transition(OWNER, LAUNCH, row.revision, "retiring")


def test_stale_revision_conflicts(root: Path) -> None:
    with open_registry(root) as registry:
        row = create(registry)
        registry.record_assignment(OWNER, LAUNCH, row.revision, assignment())
        with pytest.raises(production().RegistryConflict):
            registry.transition(OWNER, LAUNCH, row.revision, "retiring")


def test_other_owner_reads_nothing(root: Path) -> None:
    with open_registry(root) as registry:
        create(registry)
        with pytest.raises(production().RegistryUnauthorized):
            registry.get("other-worker", LAUNCH)
        assert registry.find(OWNER, OTHER_LAUNCH) is None
        assert registry.find(OWNER, LAUNCH) == registry.get(OWNER, LAUNCH)


def test_restart_retires_every_live_launch_and_keeps_its_names(root: Path) -> None:
    errors = production()
    with open_registry(root) as registry:
        create(registry)
    with open_registry(root, boot=NEXT_BOOT) as registry:
        row = registry.get(OWNER, LAUNCH)
        assert row.state == "retiring" and row.boot_id == BOOT
        assert row.objects.pod_name == api().object_names(LAUNCH)[0]
        assert [r.launch_id for r in registry.enumerate_retiring(limit=64)] == [LAUNCH]
        # The old boot's replay never resumes creation.
        with pytest.raises(errors.RegistryRetired):
            create(registry)
        with pytest.raises(errors.RegistryConflict):
            registry.record_object_uid(OWNER, LAUNCH, row.revision, "pod", POD_UID, "7")


def test_positive_effects_refuse_on_an_old_boot_row(root: Path) -> None:
    with open_registry(root) as registry:
        row = create(registry)
    with open_registry(root, boot=NEXT_BOOT) as registry:
        with pytest.raises(production().RegistryConflict):
            registry.record_assignment(
                OWNER, LAUNCH, registry.get(OWNER, LAUNCH).revision, assignment()
            )
        assert row.boot_id == BOOT


def test_binding_and_snapshot_are_closed_values() -> None:
    module = api()
    with pytest.raises(production().RegistryInvalid):
        binding(permit_digest="not-a-digest")
    with pytest.raises(production().RegistryInvalid):
        binding(launch_reservation_id="not-a-uuid")
    with pytest.raises(production().RegistryInvalid):
        module.CommissioningLaunchRegistry()
    assert not hasattr(module, "LaunchOwnership")


def test_module_imports_no_commissioning_record() -> None:
    source = Path(api().__file__).read_text(encoding="utf-8")
    assert "curie_protected_hooks" not in source
