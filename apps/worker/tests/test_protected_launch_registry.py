"""Actual durable bookkeeping, never launch qualification. @spec PROTECTED-HOOK-LANE-8."""

from __future__ import annotations

import dataclasses
import importlib
import importlib.util
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

BOOT = "11111111-1111-4111-8111-111111111111"
NEXT_BOOT = "22222222-2222-4222-8222-222222222222"
REQUEST = "33333333-3333-4333-8333-333333333333"
OTHER_REQUEST = "44444444-4444-4444-8444-444444444444"
OPERATION = "55555555-5555-4555-8555-555555555555"
RUNTIME = "66666666-6666-4666-8666-666666666666"
QUALIFICATION = "77777777-7777-4777-8777-777777777777"
GUARD = "88888888-8888-4888-8888-888888888888"
POD_UID = "99999999-9999-4999-8999-999999999999"
SERVICE_UID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
SECRET_UID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
OWNER = "worker:acme-a"
OTHER_OWNER = "worker:acme-b"
DIGEST = "a" * 64
OTHER_DIGEST = "b" * 64
EPOCH = "e" * 43
CONTAINER = "containerd://" + "c" * 64


def api() -> Any:
    name = "curie_worker.protected_launch_registry"
    assert importlib.util.find_spec(name) is not None, "durable launch registry is missing"
    return importlib.import_module(name)


def ownership(**changes: object) -> Any:
    values: dict[str, object] = {
        "runtime_id": RUNTIME,
        "runtime_generation": "1",
        "manifest_digest": DIGEST,
        "qualification_id": QUALIFICATION,
        "qualification_generation": "1",
        "measurement_ref": "measurement:acme-v1",
        "execution_config_digest": DIGEST,
        "event_id": "event:acme-1",
        "committed_stream_id": "100-1",
        "payload_digest": DIGEST,
        "execution_session_key": f"protected:{RUNTIME}:1:{DIGEST}",
        "projection_digest": DIGEST,
        "guard_control_id": GUARD,
        "guard_revision": "1",
        "guard_config_digest": DIGEST,
    }
    values.update(changes)
    return api().LaunchOwnership(**values)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    location = tmp_path.resolve() / "owned"
    location.mkdir(mode=0o700)
    return location


def open_registry(root: Path, boot: str = BOOT) -> Any:
    return api().ProtectedLaunchRegistry.open(str(root), boot_id=boot)


def create(registry: Any, **changes: object) -> Any:
    values: dict[str, object] = {
        "worker_principal_id": OWNER,
        "request_id": REQUEST,
        "request_digest": DIGEST,
        "ownership": ownership(),
        "creation_budget_ms": 120000,
    }
    values.update(changes)
    return registry.create(**values)


def idle(registry: Any) -> Any:
    row = create(registry)
    return registry.transition(OWNER, row.launch_id, row.revision, "idle")


def plan(registry: Any, row: Any) -> Any:
    return registry.plan_objects(
        OWNER,
        row.launch_id,
        row.revision,
        namespace="acme",
        pod_name="acme-pod",
        service_name="acme-service",
        secret_name="acme-secret",
    )


def objects(registry: Any, row: Any) -> Any:
    row = plan(registry, row)
    for kind, uid in (("pod", POD_UID), ("service", SERVICE_UID), ("secret", SECRET_UID)):
        row = registry.record_object_uid(
            OWNER,
            row.launch_id,
            row.revision,
            kind,
            uid,
            "1",
        )
    return registry.record_container_identity(
        OWNER,
        row.launch_id,
        row.revision,
        CONTAINER,
        GUARD,
    )


def retirement(registry: Any, row: Any) -> Any:
    row = registry.transition(OWNER, row.launch_id, row.revision, "retiring")
    observation = api().TerminationObservation(
        pod_uid=POD_UID,
        container_id=CONTAINER,
        resource_version="2",
        exit_code=0,
        finished_at="2026-01-01T00:00:00Z",
    )
    return registry.record_termination_observation(
        OWNER,
        row.launch_id,
        row.revision,
        observation,
    )


def test_create_exact_retry_returns_one_persisted_launch(root: Path) -> None:
    with open_registry(root) as registry:
        row = create(registry)
        assert row.state == "preparing"
        assert row.revision == "1"
        assert create(registry) == row
        assert registry.get(OWNER, row.launch_id) == row
        with pytest.raises(dataclasses.FrozenInstanceError):
            row.state = "idle"
        assert OWNER not in repr(row)
        assert DIGEST not in repr(row)


def test_retiring_enumeration_recovers_all_owners_with_bounded_keyset_pages(root: Path) -> None:
    with open_registry(root) as registry:
        rows = []
        for number in range(3):
            row = create(
                registry,
                worker_principal_id=OTHER_OWNER if number == 1 else OWNER,
                request_id=f"00000000-0000-4000-8000-{number:012d}",
                ownership=ownership(execution_session_key=f"protected:{RUNTIME}:1:{number:064x}"),
            )
            rows.append(row)
        assert registry.enumerate_retiring(limit=2) == ()
    with open_registry(root, NEXT_BOOT) as registry:
        expected = sorted(rows, key=lambda row: row.launch_id)
        first = registry.enumerate_retiring(limit=2)
        assert [row.launch_id for row in first] == [row.launch_id for row in expected[:2]]
        assert all(row.state == "retiring" and row.boot_id == BOOT for row in first)
        assert {row.worker_principal_id for row in first} <= {OWNER, OTHER_OWNER}
        last = registry.enumerate_retiring(limit=2, after_launch_id=first[-1].launch_id)
        assert [row.launch_id for row in last] == [expected[-1].launch_id]
        assert registry.enumerate_retiring(limit=2, after_launch_id=last[-1].launch_id) == ()
        assert registry.get(last[0].worker_principal_id, last[0].launch_id) == last[0]
        with pytest.raises(dataclasses.FrozenInstanceError):
            last[0].state = "idle"


@pytest.mark.parametrize("limit", [True, False, 0, -1, 65, 2.0, "2", None])
def test_retiring_enumeration_rejects_unbounded_or_noninteger_limit(
    root: Path, limit: object
) -> None:
    with open_registry(root) as registry:
        with pytest.raises(api().RegistryInvalid):
            registry.enumerate_retiring(limit=limit)


def test_retiring_enumeration_rejects_invalid_cursor_and_closed_registry(root: Path) -> None:
    registry = open_registry(root)
    with pytest.raises(api().RegistryInvalid):
        registry.enumerate_retiring(limit=64, after_launch_id="SYNTHETIC-PRIVATE-MARKER")
    registry.close()
    with pytest.raises(api().RegistryUnavailable):
        registry.enumerate_retiring(limit=64)


def test_exact_request_lookup_is_owner_scoped_and_retains_retired_tombstone(root: Path) -> None:
    with open_registry(root) as registry:
        row = create(registry)
        assert registry.find_request(OWNER, REQUEST) == row
        assert registry.find_request(OTHER_OWNER, REQUEST) is None
        assert registry.find_request(OWNER, OTHER_REQUEST) is None
        row = retirement(registry, objects(registry, row))
        row = registry.record_cleanup_receipt(
            OWNER, row.launch_id, row.revision,
            api().CleanupReceipt(pod_uid=POD_UID, service_uid=SERVICE_UID, secret_uid=SECRET_UID),
        )
        assert registry.find_request(OWNER, REQUEST) == row
    with open_registry(root, NEXT_BOOT) as registry:
        assert registry.find_request(OWNER, REQUEST) == row
        assert row.state == "retired"


def test_exact_request_lookup_observes_restart_retiring_without_positive_proof(root: Path) -> None:
    with open_registry(root) as registry:
        row = create(registry)
    with open_registry(root, NEXT_BOOT) as registry:
        found = registry.find_request(OWNER, REQUEST)
        assert found.launch_id == row.launch_id
        assert found.state == "retiring" and found.boot_id == BOOT


@pytest.mark.parametrize("owner,request_id", [("", REQUEST), (OWNER, "invalid-request")])
def test_exact_request_lookup_validates_identity_and_closed_lifetime(
    root: Path, owner: str, request_id: str
) -> None:
    registry = open_registry(root)
    with pytest.raises(api().RegistryInvalid):
        registry.find_request(owner, request_id)
    registry.close()
    with pytest.raises(api().RegistryUnavailable):
        registry.find_request(OWNER, REQUEST)


def test_creation_budget_is_explicit_immutable_and_persisted(root: Path) -> None:
    with open_registry(root) as registry:
        with pytest.raises(api().RegistryInvalid):
            registry.create(
                worker_principal_id=OWNER,
                request_id=REQUEST,
                request_digest=DIGEST,
                ownership=ownership(),
            )
        row = create(registry, creation_budget_ms=5000)
        assert row.creation_budget_ms == 5000
        assert create(registry, creation_budget_ms=5000) == row
        with pytest.raises(api().RegistryConflict):
            create(registry, creation_budget_ms=5001)
        with pytest.raises(dataclasses.FrozenInstanceError):
            row.creation_budget_ms = 120000
    with open_registry(root, NEXT_BOOT) as registry:
        restored = registry.get(OWNER, row.launch_id)
        assert restored.creation_budget_ms == 5000
        assert restored.state == "retiring"
        with pytest.raises(api().RegistryRetired):
            create(registry, creation_budget_ms=5000)
        with pytest.raises(api().RegistryConflict):
            create(registry, creation_budget_ms=120000)


@pytest.mark.parametrize("budget", [True, False, 0, -1, 120001, 1.0, "5000", None])
def test_creation_budget_rejects_noncontract_values(root: Path, budget: object) -> None:
    with open_registry(root) as registry:
        with pytest.raises(api().RegistryInvalid):
            create(registry, creation_budget_ms=budget)
        assert create(registry, creation_budget_ms=1).creation_budget_ms == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"request_digest": OTHER_DIGEST},
        {"ownership_changes": {"manifest_digest": OTHER_DIGEST}},
    ],
)
def test_create_retry_changed_bytes_or_metadata_conflicts(root: Path, changes: dict) -> None:
    with open_registry(root) as registry:
        create(registry)
        if "ownership_changes" in changes:
            changes = {"ownership": ownership(**changes["ownership_changes"])}
        with pytest.raises(api().RegistryConflict):
            create(registry, **changes)


def test_active_execution_lock_survives_ambiguous_failure(root: Path) -> None:
    with open_registry(root) as registry:
        row = create(registry)
        registry.transition(OWNER, row.launch_id, row.revision, "failed")
        with pytest.raises(api().RegistryConflict):
            create(registry, request_id=OTHER_REQUEST)


def test_different_execution_and_principal_can_reuse_request_id(root: Path) -> None:
    with open_registry(root) as registry:
        first = create(registry)
        second = create(
            registry,
            worker_principal_id=OTHER_OWNER,
            ownership=ownership(execution_session_key=f"protected:{RUNTIME}:1:{OTHER_DIGEST}"),
        )
        assert first.launch_id != second.launch_id


@pytest.mark.parametrize("action", ["get", "transition", "plan", "begin", "operation"])
def test_cross_principal_cannot_observe_or_mutate(root: Path, action: str) -> None:
    with open_registry(root) as registry:
        row = idle(registry)
        if action == "operation":
            registry.begin_operation(OWNER, row.launch_id, row.revision, OPERATION, "start", DIGEST)
        with pytest.raises(api().RegistryUnauthorized):
            if action == "get":
                registry.get(OTHER_OWNER, row.launch_id)
            elif action == "transition":
                registry.transition(OTHER_OWNER, row.launch_id, row.revision, "retiring")
            elif action == "plan":
                registry.plan_objects(
                    OTHER_OWNER,
                    row.launch_id,
                    row.revision,
                    namespace="acme",
                    pod_name="a",
                    service_name="b",
                    secret_name="c",
                )
            elif action == "begin":
                registry.begin_operation(
                    OTHER_OWNER, row.launch_id, row.revision, OPERATION, "start", DIGEST
                )
            else:
                registry.get_operation(OTHER_OWNER, row.launch_id, OPERATION)


def test_lifecycle_cas_refuses_stale_revision(root: Path) -> None:
    with open_registry(root) as registry:
        first = create(registry)
        next_row = registry.transition(OWNER, first.launch_id, first.revision, "idle")
        assert next_row.revision == "2"
        with pytest.raises(api().RegistryConflict):
            registry.transition(OWNER, first.launch_id, first.revision, "retiring")
        assert registry.get(OWNER, first.launch_id).state == "idle"


@pytest.mark.parametrize("state", ["preparing", "idle", "waiting", "running", "failed", "retiring"])
def test_restart_retires_every_old_nonterminal_state(root: Path, state: str) -> None:
    with open_registry(root) as registry:
        row = create(registry)
        if state in {"idle", "waiting", "running"}:
            row = registry.transition(OWNER, row.launch_id, row.revision, "idle")
        if state in {"waiting", "running"}:
            reservation = registry.begin_operation(
                OWNER, row.launch_id, row.revision, OPERATION, "start", DIGEST
            )
            row = registry.finish_operation(
                OWNER,
                row.launch_id,
                reservation.revision,
                OPERATION,
                "applied",
                epoch=EPOCH,
                state=state,
            )
        elif state in {"failed", "retiring"}:
            row = registry.transition(OWNER, row.launch_id, row.revision, state)
    with open_registry(root, NEXT_BOOT) as registry:
        recovered = registry.get(OWNER, row.launch_id)
        assert recovered.state == "retiring"
        assert recovered.boot_id == BOOT
        with pytest.raises(api().RegistryRetired):
            create(registry)
        with pytest.raises(api().RegistryRetired):
            registry.transition(OWNER, row.launch_id, recovered.revision, "idle")
        with pytest.raises(api().RegistryConflict):
            create(registry, request_id=OTHER_REQUEST)


@pytest.mark.parametrize("target", ["preparing", "idle", "waiting", "running", "failed"])
def test_retiring_cannot_return_to_active(root: Path, target: str) -> None:
    with open_registry(root) as registry:
        row = create(registry)
        row = registry.transition(OWNER, row.launch_id, row.revision, "retiring")
        with pytest.raises(api().RegistryRetired):
            registry.transition(OWNER, row.launch_id, row.revision, target)


def test_names_are_durable_before_uids_and_cannot_be_remapped(root: Path) -> None:
    with open_registry(root) as registry:
        row = create(registry)
        row = plan(registry, row)
        assert row.objects.pod_name == "acme-pod"
        assert row.objects.pod_uid is None
        with pytest.raises(api().RegistryConflict):
            registry.record_object_uid(OWNER, row.launch_id, "1", "pod", POD_UID, "1")
        row = registry.record_object_uid(OWNER, row.launch_id, row.revision, "pod", POD_UID, "1")
        with pytest.raises(api().RegistryConflict):
            registry.record_object_uid(OWNER, row.launch_id, row.revision, "pod", SERVICE_UID, "1")
        with pytest.raises(api().RegistryConflict):
            registry.plan_objects(
                OWNER,
                row.launch_id,
                row.revision,
                namespace="acme",
                pod_name="other-pod",
                service_name="s",
                secret_name="t",
            )
    with open_registry(root, NEXT_BOOT) as registry:
        assert registry.get(OWNER, row.launch_id).objects.pod_uid == POD_UID


def test_operation_replay_is_never_another_new_reservation(root: Path) -> None:
    with open_registry(root) as registry:
        row = idle(registry)
        first = registry.begin_operation(
            OWNER, row.launch_id, row.revision, OPERATION, "start", DIGEST
        )
        replay = registry.begin_operation(
            OWNER, row.launch_id, row.revision, OPERATION, "start", DIGEST
        )
        assert first.disposition == "new"
        assert replay.disposition == "existing"
        assert replay.operation.status == "pending"
        assert replay.operation == first.operation
        with pytest.raises(api().RegistryConflict):
            registry.begin_operation(
                OWNER, row.launch_id, first.revision, OPERATION, "start", OTHER_DIGEST
            )
        with pytest.raises(api().RegistryConflict):
            registry.finish_operation(
                OWNER,
                row.launch_id,
                row.revision,
                OPERATION,
                "applied",
                epoch=EPOCH,
                state="running",
            )
        row = registry.finish_operation(
            OWNER, row.launch_id, first.revision, OPERATION, "applied", epoch=EPOCH, state="running"
        )
        assert row.owned_epoch == EPOCH
        assert registry.get_operation(OWNER, row.launch_id, OPERATION).status == "applied"
        assert (
            registry.begin_operation(
                OWNER, row.launch_id, row.revision, OPERATION, "start", DIGEST
            ).disposition
            == "existing"
        )


def test_pending_operation_becomes_uncertain_after_restart(root: Path) -> None:
    with open_registry(root) as registry:
        row = idle(registry)
        registry.begin_operation(OWNER, row.launch_id, row.revision, OPERATION, "start", DIGEST)
    with open_registry(root, NEXT_BOOT) as registry:
        recovered = registry.get(OWNER, row.launch_id)
        assert recovered.state == "retiring"
        assert registry.get_operation(OWNER, row.launch_id, OPERATION).status == "uncertain"
        replay = registry.begin_operation(
            OWNER, row.launch_id, recovered.revision, OPERATION, "start", DIGEST
        )
        assert replay.disposition == "existing"
        assert replay.operation.status == "uncertain"


def test_uncertain_outbound_acceptance_irreversibly_retires(root: Path) -> None:
    with open_registry(root) as registry:
        row = idle(registry)
        reservation = registry.begin_operation(
            OWNER, row.launch_id, row.revision, OPERATION, "start", DIGEST
        )
        row = registry.finish_operation(
            OWNER, row.launch_id, reservation.revision, OPERATION, "uncertain"
        )
        assert row.state == "retiring"
        with pytest.raises(api().RegistryConflict):
            create(registry, request_id=OTHER_REQUEST)


def test_retire_journals_and_revokes_positive_operations_atomically(root: Path) -> None:
    with open_registry(root) as registry:
        row = idle(registry)
        reservation = registry.begin_operation(
            OWNER, row.launch_id, row.revision, OPERATION, "retire", DIGEST
        )
        row = registry.get(OWNER, row.launch_id)
        assert row.state == "retiring"
        assert row.revision == reservation.revision
        with pytest.raises(api().RegistryRetired):
            registry.begin_operation(OWNER, row.launch_id, row.revision, REQUEST, "start", DIGEST)


def test_admit_and_stop_require_exact_owned_epoch(root: Path) -> None:
    with open_registry(root) as registry:
        row = idle(registry)
        reservation = registry.begin_operation(
            OWNER, row.launch_id, row.revision, OPERATION, "start", DIGEST
        )
        row = registry.finish_operation(
            OWNER,
            row.launch_id,
            reservation.revision,
            OPERATION,
            "applied",
            epoch=EPOCH,
            state="waiting",
        )
        for kind in ("admit", "deny", "interrupt", "timeout"):
            with pytest.raises(api().RegistryConflict):
                registry.begin_operation(
                    OWNER, row.launch_id, row.revision, REQUEST, kind, DIGEST, epoch="x" * 43
                )
        row = registry.transition(OWNER, row.launch_id, row.revision, "retiring")
        reservation = registry.begin_operation(
            OWNER, row.launch_id, row.revision, REQUEST, "interrupt", DIGEST, epoch=EPOCH
        )
        assert reservation.operation.epoch == EPOCH


def test_retired_requires_durable_exact_termination_and_closed_cleanup_receipt(root: Path) -> None:
    with open_registry(root) as registry:
        row = objects(registry, create(registry))
        row = registry.transition(OWNER, row.launch_id, row.revision, "retiring")
        with pytest.raises(api().RegistryConflict):
            registry.transition(OWNER, row.launch_id, row.revision, "retired")
        receipt = api().CleanupReceipt(
            pod_uid=POD_UID, service_uid=SERVICE_UID, secret_uid=SECRET_UID
        )
        with pytest.raises(api().RegistryConflict):
            registry.record_cleanup_receipt(OWNER, row.launch_id, row.revision, receipt)
        observation = api().TerminationObservation(
            pod_uid=POD_UID,
            container_id=CONTAINER,
            resource_version="2",
            exit_code=0,
            finished_at="2026-01-01T00:00:00Z",
        )
        bad = dataclasses.replace(observation, pod_uid=SERVICE_UID)
        with pytest.raises(api().RegistryConflict):
            registry.record_termination_observation(OWNER, row.launch_id, row.revision, bad)
        initial = row.revision
        row = registry.record_termination_observation(
            OWNER, row.launch_id, row.revision, observation
        )
        with pytest.raises(api().RegistryConflict):
            registry.record_termination_observation(OWNER, row.launch_id, initial, observation)
        with pytest.raises(api().RegistryConflict):
            registry.record_cleanup_receipt(OWNER, row.launch_id, initial, receipt)
        with pytest.raises(api().RegistryConflict):
            registry.record_cleanup_receipt(
                OWNER, row.launch_id, row.revision, dataclasses.replace(receipt, secret_uid=POD_UID)
            )
        row = registry.record_cleanup_receipt(OWNER, row.launch_id, row.revision, receipt)
        assert row.state == "retired"
        assert row.retirement_outcome == "cleanup-recorded"
        new = create(registry, request_id=OTHER_REQUEST)
        assert new.launch_id != row.launch_id
    with open_registry(root, NEXT_BOOT) as registry:
        assert registry.get(OWNER, row.launch_id).state == "retired"


def test_restart_preserves_termination_for_owned_cleanup(root: Path) -> None:
    with open_registry(root) as registry:
        row = retirement(registry, objects(registry, create(registry)))
    with open_registry(root, NEXT_BOOT) as registry:
        row = registry.get(OWNER, row.launch_id)
        assert row.termination.pod_uid == POD_UID
        receipt = api().CleanupReceipt(
            pod_uid=POD_UID, service_uid=SERVICE_UID, secret_uid=SECRET_UID
        )
        assert (
            registry.record_cleanup_receipt(OWNER, row.launch_id, row.revision, receipt).state
            == "retired"
        )


def child(code: str, root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", code, str(root)],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


def test_actual_exclusive_os_lock_is_held_for_lifetime_and_released(root: Path) -> None:
    code = f"""
import sys
from curie_worker.protected_launch_registry import ProtectedLaunchRegistry, RegistryUnavailable
try:
    registry = ProtectedLaunchRegistry.open(sys.argv[1], boot_id={NEXT_BOOT!r})
except RegistryUnavailable:
    sys.exit(23)
registry.close()
"""
    with open_registry(root):
        result = child(code, root)
        assert result.returncode == 23, result.stderr
    result = child(code, root)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("durable", [False, True])
def test_process_exit_before_and_after_commit_does_not_create_second_route(
    root: Path, durable: bool
) -> None:
    # A real killed process releases the OS lock; SQLite's committed row is the fence.
    values = dataclasses.asdict(ownership())
    code = f"""
import os, sys
from curie_worker.protected_launch_registry import ProtectedLaunchRegistry, LaunchOwnership
r = ProtectedLaunchRegistry.open(sys.argv[1], boot_id={BOOT!r})
if {durable!r}:
    row = r.create(worker_principal_id={OWNER!r}, request_id={REQUEST!r},
                   request_digest={DIGEST!r}, ownership=LaunchOwnership(**{values!r}),
                   creation_budget_ms=120000)
    row = r.transition({OWNER!r}, row.launch_id, row.revision, 'idle')
    r.begin_operation({OWNER!r}, row.launch_id, row.revision, {OPERATION!r}, 'start', {DIGEST!r})
os._exit(29)
"""
    result = child(code, root)
    assert result.returncode == 29, result.stderr
    with open_registry(root, NEXT_BOOT) as registry:
        if durable:
            with pytest.raises(api().RegistryRetired):
                create(registry)
            with pytest.raises(api().RegistryConflict):
                create(registry, request_id=OTHER_REQUEST)
        else:
            assert create(registry).state == "preparing"


@pytest.mark.parametrize(
    "case", ["relative", "symlink-root", "symlink-parent", "mode", "db-link", "lock-link"]
)
def test_unsafe_registry_paths_refuse_without_touching_target(root: Path, case: str) -> None:
    location = root
    target = root.parent / "sentinel"
    target.write_text("SYNTHETIC-PRIVATE-MARKER")
    if case == "relative":
        location = Path("relative")
    elif case in {"symlink-root", "symlink-parent"}:
        alias = root.parent / "alias"
        alias.symlink_to(root if case == "symlink-root" else root.parent, target_is_directory=True)
        location = alias if case == "symlink-root" else alias / root.name
    elif case == "mode":
        root.chmod(0o755)
    elif case == "db-link":
        (root / "registry.sqlite").symlink_to(target)
    else:
        (root / "registry.lock").symlink_to(target)
    with pytest.raises(api().RegistryInvalid):
        open_registry(location)
    assert target.read_text() == "SYNTHETIC-PRIVATE-MARKER"


def test_refusal_exception_graph_detaches_input_path_and_ambient_caller(root: Path) -> None:
    marker = "SYNTHETIC-PROMPT-BEARER-MARKER"
    try:
        raise ValueError(marker)
    except ValueError:
        with pytest.raises(api().RegistryInvalid) as caught:
            open_registry(Path(marker))
    assert marker not in str(caught.value)
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
    with open_registry(root) as registry:
        with pytest.raises(api().RegistryInvalid) as caught:
            create(registry, request_digest=marker)
        assert caught.value.__context__ is None
        assert caught.value.__cause__ is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("runtime_generation", True),
        ("runtime_generation", "01"),
        ("runtime_generation", "9223372036854775808"),
        ("runtime_id", "runtime"),
        ("payload_digest", "SYNTHETIC-PROMPT-MARKER"),
        ("committed_stream_id", "0-0"),
        ("execution_session_key", "ordinary-session"),
        ("measurement_ref", "x\nsecret"),
    ],
)
def test_closed_ownership_rejects_noncontract_metadata(field: str, value: object) -> None:
    with pytest.raises(api().RegistryInvalid):
        ownership(**{field: value})


def test_no_raw_payload_token_or_arbitrary_json_can_enter_sqlite_or_journal(root: Path) -> None:
    marker = "SYNTHETIC-PROMPT-AND-TOKEN-MARKER"
    with open_registry(root) as registry:
        row = idle(registry)
        for field in (
            "receipt",
            "source_fields",
            "prompt",
            "capability",
            "bearer",
            "history",
            "model_key",
        ):
            with pytest.raises(TypeError):
                create(registry, **{field: marker})
        with pytest.raises(api().RegistryInvalid):
            registry.begin_operation(OWNER, row.launch_id, row.revision, OPERATION, "start", marker)
        with pytest.raises(TypeError):
            api().LaunchOwnership(**dataclasses.asdict(ownership()), receipt={"raw": marker})
        db = sqlite3.connect(root / "registry.sqlite")
        try:
            columns = {
                entry[1]
                for table in ("launches", "operations")
                for entry in db.execute(f"PRAGMA table_info({table})")
            }
            assert not columns & {
                "payload",
                "source_fields",
                "prompt",
                "receipt",
                "bearer",
                "capability",
                "history",
                "model_key",
                "json",
                "raw",
            }
        finally:
            db.close()
        for path in root.iterdir():
            if path.is_file():
                assert marker.encode() not in path.read_bytes()


def test_close_is_idempotent_does_not_release_another_owner(root: Path) -> None:
    first = open_registry(root)
    first.close()
    second = open_registry(root, NEXT_BOOT)
    try:
        first.close()
        with pytest.raises(api().RegistryUnavailable):
            open_registry(root)
        with pytest.raises(api().RegistryUnavailable):
            create(first)
    finally:
        second.close()


def assignment() -> Any:
    return api().AssignmentMetadata(
        control_capability_sha256=DIGEST,
        runner_token_sha256=DIGEST,
        history_binding_sha256=DIGEST,
        history_signing_credential_id="credential:history",
        history_signing_credential_generation="1",
        history_expires_at_ms="1893456000000",
        model_credential_id="credential:model",
        model_credential_generation="1",
    )


def test_assignment_and_container_mutations_require_owner_and_current_revision(root: Path) -> None:
    with open_registry(root) as registry:
        row = objects(registry, create(registry))
        for owner, revision, error in (
            (OTHER_OWNER, row.revision, api().RegistryUnauthorized),
            (OWNER, "1", api().RegistryConflict),
        ):
            with pytest.raises(error):
                registry.record_assignment(owner, row.launch_id, revision, assignment())
            with pytest.raises(error):
                registry.record_container_identity(owner, row.launch_id, revision, CONTAINER, GUARD)
            with pytest.raises(error):
                registry.record_object_uid(owner, row.launch_id, revision, "pod", POD_UID, "1")
        row = registry.record_assignment(OWNER, row.launch_id, row.revision, assignment())
        assert row.assignment.history_signing_credential_id == "credential:history"
        with pytest.raises(api().RegistryConflict):
            registry.record_assignment(OWNER, row.launch_id, row.revision, assignment())
        with pytest.raises(api().RegistryConflict):
            registry.record_container_identity(
                OWNER, row.launch_id, row.revision, "containerd://" + "d" * 64, GUARD
            )


def test_failed_creation_with_effect_intent_is_retiring(root: Path) -> None:
    with open_registry(root) as registry:
        row = plan(registry, create(registry))
        row = registry.transition(OWNER, row.launch_id, row.revision, "failed")
        assert row.state == "retiring"
        with pytest.raises(api().RegistryConflict):
            create(registry, request_id=OTHER_REQUEST)


def test_pending_positive_operation_blocks_different_operation_id(root: Path) -> None:
    with open_registry(root) as registry:
        row = idle(registry)
        first = registry.begin_operation(
            OWNER, row.launch_id, row.revision, OPERATION, "start", DIGEST
        )
        with pytest.raises(api().RegistryConflict):
            registry.begin_operation(OWNER, row.launch_id, first.revision, REQUEST, "start", DIGEST)
        with pytest.raises(api().RegistryConflict):
            registry.begin_operation(OWNER, row.launch_id, "1", REQUEST, "retire", DIGEST)


def test_reused_boot_id_still_retires_existing_launches(root: Path) -> None:
    with open_registry(root) as registry:
        row = idle(registry)
    with open_registry(root) as registry:
        row = registry.get(OWNER, row.launch_id)
        assert row.state == "retiring"
        with pytest.raises(api().RegistryRetired):
            registry.begin_operation(OWNER, row.launch_id, row.revision, OPERATION, "start", DIGEST)


@pytest.mark.parametrize(
    "name", ["registry.sqlite-wal", "registry.sqlite-shm", "registry.sqlite-journal"]
)
def test_preexisting_sqlite_sidecar_symlink_is_refused(root: Path, name: str) -> None:
    target = root.parent / "sentinel"
    target.write_text("SYNTHETIC-PRIVATE-MARKER")
    (root / name).symlink_to(target)
    with pytest.raises(api().RegistryInvalid):
        open_registry(root)
    assert target.read_text() == "SYNTHETIC-PRIVATE-MARKER"


def test_refusals_and_repeated_open_close_do_not_leak_descriptors(root: Path) -> None:
    before = len(os.listdir("/dev/fd"))
    for _ in range(20):
        with open_registry(root):
            with pytest.raises(api().RegistryUnavailable):
                open_registry(root)
    assert len(os.listdir("/dev/fd")) == before


def test_live_storage_mode_change_refuses_operation(root: Path) -> None:
    with open_registry(root) as registry:
        row = create(registry)
        (root / "registry.sqlite").chmod(0o644)
        with pytest.raises(api().RegistryInvalid):
            registry.get(OWNER, row.launch_id)


def test_raw_constructor_refusal_detaches_ambient_caller_data() -> None:
    try:
        raise ValueError("SYNTHETIC-PRIVATE-CALLER")
    except ValueError:
        with pytest.raises(api().RegistryInvalid) as caught:
            api().ProtectedLaunchRegistry()
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None


def test_old_boot_stop_and_uid_recovery_preserve_retained_ownership(root: Path) -> None:
    with open_registry(root) as registry:
        row = plan(registry, create(registry))
    with open_registry(root, NEXT_BOOT) as registry:
        row = registry.get(OWNER, row.launch_id)
        assert row.objects.pod_uid is None
        row = registry.record_object_uid(OWNER, row.launch_id, row.revision, "pod", POD_UID, "1")
        assert row.state == "retiring"
        with pytest.raises(api().RegistryRetired):
            create(registry)


def test_cross_owner_cannot_record_outcome_termination_or_cleanup(root: Path) -> None:
    with open_registry(root) as registry:
        row = objects(registry, create(registry))
        row = registry.transition(OWNER, row.launch_id, row.revision, "idle")
        reservation = registry.begin_operation(
            OWNER, row.launch_id, row.revision, OPERATION, "start", DIGEST
        )
        row = registry.get(OWNER, row.launch_id)
        with pytest.raises(api().RegistryUnauthorized):
            registry.finish_operation(
                OTHER_OWNER, row.launch_id, reservation.revision, OPERATION, "uncertain"
            )
        row = registry.transition(OWNER, row.launch_id, row.revision, "retiring")
        with pytest.raises(api().RegistryUnauthorized):
            registry.record_termination_observation(
                OTHER_OWNER,
                row.launch_id,
                row.revision,
                api().TerminationObservation(
                    pod_uid=POD_UID,
                    container_id=CONTAINER,
                    resource_version="2",
                    exit_code=0,
                    finished_at="2026-01-01T00:00:00Z",
                ),
            )
        with pytest.raises(api().RegistryUnauthorized):
            registry.record_cleanup_receipt(
                OTHER_OWNER,
                row.launch_id,
                row.revision,
                api().CleanupReceipt(
                    pod_uid=POD_UID, service_uid=SERVICE_UID, secret_uid=SECRET_UID
                ),
            )


def test_restart_changes_revision_even_for_already_retiring_pending_operation(root: Path) -> None:
    with open_registry(root) as registry:
        row = idle(registry)
        reserved = registry.begin_operation(
            OWNER, row.launch_id, row.revision, OPERATION, "retire", DIGEST
        )
    with open_registry(root, NEXT_BOOT) as registry:
        current = registry.get(OWNER, row.launch_id)
        assert int(current.revision) > int(reserved.revision)
        with pytest.raises(api().RegistryConflict):
            registry.begin_operation(
                OWNER, row.launch_id, reserved.revision, REQUEST, "retire", DIGEST
            )
        assert registry.get_operation(OWNER, row.launch_id, OPERATION).status == "uncertain"


def test_start_replay_cannot_add_an_epoch_to_original_closed_request(root: Path) -> None:
    with open_registry(root) as registry:
        row = idle(registry)
        reserved = registry.begin_operation(
            OWNER, row.launch_id, row.revision, OPERATION, "start", DIGEST
        )
        with pytest.raises(api().RegistryConflict):
            registry.begin_operation(
                OWNER, row.launch_id, reserved.revision, OPERATION, "start", DIGEST, epoch=EPOCH
            )


def test_database_error_rolls_back_whole_create_without_partial_ownership(root: Path) -> None:
    with open_registry(root) as registry:
        db = sqlite3.connect(root / "registry.sqlite")
        try:
            db.execute(
                "CREATE TRIGGER fail_create BEFORE INSERT ON objects "
                "BEGIN SELECT RAISE(ABORT, 'SYNTHETIC-PRIVATE-DB-DETAIL'); END"
            )
            db.commit()
            with pytest.raises(api().RegistryError) as caught:
                create(registry)
            assert caught.value.__context__ is None
            assert caught.value.__cause__ is None
            assert "SYNTHETIC-PRIVATE-DB-DETAIL" not in str(caught.value)
            assert db.execute("SELECT count(*) FROM launches").fetchone()[0] == 0
            db.execute("DROP TRIGGER fail_create")
            db.commit()
            assert create(registry).state == "preparing"
        finally:
            db.close()
