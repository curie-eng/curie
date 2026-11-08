"""Launch registries refuse primitive-probe storage in either open order.

@spec PROTECTED-COMMISSIONING-8. Actual SQLite and file ownership checks;
these tests grant no launch authority and measure no cluster.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from curie_worker.commissioning_probe_registry import ProbeRegistry
from curie_worker.commissioning_registry import CommissioningLaunchRegistry
from curie_worker.protected_launch_registry import ProtectedLaunchRegistry, RegistryError

BOOT = "b0000000-0000-4000-8000-000000000001"
PROBE = "10000000-0000-4000-8000-000000000001"
OPERATION = "20000000-0000-4000-8000-000000000001"


def launch_registry(root: Path, kind: str):
    if kind == "production":
        return ProtectedLaunchRegistry.open(str(root), boot_id=BOOT)
    return CommissioningLaunchRegistry.open(str(root), boot_id=BOOT, permit_digest="a" * 64)


@pytest.mark.parametrize("kind", ["production", "campaign"])
@pytest.mark.parametrize("closed", [False, True])
def test_launch_open_refuses_a_root_already_owned_by_probes(tmp_path, kind, closed):
    root = tmp_path / "owned"
    root.mkdir(mode=0o700)
    probe = ProbeRegistry.open(str(root), boot_id=BOOT, scope_digest="b" * 64)
    try:
        probe.create(PROBE, namespace="acme-runtime", create_operation_id=OPERATION)
        if closed:
            probe.close()
        with pytest.raises(RegistryError):
            with launch_registry(root, kind):
                pass
        # Refusal must not mutate or hide the probe's durable ownership.
        if closed:
            probe = ProbeRegistry.open(str(root), boot_id=BOOT, scope_digest="b" * 64)
        assert probe.get(PROBE).objects.pod_uid is None
        launch_file = "registry.sqlite" if kind == "production" else "commissioning.sqlite"
        assert not (root / launch_file).exists()
    finally:
        probe.close()


@pytest.mark.parametrize("kind", ["production", "campaign"])
@pytest.mark.parametrize("entry", ["probe.sqlite", "probe.lock", "probe.sqlite-wal"])
def test_a_launch_operation_refuses_when_probe_storage_appears_after_open(tmp_path, kind, entry):
    root = tmp_path / "owned"
    root.mkdir(mode=0o700)
    with launch_registry(root, kind) as registry:
        (root / entry).touch(mode=0o600)
        with pytest.raises(RegistryError):
            if kind == "production":
                registry.find_request("worker/example", PROBE)
            else:
                registry.find("worker/example", PROBE)


@pytest.mark.parametrize("kind", ["production", "campaign"])
def test_a_probe_storage_symlink_also_refuses_launch_open(tmp_path, kind):
    root = tmp_path / "owned"
    root.mkdir(mode=0o700)
    (root / "probe.sqlite").symlink_to(root / "absent")
    with pytest.raises(RegistryError):
        with launch_registry(root, kind):
            pass
