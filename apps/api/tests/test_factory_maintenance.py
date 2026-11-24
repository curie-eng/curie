from uuid import UUID

import pytest
from curie_api.config import Settings
from curie_api.factory_maintenance import MaintenanceAuthority, MaintenanceReason
from curie_api.factory_maintenance_reasons import maintenance_instructions


def authority(reason: MaintenanceReason) -> MaintenanceAuthority:
    return MaintenanceAuthority(
        repair_request_id=UUID(int=1),
        reason=reason,
        head_sha="a" * 40,
        base_ref="main",
        base_sha="b" * 40,
    )


def test_late_ci_uses_the_classified_failure_and_ordinary_publication() -> None:
    instructions = maintenance_instructions(authority("late_ci_red"), objective="Repair unit CI")
    assert instructions is not None
    assert instructions.objective == "Repair unit CI"
    assert instructions.publication_mode is None


def test_repair_setting_defaults_off_and_accepts_an_explicit_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FACTORY_MAINTENANCE_REPAIR_ENABLED", raising=False)
    assert Settings().factory_maintenance_repair_enabled is False
    monkeypatch.setenv("FACTORY_MAINTENANCE_REPAIR_ENABLED", "true")
    assert Settings().factory_maintenance_repair_enabled is True
