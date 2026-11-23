"""Instructions for the repair reason recorded by maintenance admission."""

from .factory_conflict import conflict_instructions
from .factory_maintenance import (
    MaintenanceAuthority,
    MaintenanceInstructions,
    MaintenanceReason,
    MaintenanceReasonHook,
)


def _late_ci_red(
    authority: MaintenanceAuthority, /, *, objective: str
) -> MaintenanceInstructions:
    return MaintenanceInstructions(objective=objective, publication_mode=None)


_REASON_HOOKS: dict[MaintenanceReason, MaintenanceReasonHook] = {
    "late_ci_red": _late_ci_red,
    "base_conflict": conflict_instructions,
}


def maintenance_instructions(
    authority: MaintenanceAuthority, /, *, objective: str
) -> MaintenanceInstructions | None:
    hook = _REASON_HOOKS.get(authority.reason)
    return hook(authority, objective=objective) if hook is not None else None
