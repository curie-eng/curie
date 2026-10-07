"""The closed remediation vocabularies the API stores and returns.

@spec AUTOMATED-REMEDIATION-7 @spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-20
@spec AUTOMATED-REMEDIATION-26. Frozen in ``tests/vectors/remediation-codes.json``,
which the worker's receipt renderer and the CLI read in other images; each set
here equals the vector's list of the same (lower-cased) name, so a code added or
renamed on one side fails that side's reader.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

from .remediation_policy_document import KINDS as _POLICY_KINDS

# A nomination row's ``state`` (AUTOMATED-REMEDIATION-8).
NOMINATION_STATES: Final = frozenset(
    {
        "received",
        "refused",
        "precondition_pending",
        "admitted",
        "approval_requested",
        "approved",
        "rejected",
        "expired",
        "executing",
        "verifying",
        "finished",
    }
)
# End a nomination with nothing for a person to approve (AUTOMATED-REMEDIATION-7,
# and a stopped agent under AUTOMATED-REMEDIATION-8 check 2).
NOMINATION_REFUSALS: Final = frozenset(
    {
        "nomination_malformed",
        "unknown_action",
        "nomination_duplicate",
        "arguments_schema_mismatch",
        "agent_stopped",
    }
)
# The nomination route's refusals; each writes no row (AUTOMATED-REMEDIATION-1, -6).
SUBMISSION_REFUSALS: Final = frozenset(
    {"remediation_disabled", "not_protected_event", "nomination_conflict"}
)
# Each AUTOMATED-REMEDIATION-8 check and the codes it reports.
ADMISSION_CHECKS: Final[Mapping[int, frozenset[str]]] = MappingProxyType(
    {
        1: frozenset({"remediation_disabled"}),
        2: frozenset({"agent_stopped"}),
        3: frozenset({"generation_not_current"}),
        4: frozenset({"policy_disarmed"}),
        5: frozenset({"not_automatic"}),
        6: frozenset({"qualification_missing", "qualification_stale"}),
        7: frozenset({"verifier_not_independent"}),
        8: frozenset({"out_of_bounds"}),
        9: frozenset({"not_reversible_now"}),
        10: frozenset({"breaker_open"}),
        11: frozenset(
            {
                "policy_rate_limit",
                "action_rate_limit",
                "incident_limit",
                "turn_limit",
                "target_live",
            }
        ),
        12: frozenset({"precondition_not_met", "precondition_unavailable"}),
    }
)
# Why a well-formed nomination became an approval request: every check from 3
# on, the fail-closed ``admission_unreadable`` and the claim-time ``policy_changed``.
APPROVAL_REASONS: Final = frozenset(
    {code for check, codes in ADMISSION_CHECKS.items() if check >= 3 for code in codes}
    | {"admission_unreadable", "policy_changed"}
)
APPROVAL_RESOLUTION_REFUSALS: Final = frozenset(
    {"arguments_mismatch", "policy_changed", "tune_execution_not_automated"}
)
VERIFICATION_OUTCOMES: Final = frozenset(
    {"verified", "not-recovered", "verifier-unavailable", "superseded"}
)
RECEIPT_STAGES: Final = frozenset(
    {
        "nominated",
        "refused",
        "approval_requested",
        "executed",
        "verified",
        "not-recovered",
        "verifier-unavailable",
        "superseded",
        "undo_requested",
        "undone",
        "escalated",
    }
)
KINDS: Final = _POLICY_KINDS
AUTHORITIES: Final = frozenset({"policy", "approval", "none"})
AUTHORITY_KINDS: Final = frozenset(
    {"undo_ruling", "capability_probe", "policy", "approval", "qualification"}
)
ACTOR_KINDS: Final = frozenset({"model_turn", "policy", "approval", "undo_ruling"})
POLICY_REFUSALS: Final = frozenset(
    {
        "agent_not_found",
        "remediation_policy_absent",
        "hook_not_protected",
        "operator_principal_required",
        "stale_policy_generation",
        "policy_operation_conflict",
        "route_unknown",
        "route_approvers_not_explicit",
        "policy_document_invalid",
        "policy_unknown_key",
        "policy_limit_out_of_bounds",
        "precondition_and_verifier_required",
        "delta_bound_unsupported",
        "kind_not_automatic",
        "qualification_required",
        "verifier_not_independent",
    }
)
