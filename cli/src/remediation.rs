//! The closed remediation codes the CLI renders (AUTOMATED-REMEDIATION-26).
//!
//! The API owns these vocabularies (`apps/api/src/curie_api/remediation_codes.py`);
//! the CLI renders them in other images, so they are pinned against the frozen
//! vector `tests/vectors/remediation-codes.json` by `cli/tests/remediation_vectors.rs`.
//! The `remediation-policy` verbs render the policy refusals; `remediation list`
//! and `show` render the nomination vocabularies (AUTOMATED-REMEDIATION-20).

// @spec AUTOMATED-REMEDIATION-3 @spec AUTOMATED-REMEDIATION-26
/// Every `detail.code` the remediation policy routes answer with
/// (`policy_refusals` in `tests/vectors/remediation-codes.json`).
pub const POLICY_REFUSALS: &[&str] = &[
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
];

// @spec AUTOMATED-REMEDIATION-20 @spec AUTOMATED-REMEDIATION-26
/// A nomination row's `state`.
pub const NOMINATION_STATES: &[&str] = &[
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
];

/// Why a nomination ended refused.
pub const NOMINATION_REFUSALS: &[&str] = &[
    "nomination_malformed",
    "unknown_action",
    "nomination_duplicate",
    "arguments_schema_mismatch",
    "agent_stopped",
    "reply_surface_unavailable",
    "tune_execution_not_automated",
];

/// What the nomination route itself refuses (no row is written).
pub const SUBMISSION_REFUSALS: &[&str] = &[
    "remediation_disabled",
    "not_protected_event",
    "nomination_conflict",
];

/// The admission check that sent a nomination to approval.
pub const APPROVAL_REASONS: &[&str] = &[
    "generation_not_current",
    "policy_disarmed",
    "not_automatic",
    "qualification_missing",
    "qualification_stale",
    "verifier_not_independent",
    "out_of_bounds",
    "not_reversible_now",
    "breaker_open",
    "policy_rate_limit",
    "action_rate_limit",
    "incident_limit",
    "turn_limit",
    "target_live",
    "precondition_not_met",
    "precondition_unavailable",
    "admission_unreadable",
    "policy_changed",
];

/// Why resolving an approval was refused.
pub const APPROVAL_RESOLUTION_REFUSALS: &[&str] = &[
    "arguments_mismatch",
    "policy_changed",
    "tune_execution_not_automated",
];

pub const VERIFICATION_OUTCOMES: &[&str] = &[
    "verified",
    "not-recovered",
    "verifier-unavailable",
    "superseded",
];

/// The stage a receipt names.
pub const RECEIPT_STAGES: &[&str] = &[
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
];

pub const KINDS: &[&str] = &["remediate", "prevent", "tune"];
pub const AUTHORITIES: &[&str] = &["policy", "approval", "none"];
pub const AUTHORITY_KINDS: &[&str] = &[
    "undo_ruling",
    "capability_probe",
    "policy",
    "approval",
    "qualification",
];
pub const ACTOR_KINDS: &[&str] = &["model_turn", "policy", "approval", "undo_ruling"];

// @spec AUTOMATED-REMEDIATION-11
/// The `--state` of `remediation-policy breakers` (the API's own closed set).
pub const BREAKER_STATES: &[&str] = &["open", "closed", "all"];
