//! The closed remediation codes the CLI renders (AUTOMATED-REMEDIATION-26).
//!
//! The API owns these vocabularies (`apps/api/src/curie_api/remediation_codes.py`);
//! the CLI renders them in other images, so they are pinned against the frozen
//! vector `tests/vectors/remediation-codes.json` by `cli/tests/remediation_vectors.rs`.
//! Only the policy refusals are read here: the `remediation-policy` verbs render
//! them. The nomination vocabularies land with the receipt verbs (task 13).

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
