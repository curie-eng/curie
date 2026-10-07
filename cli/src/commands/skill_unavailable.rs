//! Refusals for verbs the skill tier does not serve, with the tier that does.

/// Why `skill versions` cannot be answered at this tier.
pub const VERSIONS_REASON: &str =
    "`skill up` runs a local snapshot of the bundle on disk (its digest is on `skill status`), and nothing is deployed, so no version is assigned";
/// Where to run `versions` instead.
pub const VERSIONS_ALT: &str =
    "use `curie local versions <agent>` or `curie cluster versions <agent>` for a deployed agent";
/// Why `skill memory` cannot be answered at this tier.
pub const MEMORY_REASON: &str =
    "this tier configures no memory namespace: `skill up` never sets a memory ref, and there is no platform here to own or address one";
/// Where to run `memory` instead.
pub const MEMORY_ALT: &str =
    "use `curie local memory <agent>` or `curie cluster memory <agent>` for a deployed agent";
/// Why observability query verbs cannot be answered at this tier.
pub const OBSERVABILITY_REASON: &str =
    "the skill tier runs only a bundle runner and has no platform API or observability read service; `--otel-endpoint` can export telemetry but does not create a query API";
/// Where to query observability, or how to export telemetry from a skill runner.
pub const OBSERVABILITY_ALT: &str =
    "use `curie local observability runs|run|metrics` or `curie cluster observability runs|run|metrics`; to export this skill runner's telemetry, restart it with `curie skill up --otel-endpoint <OTLP_URL>` and query through a platform API";

/// `skill versions`: answered, but unavailable at this tier by construction.
///
/// A version exists only because the platform assigns a `bundle_sha256` and a
/// `version_label` at deploy; `skill up` runs whatever bytes are on disk, so
/// there is no release to inspect here (issue #459, ADR-0041).
pub fn skill_versions_unavailable() -> anyhow::Error {
    crate::exit::unsupported("versions", VERSIONS_REASON, VERSIONS_ALT)
}

/// `skill memory`: answered, but not a capability of this tier.
///
/// Memory is a namespace some *platform* provisions, addresses, and owns; the
/// `local`/`cluster` tiers have one, and this tier has none. `skill up` never
/// sets `CURIE_MEMORY_REF`, so the runner it boots resolves a
/// `NullMemoryStore` and nothing is persisted.
///
/// Deliberately NOT phrased as "cannot exist by construction". `--secret` has no
/// reserved-name fence (`merge_secret_env`), so an operator CAN hand-forward
/// `--secret CURIE_MEMORY_REF --secret CURIE_MEMORY_TOKEN` and the runner's
/// `resolve_memory` will dereference an `http(s)://` ref into a real
/// `StateApiMemoryStore`. That escape hatch is an operator wiring a foreign
/// tier's namespace through this one by hand -- not this tier growing the
/// capability -- and this command could not report on it regardless: it has no
/// way to read a running container's env. So the verb stays unavailable (exit 4)
/// and the reason claims only what is true: the tier configures no namespace
/// (issue #459, ADR-0041).
pub fn skill_memory_unavailable() -> anyhow::Error {
    crate::exit::unsupported("memory", MEMORY_REASON, MEMORY_ALT)
}

/// Why `skill work-items` cannot be answered at this tier.
pub const WORK_ITEMS_REASON: &str =
    "the skill tier runs one bundle against a local runner and admits no factory work items; there is no platform API here to own them";
/// Where to read factory work items instead.
pub const WORK_ITEMS_ALT: &str =
    "use `curie local work-items` or `curie cluster work-items` against a platform API";

/// `skill work-items`: understood, but unavailable at this tier (#2577,
/// ADR-0041). Work items are admitted and owned by the platform API.
pub fn skill_work_items_unavailable() -> anyhow::Error {
    crate::exit::unsupported("work-items", WORK_ITEMS_REASON, WORK_ITEMS_ALT)
}

/// Why `skill schedules` cannot be answered at this tier.
pub const SCHEDULES_REASON: &str =
    "the skill tier runs one bundle against a local runner and has no platform API or hook run record";
/// Where to read scheduled hooks instead.
pub const SCHEDULES_ALT: &str =
    "use `curie local schedules` or `curie cluster schedules` against a platform API";

/// `skill schedules`: understood, but unavailable at this tier (ADR-0041, #2933).
pub fn skill_schedules_unavailable() -> anyhow::Error {
    crate::exit::unsupported("schedules", SCHEDULES_REASON, SCHEDULES_ALT)
}

/// Why `skill hook schedule` and `skill hook record` cannot be answered here.
pub const HOOK_RECORD_REASON: &str =
    "the skill tier runs one bundle against a local runner and has no platform API or hook run record";
/// Where a durable hook record lives instead.
pub const HOOK_RECORD_ALT: &str =
    "use `curie local hook record <agent> <name> <id>` or `curie cluster hook record <agent> <name> <id>` to read the run record";

/// `skill hook schedule` and `skill hook record` (ADR-0099, #2932).
pub fn skill_hook_record_unavailable(verb: &str) -> anyhow::Error {
    let alternative = if verb == "record" {
        HOOK_RECORD_ALT
    } else {
        "use `curie local hook fire` or `curie cluster hook fire`, which print the run record"
    };
    crate::exit::unsupported(verb, HOOK_RECORD_REASON, alternative)
}

/// `skill observability runs|run|metrics`: understood, but unavailable here.
///
/// A skill runner can emit OTLP when explicitly wired, but it does not host the
/// API read models these query verbs intentionally use. Decline with ADR-0041's
/// capability exit (4) rather than bypassing the API to query a backend.
pub fn skill_observability_unavailable() -> anyhow::Error {
    crate::exit::unsupported(
        "observability runs|run|metrics",
        OBSERVABILITY_REASON,
        OBSERVABILITY_ALT,
    )
}

/// Why `skill approvals --list`/`--resolve` cannot be answered at this tier.
pub const APPROVALS_LIST_REASON: &str =
    "`skill message` talks straight to the local runner, bypassing the worker, Valkey, and the durable-Approval + resume machinery (ADR-0063), so the skill tier keeps no durable approval record to list or resolve";
/// Where to list/resolve durable approvals instead.
pub const APPROVALS_LIST_ALT: &str =
    "use `curie local approvals <agent> --list`/`--resolve` or `curie cluster approvals <agent> --list`/`--resolve` for a deployed agent, or resolve the gate within the same `skill message` session";

/// `skill approvals --list`/`--resolve`: answered, but unavailable at this tier
/// by construction (ADR-0077). The bundle's gate config (view/set/clear) still
/// works; only the durable pending-record list/resolve the local+cluster tiers
/// gained (#506/#736) has no meaning where there is no durable Approval store or
/// resume path (#766). The flags are accepted so this reports WHY (exit 4) rather
/// than erroring like an unknown-flag typo, matching `cluster deploy --secret`'s
/// decline-with-reason (issue #771, ADR-0041, ADR-0077).
pub fn skill_approvals_list_unavailable() -> anyhow::Error {
    crate::exit::unsupported(
        "approvals --list/--resolve",
        APPROVALS_LIST_REASON,
        APPROVALS_LIST_ALT,
    )
}

/// Why the administrative recovery verbs cannot be answered at the skill tier
/// (#2753).
///
/// Same root as `APPROVALS_LIST_REASON` and stated separately for the same
/// reason the route decline is: the operator needs to read that the missing
/// thing is the durable store, not that they typed a flag this build does not
/// know. There is nothing here to report on or recover, because
/// `skill message` resolves its gate inside the one live session.
pub const APPROVALS_RECOVERY_REASON: &str =
    "approval recovery reports on and administratively disposes of rows in the durable Approval store, and `skill message` talks straight to the local runner with no worker, no Valkey and no durable-Approval or resume machinery (ADR-0063), so this tier has no durable row to report on or recover";
/// Where to run an administrative recovery instead.
pub const APPROVALS_RECOVERY_ALT: &str =
    "use `curie local approvals <agent> --report-identity`/`--recover`, or the same verbs under `curie cluster approvals <agent>` for a deployed agent; a skill-tier gate is resolved within the same `skill message` session";

/// The recovery verbs are answered but unavailable at this tier by construction
/// (ADR-0041). Accepted so the tier reports WHY (exit 4) rather than erroring
/// like an unknown-flag typo, matching `--list`/`--resolve` above.
pub fn skill_approvals_recovery_unavailable() -> anyhow::Error {
    crate::exit::unsupported(
        "approvals --report-identity/--recover",
        APPROVALS_RECOVERY_REASON,
        APPROVALS_RECOVERY_ALT,
    )
}

/// Why the route-binding flags cannot be answered at the skill tier (#1052).
///
/// A different reason from `APPROVALS_LIST_REASON`, and the difference matters:
/// a pending record is missing because this tier runs no durable store, while a
/// route binding is missing because it is per-AGENT platform config and this
/// tier has no agent. Collapsing them would tell an operator to look in the
/// wrong place.
pub const APPROVALS_ROUTES_REASON: &str =
    "an approval route binding is per-agent platform config (agents.approval_routes), and the skill tier runs a bare runner with no platform, no agent record, and therefore nothing to bind a route on";
/// Where to bind approval routes instead.
pub const APPROVALS_ROUTES_ALT: &str =
    "use `curie local approvals <agent> --route-resolution <name>=<channel>` or `curie cluster approvals <agent> --route-resolution <name>=<channel>` for a deployed agent; use `--routes-from <file>` to declare a complete route with a text-only notification; the bundle-side half (which routes exist) is the manifest's approvalPolicy, which `curie skill approvals` does show";

/// The route-binding inputs are answered but unavailable at this tier by
/// construction (ADR-0041). Accepted so the tier reports WHY (exit 4) rather
/// than erroring like an unknown-flag typo, matching `--list`/`--resolve` above.
pub fn skill_approval_routes_unavailable() -> anyhow::Error {
    crate::exit::unsupported(
        "approvals --route-resolution/--route-approvers/--routes-from/--list-routes/--clear-routes",
        APPROVALS_ROUTES_REASON,
        APPROVALS_ROUTES_ALT,
    )
}
