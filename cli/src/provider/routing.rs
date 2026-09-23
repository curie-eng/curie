//! Provider routing for `curie apply` (ADR 0163 decisions 2, 3 and 5).
//!
//! With a declared provider, every provider-backed credential reaches its
//! consumers through an ESO-synced Secret and the chart's `existingSecret`
//! knobs. Helm values carry Secret NAMES only. This module plans that routing
//! from the inventory, checks Secrets Manager before anything is mutated,
//! generates the stateful internals of a fresh install once, and applies the
//! ExternalSecrets. Nothing here runs when `curie.yaml` declares no provider.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Result;
use serde_json::Value;

use super::eso::Kubectl;
use super::SecretsProvider;
use crate::installation::Installation;

/// Stored Helm revisions kept for a provider-backed release.
pub const HISTORY_MAX: u32 = 3;

/// The chart default for `langfuse.init.projectPublicKey`. The OTLP auth
/// header is derived from it and the generated project secret key.
pub const DEFAULT_LANGFUSE_PUBLIC_KEY: &str = "pk-lf-curie-dev";

/// ExternalSecret refresh interval for platform credentials.
pub const REFRESH_INTERVAL: &str = "1h";

/// The SecretStore the ExternalSecrets read through: `<release>-curie-sm`.
pub fn store_name(release: &str) -> String {
    let _ = release;
    todo!()
}

/// Why an inventory entry is routed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteReason {
    /// A stateful internal the CLI generates once on a fresh install.
    Generated,
    /// A third-party credential `curie.yaml` names.
    Declared,
    /// Routed because Secrets Manager already holds it, or the live release
    /// records a value for it that switching must not drop.
    Optional,
}

/// One routed inventory entry, resolved for this release.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedEntry {
    pub logical_name: String,
    /// `<prefix>/<release>/<logical_name>`, the Secrets Manager id and the
    /// ExternalSecret `remoteRef.key`.
    pub remote_id: String,
    /// The Secret ESO writes, with `{release}` resolved.
    pub target: String,
    pub keys: Vec<String>,
    pub reason: RouteReason,
}

/// What the planner reads. `sm` maps a logical name to the key names its
/// Secrets Manager object holds; `None` means Secrets Manager was not read
/// (an offline dry run).
pub struct RoutingInputs<'a> {
    pub cfg: &'a Installation,
    pub live: Option<&'a Value>,
    pub sm: Option<&'a BTreeMap<String, Vec<String>>>,
}

/// The routing decision for one apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingPlan {
    pub release: String,
    pub namespace: String,
    /// `<prefix>/<release>`.
    pub remote_prefix: String,
    /// True when no live release exists.
    pub fresh: bool,
    pub entries: Vec<RoutedEntry>,
    /// `--set-string` pairs, in order: knob names and key names, plus the
    /// non-secret companions (`fakeModel=false`, cleared Slack token values).
    pub knob_sets: Vec<(String, String)>,
    /// `langfuse.init.projectPublicKey` as declared, or the chart default.
    pub langfuse_public_key: String,
}

/// Chart value keys that must never carry a value with a provider declared.
pub fn dropped_value_keys() -> &'static [&'static str] {
    todo!()
}

/// Plan the routing. Refuses (naming the key, never a value) a `set:` entry
/// that names a routed knob or a dropped value key, and a mail adapter that
/// is deployed or carries credentials, which this change does not route.
pub fn plan_routing(inputs: &RoutingInputs<'_>) -> Result<RoutingPlan> {
    let _ = inputs;
    todo!()
}

/// Rewrite a completed `up` so it passes Secret names instead of values:
/// drop every [`dropped_value_keys`] entry (from `secrets`, `set` and
/// `set_string`), clear the model credential and GitHub token plans, drop
/// retained non-secret overlays of routed knobs, append `knob_sets`, and bound
/// history to [`HISTORY_MAX`].
pub fn route_up_opts(up: &mut crate::ops::UpOpts, plan: &RoutingPlan) {
    let _ = (up, plan);
    todo!()
}

/// Read-only Secrets Manager snapshot for this release: logical name to key
/// names. Never returns material.
pub fn read_sm_inventory(provider: &dyn SecretsProvider) -> Result<BTreeMap<String, Vec<String>>> {
    let _ = provider;
    todo!()
}

/// Check Secrets Manager before any mutation. Refuses when a routed entry
/// cannot be satisfied: a missing declared or optional entry, any missing
/// entry on an existing release, or an existing object missing a routed key.
/// The refusal names `<prefix>/<release>/<logical>` and key names only.
/// Returns the generated entries a fresh install must create.
pub fn preflight(plan: &RoutingPlan, sm: &BTreeMap<String, Vec<String>>) -> Result<Vec<String>> {
    let _ = (plan, sm);
    todo!()
}

/// Create each named generated entry once, as a JSON object of its keys. An
/// object that already exists is never overwritten. Returns the logical names
/// created.
pub fn generate(
    provider: &dyn SecretsProvider,
    plan: &RoutingPlan,
    to_create: &[String],
) -> Result<Vec<String>> {
    let _ = (provider, plan, to_create);
    todo!()
}

/// One ExternalSecret per target Secret, `creationPolicy: Owner`, one
/// `remoteRef` per routed key.
pub fn render_external_secrets(plan: &RoutingPlan, store: &str) -> Vec<Value> {
    let _ = (plan, store);
    todo!()
}

/// Refuse, before any mutation, when the SecretStore is absent.
pub fn check_store(k: &dyn Kubectl, namespace: &str, store: &str) -> Result<()> {
    let _ = (k, namespace, store);
    todo!()
}

/// Refuse a target Secret that exists but is not owned by the ExternalSecret
/// of the same name, then apply every ExternalSecret and wait for each sync.
pub fn sync(
    k: &dyn Kubectl,
    plan: &RoutingPlan,
    store: &str,
    timeout: Duration,
    poll: Duration,
) -> Result<()> {
    let _ = (k, plan, store, timeout, poll);
    todo!()
}

/// The lines an offline `apply --dry-run` prints for the routing.
pub fn dry_run_lines(plan: &RoutingPlan, store: &str) -> Vec<String> {
    let _ = (plan, store);
    todo!()
}
