//! `curie cluster up`: option completion, the helm value plan and its
//! preservation rules, gvisor / priority-class / controller preflight, and the
//! install runner with its gvisor event observer.

use anyhow::{bail, Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::{Child, ChildStdout, Command};

#[allow(unused_imports)]
use super::{command::*, convergence, providers::*, verbs::*};

/// Typed retained values use the same protected file lifecycle as credentials.
/// Debug and command rendering expose field names only.
#[derive(Clone, PartialEq, Eq)]
pub struct PrivateHelmValues(pub(super) serde_json::Value, BTreeMap<String, String>);

impl PrivateHelmValues {
    pub(super) fn keys(&self) -> Vec<String> {
        [
            "mailAdapter",
            "worker.adapterCredentials",
            "worker.adapterCredentialsExistingSecret",
            "worker.adapterCredentialsExistingSecretKey",
        ]
        .into_iter()
        .filter(|key| {
            self.0
                .pointer(&format!("/{}", key.replace('.', "/")))
                .is_some()
        })
        .map(str::to_string)
        .collect()
    }
}

impl std::fmt::Debug for PrivateHelmValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrivateHelmValues")
            .field("keys", &self.keys())
            .finish()
    }
}

pub struct UpOpts {
    /// Existing mail lifecycle and paired worker credentials, with explicit
    /// operator overrides removed. Populated by the one release-values read.
    pub retained_mail_values: Option<PrivateHelmValues>,

    pub common: CommonOpts,
    pub chart: String,
    pub no_expose: bool,
    pub set: Vec<String>,
    pub set_string: Vec<String>,
    /// Named model providers (validated against [`parse_egress_provider`]) whose
    /// API host(s) runner egress is opened to. Resolved to narrow host-route
    /// CIDRs at install time into [`resolved_egress_cidrs`]; empty means no
    /// provider egress. This is the explicit replacement for the old
    /// unconditional Anthropic carve-out (#362).
    pub allow_egress_host: Vec<String>,
    /// The single-host CIDRs the named providers resolved to, populated by [`up`]
    /// from [`resolve_provider_egress_cidrs`] (offline under `--dry-run`, so
    /// empty there). Empty in the pure argv tests. Emitted as the first
    /// `allowedEgress` entries, before any `allow_web_egress` destination.
    pub resolved_egress_cidrs: Vec<String>,
    /// Operator declared CIDRs to open runner egress to for skill or tool web
    /// access, additive to the resolved provider egress. Empty means fail closed
    /// by default.
    pub allow_web_egress: Vec<String>,
    /// Whether `--fake-model` was passed (forces the sealed install and
    /// suppresses the fake-model warning even when the env credential is set).
    pub fake_model: bool,
    /// The model credential to install with, resolved from `CURIE_CREDENTIALS`
    /// (or the deprecated `CURIE_MODEL_CREDENTIALS`). `Some(non-empty)` enables the real model;
    /// `None` installs sealed (fake model). A credential alone opens NO egress --
    /// the model stays unreachable behind the fail-closed sandbox until a
    /// provider (`allow_egress_host`) or a raw range (`allow_web_egress`) is
    /// named (#362).
    pub credentials: Option<String>,
    pub local_model: Option<String>,
    /// The shell `CURIE_MODEL` resolved by the caller (`None` when unset or
    /// empty), used to default `agentSandbox.runner.model` for cross-tier parity
    /// with `local up` (#361).
    pub model: Option<String>,
    /// Required chart secrets (dotted helm key -> value) the CLI supplies so a
    /// no-override install never ships the published dev defaults (see #196).
    /// Populated by [`up`] from [`resolve_generated_secrets`] on a sealed
    /// install, and from [`resolve_preserved_values`] on both sealed and
    /// `--dev` upgrades so a sibling verb's recorded values (`cluster comms`,
    /// `cluster github-app`, a previously generated sealing key) survive a
    /// full Helm upgrade (#1134). Empty in the pure argv tests and on a fresh
    /// `--dev` install, which keeps the chart's published defaults. Delivered
    /// through a private 0600 `-f` values file, never the argv.
    pub secrets: Vec<(String, String)>,
    /// What this run does with `api.githubToken`, resolved by [`up`] from
    /// `--github-token` / `CURIE_GITHUB_TOKEN` / `--clear-github-token` and the
    /// value the release already recorded. `Untouched` in the pure argv tests.
    /// A `Set` value is a live GitHub credential and is delivered through the
    /// private 0600 `-f` values file, never the argv.
    pub github_token: GithubTokenPlan,
    /// `--dev`: keep the chart's deterministic dev-default secrets instead of
    /// generating strong per-release randoms (the first-class dev escape hatch
    /// that replaces hand-passing `--set` for every secret).
    pub dev: bool,
}

impl UpOpts {
    pub(super) fn operator_sets(&self) -> Vec<String> {
        self.set.iter().chain(&self.set_string).cloned().collect()
    }
}

/// The chart secrets a bare `helm install` would otherwise render from the
/// published dev defaults in `values.yaml` (see #57): every backing-store
/// password plus the Langfuse crypto material and the first-party app keys.
/// Each entry is `(dotted helm value key, random byte length)`. `cluster up`
/// supplies a strong random for each on a fresh install so the release never
/// boots on a credential that lives in this public repo. Slack tokens and the
/// model credential are deliberately absent -- they are operator-supplied via
/// their own paths (`cluster comms`, `CURIE_MODEL_CREDENTIALS`), not
/// generated. `langfuse.encryptionKey` must be exactly 64 hex chars, so its 32
/// bytes are load-bearing.
/// Values owned by `cluster comms`, not by `cluster up`.
///
/// `comms` writes these with `helm upgrade --reuse-values`; `up` does a full
/// upgrade and therefore drops anything it does not itself pass. That silently
/// deleted the dispatcher and its Slack tokens whenever `up` ran after `comms`
/// -- the bot simply stopped answering, with no error and nothing in the diff
/// to suggest `up` had touched Slack at all (#1067).
///
/// Preserved the same way generated secrets are: read back from the release and
/// re-supplied through the values file, never argv, so a bot token cannot leak
/// into `ps` or shell history.
const COMMS_MANAGED_KEYS: &[&str] = &["dispatcher.slack.appToken", "dispatcher.slack.botToken"];

/// The chart values `cluster github-app` records (ADR-0092), preserved across a
/// plain `cluster up` for exactly the reason [`COMMS_MANAGED_KEYS`] is.
///
/// A plain `up` does a FULL upgrade and drops anything it does not re-pass, so
/// an operator who wired the App and later ran `up` to change something
/// unrelated silently lost it: `githubAppPrivateKey` back to `""`,
/// `GITHUB_APP_ID` back to empty. The platform then falls through to the PAT
/// path or to no credential at all, and every private-repo deploy 404s -- with
/// nothing in the diff mentioning the App (#1256).
///
/// The chart's `lookup` preserve pattern does not cover these: the private key
/// renders with no guard, and the other two are plain Deployment env values
/// where no lookup is possible. So it has to happen here.
///
/// The private key is preserved the same way a bot token is -- read back from
/// the release and re-supplied through the private 0600 values file, never
/// argv, so it cannot reach `ps` or shell history.
const GITHUB_APP_MANAGED_KEYS: &[&str] = &[
    "api.githubAppId",
    "api.githubAppPrivateKey",
    "api.githubAppExistingSecret",
    "api.githubAppExistingSecretKey",
    "api.githubCloneBase",
];

const REQUIRED_SECRETS: &[(&str, usize)] = &[
    ("postgres.auth.password", 24),
    ("valkey.password", 24),
    ("clickhouse.auth.password", 24),
    ("rustfs.auth.rootPassword", 24),
    ("langfuse.salt", 16),
    ("langfuse.encryptionKey", 32),
    ("langfuse.nextauthSecret", 24),
    ("api.apiKey", 24),
    ("api.githubWebhookSecret", 24),
];

/// The chart value carrying the API's OUTBOUND GitHub credential: the eval
/// commit status and the git-flow bundle clone (#1058/#1097/#1109).
///
/// Deliberately NOT in [`REQUIRED_SECRETS`]: that list GENERATES a random for a
/// key it finds absent, which is right for a credential the install must have
/// and wrong for this one. Empty here means "no GitHub credential, public repos
/// only"; a generated random would be 32 characters of noise sent to GitHub as a
/// bearer token, failing auth in a way that reads like a permissions problem
/// rather than a missing one (#1109). Preserved, never invented.
pub(crate) const GITHUB_TOKEN_KEY: &str = "api.githubToken";

/// What `cluster up` does with [`GITHUB_TOKEN_KEY`] on this run.
///
/// Three states, resolved once in [`up`] and consumed by the pure builder:
/// - `Untouched`: supply nothing. Either the operator pinned the key through
///   `--set` (theirs to own), or there is no recorded value to keep.
/// - `Set`: write this value through the private 0600 `-f` values file -- an
///   explicit `--github-token`, or the value the last run recorded.
/// - `Clear`: write an empty value, the same shape `comms --disconnect` writes,
///   so a subsequent plain `up` finds an empty record and does not resurrect it.
#[derive(Clone, PartialEq, Eq)]
pub enum GithubTokenPlan {
    Untouched,
    Set(String),
    Clear,
}

/// Hand-written so `{:?}` on this type (or on the `UpOpts` that holds it)
/// never prints the live token -- AC2 requires the credential be absent from
/// debug output too. `Set` renders the same masked form [`mask_secret`]
/// produces everywhere else, never the raw value; a derived `Debug` would
/// print it in full.
impl std::fmt::Debug for GithubTokenPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Untouched => write!(f, "Untouched"),
            Self::Set(v) => write!(f, "Set({})", mask_secret(v)),
            Self::Clear => write!(f, "Clear"),
        }
    }
}

/// `n_bytes` of OS CSPRNG output, lowercase-hex encoded (so `2 * n_bytes`
/// chars). Hex keeps the value shell-, env- and URL-safe and satisfies every
/// backing store's charset/min-length rule, and a hex `langfuse.encryptionKey`
/// is the exact `openssl rand -hex 32` shape the chart documents.
fn random_hex(n_bytes: usize) -> Result<String> {
    use std::fmt::Write;
    let mut buf = vec![0u8; n_bytes];
    getrandom::fill(&mut buf)
        .map_err(|e| anyhow::anyhow!("OS random number generator unavailable: {e}"))?;
    let mut out = String::with_capacity(n_bytes * 2);
    for b in buf {
        let _ = write!(out, "{b:02x}");
    }
    Ok(out)
}

/// Split one Helm set element into raw `(key, value)` halves. This is the one
/// entry parser shared by rendering and the consumers of operator sets.
fn operator_set_entry(part: &str) -> Option<(&str, &str)> {
    part.split_once('=')
}

/// The operator's `--set` arguments split into raw `(key, value)` halves for
/// [`operator_set_keys`] and [`set_passthrough_leaks_github_token`]. Rendering
/// uses the separate [`mask_helm_set_expression`] parser, which preserves
/// escaped commas and brace lists while masking credential-shaped values; do
/// not reuse this naive split for rendering. This is not the only reader of
/// this grammar in the file ([`explicit_runner_model`] hand-rolls a last-wins
/// prefix match with different semantics, and is deliberately left alone).
/// Handles both repeated
/// `--set` flags and helm's comma-joined `a=1,b=2` form; an element with no `=`
/// (a bare `KEY`, an empty element, the tail of a trailing comma) assigns
/// nothing and contributes no entry.
///
/// Both halves are returned VERBATIM, whitespace included, because the callers
/// want different things from it: a key is matched trimmed, while a value's
/// surrounding whitespace is only ever shell noise. Trimming here would decide
/// that for them.
pub(super) fn operator_set_entries(sets: &[String]) -> Vec<(&str, &str)> {
    sets.iter()
        .flat_map(|s| s.split(','))
        .filter_map(operator_set_entry)
        .collect()
}

/// Render one complete Helm set expression while preserving every executed
/// byte except credential values, which are replaced by their standard mask.
pub(super) fn mask_helm_set_expression(expression: &str) -> String {
    let render_part = |part: &str| match operator_set_entry(part) {
        Some((key, value))
            if !value.is_empty()
                && (is_secret_value_key(key.trim()) || is_extra_env_value_key(key.trim())) =>
        {
            format!("{key}={}", mask_secret(value))
        }
        _ => part.to_string(),
    };

    let mut rendered = String::with_capacity(expression.len());
    let mut start = 0;
    let mut in_brace_list = false;
    let mut escaped = false;
    let mut has_equals = false;
    let mut at_value_start = false;

    for (index, ch) in expression.char_indices() {
        if escaped {
            escaped = false;
            at_value_start = false;
            continue;
        }

        match ch {
            '\\' => escaped = true,
            '=' if !has_equals => {
                has_equals = true;
                at_value_start = true;
            }
            '{' if at_value_start => {
                in_brace_list = true;
                at_value_start = false;
            }
            '}' if in_brace_list => in_brace_list = false,
            ',' if !in_brace_list => {
                rendered.push_str(&render_part(&expression[start..index]));
                rendered.push(',');
                start = index + ch.len_utf8();
                has_equals = false;
                at_value_start = false;
            }
            _ => at_value_start = false,
        }
    }

    if in_brace_list {
        return "<secret helm set expression>".to_string();
    }

    rendered.push_str(&render_part(&expression[start..]));
    rendered
}

/// The bare value keys an operator already pinned through `--set` (so the CLI
/// leaves those to the operator rather than generating over them).
pub(super) fn operator_set_keys(sets: &[String]) -> std::collections::HashSet<String> {
    operator_set_entries(sets)
        .into_iter()
        .map(|(key, _)| key.trim().to_string())
        .collect()
}

/// Read a dotted helm key (`langfuse.encryptionKey`) out of a values JSON
/// object, returning the string leaf if present.
fn lookup_dotted(values: &serde_json::Value, dotted: &str) -> Option<String> {
    let mut cursor = values;
    for part in dotted.split('.') {
        cursor = cursor.get(part)?;
    }
    cursor.as_str().map(str::to_string)
}

/// Read a dotted helm key the way the chart's own truthiness gate reads it:
/// `true` for the JSON boolean `true` and for the JSON string `"true"`, and
/// `false` for everything else -- `false`, `"false"`, `"TRUE"`, `""`, any other
/// string, a number, an object, a null leaf, a missing leaf, and a missing or
/// non-object intermediate segment all read as off.
///
/// It mirrors two contracts and must not drift from either: the
/// `eq (toString ...) "true"` branch of `curie.managedSecret` in
/// `charts/curie/templates/_helpers.tpl`, which decides whether a chart-owned
/// credential renders as the published dev default, and the quoted-`"false"`
/// render assertion in `charts/curie/ci/render-assertions.sh`, which pins that
/// spelling as fail-closed. Reading as on what the chart reads as off would
/// wave through exactly the flip this exists to catch, so every ambiguous
/// gate-shaped value fails closed (#1375, #1145).
///
/// This cannot reuse [`lookup_dotted`]: helm records `--set KEY=true` as a JSON
/// *boolean*, and `lookup_dotted` ends in `as_str()`, so it returns `None` for
/// exactly the shape this key normally has.
fn lookup_dotted_flag(values: &serde_json::Value, dotted: &str) -> bool {
    let mut cursor = values;
    for part in dotted.split('.') {
        let Some(next) = cursor.get(part) else {
            return false;
        };
        cursor = next;
    }
    match cursor {
        serde_json::Value::Bool(flag) => *flag,
        serde_json::Value::String(raw) => raw == "true",
        _ => false,
    }
}

/// The value helm already recorded for `key`, when there is a real one. An
/// empty record is what a `--disconnect` / `--clear-*` wrote and is not a
/// credential; returning `None` for it is what stops a cleared value being
/// resurrected on the next plain `up`.
fn preserved_value(existing: Option<&serde_json::Value>, key: &str) -> Option<String> {
    lookup_dotted(existing?, key).filter(|current| !current.is_empty())
}

/// Which Secret and key a direct-passthrough credential (issue #1759) should
/// be read from, given the release's recorded Helm values: the operator's own
/// `existingSecret` when one is configured for this key, otherwise `None` (the
/// caller falls back to the chart's own release Secret and the published key
/// name for this credential).
///
/// MUST stay the same decision as the chart's own BYO-wins precedence
/// (`curie.secretRef.*` and the per-key if/else in
/// `charts/curie/templates/{worker,dispatcher,api,agent-sandbox}.yaml`): a CLI
/// read that resolves a different Secret than the workload's own env would
/// report a plausible but wrong value, which is worse than reporting none.
/// Pure and testable; the actual `helm get values` read stays in the async
/// caller.
pub(crate) fn resolve_existing_secret_ref(
    existing: Option<&serde_json::Value>,
    existing_secret_key: &str,
    existing_secret_key_key: &str,
    default_data_key: &str,
) -> Option<(String, String)> {
    let secret_name = lookup_dotted(existing?, existing_secret_key).filter(|s| !s.is_empty())?;
    let data_key = lookup_dotted(existing?, existing_secret_key_key)
        .filter(|k| !k.is_empty())
        .unwrap_or_else(|| default_data_key.to_string());
    Some((secret_name, data_key))
}

/// Re-supply the [`COMMS_MANAGED_KEYS`] a previous `cluster comms` recorded.
///
/// An operator `--set` for a key always wins, and a key helm has no record of
/// is left alone -- so this only ever preserves, never invents.
fn resolve_comms_values(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) -> Vec<(String, String)> {
    let overridden = operator_set_keys(operator_sets);
    COMMS_MANAGED_KEYS
        .iter()
        .filter(|key| !overridden.contains(**key))
        .filter_map(|key| {
            preserved_value(existing, key).map(|current| ((*key).to_string(), current))
        })
        .collect()
}

/// Re-supply the [`GITHUB_APP_MANAGED_KEYS`] a previous `cluster github-app` recorded.
///
/// Same contract as [`resolve_comms_values`]: an operator `--set` wins, a key
/// helm has no record of is left alone. Preserves, never invents -- inventing
/// an App id would be worse than dropping one.
fn resolve_github_app_values(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) -> Vec<(String, String)> {
    let overridden = operator_set_keys(operator_sets);
    GITHUB_APP_MANAGED_KEYS
        .iter()
        .filter(|key| !overridden.contains(**key))
        .filter_map(|key| {
            preserved_value(existing, key).map(|current| ((*key).to_string(), current))
        })
        .collect()
}

/// Supply the sealing keypair (ADR-0094), generating one only when safe.
///
/// The rules differ from [`resolve_generated_secrets`] in one place, and the
/// difference matters:
///
/// - An operator `--set` always wins.
/// - A release that already records a key gets exactly that key back. Never a
///   new one. Regenerating would render every sealed credential in every agent
///   repository permanently unreadable -- the #1256 preservation bug with a
///   blast radius no store password comes close to.
/// - A release with NO key recorded gets a fresh one, and this is where it
///   diverges from the store passwords. For those, minting on an existing
///   release would rotate a credential out from under a running store, so the
///   rule is "leave it alone". For sealing there is nothing to rotate: no key
///   means nothing has ever been sealed to this cluster, so generating one is
///   how an existing install gains the feature.
///
/// The PREVIOUS key is only ever preserved, never generated: it exists solely
/// because an operator deliberately rotated, and inventing one would claim an
/// overlap that never happened.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SealingPrivateKeyDisposition {
    OperatorSet,
    Deferred,
    Preserved,
    Generated,
}

fn sealing_private_key_disposition(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
    dry_run: bool,
) -> SealingPrivateKeyDisposition {
    if operator_set_keys(operator_sets).contains(crate::sealing::SEALING_PRIVATE_KEY) {
        return SealingPrivateKeyDisposition::OperatorSet;
    }
    if preserved_value(existing, crate::sealing::SEALING_PRIVATE_KEY).is_some() {
        return SealingPrivateKeyDisposition::Preserved;
    }
    if dry_run {
        SealingPrivateKeyDisposition::Deferred
    } else {
        SealingPrivateKeyDisposition::Generated
    }
}

fn resolve_preserved_sealing_values(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) -> Vec<(String, String)> {
    let overridden = operator_set_keys(operator_sets);
    let mut resolved = Vec::new();

    if !overridden.contains(crate::sealing::SEALING_PRIVATE_KEY) {
        if let Some(current) = preserved_value(existing, crate::sealing::SEALING_PRIVATE_KEY) {
            resolved.push((crate::sealing::SEALING_PRIVATE_KEY.to_string(), current));
        }
    }
    if !overridden.contains(crate::sealing::SEALING_PREVIOUS_PRIVATE_KEY) {
        if let Some(previous) =
            preserved_value(existing, crate::sealing::SEALING_PREVIOUS_PRIVATE_KEY)
        {
            resolved.push((
                crate::sealing::SEALING_PREVIOUS_PRIVATE_KEY.to_string(),
                previous,
            ));
        }
    }
    resolved
}

fn resolve_sealing_values(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) -> Vec<(String, String)> {
    let mut resolved = resolve_preserved_sealing_values(existing, operator_sets);
    if sealing_private_key_disposition(existing, operator_sets, false)
        == SealingPrivateKeyDisposition::Generated
    {
        resolved.insert(
            0,
            (
                crate::sealing::SEALING_PRIVATE_KEY.to_string(),
                crate::sealing::generate_keypair().private_key,
            ),
        );
    }
    resolved
}

/// Preserve the installed mail surface as typed values: booleans, arrays and
/// credential maps must not become strings when delivered through Helm -f.
/// This family includes the paired worker credential source, because preserving
/// only the adapter side can leave the channel unable to receive replies.
fn resolve_retained_mail_values(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) -> Result<Option<PrivateHelmValues>> {
    let Some(existing) = existing else {
        return Ok(None);
    };
    let mut retained = serde_json::json!({});
    if let Some(mail) = existing.get("mailAdapter") {
        retained["mailAdapter"] = mail.clone();
    }
    for key in [
        "adapterCredentials",
        "adapterCredentialsExistingSecret",
        "adapterCredentialsExistingSecretKey",
    ] {
        if let Some(value) = existing.get("worker").and_then(|worker| worker.get(key)) {
            retained["worker"][key] = value.clone();
        }
    }
    let overridden = operator_set_keys(operator_sets);
    let operator_values: BTreeMap<_, _> = operator_set_entries(operator_sets)
        .into_iter()
        .map(|(key, value)| (key.trim(), value))
        .collect();
    let replaces_inline = |inline: &str| {
        operator_values.iter().any(|(key, value)| {
            key_is_or_descends_from(key, inline)
                && !value.is_empty()
                && !(*key == inline
                    && inline == "worker.adapterCredentials"
                    && serde_json::from_str::<serde_json::Value>(value)
                        .is_ok_and(|value| value.as_object().is_some_and(|map| map.is_empty())))
        })
    };
    // An externally managed worker map is opaque: changing only the adapter's
    // source cannot safely derive or replace its paired worker credential.
    // Require an explicit paired decision instead of creating a silent 401.
    let changes_adapter_source = replaces_inline("mailAdapter.egressSecret")
        || operator_values
            .get("mailAdapter.egressSecretExistingSecret")
            .is_some_and(|value| {
                Some(*value)
                    != lookup_dotted(existing, "mailAdapter.egressSecretExistingSecret").as_deref()
            })
        || (lookup_dotted(existing, "mailAdapter.egressSecretExistingSecret")
            .is_some_and(|value| !value.is_empty())
            && operator_values
                .get("mailAdapter.egressSecretExistingSecretKey")
                .is_some_and(|value| {
                    *value
                        != lookup_dotted(existing, "mailAdapter.egressSecretExistingSecretKey")
                            .as_deref()
                            .unwrap_or("mailEgressSecret")
                }));
    let changes_worker_source = replaces_inline("worker.adapterCredentials")
        || overridden.contains("worker.adapterCredentialsExistingSecret")
        || overridden.contains("worker.adapterCredentialsExistingSecretKey");
    if changes_adapter_source
        && !changes_worker_source
        && lookup_dotted(existing, "worker.adapterCredentialsExistingSecret")
            .is_some_and(|value| !value.is_empty())
    {
        bail!(
            "changing the mail egress credential source requires an explicit paired worker \
             source decision: also set worker.adapterCredentialsExistingSecret (clear it \
             to use the chart-derived inline pair), its ExistingSecretKey, or supply \
             worker.adapterCredentials"
        );
    }
    let mut cleared = BTreeMap::new();
    for inline in [
        "mailAdapter.agentmail.apiKey",
        "mailAdapter.channelToken",
        "mailAdapter.egressSecret",
        "worker.adapterCredentials",
    ] {
        let reference = format!("{inline}ExistingSecret");
        // A nonempty inline value replaces the external source. An empty inline
        // clear leaves that source active; its explicit reference clear remains
        // the operator's way to remove it. Active sources never replay stale
        // inline copies left in Helm before token rotation.
        let remove = if replaces_inline(inline) {
            vec![reference.clone()]
        } else if overridden.contains(&reference)
            || lookup_dotted(&retained, &reference).is_some_and(|value| !value.is_empty())
        {
            vec![inline.to_string()]
        } else {
            vec![]
        };
        for path in remove {
            let (parent, leaf) = path.rsplit_once('.').expect("credential path");
            let pointer = format!("/{}", parent.replace('.', "/"));
            if let Some(object) = retained
                .pointer_mut(&pointer)
                .and_then(serde_json::Value::as_object_mut)
            {
                if let Some(removed) = object.remove(leaf) {
                    let mut removed_leaves = BTreeMap::new();
                    crate::installation::flatten_values(&removed, &path, &mut removed_leaves);
                    cleared.extend(removed_leaves.into_keys().map(|key| (key, String::new())));
                }
            }
        }
    }
    fn without_overrides(
        value: &serde_json::Value,
        path: &str,
        overridden: &std::collections::HashSet<String>,
    ) -> Option<serde_json::Value> {
        if overridden
            .iter()
            .any(|key| key_is_or_descends_from(path, key))
        {
            return None;
        }
        match value {
            serde_json::Value::Object(object) => {
                let remaining: serde_json::Map<String, serde_json::Value> = object
                    .iter()
                    .filter_map(|(key, value)| {
                        let child = if path.is_empty() {
                            key.clone()
                        } else {
                            format!("{path}.{key}")
                        };
                        without_overrides(value, &child, overridden)
                            .map(|value| (key.clone(), value))
                    })
                    .collect();
                (!remaining.is_empty() || object.is_empty())
                    .then_some(serde_json::Value::Object(remaining))
            }
            serde_json::Value::Array(_)
                if overridden
                    .iter()
                    .any(|key| key_is_or_descends_from(key, path)) =>
            {
                None
            }
            _ => Some(value.clone()),
        }
    }
    Ok(without_overrides(&retained, "", &overridden)
        .filter(|value| value.as_object().is_some_and(|object| !object.is_empty()))
        .map(|document| PrivateHelmValues(document, cleared)))
}

/// Every value a plain `cluster up` must carry forward, in one place.
///
/// `up` does a FULL upgrade and drops anything it does not re-pass, so each
/// family of keys recorded by a SIBLING verb has to be re-supplied here or it
/// is silently reverted -- Slack tokens by `comms` (#1067), the GitHub App by
/// `github-app` (#1256). Both failures looked identical to the operator:
/// something that had been working stopped, with nothing in the diff naming
/// it.
///
/// Composed rather than called separately from `up`, so a test can assert the
/// whole set. Unit-testing each family in isolation left the WIRING uncovered:
/// deleting the call from `up` kept every test green.
fn resolve_preserved_values(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) -> Vec<(String, String)> {
    let mut all = resolve_comms_values(existing, operator_sets);
    all.extend(resolve_github_app_values(existing, operator_sets));
    all.extend(resolve_preserved_sealing_values(existing, operator_sets));
    all
}

/// Resolve managed values for an actual or previewed `cluster up`.
///
/// An offline preview has no evidence that the sealing key is absent, so it
/// carries only values that are positively known to exist. A live completion
/// may generate the current sealing key after the release read proves it is
/// absent.
fn resolve_managed_values_for_up(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
    dry_run: bool,
) -> Vec<(String, String)> {
    let mut values = resolve_preserved_values(existing, operator_sets);
    if sealing_private_key_disposition(existing, operator_sets, dry_run)
        == SealingPrivateKeyDisposition::Generated
    {
        values.extend(
            resolve_sealing_values(existing, operator_sets)
                .into_iter()
                .filter(|(key, _)| key == crate::sealing::SEALING_PRIVATE_KEY),
        );
    }
    values
}

#[cfg(test)]
mod sealing_preservation_tests {
    use super::*;

    fn values(json: serde_json::Value) -> serde_json::Value {
        json
    }

    fn get<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
        pairs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// The catastrophic case. A plain `cluster up` drops anything it does not
    /// re-pass, and dropping this key makes every sealed credential in every
    /// agent repository permanently unreadable.
    #[test]
    fn an_upgrade_re_supplies_the_existing_key_unchanged() {
        let existing = values(serde_json::json!({
            "sealing": {"privateKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}
        }));
        let resolved = resolve_sealing_values(Some(&existing), &[]);
        assert_eq!(
            get(&resolved, crate::sealing::SEALING_PRIVATE_KEY),
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            "the recorded key must come back byte-identical"
        );
    }

    /// And it must be reachable through the composed set `up` actually calls,
    /// not only through the family function. Unit-testing the family alone left
    /// the WIRING uncovered for the comms and App families before (#1256).
    #[test]
    fn the_composed_preserved_set_carries_the_sealing_key() {
        let existing = values(serde_json::json!({
            "sealing": {"privateKey": "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB="}
        }));
        let all = resolve_preserved_values(Some(&existing), &[]);
        assert_eq!(
            get(&all, crate::sealing::SEALING_PRIVATE_KEY),
            Some("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB=")
        );
    }

    /// The seam that actually broke, closed by construction.
    ///
    /// `up` re-supplies a set of keys; `diff` decides which keys survive an
    /// apply via `is_preserved_by_up`. Those two answers must be the same
    /// answer. They were kept in step by a hand-written list, and sealing was
    /// added to one and not the other -- so `curie diff` against a real 0.6.0
    /// release announced `sealing.privateKey (reset to chart default)` for a
    /// key apply hands straight back.
    ///
    /// This drives the REAL resolver over a release carrying every preserved
    /// family and asserts `diff` agrees about every key it returns. A future
    /// family added to `resolve_preserved_values` alone fails here instead of
    /// reaching an operator as a false alarm about their own data.
    #[test]
    fn diff_agrees_with_every_key_up_actually_re_supplies() {
        let existing = values(serde_json::json!({
            "sealing": {
                "privateKey": "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC=",
                "previousPrivateKey": "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD="
            },
            "dispatcher": {"slack": {"appToken": "xapp-EXAMPLE", "botToken": "xoxb-EXAMPLE"}},
            "api": {
                "githubAppId": "1",
                "githubAppExistingSecret": "an-app-secret",
                "githubAppExistingSecretKey": "privateKey"
            }
        }));
        let supplied = resolve_preserved_values(Some(&existing), &[]);
        assert!(
            !supplied.is_empty(),
            "fixture must exercise the resolver, not pass vacuously"
        );
        let disagreements: Vec<&str> = supplied
            .iter()
            .map(|(k, _)| k.as_str())
            .filter(|k| !is_preserved_by_up(k))
            .collect();
        assert!(
            disagreements.is_empty(),
            "`up` re-supplies these keys but `is_preserved_by_up` says apply drops them, \
             so `curie diff` would report a reset that never happens: {disagreements:?}"
        );
    }

    /// A fresh install gets a usable key, or the feature never starts.
    #[test]
    fn a_fresh_install_generates_a_usable_key() {
        let resolved = resolve_sealing_values(None, &[]);
        let key = get(&resolved, crate::sealing::SEALING_PRIVATE_KEY).expect("generated");
        let public = crate::sealing::public_key_of(key).expect("a real keypair");
        let blob = crate::sealing::seal(&public, "value").expect("seals");
        assert_eq!(
            crate::sealing::open_with_any(&[key.to_string()], &blob).unwrap(),
            "value"
        );
    }

    /// An existing release with no key gains one -- this is how an install that
    /// predates the feature starts using it. Safe precisely because no key
    /// means nothing has ever been sealed to this cluster.
    #[test]
    fn an_existing_release_without_a_key_gains_one() {
        let existing = values(serde_json::json!({"ui": {"deploy": false}}));
        let resolved = resolve_sealing_values(Some(&existing), &[]);
        assert!(get(&resolved, crate::sealing::SEALING_PRIVATE_KEY).is_some());
    }

    /// The previous key is preserved when present, so a rotation overlap
    /// survives the upgrades that happen during it.
    #[test]
    fn a_rotation_in_progress_keeps_both_keys() {
        let existing = values(serde_json::json!({"sealing": {
            "privateKey": "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC=",
            "previousPrivateKey": "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD="
        }}));
        let resolved = resolve_sealing_values(Some(&existing), &[]);
        assert_eq!(
            get(&resolved, crate::sealing::SEALING_PREVIOUS_PRIVATE_KEY),
            Some("DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD=")
        );
    }

    /// Never invented: a previous key claims a rotation happened.
    #[test]
    fn a_previous_key_is_never_generated() {
        for existing in [None, Some(values(serde_json::json!({})))] {
            let resolved = resolve_sealing_values(existing.as_ref(), &[]);
            assert!(
                get(&resolved, crate::sealing::SEALING_PREVIOUS_PRIVATE_KEY).is_none(),
                "an overlap that never happened must not be claimed"
            );
        }
    }

    /// An operator `--set` wins, as it does for every other managed family.
    #[test]
    fn an_operator_set_is_not_overridden() {
        let sets = vec![format!("{}=mine", crate::sealing::SEALING_PRIVATE_KEY)];
        let resolved = resolve_sealing_values(None, &sets);
        assert!(get(&resolved, crate::sealing::SEALING_PRIVATE_KEY).is_none());
    }
}

/// The chart value holding the model credential. Named here so the secret
/// classifier below cannot drift from the key `up_commands` actually masks.
pub(crate) const MODEL_CREDENTIAL_KEY: &str = "agentSandbox.runner.credentials";

/// Emitted alongside [`MODEL_CREDENTIAL_KEY`], and only when a credential is
/// present -- see `up_commands`, which pushes both inside one `if let`.
pub(crate) const FAKE_MODEL_KEY: &str = "agentSandbox.runner.fakeModel";

const ALLOWED_EGRESS_KEY: &str = "security.networkPolicy.allowedEgress";

const WORKER_EXTRA_ENV_KEY: &str = "worker.extraEnv";

/// The ADDITIONAL Slack origins a per-turn reply endpoint may name (ADR-0096
/// D4.4). Named here so `up`'s preservation, `diff`'s reset reporting, and the
/// secret classifier below all read the one key.
const SLACK_TRUSTED_ORIGINS_KEY: &str = "worker.slackTrustedOrigins";

fn key_is_or_descends_from(key: &str, parent: &str) -> bool {
    key == parent
        || key
            .strip_prefix(parent)
            .is_some_and(|suffix| suffix.starts_with('[') || suffix.starts_with('.'))
}

fn escape_helm_set_string_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        if matches!(ch, '\\' | ',' | '{' | '}') {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

fn helm_set_string_entries(expression: &str) -> Vec<(String, String)> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_brace_list = false;
    let mut escaped = false;
    let mut has_equals = false;
    let mut at_value_start = false;

    for (index, ch) in expression.char_indices() {
        if escaped {
            escaped = false;
            at_value_start = false;
            continue;
        }

        match ch {
            '\\' => escaped = true,
            '=' if !has_equals => {
                has_equals = true;
                at_value_start = true;
            }
            '{' if at_value_start => {
                in_brace_list = true;
                at_value_start = false;
            }
            '}' if in_brace_list => in_brace_list = false,
            ',' if !in_brace_list => {
                parts.push(&expression[start..index]);
                start = index + ch.len_utf8();
                has_equals = false;
                at_value_start = false;
            }
            _ => at_value_start = false,
        }
    }
    parts.push(&expression[start..]);

    parts
        .into_iter()
        .filter_map(operator_set_entry)
        .map(|(key, value)| {
            let mut decoded = String::with_capacity(value.len());
            let mut chars = value.chars();
            while let Some(ch) = chars.next() {
                if ch == '\\' {
                    if let Some(next) = chars.next() {
                        decoded.push(next);
                    } else {
                        decoded.push(ch);
                    }
                } else {
                    decoded.push(ch);
                }
            }
            (key.trim().to_string(), decoded)
        })
        .collect()
}

/// Carry the runner configuration recorded by a prior real model install into
/// a plain rerun. Explicit inputs replace their recorded family.
fn resolve_preserved_runner_identity_values(
    opts: &mut UpOpts,
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) {
    let overridden = operator_set_keys(operator_sets);

    if !opts.fake_model
        && opts.local_model.is_none()
        && opts.credentials.is_none()
        && !overridden.contains(MODEL_CREDENTIAL_KEY)
    {
        opts.credentials = preserved_value(existing, MODEL_CREDENTIAL_KEY);
    }

    if !opts.fake_model
        && opts.local_model.is_none()
        && opts.model.is_none()
        && !overridden.contains(RUNNER_MODEL_KEY)
    {
        opts.model = preserved_value(existing, RUNNER_MODEL_KEY);
    }
}

/// Carry an inferred gVisor posture into a later plain `cluster up`.
///
/// The RuntimeClass admission recovery writes `security.gvisor.mode=off` only
/// on its retry. Helm records that successful retry, but a normal `up` is a
/// full upgrade rather than `--reuse-values`; without re-supplying the recorded
/// posture, the next run falls back to the chart's `auto` default and repeats
/// the failed preflight. As with the other recorded-value families, an explicit
/// operator setting owns the key and always wins.
fn resolve_preserved_gvisor_mode_value(
    opts: &mut UpOpts,
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) {
    if operator_set_keys(operator_sets).contains(GVISOR_MODE_KEY) {
        return;
    }
    if let Some(mode) = preserved_value(existing, GVISOR_MODE_KEY) {
        opts.set.push(format!("{GVISOR_MODE_KEY}={mode}"));
    }
}

/// Carry the worker environment recorded by a prior install into a plain rerun.
/// Explicit inputs replace the recorded family.
fn resolve_preserved_worker_extra_env_values(
    opts: &mut UpOpts,
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) {
    let overridden = operator_set_keys(operator_sets);
    if overridden
        .iter()
        .any(|key| key_is_or_descends_from(key, WORKER_EXTRA_ENV_KEY))
    {
        return;
    }

    let mut recorded = BTreeMap::new();
    if let Some(values) = existing {
        crate::installation::flatten_values(values, "", &mut recorded);
    }
    opts.set_string.extend(
        recorded
            .into_iter()
            .filter(|(key, _)| key_is_or_descends_from(key, WORKER_EXTRA_ENV_KEY))
            .map(|(key, value)| format!("{key}={}", escape_helm_set_string_value(&value))),
    );
}

/// Carry an operator-recorded Slack trusted-origin list into a later plain
/// `cluster up` (issue #1897).
///
/// `up` is a FULL helm upgrade, not `--reuse-values`, so a
/// `worker.slackTrustedOrigins` an operator set once is reset to the chart's
/// fail-closed `""` default by the next unrelated `up` -- the #1256
/// preservation class again, and it silently re-breaks the dev reply path the
/// operator had working. Emitted via `--set-string` with
/// [`escape_helm_set_string_value`] because the value is a COMMA-SEPARATED
/// origin list: an unescaped comma would make Helm read it as a list.
///
/// Preserves, never invents. [`preserved_value`] already filters an empty
/// record, so a release that never set the key -- or one whose value was
/// deliberately cleared -- supplies nothing and keeps the chart default. An
/// explicit operator `--set` / `--set-string` owns the key and always wins.
fn resolve_preserved_slack_trusted_origins_value(
    opts: &mut UpOpts,
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) {
    if operator_set_keys(operator_sets).contains(SLACK_TRUSTED_ORIGINS_KEY) {
        return;
    }
    if let Some(origins) = preserved_value(existing, SLACK_TRUSTED_ORIGINS_KEY) {
        opts.set_string.push(format!(
            "{SLACK_TRUSTED_ORIGINS_KEY}={}",
            escape_helm_set_string_value(&origins)
        ));
    }
}

fn resolve_preserved_runner_egress_values(
    opts: &mut UpOpts,
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
    provider_was_inferred: bool,
) -> (usize, BTreeSet<String>) {
    let overridden = operator_set_keys(operator_sets);
    let egress_replaced = (!opts.allow_egress_host.is_empty() && !provider_was_inferred)
        || !opts.allow_web_egress.is_empty()
        || overridden
            .iter()
            .any(|key| key_is_or_descends_from(key, ALLOWED_EGRESS_KEY));
    if egress_replaced {
        return (0, BTreeSet::new());
    }

    let mut recorded = BTreeMap::new();
    if let Some(values) = existing {
        crate::installation::flatten_values(values, "", &mut recorded);
    }
    let next_index = recorded
        .keys()
        .filter_map(|key| {
            key.strip_prefix(ALLOWED_EGRESS_KEY)?
                .strip_prefix('[')?
                .split_once(']')?
                .0
                .parse::<usize>()
                .ok()
        })
        .max()
        .map_or(0, |index| index + 1);
    let recorded_cidrs = recorded
        .iter()
        .filter_map(|(key, value)| {
            let suffix = key.strip_prefix(ALLOWED_EGRESS_KEY)?.strip_prefix('[')?;
            let (index, field) = suffix.split_once(']')?;
            (index.parse::<usize>().is_ok() && field == ".cidr").then(|| value.clone())
        })
        .collect();
    opts.set.extend(
        recorded
            .into_iter()
            .filter(|(key, _)| key_is_or_descends_from(key, ALLOWED_EGRESS_KEY))
            .map(|(key, value)| format!("{key}={value}")),
    );
    (next_index, recorded_cidrs)
}

fn resolve_provider_egress_for_up(opts: &mut UpOpts, resolve: bool) -> Result<()> {
    if resolve && !opts.allow_egress_host.is_empty() && opts.resolved_egress_cidrs.is_empty() {
        opts.resolved_egress_cidrs =
            resolve_provider_egress_cidrs_for_current_environment(&opts.allow_egress_host)
                .context("resolving named provider egress hosts")?;
    }
    Ok(())
}

fn reindex_inferred_provider_egress(
    opts: &mut UpOpts,
    start_index: usize,
    recorded_cidrs: &BTreeSet<String>,
) {
    let resolved = std::mem::take(&mut opts.resolved_egress_cidrs);
    for (offset, cidr) in resolved
        .into_iter()
        .filter(|cidr| !recorded_cidrs.contains(cidr))
        .enumerate()
    {
        let index = start_index + offset;
        opts.set
            .push(format!("{ALLOWED_EGRESS_KEY}[{index}].cidr={cidr}"));
        opts.set.push(format!(
            "{ALLOWED_EGRESS_KEY}[{index}].ports[0].protocol=TCP"
        ));
        opts.set.push(format!(
            "{ALLOWED_EGRESS_KEY}[{index}].ports[0].port={EGRESS_TCP_PORT}"
        ));
    }
}

fn is_retained_mail_key(key: &str) -> bool {
    key_is_or_descends_from(key, "mailAdapter")
        || key_is_or_descends_from(key, "worker.adapterCredentials")
        || key == "worker.adapterCredentialsExistingSecret"
        || key == "worker.adapterCredentialsExistingSecretKey"
}

/// Does a plain `cluster up` carry this key forward when nothing re-passes it?
///
/// The honest half of `curie diff`. `up` does a FULL upgrade, so a key present
/// on the release but absent from `curie.yaml` is normally reset to the chart
/// default -- except for the families [`resolve_preserved_values`],
/// [`resolve_preserved_runner_identity_values`], and
/// [`resolve_preserved_runner_egress_values`],
/// [`resolve_preserved_gvisor_mode_value`], and
/// [`resolve_preserved_slack_trusted_origins_value`] re-supply, which survive
/// untouched.
/// Reporting those as removals would be the exact
/// "proposing to delete what it did not create" failure ADR-0097 named.
///
/// Reads the same constants `up` reads, so a new preserved family is picked up
/// by both or neither. That claim was false for one release: sealing (ADR-0094)
/// added a family to [`resolve_preserved_values`] and not to this list, so
/// `diff` announced that apply would reset `sealing.privateKey` to the chart
/// default. Apply does no such thing -- it hands the live key straight back.
///
/// A false reset here is worse than a missing one. The note `diff` prints on a
/// reset says to declare the value in `curie.yaml` to keep it, and following
/// that for this key means pasting a PRIVATE KEY into a file whose own header
/// says it carries names and never secrets. `sealing_test` below asserts the
/// two lists agree by construction rather than by a hand-kept fixture.
pub fn is_preserved_by_up(key: &str) -> bool {
    is_retained_mail_key(key)
        || COMMS_MANAGED_KEYS.contains(&key)
        || GITHUB_APP_MANAGED_KEYS.contains(&key)
        || REQUIRED_SECRETS.iter().any(|(k, _)| *k == key)
        || crate::sealing::SEALING_MANAGED_KEYS.contains(&key)
        || key == GVISOR_MODE_KEY
        || key == SLACK_TRUSTED_ORIGINS_KEY
}

/// Substrings that mark a chart key as carrying a credential.
///
/// The masking rule has to be deny-by-default, and this is what makes it so.
/// Matching on NAME rather than on membership of a managed list is the whole
/// point: a list only knows the keys THIS chart version manages, and anything
/// outside it -- a key an older chart used, a key an operator set by hand --
/// gets printed.
///
/// Over-masking is the safe direction. Masking `api.githubCloneBase` costs a
/// reader one lookup; printing a password costs a rotation.
const SECRET_KEY_MARKERS: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "credential",
    "salt",
    "key",
    "auth",
];

/// Does this key's VALUE carry a secret?
///
/// `helm get values` returns real passwords and tokens, so anything rendering a
/// live release has to know which values must never reach a terminal, a log, or
/// a `--json` consumer.
///
/// This was an allowlist of managed keys, and it leaked. Run against a real
/// release, `curie diff` printed `minio.auth.rootPassword` in full: the chart
/// had since renamed that store to `rustfs`, so the live key matched no managed
/// list and fell through to "ordinary value". The store was still running. The
/// unit tests could not have caught it -- they chose their own fixture values,
/// so the "no secret in the output" assertion compared against a plaintext the
/// test itself invented.
///
/// The question is now "does this key NAME say it holds a credential?", which
/// is true of a renamed key, a legacy key, and an operator's own `--set`, none
/// of which any list can enumerate in advance.
pub fn is_secret_value_key(key: &str) -> bool {
    // Most preserve-on-up keys are credentials, but an inferred gVisor posture
    // is ordinary safety configuration and must remain visible in `curie diff`.
    // A Slack trusted-origin list (issue #1897) is the same shape: it is
    // operator-visible dev configuration -- hostnames, not a token -- and
    // masking it would hide the very value the operator opens `curie diff` to
    // confirm survived the upgrade.
    if (is_preserved_by_up(key) && key != GVISOR_MODE_KEY && key != SLACK_TRUSTED_ORIGINS_KEY)
        || key == GITHUB_TOKEN_KEY
        || key == MODEL_CREDENTIAL_KEY
    {
        return true;
    }
    let lowered = key.to_ascii_lowercase();
    SECRET_KEY_MARKERS
        .iter()
        .any(|marker| lowered.contains(marker))
}

/// `helm list -n <ns> -o json`, for reading the deployed chart version.
fn helm_list_cmd(o: &CommonOpts) -> OpsCommand {
    OpsCommand::new(
        "helm",
        vec![
            plain("list"),
            plain("-n"),
            plain(&o.namespace),
            plain("-o"),
            plain("json"),
        ],
    )
}

/// The chart the release was last installed with, e.g. `curie-0.5.1`.
///
/// `curie diff` compares VALUES, which says nothing about whether the chart
/// those values feed is the same chart. On a real release this mattered: the
/// cluster ran `curie-0.5.1`, whose object store is MinIO, while the CLI would
/// apply `0.6.0`, where that component is `rustfs` and does not exist under the
/// old name. A value-level diff renders that as a handful of `minio.*` resets
/// when it is really a component swap.
pub async fn fetch_release_chart(o: &CommonOpts) -> Result<Option<String>> {
    let (ok, out, _err) = run_capture(&helm_list_cmd(o)).await?;
    if !ok {
        return Ok(None);
    }
    let parsed: serde_json::Value = match serde_json::from_str(out.trim()) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    Ok(parsed
        .as_array()
        .and_then(|releases| {
            releases
                .iter()
                .find(|r| r.get("name").and_then(|n| n.as_str()) == Some(o.release.as_str()))
        })
        .and_then(|r| r.get("chart"))
        .and_then(|c| c.as_str())
        .map(str::to_string))
}

/// Has the operator told Helm to keep this resource across upgrades?
///
/// `helm.sh/resource-policy: keep` is Helm's own mechanism for "do not delete
/// this, even when the chart stops rendering it". A resource carrying it is not
/// at risk, so flagging it would be a false alarm -- and annotating it is
/// exactly how an operator detaches a store before a chart renames it, which is
/// the supported way through the very migration this guard exists to stop them
/// botching.
pub fn helm_keeps(resource: &serde_json::Value) -> bool {
    resource
        .get("metadata")
        .and_then(|m| m.get("annotations"))
        .and_then(|a| a.get("helm.sh/resource-policy"))
        .and_then(|p| p.as_str())
        .map(|p| p == "keep")
        .unwrap_or(false)
}

/// The COMPONENT identities of the StatefulSets the release currently owns.
///
/// Keyed on `app.kubernetes.io/component`, never on `metadata.name`. Resource
/// names embed the chart fullname, so `nameOverride` alone renames every one of
/// them -- and comparing names made a release installed with an override look
/// like it was losing every stateful component at once. A guard that cries wolf
/// teaches operators to pass the override flag by reflex, which is worse than
/// no guard for the one case that is real.
///
/// Returns (component, resource name): the component is the identity, the name
/// is what an operator needs to see in the error.
pub async fn live_stateful_components(o: &CommonOpts) -> Result<Vec<(String, String)>> {
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("statefulset"),
            plain("-n"),
            plain(&o.namespace),
            plain("-o"),
            plain("json"),
        ],
    );
    let (ok, out, err) = run_capture(&cmd).await?;
    if !ok {
        // Fail closed. The ONLY "there is nothing here" answer this argv
        // produces is exit 0 with an empty items array, which a namespaced LIST
        // returns even for a namespace that does not exist (verified against a
        // real apiserver, kubectl v1.36.2). So a nonzero exit always means "I
        // could not find out", and returning an empty list from it told the
        // stateful-removal guard "fresh install, nothing to lose" while the
        // upgrade went on to prune the StatefulSet (#1351). The helm half of
        // this same guard already bails; this is the symmetric half.
        //
        // The wording stays NEUTRAL about why the read was wanted. Three of this
        // helper's four callers are running a migration rather than the
        // stateful-removal guard, so naming that guard here reported a check the
        // caller was not performing (#1351); the guard adds its own framing with
        // `.context(..)` at its call site.
        //
        // kubectl can exit nonzero with an empty stderr (a signal, a wrapper
        // script), so fall back to stdout and then to a literal rather than
        // handing the operator a message ending in a bare colon.
        let detail = [err.trim(), out.trim()]
            .into_iter()
            .find(|s| !s.is_empty())
            .unwrap_or("kubectl exited nonzero with no stderr");
        let message = format!(
            "could not list the StatefulSets in namespace {}: {detail}",
            o.namespace
        );
        // The bail is UNCONDITIONAL: every nonzero exit fails, so a Forbidden
        // still fails (as Failure) and is never swallowed into a vacuous pass.
        // `is_connectivity_failure` only picks the exit CLASS once that decision
        // is made. An unreachable apiserver is the retryable condition exit 3
        // names (`exit.rs`), and the helm half of a teardown already reports it
        // that way, so an automation loop retries the same argv instead of
        // reading a rolling restart as permanent (#1351).
        return Err((if is_connectivity_failure(&err) {
            crate::exit::CliError::transient(message)
        } else {
            crate::exit::CliError::failure(message)
        }
        .with_fix(
            "check the cluster is reachable and the kubeconfig points at the right context: `kubectl config current-context` then `kubectl get ns`",
        ))
        .into());
    }
    let parsed: serde_json::Value = match serde_json::from_str(out.trim()) {
        Ok(v) => v,
        Err(_) => return Ok(Vec::new()),
    };
    Ok(stateful_components_from_list(&parsed))
}

/// The pure half of [`live_stateful_components`]: a kubectl List in, the
/// at-risk components out.
///
/// Split out so the `helm.sh/resource-policy: keep` filter is covered by a test
/// that fails when the filter is REMOVED. Testing `helm_keeps` alone did not:
/// deleting its call site left every test green, which is the same vacuous
/// shape that hid three earlier bugs in this file's history.
pub fn stateful_components_from_list(list: &serde_json::Value) -> Vec<(String, String)> {
    list.get("items")
        .and_then(|i| i.as_array())
        .map(|items| {
            items
                .iter()
                .filter(|i| !helm_keeps(i))
                .filter_map(|i| {
                    let component = i
                        .get("spec")?
                        .get("selector")?
                        .get("matchLabels")?
                        .get("app.kubernetes.io/component")?
                        .as_str()?;
                    let name = i.get("metadata")?.get("name")?.as_str()?;
                    Some((component.to_string(), name.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// StatefulSet names the target chart would render for this release.
///
/// `helm template` rather than a dry-run upgrade: it needs no cluster and
/// cannot mutate, so the guard is safe to run before deciding whether to
/// proceed.
pub(crate) async fn chart_stateful_components(
    chart: &str,
    o: &CommonOpts,
    value_plan: &UpValuePlan,
) -> Result<Vec<(String, String)>> {
    let mut args = vec![
        plain("template"),
        plain(&o.release),
        plain(chart),
        plain("-n"),
        plain(&o.namespace),
    ];
    value_plan.append_command_args(&mut args);
    let (ok, out, err) = run_capture(&OpsCommand::new("helm", args)).await?;
    if !ok {
        bail!("could not render the target chart to check for removed stateful components: {err}");
    }
    Ok(parse_statefulset_components(&out))
}

/// The component identities of StatefulSets in a multi-document helm render.
///
/// Split-and-parse rather than a regex: a `kind: StatefulSet` line can appear
/// inside an annotation or a ConfigMap payload, and matching that would invent
/// a component the chart does not actually create.
pub fn parse_statefulset_components(rendered: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for doc in rendered.split("\n---") {
        let Ok(value) = serde_norway::from_str::<serde_json::Value>(doc) else {
            continue;
        };
        if value.get("kind").and_then(|k| k.as_str()) != Some("StatefulSet") {
            continue;
        }
        let component = value
            .get("spec")
            .and_then(|s| s.get("selector"))
            .and_then(|s| s.get("matchLabels"))
            .and_then(|l| l.get("app.kubernetes.io/component"))
            .and_then(|c| c.as_str());
        let name = value
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str());
        if let (Some(component), Some(name)) = (component, name) {
            if !out.iter().any(|(c, _)| c == component) {
                out.push((component.to_string(), name.to_string()));
            }
        }
    }
    out
}

/// Why a live stateful component would not survive the apply.
///
/// Carried rather than collapsed to a bare name because the two causes have
/// DIFFERENT remedies, and the refusal is the only place an operator learns
/// which one they need. Offering `--migrate-store` for a rename would send
/// them to a flag that cannot help: both releases run the same store, so
/// there is nothing to migrate between.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemovalCause {
    /// The component is absent from the render entirely -- a chart version
    /// renamed or dropped it (minio -> rustfs). The data must be carried
    /// across, which is what `--migrate-store` does.
    ComponentGone,
    /// The component survives, under a different resource name. Nothing about
    /// the chart changed; the values did.
    RenamedTo(String),
}

/// A live stateful component the target chart would not recreate in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatefulRemoval {
    /// The live resource name, which is what an operator recognises in their
    /// own cluster.
    pub name: String,
    pub component: String,
    pub cause: RemovalCause,
}

/// Live stateful components the target chart would not recreate.
///
/// Pure, so the decision this guard turns on is testable without a cluster.
pub fn removed_stateful_components(
    live: &[(String, String)],
    rendered: &[(String, String)],
) -> Vec<StatefulRemoval> {
    live.iter()
        .filter_map(|(component, name)| {
            let cause = match rendered.iter().find(|(c, _)| c == component) {
                // The component is gone entirely: a rename or removal between
                // chart versions. This is the case the guard was built for.
                None => RemovalCause::ComponentGone,
                // The component survives under a DIFFERENT resource name. Helm
                // deletes the old object and creates the new one, so the
                // StatefulSet's volumes are orphaned and it comes up EMPTY.
                // Just as destructive, and far likelier: any curie.yaml that
                // does not reproduce the release's `nameOverride` renames every
                // object at once.
                //
                // Comparing components alone missed this, and comparing names
                // alone produced a 4-of-4 false positive on a release that
                // merely uses an override (#1323). Neither is the rule; the
                // rule is "same component, same name".
                Some((_, rendered_name)) if rendered_name != name => {
                    RemovalCause::RenamedTo(rendered_name.clone())
                }
                Some(_) => return None,
            };
            Some(StatefulRemoval {
                name: name.clone(),
                component: component.clone(),
                cause,
            })
        })
        .collect()
}

#[cfg(test)]
mod stateful_guard_tests {
    use super::*;

    fn render(component: &str, name: &str) -> String {
        format!(
            "apiVersion: apps/v1\nkind: StatefulSet\nmetadata:\n  name: {name}\nspec:\n  \
             selector:\n    matchLabels:\n      app.kubernetes.io/component: {component}\n"
        )
    }

    fn pair(component: &str, name: &str) -> (String, String) {
        (component.to_string(), name.to_string())
    }

    #[test]
    fn components_and_names_both_come_from_the_render() {
        let rendered = format!(
            "{}\n---\n{}",
            render("rustfs", "acme-bot-curie-rustfs"),
            render("postgres", "acme-bot-curie-postgres")
        );
        assert_eq!(
            parse_statefulset_components(&rendered),
            vec![
                pair("rustfs", "acme-bot-curie-rustfs"),
                pair("postgres", "acme-bot-curie-postgres"),
            ]
        );
    }

    /// `kind: StatefulSet` inside a ConfigMap payload is data, not a component.
    #[test]
    fn a_kind_line_inside_another_document_is_not_a_component() {
        let rendered = "\
apiVersion: v1
kind: ConfigMap
metadata:
  name: acme-bot-docs
data:
  example: |
    kind: StatefulSet
    metadata:
      name: not-a-real-component
";
        assert!(parse_statefulset_components(rendered).is_empty());
    }

    /// The original case: the release runs minio, the chart renders rustfs.
    #[test]
    fn a_renamed_component_is_reported_as_a_removal() {
        let live = vec![
            pair("minio", "acme-bot-minio"),
            pair("postgres", "acme-bot-postgres"),
        ];
        let rendered = vec![
            pair("rustfs", "acme-bot-rustfs"),
            pair("postgres", "acme-bot-postgres"),
        ];
        let removed = removed_stateful_components(&live, &rendered);
        assert_eq!(
            removed,
            vec![StatefulRemoval {
                name: "acme-bot-minio".to_string(),
                component: "minio".to_string(),
                cause: RemovalCause::ComponentGone,
            }]
        );
    }

    /// The case a live `apply --dry-run` exposed, and the reason this rule is
    /// not "same component".
    ///
    /// A curie.yaml that does not reproduce the release's `nameOverride`
    /// renames EVERY object. The component labels are identical either way, so
    /// a component-only comparison sees nothing wrong -- while helm deletes
    /// `acme-bot-postgres` and creates an empty `acme-bot-curie-postgres`
    /// beside the orphaned volume. All four data stores, silently.
    #[test]
    fn the_same_component_under_a_new_name_is_still_a_deletion() {
        let live = vec![
            pair("clickhouse", "acme-bot-clickhouse"),
            pair("postgres", "acme-bot-postgres"),
            pair("rustfs", "acme-bot-rustfs"),
            pair("valkey", "acme-bot-valkey"),
        ];
        // What the chart renders when nameOverride is absent from the file.
        let rendered = vec![
            pair("clickhouse", "acme-bot-curie-clickhouse"),
            pair("postgres", "acme-bot-curie-postgres"),
            pair("rustfs", "acme-bot-curie-rustfs"),
            pair("valkey", "acme-bot-curie-valkey"),
        ];
        let removed = removed_stateful_components(&live, &rendered);
        assert_eq!(
            removed.len(),
            4,
            "every store is deleted and recreated empty; none may be missed"
        );
        // The cause is what steers the operator to `nameOverride` instead of
        // `--migrate-store`, so it is part of the answer, not a detail.
        assert_eq!(
            removed[1].cause,
            RemovalCause::RenamedTo("acme-bot-curie-postgres".to_string())
        );
    }

    /// And the false positive #1323 fixed must not come back: matching names
    /// under matching components is not a removal.
    #[test]
    fn an_unchanged_component_set_is_not_a_removal() {
        let live = vec![pair("postgres", "r-postgres"), pair("minio", "r-minio")];
        let rendered = vec![pair("postgres", "r-postgres"), pair("minio", "r-minio")];
        assert!(removed_stateful_components(&live, &rendered).is_empty());
    }

    #[test]
    fn a_new_component_is_not_a_removal() {
        let live = vec![pair("postgres", "r-postgres")];
        let rendered = vec![
            pair("postgres", "r-postgres"),
            pair("clickhouse", "r-clickhouse"),
        ];
        assert!(removed_stateful_components(&live, &rendered).is_empty());
    }

    /// Source: https://github.com/helm/helm/blob/v3.16.4/pkg/kube/client.go#L452-L455
    /// Helm upgrade honors only the exact lowercase `keep` value.
    #[test]
    fn helm_upgrade_exact_keep_annotation_is_not_at_risk() {
        let kept = serde_json::json!({
            "metadata": {"annotations": {"helm.sh/resource-policy": "keep"}}
        });
        assert!(helm_keeps(&kept), "exact lowercase keep must be accepted");

        for value in ["Keep", "KEEP", " keep "] {
            let kept = serde_json::json!({
                "metadata": {"annotations": {"helm.sh/resource-policy": value}}
            });
            assert!(!helm_keeps(&kept), "{value:?} must not read as exact keep");
        }
    }

    #[test]
    fn other_resource_policies_do_not_count_as_kept() {
        for value in ["delete", "", "keepalive"] {
            let r = serde_json::json!({
                "metadata": {"annotations": {"helm.sh/resource-policy": value}}
            });
            assert!(!helm_keeps(&r), "{value:?} must not read as keep");
        }
        assert!(!helm_keeps(&serde_json::json!({"metadata": {}})));
    }

    #[test]
    fn a_kept_component_is_excluded_from_the_at_risk_list() {
        let list = serde_json::json!({"items": [
            {
                "metadata": {
                    "name": "acme-bot-minio",
                    "annotations": {"helm.sh/resource-policy": "keep"}
                },
                "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "minio"}}}
            },
            {
                "metadata": {"name": "acme-bot-postgres"},
                "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "postgres"}}}
            }
        ]});
        assert_eq!(
            stateful_components_from_list(&list),
            vec![pair("postgres", "acme-bot-postgres")]
        );
    }

    #[test]
    fn an_unannotated_component_is_still_at_risk() {
        let list = serde_json::json!({"items": [
            {
                "metadata": {"name": "acme-bot-minio"},
                "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "minio"}}}
            }
        ]});
        assert_eq!(
            stateful_components_from_list(&list),
            vec![pair("minio", "acme-bot-minio")]
        );
    }
}

/// The user supplied values Helm recorded for a release. `None` means Helm
/// positively reported that the release does not exist. Read failures and
/// malformed values fail closed before callers can plan a mutation or diff.
/// This is the read only half of [`fetch_existing_values`], exposed for
/// `curie diff`.
pub async fn fetch_release_values(o: &CommonOpts) -> Result<Option<serde_json::Value>> {
    fetch_existing_values(o).await
}

/// Transactional upgrade keeps every read on its captured Kubernetes target.
/// Parsing and absent-release semantics stay shared with ordinary installation.
pub(super) async fn fetch_release_values_with_environment(
    o: &CommonOpts,
    environment: Vec<(String, String)>,
) -> Result<Option<serde_json::Value>> {
    fetch_helm_values(
        o,
        helm_get_values_cmd(o).with_env(environment),
        "Helm values",
    )
    .await
}

/// The **computed** values of an existing release: the chart's own defaults
/// merged with whatever the operator overrode.
///
/// That merge is the whole difference from [`fetch_release_values`], which
/// reports only what the operator supplied. An operator who ran `curie cluster
/// up` and never set a model supplied nothing, so helm recorded nothing, and
/// the user-supplied read cannot see the chart default the sandboxes actually
/// boot. `--all` is the only way to observe a default nobody supplied (#1950).
///
/// `None` only when Helm positively reports the release does not exist; read
/// failures, malformed JSON, and non-object/non-null shapes fail closed.
pub async fn fetch_release_computed_values(o: &CommonOpts) -> Result<Option<serde_json::Value>> {
    fetch_helm_values(o, helm_get_all_values_cmd(o), "computed Helm values").await
}

/// Resolve [`GITHUB_TOKEN_KEY`] for this run.
///
/// - `flag` is the `--github-token` value: `None` when the flag and
///   `CURIE_GITHUB_TOKEN` are both unset.
/// - `clear` is `--clear-github-token`.
///
/// Precedence, and why:
/// 1. An operator `--set api.githubToken=` wins outright and we supply nothing,
///    matching every other secret (`operator_set_secret_is_left_to_the_operator`).
///    Passing BOTH is rejected earlier by [`check_github_token_conflict`].
/// 2. An explicit clear removes it.
/// 3. An explicit non-empty value replaces it.
/// 4. Otherwise preserve whatever helm recorded -- so a plain `cluster up`
///    (which does a FULL upgrade and drops anything it does not re-pass) cannot
///    silently reset a working credential to the chart's empty default.
///
/// An EMPTY explicit value is state 4, not state 2. An exported-but-empty
/// `CURIE_GITHUB_TOKEN` is a routine shell accident, and letting an ambiguous
/// signal destroy a live credential is the wrong failure direction; destroying
/// requires the unambiguous `--clear-github-token`.
///
/// Pure and non-interactive, like the resolvers around it.
fn resolve_github_token(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
    flag: Option<&str>,
    clear: bool,
) -> GithubTokenPlan {
    if operator_set_keys(operator_sets).contains(GITHUB_TOKEN_KEY) {
        return GithubTokenPlan::Untouched;
    }
    if clear {
        return GithubTokenPlan::Clear;
    }
    if let Some(value) = flag.filter(|v| !v.is_empty()) {
        return GithubTokenPlan::Set(value.to_string());
    }
    match preserved_value(existing, GITHUB_TOKEN_KEY) {
        Some(current) => GithubTokenPlan::Set(current),
        None => GithubTokenPlan::Untouched,
    }
}

fn complete_up_opts_without_runner_egress(
    mut opts: UpOpts,
    existing: Option<&serde_json::Value>,
    github_token: Option<&str>,
    clear_github_token: bool,
    overlay_live: bool,
) -> Result<UpOpts> {
    let operator_sets = opts.operator_sets();
    opts.retained_mail_values = resolve_retained_mail_values(existing, &operator_sets)?;
    let migrated_owned;
    let existing = if let Some(values) = existing {
        let outcome = crate::config_migrate::migrate_installed_config(values.clone(), None)?;
        stamp_config_schema(&mut opts, &outcome);
        overlay_migration_results(&mut opts, &outcome.values, &operator_sets);
        if overlay_live {
            overlay_live_operator_values(&mut opts, &outcome.values, &operator_sets);
        }
        migrated_owned = Some(outcome.values);
        migrated_owned.as_ref()
    } else {
        stamp_target_schema(&mut opts);
        existing
    };
    resolve_preserved_runner_identity_values(&mut opts, existing, &operator_sets);
    resolve_preserved_gvisor_mode_value(&mut opts, existing, &operator_sets);
    resolve_preserved_worker_extra_env_values(&mut opts, existing, &operator_sets);
    resolve_preserved_slack_trusted_origins_value(&mut opts, existing, &operator_sets);
    if !opts.dev {
        opts.secrets = resolve_generated_secrets(existing, &operator_sets)?;
        opts.secrets.extend(resolve_managed_values_for_up(
            existing,
            &operator_sets,
            opts.common.dry_run,
        ));
    } else {
        // `--dev` keeps the chart's published credential defaults (#195) and
        // must not mint a sealing key, but it is still a FULL helm upgrade:
        // values a sibling verb recorded have to be re-supplied or they reset
        // to the empty chart default (#1134, #1125). Preserve, never invent.
        opts.secrets
            .extend(resolve_preserved_values(existing, &operator_sets));
    }
    opts.github_token =
        resolve_github_token(existing, &operator_sets, github_token, clear_github_token);
    Ok(opts)
}

fn stamp_target_schema(opts: &mut UpOpts) {
    if operator_set_keys(&opts.operator_sets()).contains("config.schemaVersion") {
        return;
    }
    opts.set_string.push(format!(
        "config.schemaVersion={}",
        crate::config_migrate::TARGET_SCHEMA_VERSION
    ));
}

fn stamp_config_schema(opts: &mut UpOpts, outcome: &crate::config_migrate::MigrationOutcome) {
    stamp_target_schema(opts);
    if let Some(from) = &outcome.migrated_from {
        if !operator_set_keys(&opts.operator_sets()).contains("config.migratedFrom") {
            opts.set_string.push(format!("config.migratedFrom={from}"));
        }
    }
}

fn is_external_secret_ref_key(key: &str) -> bool {
    let leaf = key.rsplit('.').next().unwrap_or(key);
    leaf == "existingSecret"
        || leaf.ends_with("ExistingSecret")
        || leaf.ends_with("ExistingSecretKey")
        || leaf == "headersSecretKey"
}

fn is_extra_env_value_key(key: &str) -> bool {
    key.contains(".extraEnv[") && (key.ends_with(".value") || key.contains(".valueFrom"))
}

/// Stamp first-class extraEnv successors and external Secret references from
/// the migrated document. Shared by `cluster up` and `apply` so a legacy
/// extraEnv entry is promoted even when `curie.yaml` does not mention it.
fn overlay_migration_results(
    opts: &mut UpOpts,
    values: &serde_json::Value,
    operator_sets: &[String],
) {
    let overridden = operator_set_keys(operator_sets);
    for (_, helm_key) in crate::config_migrate::extra_env_successors() {
        if overridden.contains(*helm_key)
            || overridden
                .iter()
                .any(|key| key_is_or_descends_from(key, helm_key))
        {
            continue;
        }
        overlay_leaf(opts, values, helm_key, &overridden);
    }
    overlay_secret_refs(opts, values, "", &overridden);
}

/// Overlay remaining live operator values on `cluster up` only. `apply`/`diff`
/// keep `curie.yaml` as whole intent (ADR-0097) and must not copy undeclared
/// live keys into the desired plan.
fn overlay_live_operator_values(
    opts: &mut UpOpts,
    values: &serde_json::Value,
    operator_sets: &[String],
) {
    let overridden = operator_set_keys(operator_sets);
    overlay_json(opts, values, "", &overridden);
}

fn overlay_family_is_managed(key: &str) -> bool {
    key_is_or_descends_from(key, MODEL_CREDENTIAL_KEY)
        || key_is_or_descends_from(key, RUNNER_MODEL_KEY)
        || key_is_or_descends_from(key, FAKE_MODEL_KEY)
        || key_is_or_descends_from(key, GVISOR_MODE_KEY)
        || key_is_or_descends_from(key, ALLOWED_EGRESS_KEY)
        || key_is_or_descends_from(key, SLACK_TRUSTED_ORIGINS_KEY)
        || key_is_or_descends_from(key, WORKER_EXTRA_ENV_KEY)
        || key_is_or_descends_from(key, "api.extraEnv")
        || key_is_or_descends_from(key, "dispatcher.extraEnv")
        || key_is_or_descends_from(key, "agentSandbox.runner.extraEnv")
        || COMMS_MANAGED_KEYS
            .iter()
            .any(|managed| key_is_or_descends_from(key, managed))
        || GITHUB_APP_MANAGED_KEYS
            .iter()
            .any(|managed| key_is_or_descends_from(key, managed))
        || REQUIRED_SECRETS
            .iter()
            .any(|(managed, _)| key_is_or_descends_from(key, managed))
        || crate::sealing::SEALING_MANAGED_KEYS
            .iter()
            .any(|managed| key_is_or_descends_from(key, managed))
        || key_is_or_descends_from(key, GITHUB_TOKEN_KEY)
}

fn overlay_secret_refs(
    opts: &mut UpOpts,
    value: &serde_json::Value,
    prefix: &str,
    overridden: &std::collections::HashSet<String>,
) {
    let Some(map) = value.as_object() else {
        return;
    };
    for (key, child) in map {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        // Retained mail values already own typed preservation and source changes.
        if is_retained_mail_key(&path) {
            continue;
        }
        if is_external_secret_ref_key(&path) && !overridden.contains(&path) {
            match child {
                serde_json::Value::String(raw) if raw.is_empty() => {}
                serde_json::Value::String(raw) => opts
                    .set_string
                    .push(format!("{path}={}", escape_helm_set_string_value(raw))),
                _ => {}
            }
        }
        if child.is_object() {
            overlay_secret_refs(opts, child, &path, overridden);
        }
    }
}

fn overlay_leaf(
    opts: &mut UpOpts,
    root: &serde_json::Value,
    path: &str,
    overridden: &std::collections::HashSet<String>,
) {
    if overridden.contains(path) {
        return;
    }
    let mut cursor = root;
    for part in path.split('.') {
        let Some(next) = cursor.get(part) else {
            return;
        };
        cursor = next;
    }
    match cursor {
        serde_json::Value::String(raw) if raw.is_empty() => {}
        serde_json::Value::String(raw) => opts
            .set_string
            .push(format!("{path}={}", escape_helm_set_string_value(raw))),
        serde_json::Value::Bool(flag) => opts.set.push(format!("{path}={flag}")),
        serde_json::Value::Number(number) => opts.set.push(format!("{path}={number}")),
        _ => {}
    }
}

fn overlay_json(
    opts: &mut UpOpts,
    value: &serde_json::Value,
    prefix: &str,
    overridden: &std::collections::HashSet<String>,
) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                overlay_json(opts, child, &path, overridden);
            }
        }
        serde_json::Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                overlay_json(opts, child, &format!("{prefix}[{index}]"), overridden);
            }
        }
        serde_json::Value::Null => {}
        other => {
            if prefix.is_empty() {
                return;
            }
            if prefix == "config.schemaVersion" || prefix == "config.migratedFrom" {
                return;
            }
            if overridden.contains(prefix)
                || overridden.iter().any(|key| {
                    key_is_or_descends_from(prefix, key) || key_is_or_descends_from(key, prefix)
                })
            {
                return;
            }
            if is_retained_mail_key(prefix) {
                return;
            }
            if overlay_family_is_managed(prefix) && !is_external_secret_ref_key(prefix) {
                return;
            }
            if is_secret_value_key(prefix) && !is_external_secret_ref_key(prefix) {
                return;
            }
            match other {
                serde_json::Value::String(raw) if raw.is_empty() => {}
                serde_json::Value::String(raw) => opts
                    .set_string
                    .push(format!("{prefix}={}", escape_helm_set_string_value(raw))),
                serde_json::Value::Bool(flag) => opts.set.push(format!("{prefix}={flag}")),
                serde_json::Value::Number(number) => opts.set.push(format!("{prefix}={number}")),
                _ => {}
            }
        }
    }
}

/// Finish an already validated up plan with the one live values read and, when
/// requested, resolved provider addresses. This is kept separate from command
/// execution so apply and diff can compare the same completed values.
pub(crate) fn complete_up_opts(
    opts: UpOpts,
    existing: Option<&serde_json::Value>,
    github_token: Option<&str>,
    clear_github_token: bool,
    resolve_provider_egress: bool,
) -> Result<UpOpts> {
    let mut opts = complete_up_opts_without_runner_egress(
        opts,
        existing,
        github_token,
        clear_github_token,
        false,
    )?;
    let operator_sets = opts.operator_sets();
    resolve_preserved_runner_egress_values(&mut opts, existing, &operator_sets, false);
    resolve_provider_egress_for_up(&mut opts, resolve_provider_egress)?;
    Ok(opts)
}

/// Whether an operator `--set` assigns a NON-EMPTY value to
/// [`GITHUB_TOKEN_KEY`], i.e. whether the complete token is riding in argv.
///
/// The pass-through stays legal (it is verbatim by design and breaking it would
/// break existing operators), but a non-empty one leaks into the process table
/// and shell history, so `up` steers the operator to the private input. An EMPTY
/// assignment is the operator clearing the key by hand:
/// nothing leaks, so warning about it would be noise on a correct command.
/// Reads the same [`operator_set_entries`] parse the rest of this file does,
/// since helm accepts the comma-joined `a=1,b=2` form.
///
/// The trims are asymmetric on purpose. Whitespace AROUND an assignment is
/// shell noise (`--set " api.githubToken=x "` still leaks a token), so it is
/// trimmed off both ends; whitespace INSIDE the key is not (`api.githubToken =x`
/// assigns a differently-named helm key, which this credential does not ride
/// on), so a key is compared with its leading noise removed and nothing else.
fn set_passthrough_leaks_github_token(set: &[String]) -> bool {
    operator_set_entries(set)
        .into_iter()
        .any(|(key, value)| key.trim_start() == GITHUB_TOKEN_KEY && !value.trim_end().is_empty())
}

/// Decide which [`REQUIRED_SECRETS`] values `cluster up` supplies, and how.
///
/// - `existing` is `Some(user-supplied values JSON)` when the release already
///   exists (from `helm get values -o json`), `None` on a fresh install.
/// - An operator `--set <key>=...` for a secret always wins: we supply nothing
///   for it.
/// - Fresh install: generate a strong random for every remaining key.
/// - Existing release: re-supply exactly the value helm already recorded for a
///   key (so a `helm upgrade` never rotates a live store's credential -- the
///   chart has no `lookup`-persist yet, that is #195), and never mint a new one
///   for a key helm has no record of (leaving a pre-existing release on
///   whatever it already booted with rather than rotating it out from under a
///   running data store).
///
/// Pure and non-interactive by construction: it never reads a TTY, so a
/// non-interactive / CI `cluster up` cannot hang here.
fn resolve_generated_secrets(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) -> Result<Vec<(String, String)>> {
    let overridden = operator_set_keys(operator_sets);
    let mut resolved = Vec::new();
    for (key, len) in REQUIRED_SECRETS {
        if overridden.contains(*key) {
            continue;
        }
        match existing {
            Some(values) => {
                if let Some(current) = preserved_value(Some(values), key) {
                    resolved.push(((*key).to_string(), current));
                }
            }
            None => resolved.push(((*key).to_string(), random_hex(*len)?)),
        }
    }
    Ok(resolved)
}

/// `helm get values <release> -n <ns> [--all] -o json`. `all` is the only
/// difference between the two wrappers below, and it is load bearing rather
/// than stylistic: without it helm reports only what the operator supplied.
fn helm_get_values_cmd_with(o: &CommonOpts, all: bool) -> OpsCommand {
    let mut args = vec![
        plain("get"),
        plain("values"),
        plain(&o.release),
        plain("-n"),
        plain(&o.namespace),
    ];
    if all {
        args.push(plain("--all"));
    }
    args.push(plain("-o"));
    args.push(plain("json"));
    OpsCommand::new("helm", args)
}

/// `helm get values <release> -n <ns> -o json`: helm's record of the values a
/// prior install supplied. `cluster up` reads it back so an upgrade re-supplies
/// the same generated secrets instead of rotating them.
fn helm_get_values_cmd(o: &CommonOpts) -> OpsCommand {
    helm_get_values_cmd_with(o, false)
}

/// `helm get values <release> -n <ns> --all -o json`: the COMPUTED values --
/// chart defaults merged with the operator's overrides. The sibling above
/// reports only what the operator supplied, which is why `--all` is load
/// bearing here and not a stylistic difference.
fn helm_get_all_values_cmd(o: &CommonOpts) -> OpsCommand {
    helm_get_values_cmd_with(o, true)
}

/// Whether Helm positively reported that the requested release does not exist.
/// Other failures must remain errors because the release state is unknown.
fn helm_release_is_absent(stderr: &str) -> bool {
    failure_reason(stderr) == "Error: release: not found"
}

/// The user supplied values of an existing release, or `None` only when Helm
/// positively reports that the release does not exist. A valid JSON object or
/// `null` is returned as `Some`; failed reads, malformed JSON, and other JSON
/// shapes fail closed. Helm prints `null` for an existing release with no user
/// supplied values.
pub(super) async fn fetch_existing_values(o: &CommonOpts) -> Result<Option<serde_json::Value>> {
    fetch_helm_values(o, helm_get_values_cmd(o), "Helm values").await
}

/// The shared read for both `helm get values` shapes. `what` names the read in
/// the operator facing message ("Helm values" or "computed Helm values") and is
/// the only thing that varies between callers; the absent-release, connectivity
/// vs failure, malformed-JSON and non-object ladders are deliberately single
/// sourced so the fail-closed contract cannot drift between them.
async fn fetch_helm_values(
    o: &CommonOpts,
    cmd: OpsCommand,
    what: &str,
) -> Result<Option<serde_json::Value>> {
    let (ok, out, err) = run_capture(&cmd).await?;
    let fix = format!(
        "verify the release and cluster access with `helm status {} -n {}`, then retry",
        o.release, o.namespace
    );
    if !ok {
        if helm_release_is_absent(&err) {
            return Ok(None);
        }
        let reason = failure_reason(&err);
        let message = format!(
            "could not read {what} for release {} in namespace {}: {reason}",
            o.release, o.namespace
        );
        let error = if is_connectivity_failure(&err) {
            crate::exit::CliError::transient(message)
        } else {
            crate::exit::CliError::failure(message)
        };
        return Err(error.with_fix(fix).into());
    }

    let values: serde_json::Value = serde_json::from_str(out.trim()).map_err(|error| {
        crate::exit::CliError::failure(format!(
            "could not read {what} for release {} in namespace {}: malformed Helm values JSON ({error})",
            o.release, o.namespace
        ))
        .with_fix(fix.clone())
    })?;
    if !values.is_object() && !values.is_null() {
        return Err(crate::exit::CliError::failure(format!(
            "could not read {what} for release {} in namespace {}: malformed Helm values JSON, expected an object or null",
            o.release, o.namespace
        ))
        .with_fix(fix)
        .into());
    }
    Ok(Some(values))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiffParticipation {
    Include,
    Preserve,
}

#[derive(Clone, PartialEq, Eq)]
enum PlannedHelmValues {
    RetainedMail(PrivateHelmValues),
    Set {
        flag: HelmSetFlag,
        expression: String,
        effective: Vec<(String, String)>,
    },
    SecretFile {
        values: Vec<(String, String)>,
        diff: DiffParticipation,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum HelmSetFlag {
    Set,
    SetString,
    SetJson,
}

fn typed_empty_worker_map_override(expression: &str) -> Option<String> {
    let (key, value) = expression.split_once('=')?;
    (key.trim() == "worker.adapterCredentials"
        && (value.is_empty()
            || serde_json::from_str::<serde_json::Value>(value)
                .is_ok_and(|value| value.as_object().is_some_and(|map| map.is_empty()))))
    .then(|| "worker.adapterCredentials={}".to_string())
}

#[derive(Clone, Default, PartialEq, Eq)]
pub(crate) struct UpValuePlan {
    entries: Vec<PlannedHelmValues>,
}

impl UpValuePlan {
    fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        let value = value.into();
        self.entries.push(PlannedHelmValues::Set {
            flag: HelmSetFlag::Set,
            expression: format!("{key}={value}"),
            effective: vec![(key, value)],
        });
    }

    fn set_expression(&mut self, expression: String) {
        let effective = operator_set_entries(std::slice::from_ref(&expression))
            .into_iter()
            .map(|(key, value)| (key.trim().to_string(), value.to_string()))
            .collect();
        let typed = typed_empty_worker_map_override(&expression);
        self.entries.push(PlannedHelmValues::Set {
            flag: if typed.is_some() {
                HelmSetFlag::SetJson
            } else {
                HelmSetFlag::Set
            },
            expression: typed.unwrap_or(expression),
            effective,
        });
    }

    fn set_string_expression(&mut self, expression: String) {
        let effective = if expression.split_once('=').is_some_and(|(key, _)| {
            key_is_or_descends_from(key.trim(), WORKER_EXTRA_ENV_KEY)
                || key_is_or_descends_from(key.trim(), SLACK_TRUSTED_ORIGINS_KEY)
        }) {
            helm_set_string_entries(&expression)
        } else {
            operator_set_entries(std::slice::from_ref(&expression))
                .into_iter()
                .map(|(key, value)| (key.trim().to_string(), value.to_string()))
                .collect()
        };
        let typed = typed_empty_worker_map_override(&expression);
        self.entries.push(PlannedHelmValues::Set {
            flag: if typed.is_some() {
                HelmSetFlag::SetJson
            } else {
                HelmSetFlag::SetString
            },
            expression: typed.unwrap_or(expression),
            effective,
        });
    }

    fn secret_file(&mut self, values: Vec<(String, String)>, diff: DiffParticipation) {
        if !values.is_empty() {
            self.entries
                .push(PlannedHelmValues::SecretFile { values, diff });
        }
    }

    fn append_command_args(&self, args: &mut Vec<CmdArg>) {
        for entry in &self.entries {
            match entry {
                PlannedHelmValues::RetainedMail(values) => {
                    args.push(CmdArg::PrivateJsonValuesFile(values.clone()));
                }
                PlannedHelmValues::Set {
                    flag, expression, ..
                } => {
                    args.push(plain(match flag {
                        HelmSetFlag::Set => "--set",
                        HelmSetFlag::SetString => "--set-string",
                        HelmSetFlag::SetJson => "--set-json",
                    }));
                    args.push(CmdArg::HelmSetExpression(expression.clone()));
                }
                PlannedHelmValues::SecretFile { values, .. } => {
                    args.push(CmdArg::SecretValuesFile(values.clone()));
                }
            }
        }
    }

    pub(crate) fn effective_values(&self) -> BTreeMap<String, String> {
        let mut values = BTreeMap::new();
        for entry in &self.entries {
            match entry {
                PlannedHelmValues::RetainedMail(retained) => {
                    values.extend(
                        retained
                            .1
                            .iter()
                            .map(|(key, value)| (key.clone(), value.clone())),
                    );
                }
                PlannedHelmValues::Set { effective, .. } => {
                    values.extend(effective.iter().cloned());
                }
                PlannedHelmValues::SecretFile {
                    values: secrets,
                    diff: DiffParticipation::Include,
                } => {
                    values.extend(secrets.iter().cloned());
                }
                PlannedHelmValues::SecretFile {
                    diff: DiffParticipation::Preserve,
                    ..
                } => {}
            }
        }
        values
    }
}

/// The ordered chart values supplied by one `cluster up`. Both the Helm command
/// and installation diff consume this representation, so adding a value cannot
/// update one path without updating the other.
pub(crate) fn up_value_plan(o: &UpOpts) -> UpValuePlan {
    let mut plan = UpValuePlan::default();
    if o.dev {
        plan.set(ALLOW_DEV_DEFAULTS_KEY, "true");
    }
    if !o.no_expose {
        plan.set("ui.service.type", "NodePort");
        plan.set("langfuse.web.service.type", "NodePort");
    }
    if let Some(model) = &o.local_model {
        plan.set("inference.deploy", "true");
        plan.set("inference.model", model);
    }
    if let Some(credentials) = &o.credentials {
        plan.set(FAKE_MODEL_KEY, "false");
        plan.secret_file(
            vec![(MODEL_CREDENTIAL_KEY.to_string(), credentials.clone())],
            DiffParticipation::Include,
        );
    }
    match &o.github_token {
        GithubTokenPlan::Untouched => {}
        GithubTokenPlan::Set(token) => {
            plan.secret_file(
                vec![(GITHUB_TOKEN_KEY.to_string(), token.clone())],
                DiffParticipation::Include,
            );
        }
        GithubTokenPlan::Clear => {
            plan.set(GITHUB_TOKEN_KEY, "");
        }
    }
    for (index, cidr) in o
        .resolved_egress_cidrs
        .iter()
        .chain(o.allow_web_egress.iter())
        .enumerate()
    {
        plan.set(
            format!("security.networkPolicy.allowedEgress[{index}].cidr"),
            cidr,
        );
        plan.set(
            format!("security.networkPolicy.allowedEgress[{index}].ports[0].protocol"),
            "TCP",
        );
        plan.set(
            format!("security.networkPolicy.allowedEgress[{index}].ports[0].port"),
            EGRESS_TCP_PORT.to_string(),
        );
    }
    plan.secret_file(o.secrets.clone(), DiffParticipation::Preserve);
    if let Some(values) = &o.retained_mail_values {
        plan.entries
            .push(PlannedHelmValues::RetainedMail(values.clone()));
    }
    if let Some(model) = &o.model {
        if explicit_runner_model(&o.operator_sets()).is_none() {
            plan.set(RUNNER_MODEL_KEY, model);
        }
    }
    for expression in &o.set {
        plan.set_expression(expression.clone());
    }
    // Helm merges `--set-string` after `--set`, while `effective_values` uses
    // insertion order. Keep the declared lane after the typed lane so duplicate
    // keys resolve identically.
    for expression in &o.set_string {
        plan.set_string_expression(expression.clone());
    }
    plan
}

fn gvisor_preflight_job_name_from_render(rendered: &str) -> Result<Option<String>> {
    let mut found = None;
    for document in rendered.split("\n---") {
        let document = document.trim();
        if document.is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_norway::from_str(document)
            .context("could not parse the rendered gVisor preflight Job")?;
        if value.is_null() {
            continue;
        }
        if value.get("kind").and_then(|kind| kind.as_str()) != Some("Job") {
            continue;
        }
        let name = value
            .get("metadata")
            .and_then(|metadata| metadata.get("name"))
            .and_then(|name| name.as_str())
            .filter(|name| !name.is_empty())
            .context("the rendered gVisor preflight Job has no name")?;
        if found.replace(name.to_string()).is_some() {
            bail!("the gVisor preflight template rendered more than one Job");
        }
    }
    Ok(found)
}

async fn rendered_gvisor_preflight_job(
    chart: &str,
    common: &CommonOpts,
    plan: &UpValuePlan,
) -> Result<Option<String>> {
    let mut args = vec![
        plain("template"),
        plain(&common.release),
        plain(chart),
        plain("-n"),
        plain(&common.namespace),
    ];
    plan.append_command_args(&mut args);
    args.push(plain("--show-only"));
    args.push(plain("templates/preflight-gvisor.yaml"));
    let (ok, out, err) = run_capture(&OpsCommand::new("helm", args)).await?;
    if !ok {
        if err.trim() == "Error: could not find template templates/preflight-gvisor.yaml in chart" {
            return Ok(None);
        }
        bail!(
            "could not render the gVisor preflight Job: {}",
            failure_reason(&err)
        );
    }
    gvisor_preflight_job_name_from_render(&out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PriorityClassRole {
    Platform,
    Sandbox,
}

impl PriorityClassRole {
    const ALL: [Self; 2] = [Self::Platform, Self::Sandbox];

    fn key(self) -> &'static str {
        match self {
            Self::Platform => "platform",
            Self::Sandbox => "sandbox",
        }
    }

    fn sibling(self) -> Self {
        match self {
            Self::Platform => Self::Sandbox,
            Self::Sandbox => Self::Platform,
        }
    }
}

const CONTROLLER_DEPLOYMENT_NAME: &str = "agent-sandbox-controller";

const CONTROLLER_DEPLOYMENT_NAMESPACE: &str = "agent-sandbox-system";

const CONTROLLER_DEPLOY_KEY: &str = "agentSandbox.controller.deploy";

const GVISOR_MODE_KEY: &str = "security.gvisor.mode";

/// The chart value that switches every chart-owned credential to its published
/// dev default (`curie.managedSecret` in `charts/curie/templates/_helpers.tpl`).
/// Named once because three call sites are ONE decision and must not drift
/// (#1145): the value `up_value_plan` emits for `--dev`, the recorded-value read
/// [`guard_dev_defaults_flip`] does to decide whether a release is already on
/// dev defaults, and the operator-override membership test that exempts a run
/// the operator has taken ownership of. A literal in any one of them silently
/// desynchronises the guard from what helm actually renders.
const ALLOW_DEV_DEFAULTS_KEY: &str = "security.allowDevDefaults";

#[derive(Debug, Clone, PartialEq, Eq)]
enum ClusterUpInference {
    Provider {
        provider: &'static str,
    },
    PriorityClassReuse {
        role: PriorityClassRole,
        name: String,
        owner_release: String,
    },
    ControllerReuse {
        owner_release: String,
    },
    GvisorOff,
}

impl ClusterUpInference {
    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            Self::Provider { provider } => ui.note(&format!(
                "inferred model provider from the bound credential prefix; applying `--allow-egress-host {provider}`"
            )),
            Self::PriorityClassReuse {
                role,
                name,
                owner_release,
            } => ui.note(&format!(
                "inferred reuse of PriorityClass `{name}` from Helm release `{owner_release}`; applying `--set priorityClasses.{}.create=false`",
                role.key()
            )),
            Self::ControllerReuse { owner_release } => ui.note(&format!(
                "inferred reuse of `{CONTROLLER_DEPLOYMENT_NAME}` from Helm release `{owner_release}`; applying `--set {CONTROLLER_DEPLOY_KEY}=false`"
            )),
            Self::GvisorOff => ui.note(&format!(
                "inferred that the cluster has no `gvisor` RuntimeClass from admission; applying `--set {GVISOR_MODE_KEY}=off`"
            )),
        }
    }
}

fn final_operator_value<'a>(opts: &'a UpOpts, key: &str) -> Option<&'a str> {
    let in_lane = |sets: &'a [String]| {
        operator_set_entries(sets)
            .into_iter()
            .rev()
            .find_map(|(candidate, value)| (candidate.trim() == key).then_some(value.trim()))
    };
    in_lane(&opts.set_string).or_else(|| in_lane(&opts.set))
}

fn detected_provider_from_plan(opts: &UpOpts, plan: &UpValuePlan) -> Option<&'static str> {
    if opts.fake_model || opts.local_model.is_some() {
        return None;
    }
    plan.effective_values()
        .get(MODEL_CREDENTIAL_KEY)
        .and_then(|credential| provider_from_credential_prefix(credential))
}

fn provider_contradiction(opts: &UpOpts, plan: &UpValuePlan) -> Result<()> {
    let Some(provider) = detected_provider_from_plan(opts, plan) else {
        return Ok(());
    };
    if opts.allow_egress_host.is_empty()
        || opts
            .allow_egress_host
            .iter()
            .any(|declared| declared == provider)
    {
        return Ok(());
    }
    let declared = opts
        .allow_egress_host
        .iter()
        .map(|value| format!("--allow-egress-host {value}"))
        .collect::<Vec<_>>()
        .join(" ");
    let fix =
        format!("include `--allow-egress-host {provider}`, or remove the explicit provider list");
    Err(crate::exit::CliError::usage(format!(
        "the bound credential selects provider `{provider}`, but the explicit provider list `{declared}` omits it; {fix}"
    ))
    .with_fix(fix)
    .into())
}

fn reconcile_provider_inference(
    opts: &mut UpOpts,
    plan: &UpValuePlan,
) -> Result<Option<ClusterUpInference>> {
    provider_contradiction(opts, plan)?;
    let Some(provider) = detected_provider_from_plan(opts, plan) else {
        return Ok(None);
    };
    if !opts.allow_egress_host.is_empty() {
        return Ok(None);
    }
    opts.allow_egress_host.push(provider.to_string());
    Ok(Some(ClusterUpInference::Provider { provider }))
}

#[derive(Debug, PartialEq, Eq)]
struct PriorityClassOwner {
    release: String,
    namespace: String,
}

#[derive(Debug, PartialEq, Eq)]
enum PriorityClassOwnership {
    Absent,
    Existing(Option<PriorityClassOwner>),
}

#[derive(Debug, PartialEq, Eq)]
struct PriorityClassConflict {
    role: PriorityClassRole,
    name: String,
    owner: Option<PriorityClassOwner>,
}

fn priority_class_name_from_render(
    rendered: &str,
    role: PriorityClassRole,
) -> Result<Option<String>> {
    let mut found = None;
    for document in rendered.split("\n---") {
        let document = document.trim();
        if document.is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_norway::from_str(document).with_context(|| {
            format!(
                "could not parse the rendered PriorityClass for role {}",
                role.key()
            )
        })?;
        if value.is_null() {
            continue;
        }
        if value.get("kind").and_then(|kind| kind.as_str()) != Some("PriorityClass") {
            bail!(
                "the PriorityClass template rendered an unexpected object for role {}",
                role.key()
            );
        }
        let name = value
            .get("metadata")
            .and_then(|metadata| metadata.get("name"))
            .and_then(|name| name.as_str())
            .filter(|name| !name.is_empty())
            .with_context(|| {
                format!(
                    "the rendered PriorityClass for role {} has no name",
                    role.key()
                )
            })?;
        if found.replace(name.to_string()).is_some() {
            bail!(
                "the PriorityClass template rendered more than one object for role {}",
                role.key()
            );
        }
    }
    Ok(found)
}

async fn rendered_priority_classes(
    chart: &str,
    common: &CommonOpts,
    plan: &UpValuePlan,
) -> Result<Vec<(PriorityClassRole, String)>> {
    let mut rendered = Vec::new();
    for role in PriorityClassRole::ALL {
        let mut args = vec![
            plain("template"),
            plain(&common.release),
            plain(chart),
            plain("-n"),
            plain(&common.namespace),
        ];
        plan.append_command_args(&mut args);
        args.push(plain("--show-only"));
        args.push(plain("templates/priorityclass.yaml"));
        args.push(plain("--set"));
        args.push(plain(format!(
            "priorityClasses.{}.create=false",
            role.sibling().key()
        )));
        let (ok, out, err) = run_capture(&OpsCommand::new("helm", args)).await?;
        if !ok {
            if err.trim() == "Error: could not find template templates/priorityclass.yaml in chart"
            {
                continue;
            }
            bail!(
                "could not render the PriorityClass for role {}: {}",
                role.key(),
                failure_reason(&err)
            );
        }
        if let Some(name) = priority_class_name_from_render(&out, role)? {
            rendered.push((role, name));
        }
    }
    Ok(rendered)
}

fn priority_class_read_error(
    name: &str,
    detail: impl std::fmt::Display,
    transient: bool,
) -> anyhow::Error {
    let fix = "run `curie cluster status`".to_string();
    let message = format!("could not inspect PriorityClass `{name}`: {detail}; {fix}");
    let error = if transient {
        crate::exit::CliError::transient(message)
    } else {
        crate::exit::CliError::failure(message)
    };
    error.with_fix(fix).into()
}

fn priority_class_metadata_map<'a>(
    metadata: &'a serde_json::Map<String, serde_json::Value>,
    field: &str,
    name: &str,
) -> Result<Option<&'a serde_json::Map<String, serde_json::Value>>> {
    match metadata.get(field) {
        None => Ok(None),
        Some(serde_json::Value::Object(values)) => Ok(Some(values)),
        Some(_) => Err(priority_class_read_error(
            name,
            format!("kubectl returned invalid object JSON with nonobject metadata.{field}"),
            false,
        )),
    }
}

fn priority_class_metadata_value<'a>(
    values: Option<&'a serde_json::Map<String, serde_json::Value>>,
    key: &str,
    name: &str,
) -> Result<Option<&'a str>> {
    match values.and_then(|values| values.get(key)) {
        None => Ok(None),
        Some(serde_json::Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(priority_class_read_error(
            name,
            format!("kubectl returned invalid object JSON at metadata key `{key}`"),
            false,
        )),
    }
}

async fn priority_class_owner(name: &str) -> Result<PriorityClassOwnership> {
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("priorityclass"),
            plain(name),
            plain("--ignore-not-found"),
            plain("-o"),
            plain("json"),
        ],
    );
    let (ok, out, err) = run_capture(&cmd).await?;
    if !ok {
        return Err(priority_class_read_error(
            name,
            failure_reason(&err),
            is_connectivity_failure(&err),
        ));
    }
    if out.trim().is_empty() {
        return Ok(PriorityClassOwnership::Absent);
    }
    let value: serde_json::Value = serde_json::from_str(out.trim())
        .map_err(|_| priority_class_read_error(name, "kubectl returned invalid JSON", false))?;
    let object = value.as_object().ok_or_else(|| {
        priority_class_read_error(name, "kubectl returned invalid object JSON", false)
    })?;
    let metadata = object
        .get("metadata")
        .and_then(|metadata| metadata.as_object())
        .ok_or_else(|| {
            priority_class_read_error(
                name,
                "kubectl returned invalid object JSON without metadata",
                false,
            )
        })?;
    if metadata.get("name").and_then(|value| value.as_str()) != Some(name) {
        return Err(priority_class_read_error(
            name,
            "kubectl returned invalid object JSON with another metadata.name",
            false,
        ));
    }

    let labels = priority_class_metadata_map(metadata, "labels", name)?;
    if priority_class_metadata_value(labels, "app.kubernetes.io/managed-by", name)? != Some("Helm")
    {
        return Ok(PriorityClassOwnership::Existing(None));
    }
    let annotations = priority_class_metadata_map(metadata, "annotations", name)?;
    let Some(release) =
        priority_class_metadata_value(annotations, "meta.helm.sh/release-name", name)?
    else {
        return Ok(PriorityClassOwnership::Existing(None));
    };
    let Some(namespace) =
        priority_class_metadata_value(annotations, "meta.helm.sh/release-namespace", name)?
    else {
        return Ok(PriorityClassOwnership::Existing(None));
    };
    Ok(PriorityClassOwnership::Existing(Some(PriorityClassOwner {
        release: release.to_string(),
        namespace: namespace.to_string(),
    })))
}

async fn priority_class_observations(
    opts: &UpOpts,
    plan: &UpValuePlan,
) -> Result<Vec<PriorityClassConflict>> {
    let rendered = rendered_priority_classes(&opts.chart, &opts.common, plan).await?;
    let mut observations = Vec::new();
    for (role, name) in rendered {
        let owner = match priority_class_owner(&name).await? {
            PriorityClassOwnership::Absent => continue,
            PriorityClassOwnership::Existing(owner) => owner,
        };
        observations.push(PriorityClassConflict { role, name, owner });
    }
    Ok(observations)
}

fn priority_class_conflict_error(conflicts: Vec<PriorityClassConflict>) -> anyhow::Error {
    let mut message = String::from("PriorityClass ownership conflicts block installation:");
    for conflict in conflicts {
        if let Some(owner) = conflict.owner {
            message.push_str(&format!(
                "\nPriorityClass `{}` is owned by Helm release `{}` in namespace `{}`.",
                conflict.name, owner.release, owner.namespace
            ));
        } else {
            message.push_str(&format!(
                "\nPriorityClass `{}` exists without complete Helm ownership metadata.",
                conflict.name
            ));
        }
        message.push_str(&format!(
            "\nReuse it with `--set priorityClasses.{}.create=false --set priorityClasses.{}.name={}`.",
            conflict.role.key(),
            conflict.role.key(),
            conflict.name
        ));
        message.push_str(&format!(
            "\nKeep creation enabled with `--set priorityClasses.{}.name=<different-name>`.",
            conflict.role.key()
        ));
    }
    crate::exit::CliError::failure(message).into()
}

async fn preflight_priority_class_ownership(opts: &UpOpts, plan: &UpValuePlan) -> Result<()> {
    let mut conflicts = Vec::new();
    for observation in priority_class_observations(opts, plan).await? {
        let PriorityClassConflict { role, name, owner } = observation;
        let conflicts_with_target = match owner.as_ref() {
            None => true,
            Some(owner) => {
                owner.release != opts.common.release || owner.namespace != opts.common.namespace
            }
        };
        if conflicts_with_target {
            conflicts.push(PriorityClassConflict { role, name, owner });
        }
    }
    if conflicts.is_empty() {
        return Ok(());
    }

    Err(priority_class_conflict_error(conflicts))
}

async fn reconcile_priority_class_ownership(
    opts: &UpOpts,
    plan: &mut UpValuePlan,
) -> Result<Vec<ClusterUpInference>> {
    let mut conflicts = Vec::new();
    let mut inferred = Vec::new();
    for observation in priority_class_observations(opts, plan).await? {
        let PriorityClassConflict { role, name, owner } = observation;
        let Some(owner) = owner else {
            conflicts.push(PriorityClassConflict {
                role,
                name,
                owner: None,
            });
            continue;
        };
        if owner.release == opts.common.release && owner.namespace == opts.common.namespace {
            continue;
        }
        let key = format!("priorityClasses.{}.create", role.key());
        match final_operator_value(opts, &key) {
            Some("true") => {
                let assignment = format!("{key}=true");
                let fix = format!("remove `--set {assignment}`, or pass `--set {key}=false`");
                return Err(crate::exit::CliError::usage(format!(
                    "PriorityClass `{name}` is owned by Helm release `{}` in namespace `{}`, which contradicts explicit `{assignment}`; {fix}",
                    owner.release, owner.namespace
                ))
                .with_fix(fix)
                .into());
            }
            Some(_) => {}
            None => inferred.push(ClusterUpInference::PriorityClassReuse {
                role,
                name,
                owner_release: owner.release,
            }),
        }
    }
    if !conflicts.is_empty() {
        return Err(priority_class_conflict_error(conflicts));
    }
    for inference in &inferred {
        if let ClusterUpInference::PriorityClassReuse { role, .. } = inference {
            plan.set(format!("priorityClasses.{}.create", role.key()), "false");
        }
    }
    Ok(inferred)
}

#[derive(Debug, PartialEq, Eq)]
enum ControllerOwnership {
    Absent,
    Existing(Option<PriorityClassOwner>),
}

fn controller_read_error(detail: impl std::fmt::Display, transient: bool) -> anyhow::Error {
    let fix = "run `curie cluster status`".to_string();
    let message = format!(
        "could not inspect Deployment `{CONTROLLER_DEPLOYMENT_NAME}` in namespace `{CONTROLLER_DEPLOYMENT_NAMESPACE}`: {detail}; {fix}"
    );
    let error = if transient {
        crate::exit::CliError::transient(message)
    } else {
        crate::exit::CliError::failure(message)
    };
    error.with_fix(fix).into()
}

fn controller_metadata_map<'a>(
    metadata: &'a serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<&'a serde_json::Map<String, serde_json::Value>>> {
    match metadata.get(field) {
        None => Ok(None),
        Some(serde_json::Value::Object(values)) => Ok(Some(values)),
        Some(_) => Err(controller_read_error(
            format!("kubectl returned invalid object JSON with nonobject metadata.{field}"),
            false,
        )),
    }
}

fn controller_metadata_value<'a>(
    values: Option<&'a serde_json::Map<String, serde_json::Value>>,
    key: &str,
) -> Result<Option<&'a str>> {
    match values.and_then(|values| values.get(key)) {
        None => Ok(None),
        Some(serde_json::Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(controller_read_error(
            format!("kubectl returned invalid object JSON at metadata key `{key}`"),
            false,
        )),
    }
}

async fn controller_owner() -> Result<ControllerOwnership> {
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("deployment"),
            plain(CONTROLLER_DEPLOYMENT_NAME),
            plain("-n"),
            plain(CONTROLLER_DEPLOYMENT_NAMESPACE),
            plain("--ignore-not-found"),
            plain("-o"),
            plain("json"),
        ],
    );
    let (ok, out, err) = run_capture(&cmd).await?;
    if !ok {
        let missing_namespace = err.contains("NotFound")
            && err.contains("namespaces")
            && err.contains(CONTROLLER_DEPLOYMENT_NAMESPACE);
        if missing_namespace {
            return Ok(ControllerOwnership::Absent);
        }
        return Err(controller_read_error(
            failure_reason(&err),
            is_connectivity_failure(&err),
        ));
    }
    if out.trim().is_empty() {
        return Ok(ControllerOwnership::Absent);
    }
    let value: serde_json::Value = serde_json::from_str(out.trim())
        .map_err(|_| controller_read_error("kubectl returned invalid JSON", false))?;
    let object = value
        .as_object()
        .ok_or_else(|| controller_read_error("kubectl returned invalid object JSON", false))?;
    if object.get("kind").and_then(|value| value.as_str()) != Some("Deployment") {
        return Err(controller_read_error(
            "kubectl returned an object that is not a Deployment",
            false,
        ));
    }
    let metadata = object
        .get("metadata")
        .and_then(|metadata| metadata.as_object())
        .ok_or_else(|| {
            controller_read_error(
                "kubectl returned invalid object JSON without metadata",
                false,
            )
        })?;
    if metadata.get("name").and_then(|value| value.as_str()) != Some(CONTROLLER_DEPLOYMENT_NAME) {
        return Err(controller_read_error(
            "kubectl returned invalid object JSON with another metadata.name",
            false,
        ));
    }
    let labels = controller_metadata_map(metadata, "labels")?;
    if controller_metadata_value(labels, "app.kubernetes.io/managed-by")? != Some("Helm") {
        return Ok(ControllerOwnership::Existing(None));
    }
    let annotations = controller_metadata_map(metadata, "annotations")?;
    let Some(release) = controller_metadata_value(annotations, "meta.helm.sh/release-name")? else {
        return Ok(ControllerOwnership::Existing(None));
    };
    let Some(namespace) = controller_metadata_value(annotations, "meta.helm.sh/release-namespace")?
    else {
        return Ok(ControllerOwnership::Existing(None));
    };
    Ok(ControllerOwnership::Existing(Some(PriorityClassOwner {
        release: release.to_string(),
        namespace: namespace.to_string(),
    })))
}

async fn reconcile_controller_ownership(
    opts: &UpOpts,
    plan: &mut UpValuePlan,
) -> Result<Option<ClusterUpInference>> {
    let explicit = final_operator_value(opts, CONTROLLER_DEPLOY_KEY);
    if explicit == Some("false") {
        return Ok(None);
    }
    let owner = match controller_owner().await? {
        ControllerOwnership::Absent => return Ok(None),
        ControllerOwnership::Existing(Some(owner)) => owner,
        ControllerOwnership::Existing(None) => {
            return Err(controller_read_error(
                "the Deployment exists without complete Helm ownership metadata",
                false,
            ));
        }
    };
    if owner.release == opts.common.release && owner.namespace == opts.common.namespace {
        return Ok(None);
    }
    if explicit == Some("true") {
        let assignment = format!("{CONTROLLER_DEPLOY_KEY}=true");
        let fix =
            format!("remove `--set {assignment}`, or pass `--set {CONTROLLER_DEPLOY_KEY}=false`");
        return Err(crate::exit::CliError::usage(format!(
            "Deployment `{CONTROLLER_DEPLOYMENT_NAME}` is owned by Helm release `{}` in namespace `{}`, which contradicts explicit `{assignment}`; {fix}",
            owner.release, owner.namespace
        ))
        .with_fix(fix)
        .into());
    }
    if explicit.is_some() {
        return Ok(None);
    }
    plan.set(CONTROLLER_DEPLOY_KEY, "false");
    Ok(Some(ClusterUpInference::ControllerReuse {
        owner_release: owner.release,
    }))
}

fn up_commands_with_plan(o: &UpOpts, plan: &UpValuePlan) -> Vec<OpsCommand> {
    let mut args = vec![
        plain("upgrade"),
        plain("--install"),
        plain(&o.common.release),
        plain(&o.chart),
        plain("-n"),
        plain(&o.common.namespace),
        plain("--create-namespace"),
    ];
    plan.append_command_args(&mut args);
    vec![OpsCommand::new("helm", args)]
}

/// `helm upgrade --install` for the release. Its chart values are rendered from
/// [`up_value_plan`], which is also the source of the installation diff.
pub fn up_commands(o: &UpOpts) -> Vec<OpsCommand> {
    up_commands_with_plan(o, &up_value_plan(o))
}

struct RunningInstall {
    child: Child,
    stdout: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
    stderr: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
    _secret_files: Vec<SecretValuesFileGuard>,
    program: String,
}

impl RunningInstall {
    fn spawn(cmd: &OpsCommand) -> Result<Self> {
        let (cmd, secret_files) = cmd.materialize_secret_files()?;
        let mut child = Command::new(&cmd.program)
            .args(cmd.argv())
            .envs(cmd.env.iter().chain(cmd.secret_env.iter()).cloned())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("failed to invoke `{}`; is it on PATH?", cmd.program))?;
        let mut stdout = child
            .stdout
            .take()
            .expect("piped install stdout must be available");
        let mut stderr = child
            .stderr
            .take()
            .expect("piped install stderr must be available");
        let stdout = tokio::spawn(async move {
            let mut output = Vec::new();
            stdout.read_to_end(&mut output).await?;
            Ok(output)
        });
        let stderr = tokio::spawn(async move {
            let mut output = Vec::new();
            stderr.read_to_end(&mut output).await?;
            Ok(output)
        });
        Ok(Self {
            child,
            stdout,
            stderr,
            _secret_files: secret_files,
            program: cmd.program,
        })
    }

    async fn finish(
        mut self,
        status: std::io::Result<std::process::ExitStatus>,
    ) -> Result<(bool, String, String)> {
        let status = match status {
            Ok(status) => status,
            Err(error) => {
                let _ = terminate_process(&mut self.child).await;
                return Err(error).with_context(|| {
                    format!("failed to invoke `{}`; is it on PATH?", self.program)
                });
            }
        };
        let stdout = self
            .stdout
            .await
            .context("joining the Helm stdout reader")??;
        let stderr = self
            .stderr
            .await
            .context("joining the Helm stderr reader")??;
        Ok((
            status.success(),
            String::from_utf8_lossy(&stdout).to_string(),
            String::from_utf8_lossy(&stderr).to_string(),
        ))
    }

    async fn terminate(mut self) {
        let _ = terminate_helm_process(&mut self.child).await;
        self.stdout.abort();
        self.stderr.abort();
        let _ = self.stdout.await;
        let _ = self.stderr.await;
    }
}

struct RunningGvisorEventWatch {
    child: Child,
    stdout: tokio::io::Lines<BufReader<ChildStdout>>,
    existing_event_uids: BTreeSet<String>,
}

fn gvisor_event_selector(namespace: &str, job: &str) -> String {
    format!(
        "involvedObject.kind=Job,involvedObject.namespace={namespace},involvedObject.name={job},reason=FailedCreate"
    )
}

fn gvisor_event_watch_cmd(namespace: &str, job: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("events"),
            plain("-n"),
            plain(namespace),
            plain("--field-selector"),
            plain(gvisor_event_selector(namespace, job)),
            plain("--watch"),
            plain("--output-watch-events"),
            plain("-o"),
            plain(
                r#"jsonpath={.type}{"\u001f"}{.object.metadata.uid}{"\u001f"}{.object.involvedObject.kind}{"\u001f"}{.object.involvedObject.namespace}{"\u001f"}{.object.involvedObject.name}{"\u001f"}{.object.reason}{"\u001f"}{.object.message}{"\n"}"#,
            ),
        ],
    )
}

fn gvisor_existing_event_uids_cmd(namespace: &str, job: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("events"),
            plain("-n"),
            plain(namespace),
            plain("--field-selector"),
            plain(gvisor_event_selector(namespace, job)),
            plain("-o"),
            plain(r#"jsonpath={range .items[*]}{.metadata.uid}{"\n"}{end}"#),
        ],
    )
}

enum GvisorEventWatchStart {
    Watching(Box<RunningGvisorEventWatch>),
    Unavailable,
}

enum GvisorEventWatchLine {
    RuntimeClassRejected(String),
    Ignore,
}

async fn gvisor_existing_event_uids(namespace: &str, job: &str) -> Option<BTreeSet<String>> {
    let ui = crate::ui::ui();
    let snapshot = gvisor_existing_event_uids_cmd(namespace, job);
    ui.plumbing(&format!("+ {}", snapshot.display()));
    match run_capture(&snapshot).await {
        Ok((true, out, _)) => Some(
            out.lines()
                .map(str::trim)
                .filter(|uid| !uid.is_empty())
                .map(str::to_string)
                .collect(),
        ),
        Ok((false, out, err)) => {
            let detail = [err.trim(), out.trim()]
                .into_iter()
                .find(|detail| !detail.is_empty())
                .unwrap_or("kubectl exited nonzero with no output");
            ui.plumbing(&format!("gVisor event snapshot unavailable: {detail}"));
            None
        }
        Err(error) => {
            ui.plumbing(&format!("gVisor event snapshot unavailable: {error}"));
            None
        }
    }
}

fn start_gvisor_event_watch(
    namespace: &str,
    job: &str,
    existing_event_uids: BTreeSet<String>,
) -> GvisorEventWatchStart {
    let ui = crate::ui::ui();
    let watch = gvisor_event_watch_cmd(namespace, job);
    ui.plumbing(&format!("+ {}", watch.display()));
    let mut child = match Command::new(&watch.program)
        .args(watch.argv())
        .envs(watch.env.iter().chain(watch.secret_env.iter()).cloned())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            ui.plumbing(&format!("gVisor event watch unavailable: {error}"));
            return GvisorEventWatchStart::Unavailable;
        }
    };
    let stdout = child
        .stdout
        .take()
        .expect("piped kubectl event stdout must be available");
    GvisorEventWatchStart::Watching(Box::new(RunningGvisorEventWatch {
        child,
        stdout: BufReader::new(stdout).lines(),
        existing_event_uids,
    }))
}

fn gvisor_event_watch_line(
    line: &str,
    namespace: &str,
    job: &str,
    existing_event_uids: &BTreeSet<String>,
) -> GvisorEventWatchLine {
    let mut fields = line.splitn(7, '\u{001f}');
    let source = fields.next().unwrap_or_default();
    let uid = fields.next().unwrap_or_default();
    let inspect = match source {
        "ADDED" | "MODIFIED" => !existing_event_uids.contains(uid),
        _ => false,
    };
    if uid.is_empty() || !inspect {
        return GvisorEventWatchLine::Ignore;
    }
    let kind = fields.next().unwrap_or_default();
    let event_namespace = fields.next().unwrap_or_default();
    let name = fields.next().unwrap_or_default();
    let reason = fields.next().unwrap_or_default();
    let message = fields.next().unwrap_or_default();
    let missing_runtimeclass = message.contains("RuntimeClass \"gvisor\" not found");
    if kind == "Job"
        && event_namespace == namespace
        && name == job
        && reason == "FailedCreate"
        && missing_runtimeclass
    {
        GvisorEventWatchLine::RuntimeClassRejected(message.to_string())
    } else {
        GvisorEventWatchLine::Ignore
    }
}

async fn terminate_process(child: &mut Child) -> std::io::Result<std::process::ExitStatus> {
    if let Ok(Some(status)) = child.try_wait() {
        return Ok(status);
    }
    let _ = child.start_kill();
    child.wait().await
}

/// Give Helm its interrupt path so it can mark the release failed before a
/// bounded forced cleanup. A pending install would block the printed recovery.
async fn terminate_helm_process(child: &mut Child) -> std::io::Result<std::process::ExitStatus> {
    if let Ok(Some(status)) = child.try_wait() {
        return Ok(status);
    }
    let interrupted = match child.id() {
        Some(pid) => Command::new("kill")
            .arg("-INT")
            .arg(pid.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .is_ok_and(|status| status.success()),
        None => false,
    };
    if interrupted {
        if let Ok(status) = tokio::time::timeout(Duration::from_secs(2), child.wait()).await {
            return status;
        }
    }
    terminate_process(child).await
}

enum GvisorInstallRace {
    Helm(std::io::Result<std::process::ExitStatus>),
    RuntimeClassRejected(String),
}

enum GvisorInstallOutcome {
    Installed,
    RuntimeClassRejected {
        rejection: String,
        step: crate::ui::Step,
    },
}

async fn run_install_with_gvisor_observer(
    cl: &crate::ui::Checklist,
    label: &str,
    ok_detail: &str,
    cmd: &OpsCommand,
    namespace: &str,
    job: &str,
    namespace_existed_before_install: bool,
) -> Result<GvisorInstallOutcome> {
    let ui = crate::ui::ui();
    ui.plumbing(&format!("+ {}", cmd.display()));
    let step = cl.step(label);
    let mut watch_start = if namespace_existed_before_install {
        Some(match gvisor_existing_event_uids(namespace, job).await {
            Some(existing_event_uids) => {
                start_gvisor_event_watch(namespace, job, existing_event_uids)
            }
            None => GvisorEventWatchStart::Unavailable,
        })
    } else {
        None
    };
    let mut install = match RunningInstall::spawn(cmd) {
        Ok(install) => install,
        Err(error) => {
            if let Some(GvisorEventWatchStart::Watching(watch)) = &mut watch_start {
                let _ = terminate_process(&mut watch.child).await;
            }
            step.fail("failed");
            return Err(error);
        }
    };

    let mut early_helm_status = None;
    if !namespace_existed_before_install {
        // Helm owns `--create-namespace`. Wait for that one object, then use a
        // single list and watch request that cannot lose an Event between calls.
        let mut retry_delay = Duration::from_millis(50);
        loop {
            match namespace_exists(namespace).await {
                Ok(true) => {
                    watch_start = Some(start_gvisor_event_watch(namespace, job, BTreeSet::new()));
                    break;
                }
                Ok(false) => {}
                Err(error) => {
                    ui.plumbing(&format!(
                        "gVisor event watch unavailable while waiting for namespace: {error:#}"
                    ));
                    watch_start = Some(GvisorEventWatchStart::Unavailable);
                    break;
                }
            }
            match install.child.try_wait() {
                Ok(Some(status)) => {
                    early_helm_status = Some(Ok(status));
                    break;
                }
                Err(error) => {
                    early_helm_status = Some(Err(error));
                    break;
                }
                Ok(None) => {}
            }
            tokio::time::sleep(retry_delay).await;
            retry_delay = retry_delay
                .saturating_mul(2)
                .min(Duration::from_millis(500));
        }
    }

    let watch_start = watch_start.unwrap_or(GvisorEventWatchStart::Unavailable);
    let mut watch = None;
    let race = if let Some(status) = early_helm_status {
        GvisorInstallRace::Helm(status)
    } else {
        match watch_start {
            GvisorEventWatchStart::Watching(running_watch) => {
                watch = Some(*running_watch);
                let running_watch = watch.as_mut().expect("watch was just installed");
                let existing_event_uids = running_watch.existing_event_uids.clone();
                loop {
                    let outcome = tokio::select! {
                        status = install.child.wait() => Some(GvisorInstallRace::Helm(status)),
                        line = running_watch.stdout.next_line() => {
                            match line {
                                Ok(Some(line)) => match gvisor_event_watch_line(
                                    &line,
                                    namespace,
                                    job,
                                    &existing_event_uids,
                                ) {
                                    GvisorEventWatchLine::RuntimeClassRejected(rejection) => {
                                        Some(GvisorInstallRace::RuntimeClassRejected(rejection))
                                    }
                                    GvisorEventWatchLine::Ignore => None,
                                },
                                Ok(None) | Err(_) => {
                                    let _ = terminate_process(&mut running_watch.child).await;
                                    Some(GvisorInstallRace::Helm(install.child.wait().await))
                                }
                            }
                        }
                    };
                    if let Some(outcome) = outcome {
                        break outcome;
                    }
                }
            }
            GvisorEventWatchStart::Unavailable => {
                GvisorInstallRace::Helm(install.child.wait().await)
            }
        }
    };

    match race {
        GvisorInstallRace::Helm(status) => {
            if let Some(watch) = watch.as_mut() {
                let _ = terminate_process(&mut watch.child).await;
            }
            let captured = install.finish(status).await;
            match captured {
                Ok((ok, out, err)) => {
                    finish_captured_step(step, ok_detail, cmd, ok, out, err)?;
                    Ok(GvisorInstallOutcome::Installed)
                }
                Err(error) => {
                    step.fail("failed");
                    Err(error)
                }
            }
        }
        GvisorInstallRace::RuntimeClassRejected(rejection) => {
            if let Some(watch) = watch.as_mut() {
                let _ = terminate_process(&mut watch.child).await;
            }
            install.terminate().await;
            Ok(GvisorInstallOutcome::RuntimeClassRejected { rejection, step })
        }
    }
}

// ---------------------------------------------------------------------------
// Verb handlers
// ---------------------------------------------------------------------------

/// Output of `cluster up`: the dry-run plan, or the installed release. `--json`
/// emits a JSON object; the real path formerly ended in `ui.payload` (suppressed
/// under `--json`, #485).
#[derive(Debug)]
pub enum ClusterUpOutput {
    DryRun(crate::ui::DryRunPlan),
    Up { namespace: String, release: String },
}

impl crate::ui::CliOutput for ClusterUpOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ClusterUpOutput::DryRun(plan) => plan.to_json(),
            ClusterUpOutput::Up { namespace, release } => serde_json::json!({
                "status": "up",
                "namespace": namespace,
                "release": release,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ClusterUpOutput::DryRun(plan) => plan.render(ui),
            ClusterUpOutput::Up { .. } => {
                ui.payload("curie is up");
                ui.note("Run `curie cluster status` for pod health and URLs.");
            }
        }
    }
}

/// Whether `up()` should read the release's existing helm values before
/// resolving secrets and the GitHub credential.
///
/// Deliberately does NOT depend on `dev`: a `cluster up --dev` must still
/// preserve whatever credential (e.g. the GitHub token, #1124) a real release
/// already recorded -- `--dev` only governs the chart's dev-default secret
/// values, not whether the read happens. Re-adding a `dev` term here is the
/// specific regression this function exists to make visible. `dry_run` is the
/// only thing that skips the read, since `--dry-run` stays fully offline and
/// never touches helm.
fn should_read_existing(dev: bool, dry_run: bool) -> bool {
    let _ = dev;
    !dry_run
}

/// Refuse a `--dev` run that would flip an already-installed release onto the
/// chart's published dev defaults (#1145).
///
/// `curie.managedSecret`'s dev-defaults branch short-circuits *ahead of* its
/// `hasKey .existingData` preservation branch, so pointing an existing sealed
/// release at dev defaults rewrites every chart-owned credential in the release
/// Secret while the PVC-backed Postgres and RustFS data keeps the originals.
/// That leaves the install unbootable until those PVCs are wiped -- a state the
/// operator cannot back out of -- so the run is refused up front rather than
/// diagnosed afterwards.
///
/// The decision, in order:
///
/// 1. `dev == false` -> always allowed. A plain `up` over a *dev* release is
///    safe by construction: [`resolve_generated_secrets`] re-supplies only what
///    the release already recorded and never mints a value for an unrecorded
///    key, so `managedSecret` sees `value == default` and preserves. If that
///    ever became mint-on-missing, this arm has to change with it.
/// 2. `existing.is_none()` -> allowed. Either helm positively reported
///    "release: not found" (the fresh install `--dev` exists for), or the read
///    was skipped. `--dry-run` is that second case -- `should_read_existing`
///    returns `false` for it -- so a `--dev --dry-run` plan is never refused:
///    it reaches no helm invocation and so has no release to damage.
/// 3. The release already records dev defaults -> allowed. That is the
///    idempotent `--dev` re-run, and also the retry after a `--dev` install
///    that failed partway, since helm records a failed install's values.
/// 4. The operator explicitly supplied [`ALLOW_DEV_DEFAULTS_KEY`] through
///    `--set` or `--set-string` -> allowed. `up_value_plan` emits the CLI's own
///    `=true` FIRST and appends the operator's expressions AFTER it, and helm is
///    last-wins, so the operator's value is the one the chart actually renders:
///    `--dev --set security.allowDevDefaults=false` renders the flag OFF and
///    preserves the recorded credentials. Honouring it is therefore not a
///    bypass, it is reading the same effective value the chart will. Kept
///    DELIBERATELY NARROW: membership of that EXACT dotted key, never a
///    substring and never "some override was passed" -- almost every real
///    `cluster up` carries unrelated overrides, so the wider spelling would
///    disable the guard outright.
/// 5. Anything else -> refused, Usage class (exit 2), a deterministic input
///    error rather than a runtime failure. The truthiness read fails closed, so
///    an unrecognised recorded shape refuses rather than waves through.
///
/// Arm 4 sits ahead of the refusal because it decides what helm renders, not
/// what the CLI intended. It also subsumes `--set security.allowDevDefaults=true`
/// staying unguarded on purpose: that is the documented verbatim escape hatch,
/// and this file's standing rule is that an operator `--set` always wins. Do not
/// "close the gap" by guarding it. It is kept out of the operator-facing message
/// and fix hint too, because against an existing release it is destructive
/// advice.
fn guard_dev_defaults_flip(
    dev: bool,
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) -> Result<()> {
    if !dev {
        return Ok(());
    }
    let Some(values) = existing else {
        return Ok(());
    };
    if lookup_dotted_flag(values, ALLOW_DEV_DEFAULTS_KEY) {
        return Ok(());
    }
    if operator_set_entries(operator_sets)
        .into_iter()
        .any(|(key, _)| key.trim() == ALLOW_DEV_DEFAULTS_KEY)
    {
        return Ok(());
    }
    // `curie cluster down` is deliberately NOT the headline advice. It deletes
    // only the namespaces the release itself created and leaves a pre-existing
    // one untouched (#707), and the runtime PVCs are not helm-owned -- they go
    // only with that namespace sweep. Followed there it would delete the release
    // Secret holding the store credentials while the data outlives it, which is
    // the very lockout this guard exists to prevent, so the teardown path has to
    // name the PVCs explicitly (#1145).
    let fix = "re-run without `--dev`, which preserves the credentials the release already \
               recorded; if a dev-default stack is genuinely what you want here, the \
               backing-store PVCs have to go WITH the release before you reinstall -- \
               `curie cluster down` alone does not remove them when the release was \
               installed into a pre-existing namespace, and dropping the release while \
               that data survives locks you out of your own stores";
    Err(crate::exit::CliError::usage(
        "this release was not installed with `--dev`, so `--dev` is refused: it would rewrite \
         every chart-owned credential in the release Secret to the published dev defaults \
         while the Postgres and RustFS data on the release's PVCs still holds the original \
         generated credentials, so the store init hook times out and the API migrate init \
         container fails password authentication, leaving the install broken until the PVCs \
         are wiped",
    )
    .with_fix(fix)
    .into())
}

enum UpInferencePolicy {
    Detect(Vec<ClusterUpInference>),
    Disabled,
}

pub async fn up(
    mut opts: UpOpts,
    github_token: Option<String>,
    clear_github_token: bool,
) -> Result<ClusterUpOutput> {
    validate_up_inputs(&opts, github_token.as_deref(), clear_github_token)?;
    validate_credential_egress_consistency(&opts)?;
    provider_contradiction(&opts, &up_value_plan(&opts))?;
    let resolve_provider_egress = !opts.common.dry_run;
    let existing = if should_read_existing(opts.dev, opts.common.dry_run) {
        require_on_path("helm")?;
        fetch_existing_values(&opts.common).await?
    } else {
        None
    };
    opts = complete_up_opts_without_runner_egress(
        opts,
        existing.as_ref(),
        github_token.as_deref(),
        clear_github_token,
        true,
    )?;
    validate_credential_egress_consistency(&opts)?;
    let completed_identity_plan = up_value_plan(&opts);
    let mut inferences = Vec::new();
    let provider_inference = reconcile_provider_inference(&mut opts, &completed_identity_plan)?;
    let provider_was_inferred = provider_inference.is_some();
    if let Some(inference) = provider_inference {
        inferences.push(inference);
    }
    let operator_sets = opts.operator_sets();
    let (next_preserved_egress_index, recorded_egress_cidrs) =
        resolve_preserved_runner_egress_values(
            &mut opts,
            existing.as_ref(),
            &operator_sets,
            provider_was_inferred,
        );
    resolve_provider_egress_for_up(&mut opts, resolve_provider_egress)?;
    if provider_was_inferred && next_preserved_egress_index > 0 {
        reindex_inferred_provider_egress(
            &mut opts,
            next_preserved_egress_index,
            &recorded_egress_cidrs,
        );
    }
    let value_plan = up_value_plan(&opts);
    run_prepared_up(
        opts,
        value_plan,
        existing,
        github_token.as_deref(),
        UpInferencePolicy::Detect(inferences),
    )
    .await
}

/// Execute an up plan whose local validation and live completion already ran.
/// Installation apply uses this after it has shared that completed plan with
/// diff, avoiding another values read or provider lookup.
pub(crate) async fn up_prepared(
    opts: UpOpts,
    value_plan: UpValuePlan,
    existing: Option<serde_json::Value>,
    github_token: Option<String>,
) -> Result<ClusterUpOutput> {
    validate_up_inputs(&opts, github_token.as_deref(), false)?;
    run_prepared_up(
        opts,
        value_plan,
        existing,
        github_token.as_deref(),
        UpInferencePolicy::Disabled,
    )
    .await
}

async fn run_prepared_up(
    opts: UpOpts,
    mut value_plan: UpValuePlan,
    existing: Option<serde_json::Value>,
    github_token: Option<&str>,
    inference_policy: UpInferencePolicy,
) -> Result<ClusterUpOutput> {
    // The single call site, and deliberately the first statement of the single
    // choke point both `up()` and `up_prepared()` funnel through: upstream of
    // the `inference.render(ui)` loop, of the preservation notes, and of every
    // helm invocation, so a refused run (#1145) emits the error and nothing
    // else.
    guard_dev_defaults_flip(opts.dev, existing.as_ref(), &opts.operator_sets())?;
    let ui = crate::ui::ui();
    let (detect_facts, initial_inferences) = match inference_policy {
        UpInferencePolicy::Detect(inferences) => (true, inferences),
        UpInferencePolicy::Disabled => (false, Vec::new()),
    };
    for inference in &initial_inferences {
        inference.render(ui);
    }
    let operator_sets = opts.operator_sets();
    if let Some(values) = &opts.retained_mail_values {
        ui.note(&format!(
            "preserving {} mail value(s) recorded by the release",
            values.keys().len()
        ));
    }
    let preserved = resolve_preserved_values(existing.as_ref(), &operator_sets);
    if !preserved.is_empty() {
        let sealing_values = preserved
            .iter()
            .filter(|(key, _)| crate::sealing::SEALING_MANAGED_KEYS.contains(&key.as_str()))
            .count();
        let message = if sealing_values == preserved.len() {
            format!(
                "preserving {} sealing value(s) recorded by the release",
                preserved.len()
            )
        } else if sealing_values == 0 {
            format!(
                "preserving {} value(s) recorded by `cluster comms` / `cluster github-app`; re-run those verbs only to change them",
                preserved.len()
            )
        } else {
            format!(
                "preserving {} value(s), including {} sealing value(s), recorded by the release",
                preserved.len(),
                sealing_values
            )
        };
        ui.note(&message);
    }
    if !opts.dev {
        match sealing_private_key_disposition(
            existing.as_ref(),
            &operator_sets,
            opts.common.dry_run,
        ) {
            SealingPrivateKeyDisposition::Generated => {
                ui.note("generated a sealing private key for this release; later cluster up runs preserve it");
            }
            SealingPrivateKeyDisposition::Deferred => {
                ui.note("a live run discovers sealing state and preserves an existing private key or generates one when absent; skipped here to keep --dry-run offline");
            }
            SealingPrivateKeyDisposition::OperatorSet | SealingPrivateKeyDisposition::Preserved => {
            }
        }
        if existing.is_none() && !opts.common.dry_run {
            let generated_required_secrets = opts
                .secrets
                .iter()
                .filter(|(key, _)| {
                    REQUIRED_SECRETS
                        .iter()
                        .any(|(required, _)| *required == key.as_str())
                })
                .count();
            if generated_required_secrets > 0 {
                ui.note(&format!(
                    "generated strong per-release secrets for {generated_required_secrets} required chart credential(s); re-running `cluster up` reuses them"
                ));
            }
        }
    }
    // `existing.is_some()` means this is an UPGRADE: an API pod is already
    // running on the old value and keeps it until it restarts (the api pod
    // template carries no checksum/secret annotation). On a FRESH install the
    // pod has not started yet and comes up with the new value, so the restart
    // advice would be false and is suppressed.
    let upgrading = existing.is_some();
    // Named by label selector, not `{release}-api`: the chart derives the
    // actual Deployment name through `curie.fullname`, which is only
    // `{release}-curie` unless the release name already contains "curie"
    // (`charts/curie/templates/_helpers.tpl:15-25`), so a literal `-api`
    // suffix on the release name is wrong under a non-default `--release`.
    // `app.kubernetes.io/instance={release}` plus the fixed `component: api`
    // label match the Deployment's own `selectorLabels` regardless of any
    // `nameOverride`/`fullnameOverride`, so this is correct in every case.
    let restart_hint = format!(
        "kubectl -n {} rollout restart deployment -l {}",
        opts.common.namespace,
        component_selector(&opts.common.release, "api")
    );
    // An explicit but EMPTY `--github-token` / `CURIE_GITHUB_TOKEN`: a routine
    // shell accident that preserves rather than clears (`resolve_github_token`),
    // so each arm below folds it into its own single note.
    let empty_flag = github_token.is_some_and(str::is_empty);
    match &opts.github_token {
        GithubTokenPlan::Set(_) if github_token.is_some_and(|v| !v.is_empty()) => {
            if upgrading {
                ui.note(&format!(
                    "GitHub credential set through the private values path; the running API keeps the old one until it restarts: {restart_hint}"
                ));
            } else {
                ui.note("GitHub credential set through the private values path");
            }
        }
        GithubTokenPlan::Clear => {
            // A clear only actually removes something when a credential was
            // recorded; on a fresh install (or an existing release that never
            // had one) there is nothing to remove, so the wording must not
            // claim there was.
            if preserved_value(existing.as_ref(), GITHUB_TOKEN_KEY).is_some() {
                // This is an incident-response verb, not a revocation: the
                // running API keeps the old token until it restarts, and the
                // token stays valid at GitHub until revoked there directly.
                ui.note(&format!(
                    "GitHub credential cleared here, not revoked: the running API keeps the old token until it restarts ({restart_hint}), and the token is still valid at GitHub until you revoke it at https://github.com/settings/tokens (or rotate the App installation credential)"
                ));
            } else {
                ui.note("no GitHub credential was recorded; nothing to clear");
            }
        }
        GithubTokenPlan::Set(_) => {
            // Preservation, reached either by a plain `up` or by an EMPTY
            // explicit value (state 4 in `resolve_github_token`). One note per
            // outcome: when the value was empty, say so here rather than
            // trailing a second, overlapping note after this match.
            if empty_flag {
                ui.note("--github-token (or CURIE_GITHUB_TOKEN) was empty; preserving the GitHub credential recorded by an earlier cluster up. Pass --clear-github-token to remove it.");
            } else {
                ui.note("preserving the GitHub credential recorded by an earlier cluster up; pass --github-token to change it or --clear-github-token to remove it");
            }
        }
        GithubTokenPlan::Untouched => {
            // Distinct wording: an empty value with nothing recorded preserves
            // nothing, so it must not read as if it kept a credential.
            if empty_flag {
                ui.note("--github-token (or CURIE_GITHUB_TOKEN) was empty; no GitHub credential is recorded to keep.");
            }
        }
    }
    if set_passthrough_leaks_github_token(&opts.operator_sets()) {
        ui.warn("a GitHub credential passed with --set lands in the process table and shell history; use --github-token, or CURIE_GITHUB_TOKEN to keep it out of shell history too");
    }

    if !opts.allow_egress_host.is_empty()
        && opts.common.dry_run
        && opts.resolved_egress_cidrs.is_empty()
    {
        let named = opts
            .allow_egress_host
            .iter()
            .map(|provider| match provider_egress_hosts(provider) {
                Some(hosts) if !hosts.is_empty() => format!("{provider} ({})", hosts.join(", ")),
                _ => provider.clone(),
            })
            .collect::<Vec<_>>()
            .join(", ");
        ui.note(&format!(
            "a live run resolves {named} to narrow /32+/128 host routes and opens runner egress to the resolved addresses (skipped here to keep --dry-run offline)"
        ));
    }

    let mut cmds = up_commands_with_plan(&opts, &value_plan);

    // #707 record ownership only on namespaces THIS run creates, so a later
    // `down` sweeps exactly what `up` made and leaves pre-existing state alone.
    // `up` may create the release namespace (via `helm --create-namespace`) and
    // the chart-created `agent-sandbox-system` -- but the latter is
    // chart-conditional (created only when `agentSandbox.controller.deploy` is
    // true), so a `--set agentSandbox.controller.deploy=false` release never
    // creates it. Probe each candidate BEFORE the install so a namespace that
    // already existed is adopted, not stamped (`existed_before`, recorded in
    // `ownership_candidates`); the actual stamp attempt is gated a SECOND time
    // AFTER the install (see `should_stamp_ownership` below, run once `cmds` has
    // executed) against whether the namespace exists now, so a namespace the
    // chart never created is simply not stamped instead of failing `kubectl
    // label namespace <missing>`. Mirror the resolve_generated_secrets
    // existing/fresh split: both runtime probes live here in the executor, the
    // argv stays in the pure `ownership_label_commands` builder. `--dry-run`
    // stays offline and previews the fresh-install stamp for every candidate
    // (existed == false), never touching the cluster and never running the
    // post-install probe.
    let mut owned_namespaces = vec![opts.common.namespace.clone()];
    if opts.common.namespace != "agent-sandbox-system" {
        owned_namespaces.push("agent-sandbox-system".to_string());
    }
    if !opts.common.dry_run {
        require_on_path("kubectl")?;
    }
    let mut ownership_candidates: Vec<(String, bool)> = Vec::new();
    for ns in owned_namespaces {
        let existed_before = if opts.common.dry_run {
            false
        } else {
            namespace_exists(&ns).await?
        };
        if opts.common.dry_run {
            // existed_before is provably false on this branch (set above).
            let common = ns_common(&opts.common, &ns, true);
            cmds.extend(ownership_label_commands(
                &common,
                &opts.common.namespace,
                false,
            ));
        }
        ownership_candidates.push((ns, existed_before));
    }

    // Count named provider intent on both live and dry runs, plus egress
    // implied by allow_egress_host resolution. Preserve explicit web egress
    // and nonempty allowedEgress overrides supplied through either value lane.
    let any_egress = !opts.allow_egress_host.is_empty()
        || !opts.allow_web_egress.is_empty()
        || operator_set_entries(&opts.operator_sets())
            .into_iter()
            .any(|(key, value)| {
                key_is_or_descends_from(key.trim(), ALLOWED_EGRESS_KEY)
                    && !matches!(value.trim(), "" | "[]")
            });
    for (warn, msg) in model_egress_status_lines(
        opts.credentials.is_some(),
        opts.local_model.is_some(),
        opts.fake_model,
        &opts.allow_egress_host,
        any_egress,
        opts.common.dry_run,
    ) {
        if warn {
            ui.warn(&msg)
        } else {
            ui.note(&msg)
        }
    }
    if let Some(warning) = default_route_egress_warning(&opts.allow_web_egress) {
        ui.warn(&warning);
    }
    if !opts.allow_web_egress.is_empty() {
        ui.note(&format!(
            "web egress opened to {} declared destination(s)",
            opts.allow_web_egress.len()
        ));
    }
    if opts.common.dry_run {
        let mut lines: Vec<String> = cmds.iter().map(OpsCommand::display).collect();
        lines.push("# After Helm and ownership stamping, verify convergence for at most 300 seconds; repeat observations every 2 seconds while rollout is pending.".to_owned());
        lines.push(convergence::DRY_RUN_NOTE.to_owned());
        lines.extend(
            convergence::dry_run_commands(&opts.common)
                .iter()
                .map(OpsCommand::display),
        );
        return Ok(ClusterUpOutput::DryRun(crate::ui::DryRunPlan { lines }));
    }
    let release_namespace_existed_before_install = ownership_candidates
        .iter()
        .find_map(|(namespace, existed)| (namespace == &opts.common.namespace).then_some(*existed))
        .unwrap_or(false);
    require_on_path("helm")?;
    if detect_facts {
        for inference in reconcile_priority_class_ownership(&opts, &mut value_plan).await? {
            inference.render(ui);
        }
        if let Some(inference) = reconcile_controller_ownership(&opts, &mut value_plan).await? {
            inference.render(ui);
        }
    } else {
        preflight_priority_class_ownership(&opts, &value_plan).await?;
    }
    cmds = up_commands_with_plan(&opts, &value_plan);
    let gvisor_preflight_job =
        rendered_gvisor_preflight_job(&opts.chart, &opts.common, &value_plan).await?;
    let cl = ui.checklist();
    let label = format!("installing release {}", opts.common.release);
    for cmd in &cmds {
        if let Some(job) = gvisor_preflight_job.as_deref() {
            let outcome = match run_install_with_gvisor_observer(
                &cl,
                &label,
                "installed",
                cmd,
                &opts.common.namespace,
                job,
                release_namespace_existed_before_install,
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    return Err(convergence::installation_failure(&opts.common, error).await)
                }
            };
            match outcome {
                GvisorInstallOutcome::Installed => {}
                GvisorInstallOutcome::RuntimeClassRejected { rejection, step } if detect_facts => {
                    if let Some(mode @ ("auto" | "require")) =
                        final_operator_value(&opts, GVISOR_MODE_KEY)
                    {
                        step.fail("failed");
                        let assignment = format!("{GVISOR_MODE_KEY}={mode}");
                        let fix = format!(
                            "remove the explicit `{assignment}` setting and rerun to accept the inferred gVisor posture"
                        );
                        return Err(crate::exit::CliError::usage(format!(
                            "explicit `{assignment}` contradicts the detected admission result `{rejection}`; {fix}"
                        ))
                        .with_fix(fix)
                        .into());
                    }
                    step.warn("retrying");
                    value_plan.set(GVISOR_MODE_KEY, "off");
                    ClusterUpInference::GvisorOff.render(ui);
                    let retry = up_commands_with_plan(&opts, &value_plan)
                        .into_iter()
                        .next()
                        .expect("cluster up always has one Helm command");
                    if let Err(error) = run_step(&cl, &label, "installed", &retry).await {
                        return Err(convergence::installation_failure(&opts.common, error).await);
                    }
                }
                GvisorInstallOutcome::RuntimeClassRejected { rejection, step } => {
                    step.fail("failed");
                    let fix = "curie cluster up --set security.gvisor.mode=off";
                    return Err(crate::exit::CliError::failure(format!(
                        "gVisor preflight Job `{job}` could not create its pod: {rejection}. To install without gVisor isolation, run `{fix}`."
                    ))
                    .with_fix(fix)
                    .into());
                }
            }
        } else {
            if let Err(error) = run_step(&cl, &label, "installed", cmd).await {
                return Err(convergence::installation_failure(&opts.common, error).await);
            }
        }
    }

    // #707 stamp ownership only on namespaces this run actually created. A
    // candidate that did not exist before the install may still not exist
    // after it -- `agent-sandbox-system` under
    // `--set agentSandbox.controller.deploy=false` is the concrete case, since
    // the chart only creates that namespace when the sandbox controller
    // subchart is deployed -- so re-probe existence here, post-helm, and skip
    // the label attempt for anything still absent rather than let `kubectl
    // label namespace <missing>` fail the whole `up`. A namespace that
    // pre-existed is never re-probed or stamped at all.
    for (ns, existed_before) in &ownership_candidates {
        if *existed_before {
            continue;
        }
        let exists_after = namespace_exists(ns).await?;
        if !should_stamp_ownership(false, exists_after) {
            continue;
        }
        let common = ns_common(&opts.common, ns, false);
        for cmd in ownership_label_commands(&common, &opts.common.namespace, false) {
            run_step(&cl, &label, "installed", &cmd).await?;
        }
    }

    let step = cl.step("waiting for exact target workload convergence");
    if let Err(error) = convergence::wait(&opts.common).await {
        step.fail("not converged");
        return Err(error);
    }
    step.done("converged");
    Ok(ClusterUpOutput::Up {
        namespace: opts.common.namespace.clone(),
        release: opts.common.release.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ops::testsupport::*;

    fn mail_upgrade_opts(existing: &serde_json::Value, set: Vec<String>) -> UpOpts {
        complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                chart: "charts/curie".into(),
                no_expose: true,
                set,
                set_string: vec![],
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
                secrets: vec![],
                github_token: GithubTokenPlan::Untouched,
                dev: true,
            },
            Some(existing),
            None,
            false,
            true,
        )
        .unwrap()
    }

    fn retained_mail_fixture() -> serde_json::Value {
        serde_json::json!({
            "mailAdapter": {
                "deploy": true,
                "inbox": "mail@example.com",
                "allowedSenders": ["operator@example.com"],
                "pollIntervalSeconds": 37,
                "persistence": {"existingClaim": "acme-mail-state"},
                "agentmail": {
                    "apiKeyExistingSecret": "acme-mail-credentials",
                    "apiKeyExistingSecretKey": "provider-key",
                    "httpsCidrs": ["203.0.113.8/32"]
                },
                "channelTokenExistingSecret": "acme-mail-credentials",
                "channelTokenExistingSecretKey": "channel-key",
                "egressSecretExistingSecret": "acme-mail-credentials",
                "egressSecretExistingSecretKey": "egress-key"
            },
            "worker": {
                "adapterCredentialsExistingSecret": "acme-mail-credentials",
                "adapterCredentialsExistingSecretKey": "worker-map"
            }
        })
    }

    fn materialized_mail_values(opts: &UpOpts) -> serde_json::Value {
        let commands = up_commands(opts);
        let command = &commands[0];
        let (materialized, _guards) = command.materialize_secret_files().unwrap();
        let argv = materialized.argv();
        for pair in argv.windows(2).filter(|pair| pair[0] == "-f") {
            let value: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&pair[1]).unwrap()).unwrap();
            if value.get("mailAdapter").is_some() {
                return value;
            }
        }
        panic!("plain cluster up dropped the installed mail values");
    }

    #[test]
    fn plain_up_preserves_mail_lifecycle_and_external_secret_pairs() {
        let existing = retained_mail_fixture();
        let opts = mail_upgrade_opts(&existing, vec![]);
        let actual = materialized_mail_values(&opts);
        assert_eq!(
            actual, existing,
            "mail and worker credential state must survive unchanged"
        );
        let mut leaves = BTreeMap::new();
        crate::installation::flatten_values(&actual, "", &mut leaves);
        for key in leaves.keys() {
            assert!(is_preserved_by_up(key), "diff disagrees with up for {key}");
        }
        let shown = up_commands(&opts)[0].display();
        for private in ["mail@example.com", "acme-mail-credentials", "provider-key"] {
            assert!(
                !shown.contains(private),
                "private retained mail values leaked"
            );
        }
    }

    #[test]
    fn retained_mail_command_and_debug_hide_dynamic_keys_and_inline_secrets() {
        let mut existing = retained_mail_fixture();
        existing["worker"]["adapterCredentials"] = serde_json::json!({
            "private-adapter-identity": "private-inline-credential"
        });
        existing["worker"]
            .as_object_mut()
            .unwrap()
            .remove("adapterCredentialsExistingSecret");
        existing["worker"]
            .as_object_mut()
            .unwrap()
            .remove("adapterCredentialsExistingSecretKey");
        existing["mailAdapter"]["podAnnotations"] = serde_json::json!({
            "private.example.com/identity": "private-annotation-value"
        });
        let opts = mail_upgrade_opts(&existing, vec![]);
        let command = &up_commands(&opts)[0];
        let display = command.display();
        let debug = format!("{command:?}");
        let (materialized, _guards) = command.materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        for private in [
            "private-adapter-identity",
            "private-inline-credential",
            "private.example.com/identity",
            "private-annotation-value",
        ] {
            for surface in [&display, &debug, &argv] {
                assert!(!surface.contains(private), "retained mail metadata leaked");
            }
        }
        assert_eq!(materialized_mail_values(&opts), existing);
    }

    #[test]
    fn plain_mail_upgrade_never_replays_inline_copies_behind_external_sources() {
        let expected = retained_mail_fixture();
        let mut existing = expected.clone();
        existing["mailAdapter"]["channelToken"] = "stale-channel-token".into();
        existing["mailAdapter"]["egressSecret"] = "stale-egress-secret".into();
        existing["mailAdapter"]["agentmail"]["apiKey"] = "stale-provider-key".into();
        existing["worker"]["adapterCredentials"] =
            serde_json::json!({"mail-adapter": "stale-egress-secret"});
        let opts = mail_upgrade_opts(&existing, vec![]);
        assert_eq!(materialized_mail_values(&opts), expected);
    }

    #[test]
    fn explicit_mail_disable_and_secret_clear_override_retained_values() {
        let mut existing = retained_mail_fixture();
        existing["mailAdapter"]["channelToken"] = "obsolete-inline-token".into();
        let opts = mail_upgrade_opts(
            &existing,
            vec![
                "mailAdapter.deploy=false".into(),
                "mailAdapter.channelTokenExistingSecret=".into(),
                "mailAdapter.allowedSenders={}".into(),
            ],
        );
        let actual = materialized_mail_values(&opts);
        assert!(actual["mailAdapter"].get("deploy").is_none());
        assert!(actual["mailAdapter"].get("allowedSenders").is_none());
        assert!(
            actual["mailAdapter"].get("channelToken").is_none(),
            "clearing an external reference cannot resurrect an obsolete inline token"
        );
        assert!(actual["mailAdapter"]
            .get("channelTokenExistingSecret")
            .is_none());
        assert_eq!(actual["mailAdapter"]["pollIntervalSeconds"], 37);
        let effective = up_value_plan(&opts).effective_values();
        assert_eq!(
            effective.get("mailAdapter.deploy").map(String::as_str),
            Some("false")
        );
    }

    #[test]
    fn explicit_inline_mail_credential_replaces_the_external_reference() {
        let opts = mail_upgrade_opts(
            &retained_mail_fixture(),
            vec!["mailAdapter.channelToken=new-inline-token".into()],
        );
        let actual = materialized_mail_values(&opts);
        assert!(actual["mailAdapter"]
            .get("channelTokenExistingSecret")
            .is_none());
        assert_eq!(
            actual["mailAdapter"]["channelTokenExistingSecretKey"],
            "channel-key"
        );
        let command = &up_commands(&opts)[0];
        assert!(
            !command.display().contains("channelTokenExistingSecret="),
            "migration overlay must not resurrect the replaced external reference"
        );
        assert_eq!(
            actual["worker"]["adapterCredentialsExistingSecretKey"],
            "worker-map"
        );
    }

    #[test]
    fn up_defaults_expose_ui_and_langfuse() {
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        assert_eq!(cmds.len(), 1);
        let line = cmds[0].display();
        assert_eq!(
            line,
            "helm upgrade --install curie charts/curie -n curie --create-namespace \
             --set ui.service.type=NodePort --set langfuse.web.service.type=NodePort"
        );
    }

    #[test]
    fn plain_up_re_supplies_recorded_worker_extra_env_without_reuse_values() {
        let existing = serde_json::json!({
            "worker": {
                "extraEnv": [
                    {
                        "name": "PROVIDER_BASE_URL",
                        "value": "https://provider.example.com/v1"
                    },
                    {
                        "name": "FALLBACK_BASE_URL",
                        "value": "https://fallback.example.com/v1"
                    }
                ]
            }
        });
        let opts = complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                github_token: GithubTokenPlan::Untouched,
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                chart: "charts/curie".into(),
                secrets: vec![],
                dev: false,
                no_expose: false,
                set: vec![],
                set_string: vec![],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
            },
            Some(&existing),
            None,
            false,
            true,
        )
        .unwrap();

        let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        for assignment in [
            "worker.extraEnv[0].name=PROVIDER_BASE_URL",
            "worker.extraEnv[0].value=https://provider.example.com/v1",
            "worker.extraEnv[1].name=FALLBACK_BASE_URL",
            "worker.extraEnv[1].value=https://fallback.example.com/v1",
        ] {
            assert!(
                argv.contains(&format!("--set-string {assignment}")),
                "plain up dropped recorded worker extraEnv leaf {assignment}: {argv}"
            );
        }
        assert!(
            !argv.contains("--reuse-values"),
            "up must remain a full Helm upgrade: {argv}"
        );
    }

    #[test]
    fn explicit_worker_extra_env_leaves_override_the_recorded_family() {
        let existing = serde_json::json!({
            "worker": {
                "extraEnv": [{
                    "name": "RECORDED_PROVIDER_BASE_URL",
                    "value": "https://recorded.example.com/v1"
                }]
            }
        });
        let opts = complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                github_token: GithubTokenPlan::Untouched,
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                chart: "charts/curie".into(),
                secrets: vec![],
                dev: false,
                no_expose: true,
                set: vec![],
                set_string: vec![
                    "worker.extraEnv[0].name=OPERATOR_PROVIDER_BASE_URL".into(),
                    "worker.extraEnv[0].value=https://operator.example.com/v1".into(),
                ],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
            },
            Some(&existing),
            None,
            false,
            true,
        )
        .unwrap();

        let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(
            argv.contains("--set-string worker.extraEnv[0].name=OPERATOR_PROVIDER_BASE_URL"),
            "the explicit worker extraEnv name must reach Helm: {argv}"
        );
        assert!(
            argv.contains("--set-string worker.extraEnv[0].value=https://operator.example.com/v1"),
            "the explicit worker extraEnv value must reach Helm: {argv}"
        );
        assert!(
            !argv.contains("RECORDED_PROVIDER_BASE_URL")
                && !argv.contains("https://recorded.example.com/v1"),
            "explicit worker extraEnv input must suppress the recorded family: {argv}"
        );
    }

    #[test]
    fn plain_up_escapes_commas_in_recorded_worker_extra_env_values() {
        let existing = serde_json::json!({
            "worker": {
                "extraEnv": [{
                    "name": "NO_PROXY",
                    "value": "10.0.0.0/8,localhost"
                }]
            }
        });
        let opts = complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                github_token: GithubTokenPlan::Untouched,
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                chart: "charts/curie".into(),
                secrets: vec![],
                dev: false,
                no_expose: true,
                set: vec![],
                set_string: vec![],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
            },
            Some(&existing),
            None,
            false,
            true,
        )
        .unwrap();

        assert_eq!(
            up_value_plan(&opts)
                .effective_values()
                .get("worker.extraEnv[0].value"),
            Some(&"10.0.0.0/8,localhost".to_string()),
            "the escaped Helm expression must retain the recorded semantic value"
        );

        let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
        let argv = materialized.argv();
        assert!(
            argv.contains(&"worker.extraEnv[0].value=10.0.0.0/8\\,localhost".into()),
            "recorded worker extraEnv values must escape commas for Helm: {argv:?}"
        );
    }

    /// Issue #2299: a released v0.8.x user-values overlay is migrated and
    /// re-supplied without `--reuse-values`, with schema version visible and
    /// inline credentials omitted when an external Secret is the source.
    #[test]
    fn upgrade_overlay_promotes_timeout_stamps_schema_and_keeps_secret_refs() {
        let existing = serde_json::json!({
            "ui": {"deploy": false},
            "worker": {
                "extraEnv": [
                    {"name": "CURIE_RUNNER_TOTAL_TIMEOUT_S", "value": "120"},
                    {"name": "PROVIDER_BASE_URL", "value": "https://provider.example.com/v1"}
                ]
            },
            "dispatcher": {
                "slack": {
                    "botTokenExistingSecret": "acme-slack",
                    "botTokenExistingSecretKey": "botToken",
                    "botToken": "xoxb-test-token-must-not-leak"
                }
            }
        });
        let opts = complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                github_token: GithubTokenPlan::Untouched,
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                chart: "charts/curie".into(),
                secrets: vec![],
                dev: false,
                no_expose: true,
                set: vec![],
                set_string: vec![],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
            },
            Some(&existing),
            None,
            false,
            true,
        )
        .unwrap();

        let effective = up_value_plan(&opts).effective_values();
        assert_eq!(
            effective.get("config.schemaVersion").map(String::as_str),
            Some("0.9.0")
        );
        assert_eq!(
            effective
                .get("worker.runnerTotalTimeoutSeconds")
                .map(String::as_str),
            Some("120")
        );
        assert_eq!(
            effective
                .get("dispatcher.slack.botTokenExistingSecret")
                .map(String::as_str),
            Some("acme-slack")
        );
        assert_eq!(
            effective
                .get("dispatcher.slack.botTokenExistingSecretKey")
                .map(String::as_str),
            Some("botToken")
        );
        assert!(
            effective
                .keys()
                .all(|key| !key.contains("botToken") || key.contains("Existing")),
            "inline botToken must not return on the overlay: {effective:?}"
        );

        let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        let display = up_commands(&opts)[0].display();
        assert!(
            !argv.contains("--reuse-values") && !display.contains("--reuse-values"),
            "upgrade must remain a full Helm upgrade: {display}"
        );
        assert!(
            display.contains("config.schemaVersion=0.9.0"),
            "redacted plan must expose the schema version: {display}"
        );
        assert!(
            !display.contains("xoxb-test-token-must-not-leak")
                && !argv.contains("xoxb-test-token-must-not-leak"),
            "inline token must not appear in plan or argv: {display}"
        );
    }

    /// A plain `cluster up` for an unrelated reason must not silently switch
    /// the worker back to refusing every dev reply endpoint the operator had
    /// already trusted (issue #1897).
    #[test]
    fn plain_up_re_supplies_recorded_slack_trusted_origins_without_reuse_values() {
        let existing = serde_json::json!({
            "worker": { "slackTrustedOrigins": "http://host.docker.internal" }
        });
        let opts = complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                github_token: GithubTokenPlan::Untouched,
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                chart: "charts/curie".into(),
                secrets: vec![],
                dev: false,
                no_expose: true,
                set: vec![],
                set_string: vec![],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
            },
            Some(&existing),
            None,
            false,
            true,
        )
        .unwrap();

        let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(
            argv.contains("--set-string worker.slackTrustedOrigins=http://host.docker.internal"),
            "plain up dropped the recorded Slack trusted origin: {argv}"
        );
        assert!(
            !argv.contains("--reuse-values"),
            "up must remain a full Helm upgrade: {argv}"
        );
    }

    /// An operator who names the trusted origins on this run owns the key: the
    /// stale recorded list must not be smuggled back alongside it, or the
    /// worker keeps trusting a host the operator just removed.
    #[test]
    fn explicit_slack_trusted_origins_override_the_recorded_value() {
        let existing = serde_json::json!({
            "worker": { "slackTrustedOrigins": "https://recorded.example.com" }
        });
        let opts = complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                github_token: GithubTokenPlan::Untouched,
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                chart: "charts/curie".into(),
                secrets: vec![],
                dev: false,
                no_expose: true,
                set: vec![],
                set_string: vec!["worker.slackTrustedOrigins=https://trusted.example.com".into()],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
            },
            Some(&existing),
            None,
            false,
            true,
        )
        .unwrap();

        let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(
            argv.contains("--set-string worker.slackTrustedOrigins=https://trusted.example.com"),
            "the explicit Slack trusted origin must reach Helm: {argv}"
        );
        assert!(
            !argv.contains("recorded.example.com"),
            "explicit trusted origins must suppress the recorded value: {argv}"
        );
    }

    /// A plain `--set` names the trusted origins on this run just as much as
    /// `--set-string` does -- an operator using the shorthand flag must still
    /// own the key, not have the stale recorded list smuggled back alongside it.
    #[test]
    fn explicit_set_slack_trusted_origins_override_the_recorded_value() {
        let existing = serde_json::json!({
            "worker": { "slackTrustedOrigins": "https://recorded.example.com" }
        });
        let opts = complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                github_token: GithubTokenPlan::Untouched,
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                chart: "charts/curie".into(),
                secrets: vec![],
                dev: false,
                no_expose: true,
                set: vec!["worker.slackTrustedOrigins=https://trusted.example.com".into()],
                set_string: vec![],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
            },
            Some(&existing),
            None,
            false,
            true,
        )
        .unwrap();

        let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(
            argv.contains("--set worker.slackTrustedOrigins=https://trusted.example.com"),
            "the explicit Slack trusted origin must reach Helm: {argv}"
        );
        assert!(
            !argv.contains("recorded.example.com"),
            "explicit trusted origins must suppress the recorded value: {argv}"
        );
    }

    /// The key is a COMMA-SEPARATED origin list, so an operator who trusts two
    /// hosts must not have Helm read the second one as a list element and the
    /// diff report an origin the worker was never sent.
    #[test]
    fn plain_up_round_trips_multi_origin_slack_trusted_origins() {
        let recorded = "http://host.docker.internal,http://10.20.30.40";
        let existing = serde_json::json!({
            "worker": { "slackTrustedOrigins": recorded }
        });
        let opts = complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                github_token: GithubTokenPlan::Untouched,
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                chart: "charts/curie".into(),
                secrets: vec![],
                dev: false,
                no_expose: true,
                set: vec![],
                set_string: vec![],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
            },
            Some(&existing),
            None,
            false,
            true,
        )
        .unwrap();

        assert_eq!(
            up_value_plan(&opts)
                .effective_values()
                .get("worker.slackTrustedOrigins"),
            Some(&recorded.to_string()),
            "the escaped Helm expression must retain the recorded origin list verbatim"
        );

        let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
        let argv = materialized.argv();
        assert!(
            argv.contains(
                &"worker.slackTrustedOrigins=http://host.docker.internal\\,http://10.20.30.40"
                    .into()
            ),
            "a recorded origin list must escape its commas for Helm: {argv:?}"
        );
    }

    /// Preservation must never widen where the platform bot token can go: a
    /// release that never trusted an extra origin -- whose list was
    /// deliberately cleared, or that has no recorded release at all (a fresh
    /// install) -- keeps the chart's fail-closed empty default.
    #[test]
    fn up_invents_no_slack_trusted_origins_when_none_is_recorded() {
        for existing in [
            Some(serde_json::json!({ "worker": { "slackTrustedOrigins": "" } })),
            Some(serde_json::json!({ "worker": {} })),
            None,
        ] {
            let opts = complete_up_opts_without_runner_egress(
                UpOpts {
                    retained_mail_values: None,
                    common: common(),
                    github_token: GithubTokenPlan::Untouched,
                    allow_egress_host: vec![],
                    resolved_egress_cidrs: vec![],
                    chart: "charts/curie".into(),
                    secrets: vec![],
                    dev: false,
                    no_expose: true,
                    set: vec![],
                    set_string: vec![],
                    allow_web_egress: vec![],
                    fake_model: false,
                    credentials: None,
                    local_model: None,
                    model: None,
                },
                existing.as_ref(),
                None,
                false,
                true,
            )
            .unwrap();

            let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
            let argv = materialized.argv().join(" ");
            assert!(
                !argv.contains("slackTrustedOrigins"),
                "up must not supply a trusted-origin value it has no record of: {argv}"
            );
        }
    }

    /// `curie diff` has to agree with what `up` actually does: announcing a
    /// reset for a value `up` hands straight back sends the operator chasing a
    /// change that never happens. And the list is hostnames, not a credential
    /// -- masking it would hide the very dev configuration the operator opens
    /// `diff` to confirm.
    #[test]
    fn slack_trusted_origins_are_preserved_and_never_masked() {
        assert!(
            is_preserved_by_up("worker.slackTrustedOrigins"),
            "diff must not report a reset for a key up re-supplies"
        );
        assert!(
            !is_secret_value_key("worker.slackTrustedOrigins"),
            "the trusted-origin list is operator-visible configuration, not a credential"
        );
    }

    #[test]
    fn up_no_expose_drops_the_nodeport_sets() {
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(!line.contains("NodePort"), "{line}");
        assert!(line.ends_with("--create-namespace"), "{line}");
    }

    #[test]
    fn up_passthrough_set_is_appended_verbatim() {
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec!["worker.replicas=2".into(), "dispatcher.deploy=false".into()],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(
            line.ends_with("--set worker.replicas=2 --set dispatcher.deploy=false"),
            "{line}"
        );
    }

    #[test]
    fn up_without_credentials_installs_sealed() {
        // No credential and not --fake-model: a plain install with no real-model
        // or egress sets (the fake model stays on, egress stays fail-closed).
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(!line.contains("agentSandbox.runner.fakeModel"), "{line}");
        assert!(!line.contains("agentSandbox.runner.credentials"), "{line}");
        assert!(!line.contains("allowedEgress"), "{line}");
    }

    #[test]
    fn up_fake_model_installs_sealed_like_no_credential() {
        // --fake-model resolves to no credential, so the argv is the sealed
        // install even when the caller had a credential in the environment.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: true,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(!line.contains("agentSandbox.runner"), "{line}");
        assert!(!line.contains("allowedEgress"), "{line}");
    }

    #[test]
    fn up_with_credentials_enables_real_model_and_masks() {
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec!["anthropic".into()],
            resolved_egress_cidrs: vec!["192.0.2.10/32".into()],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: Some("sk-ant-secretsecret".into()),
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(
            line.contains("agentSandbox.runner.fakeModel=false"),
            "{line}"
        );
        // Credential is masked in the printed form and never leaks. It is now
        // shown as part of a `-f` secret values file, not a `--set`.
        assert!(
            line.contains("agentSandbox.runner.credentials=sk-ant-s***"),
            "{line}"
        );
        assert!(
            line.contains("-f '<secret values file:"),
            "credential should be delivered via a -f values file: {line}"
        );
        assert!(!line.contains("secretsecret"), "secret leaked: {line}");
        // Model-provider egress entry (array-index keys print single-quoted).
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].cidr=192.0.2.10/32'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].ports[0].protocol=TCP'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].ports[0].port=443'"),
            "{line}"
        );

        // Success criterion: the live credential must NOT reach the executed argv
        // (the process table). Instead helm gets `-f <path>` pointing at a private
        // 0600 file that carries the secret. Materialize the command the way the
        // executor does and inspect the real argv + file.
        let (materialized, guards) = cmds[0]
            .materialize_secret_files()
            .expect("materializing the secret values file");
        let argv = materialized.argv();
        let argv_joined = argv.join(" ");
        assert!(
            !argv_joined.contains("secretsecret"),
            "credential leaked into argv: {argv_joined}"
        );
        assert!(
            !argv_joined.contains("agentSandbox.runner.credentials="),
            "credential --set leaked into argv: {argv_joined}"
        );

        // A `-f <values-file>` pair is present; the file exists, is 0600, and
        // contains the real credential (as nested YAML/JSON helm can read).
        let f_pos = argv
            .iter()
            .position(|a| a == "-f")
            .expect("a -f flag in the materialized argv");
        let values_path = std::path::PathBuf::from(&argv[f_pos + 1]);
        assert!(values_path.exists(), "values file {values_path:?} missing");
        let body = std::fs::read_to_string(&values_path).expect("reading the values file");
        assert!(
            body.contains("sk-ant-secretsecret"),
            "credential missing from values file: {body}"
        );
        // It nests the dotted key correctly for helm.
        assert!(
            body.contains("agentSandbox")
                && body.contains("runner")
                && body.contains("credentials"),
            "values file is not the expected nested shape: {body}"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&values_path)
                .expect("stat values file")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600, "values file must be 0600, was {mode:o}");
        }

        // The guard removes the file when dropped, so the secret never outlives
        // the helm run.
        drop(guards);
        assert!(
            !values_path.exists(),
            "values file should be deleted once the guard drops"
        );
    }

    #[test]
    fn up_local_model_adds_inference_sets() {
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: Some("qwen3:4b".into()),
            model: None,
        });
        let line = cmds[0].display();
        assert!(line.contains("--set inference.deploy=true"), "{line}");
        assert!(line.contains("--set inference.model=qwen3:4b"), "{line}");
    }

    #[test]
    fn up_without_local_model_omits_inference_sets() {
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(!line.contains("inference.deploy"), "{line}");
        assert!(!line.contains("inference.model"), "{line}");
    }

    #[test]
    fn up_defaults_runner_model_from_env() {
        // CURIE_MODEL set, no explicit --set: inject the runner model (#361).
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: Some("z-ai/glm-5.2".into()),
        });
        let line = cmds[0].display();
        assert!(
            line.contains("agentSandbox.runner.model=z-ai/glm-5.2"),
            "{line}"
        );
    }

    #[test]
    fn up_without_env_model_omits_runner_model_set() {
        // No CURIE_MODEL: inject nothing, the chart default stands (#361).
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(!line.contains("agentSandbox.runner.model="), "{line}");
    }

    #[test]
    fn up_explicit_set_model_suppresses_env_injection() {
        // CURIE_MODEL set AND an explicit matching --set: the operator's set
        // already carries it, so no duplicate injection (#361).
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec!["agentSandbox.runner.model=z-ai/glm-5.2".into()],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: Some("z-ai/glm-5.2".into()),
        });
        let line = cmds[0].display();
        assert_eq!(
            line.matches("agentSandbox.runner.model=z-ai/glm-5.2")
                .count(),
            1,
            "runner model should appear exactly once (no duplicate injection): {line}"
        );
    }

    #[test]
    fn up_commands_comma_joined_explicit_suppresses_injection() {
        // The runner model pinned alongside another key in a comma-joined
        // `--set` must be detected so `up` does not inject a redundant
        // `--set agentSandbox.runner.model=<model>` on top of it (#361).
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec!["worker.replicas=2,agentSandbox.runner.model=glm".into()],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: Some("glm".into()),
        });
        let line = cmds[0].display();
        assert_eq!(
            line.matches("agentSandbox.runner.model=glm").count(),
            1,
            "runner model should appear exactly once (no duplicate injection): {line}"
        );
    }

    #[test]
    fn up_opens_web_egress_after_model() {
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec!["anthropic".into()],
            resolved_egress_cidrs: vec!["192.0.2.10/32".into()],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec!["203.0.113.0/24".into()],
            fake_model: false,
            credentials: Some("sk-ant-secretsecret".into()),
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].cidr=192.0.2.10/32'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[1].cidr=203.0.113.0/24'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[1].ports[0].protocol=TCP'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[1].ports[0].port=443'"),
            "{line}"
        );
    }

    #[test]
    fn up_web_egress_without_model_uses_index_zero() {
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
            allow_web_egress: vec!["0.0.0.0/0".into()],
            fake_model: true,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(!line.contains("160.79.104.0/23"), "{line}");
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].cidr=0.0.0.0/0'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].ports[0].protocol=TCP'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].ports[0].port=443'"),
            "{line}"
        );
    }

    #[test]
    fn up_web_egress_multiple_cidrs_contiguous() {
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec!["anthropic".into()],
            resolved_egress_cidrs: vec!["192.0.2.10/32".into()],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec!["203.0.113.0/24".into(), "198.51.100.0/24".into()],
            fake_model: false,
            credentials: Some("sk-ant-secretsecret".into()),
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].cidr=192.0.2.10/32'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[1].cidr=203.0.113.0/24'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[2].cidr=198.51.100.0/24'"),
            "{line}"
        );
    }

    #[test]
    fn up_no_web_egress_stays_sealed() {
        let sealed_cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let sealed_line = sealed_cmds[0].display();
        assert!(!sealed_line.contains("allowedEgress"), "{sealed_line}");

        let model_cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: Some("sk-ant-secretsecret".into()),
            local_model: None,
            model: None,
        });
        let model_line = model_cmds[0].display();
        assert!(!model_line.contains("allowedEgress[1]"), "{model_line}");
    }

    // -- #196: generate / reuse the required chart secrets ------------------

    #[test]
    fn random_hex_is_the_right_length_hex_and_unpredictable() {
        let a = random_hex(24).unwrap();
        let b = random_hex(24).unwrap();
        assert_eq!(a.len(), 48, "24 bytes -> 48 hex chars");
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()), "{a}");
        assert_ne!(a, b, "two draws must differ");
        // The langfuse ENCRYPTION_KEY contract: exactly 64 hex chars.
        assert_eq!(random_hex(32).unwrap().len(), 64);
    }

    #[test]
    fn operator_set_keys_parses_repeated_and_comma_joined() {
        let keys = operator_set_keys(&[
            "api.apiKey=x".into(),
            "postgres.auth.password=y,valkey.password=z".into(),
        ]);
        assert!(keys.contains("api.apiKey"));
        assert!(keys.contains("postgres.auth.password"));
        assert!(keys.contains("valkey.password"));
        assert!(!keys.contains("api.githubWebhookSecret"));
    }

    #[test]
    fn lookup_dotted_navigates_nested_values() {
        let v: serde_json::Value =
            serde_json::from_str(r#"{"postgres":{"auth":{"password":"secretpw"}}}"#).unwrap();
        assert_eq!(
            lookup_dotted(&v, "postgres.auth.password").as_deref(),
            Some("secretpw")
        );
        assert_eq!(lookup_dotted(&v, "postgres.auth.missing"), None);
        assert_eq!(lookup_dotted(&serde_json::Value::Null, "api.apiKey"), None);
    }

    #[test]
    fn resolve_existing_secret_ref_prefers_operator_secret_and_custom_key() {
        // #1759 follow-up: a release with a BYO existingSecret configured must
        // resolve to that Secret and its (possibly overridden) key name, not
        // the chart's own Secret -- this is the read-path half of the bug
        // `curie seal`/`curie cluster message` hit before this fix, where they
        // read straight from the chart's own Secret and never checked for a
        // BYO override.
        let v: serde_json::Value = serde_json::from_str(
            r#"{"sealing":{"privateKeyExistingSecret":"my-sealing-secret","privateKeyExistingSecretKey":"customKey"}}"#,
        )
        .unwrap();
        assert_eq!(
            resolve_existing_secret_ref(
                Some(&v),
                "sealing.privateKeyExistingSecret",
                "sealing.privateKeyExistingSecretKey",
                "sealingPrivateKey"
            ),
            Some(("my-sealing-secret".to_string(), "customKey".to_string()))
        );
    }

    #[test]
    fn resolve_existing_secret_ref_defaults_key_when_key_field_absent() {
        // The chart's own default for *ExistingSecretKey is the published key
        // name (e.g. `botTokenExistingSecretKey: slackBotToken`), so a release
        // that set only the Secret name and left the key at its default must
        // still resolve to the published key, not an empty/missing key.
        let v: serde_json::Value = serde_json::from_str(
            r#"{"dispatcher":{"slack":{"botTokenExistingSecret":"my-slack-secret"}}}"#,
        )
        .unwrap();
        assert_eq!(
            resolve_existing_secret_ref(
                Some(&v),
                "dispatcher.slack.botTokenExistingSecret",
                "dispatcher.slack.botTokenExistingSecretKey",
                "slackBotToken"
            ),
            Some(("my-slack-secret".to_string(), "slackBotToken".to_string()))
        );
    }

    #[test]
    fn resolve_existing_secret_ref_none_when_not_configured() {
        // Negative control: no release at all, and a release that only set the
        // plain value (no BYO escape) -- both must fall through to None so the
        // caller reads the chart's own Secret, exactly as it did before #1759.
        assert_eq!(
            resolve_existing_secret_ref(
                None,
                "sealing.privateKeyExistingSecret",
                "sealing.privateKeyExistingSecretKey",
                "sealingPrivateKey"
            ),
            None
        );
        let v: serde_json::Value =
            serde_json::from_str(r#"{"sealing":{"privateKey":"plain-value"}}"#).unwrap();
        assert_eq!(
            resolve_existing_secret_ref(
                Some(&v),
                "sealing.privateKeyExistingSecret",
                "sealing.privateKeyExistingSecretKey",
                "sealingPrivateKey"
            ),
            None
        );
    }

    #[test]
    fn fresh_install_generates_every_required_secret() {
        // No existing release -> a strong random for each required key.
        let secrets = resolve_generated_secrets(None, &[]).unwrap();
        assert_eq!(secrets.len(), REQUIRED_SECRETS.len());
        for (key, _) in REQUIRED_SECRETS {
            let (_, value) = secrets
                .iter()
                .find(|(k, _)| k == key)
                .unwrap_or_else(|| panic!("missing generated secret for {key}"));
            assert!(!value.is_empty(), "{key} generated empty");
            assert!(
                value.chars().all(|c| c.is_ascii_hexdigit()),
                "{key}={value}"
            );
        }
        // encryptionKey keeps its exact 64-hex-char contract.
        let enc = secrets
            .iter()
            .find(|(k, _)| k == "langfuse.encryptionKey")
            .unwrap();
        assert_eq!(enc.1.len(), 64);
    }

    #[test]
    fn fresh_install_secrets_are_unpredictable_per_release() {
        let a = resolve_generated_secrets(None, &[]).unwrap();
        let b = resolve_generated_secrets(None, &[]).unwrap();
        assert_ne!(a, b, "each release must get its own randoms");
    }

    #[test]
    fn operator_set_secret_is_left_to_the_operator() {
        // A secret the operator pinned via --set is not generated over.
        let secrets = resolve_generated_secrets(None, &["api.apiKey=my-own-key".into()]).unwrap();
        assert!(
            !secrets.iter().any(|(k, _)| k == "api.apiKey"),
            "operator --set must win: {secrets:?}"
        );
        // Every other required secret is still generated.
        assert_eq!(secrets.len(), REQUIRED_SECRETS.len() - 1);
    }

    #[test]
    fn upgrade_reuses_recorded_secrets_and_never_rotates() {
        // helm get values shows what a prior install supplied; upgrade must
        // re-supply exactly those so a live store's credential is unchanged, and
        // must NOT mint a new value for a key with no record (leaving the
        // running release as-is rather than rotating it out from under a store).
        let existing: serde_json::Value = serde_json::from_str(
            r#"{"postgres":{"auth":{"password":"kept-pg-pw"}},"api":{"apiKey":"kept-api-key"}}"#,
        )
        .unwrap();
        let secrets = resolve_generated_secrets(Some(&existing), &[]).unwrap();
        assert_eq!(
            secrets,
            vec![
                (
                    "postgres.auth.password".to_string(),
                    "kept-pg-pw".to_string()
                ),
                ("api.apiKey".to_string(), "kept-api-key".to_string()),
            ],
            "upgrade must reuse recorded secrets and generate none: {secrets:?}"
        );
    }

    #[test]
    fn upgrade_ignores_empty_recorded_secret() {
        // An empty recorded value is not a real secret; do not re-supply it.
        let existing: serde_json::Value =
            serde_json::from_str(r#"{"valkey":{"password":""}}"#).unwrap();
        let secrets = resolve_generated_secrets(Some(&existing), &[]).unwrap();
        assert!(secrets.is_empty(), "{secrets:?}");
    }

    #[test]
    fn resolve_is_non_interactive_and_cannot_hang() {
        // The whole generate/reuse path is a pure function: no stdin, no TTY, so
        // a non-interactive / CI `cluster up` resolves secrets without blocking.
        // (Exercising it here would hang the test run if it ever read a TTY.)
        let _ = resolve_generated_secrets(None, &[]).unwrap();
        let _ = resolve_generated_secrets(Some(&serde_json::Value::Null), &[]).unwrap();
    }

    #[test]
    fn up_routes_preserved_slack_tokens_through_the_values_file_not_argv() {
        // End of the path, not just the resolver: a preserved bot token is a
        // live credential, so it must land in the private -f file like any other
        // secret and never appear in argv or the printed line.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![(
                "dispatcher.slack.botToken".into(),
                "xoxb-preserved-secret".into(),
            )],
            dev: false,
            no_expose: true,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(!line.contains("xoxb-preserved-secret"), "leaked: {line}");

        let (materialized, _guards) = cmds[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(!argv.contains("xoxb-preserved-secret"), "argv leak: {argv}");
        let f_pos = materialized.argv().iter().position(|a| a == "-f").unwrap();
        let body = std::fs::read_to_string(&materialized.argv()[f_pos + 1]).unwrap();
        assert!(body.contains("xoxb-preserved-secret"), "{body}");
    }

    #[test]
    fn up_preserves_slack_tokens_a_previous_comms_recorded() {
        // `comms` writes with --reuse-values; `up` does a full upgrade and drops
        // whatever it does not pass. That deleted the dispatcher and its tokens
        // with no error and nothing in the diff mentioning Slack (#1067).
        let existing = serde_json::json!({
            "dispatcher": {"slack": {"appToken": "xapp-existing", "botToken": "xoxb-existing"}}
        });
        let preserved = resolve_comms_values(Some(&existing), &[]);
        assert_eq!(
            preserved,
            vec![
                (
                    "dispatcher.slack.appToken".to_string(),
                    "xapp-existing".to_string()
                ),
                (
                    "dispatcher.slack.botToken".to_string(),
                    "xoxb-existing".to_string()
                ),
            ]
        );
    }

    #[test]
    fn up_carries_forward_both_families_of_sibling_verb_values() {
        // The wiring, not the helpers. Testing each family in isolation left
        // this uncovered: deleting the call from `up` kept every test green,
        // which is how #1256 shipped in the first place.
        let existing = serde_json::json!({
            "dispatcher": {"slack": {"appToken": "xapp-x", "botToken": "xoxb-x"}},
            "api": {"githubAppId": "1234567", "githubAppPrivateKey": "FAKEKEY"},
        });
        let keys: Vec<String> = resolve_preserved_values(Some(&existing), &[])
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert!(
            keys.contains(&"dispatcher.slack.botToken".to_string()),
            "lost the Slack tokens (#1067): {keys:?}"
        );
        assert!(
            keys.contains(&"api.githubAppPrivateKey".to_string()),
            "lost the GitHub App (#1256): {keys:?}"
        );
    }

    #[test]
    fn a_plain_up_preserves_the_github_app_recorded_by_github_app() {
        // A full upgrade drops anything it does not re-pass. An operator who
        // wired the App and later ran `up` for something unrelated silently
        // lost it, and every private-repo deploy 404d with nothing in the diff
        // mentioning the App (#1256).
        let existing = serde_json::json!({
            "api": {
                "githubAppId": "1234567",
                "githubAppPrivateKey": "FAKEKEY",
                "githubCloneBase": "https://github.example.com",
            }
        });
        let preserved = resolve_github_app_values(Some(&existing), &[]);
        assert_eq!(
            preserved,
            vec![
                ("api.githubAppId".to_string(), "1234567".to_string()),
                ("api.githubAppPrivateKey".to_string(), "FAKEKEY".to_string()),
                (
                    "api.githubCloneBase".to_string(),
                    "https://github.example.com".to_string()
                ),
            ]
        );
    }

    #[test]
    fn the_byo_secret_reference_is_preserved_too() {
        // The recommended path (#1236) records a Secret NAME rather than the
        // key. Dropping that reference reverts the API to the chart's own
        // Secret, whose default is empty -- the same silent revocation.
        let existing = serde_json::json!({
            "api": {
                "githubAppId": "1234567",
                "githubAppExistingSecret": "curie-github-app",
                "githubAppExistingSecretKey": "privateKey",
            }
        });
        let keys: Vec<String> = resolve_github_app_values(Some(&existing), &[])
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert!(keys.contains(&"api.githubAppExistingSecret".to_string()));
        assert!(keys.contains(&"api.githubAppExistingSecretKey".to_string()));
    }

    #[test]
    fn an_operator_set_wins_over_the_recorded_app_value() {
        // Preserve, never override. `--set` is the operator saying they own it.
        let existing = serde_json::json!({"api": {"githubAppId": "1234567"}});
        let preserved =
            resolve_github_app_values(Some(&existing), &["api.githubAppId=999".to_string()]);
        assert!(preserved.is_empty());
    }

    #[test]
    fn nothing_is_invented_when_no_app_was_ever_configured() {
        // Preserving must not fabricate. An invented App id is worse than a
        // dropped one: it fails auth in a way that reads as a permissions
        // problem rather than a missing credential.
        assert!(resolve_github_app_values(None, &[]).is_empty());
        assert!(resolve_github_app_values(Some(&serde_json::json!({})), &[]).is_empty());
    }

    #[test]
    fn up_lets_an_explicit_set_override_a_preserved_comms_value() {
        let existing = serde_json::json!({
            "dispatcher": {"slack": {"appToken": "xapp-old", "botToken": "xoxb-old"}}
        });
        let preserved = resolve_comms_values(
            Some(&existing),
            &["dispatcher.slack.botToken=xoxb-new".to_string()],
        );
        // Only the untouched key is re-supplied; the operator's --set wins.
        assert_eq!(
            preserved,
            vec![(
                "dispatcher.slack.appToken".to_string(),
                "xapp-old".to_string()
            )]
        );
    }

    #[test]
    fn up_preserves_nothing_on_a_fresh_install_or_when_slack_was_never_set() {
        assert!(resolve_comms_values(None, &[]).is_empty());
        let no_slack = serde_json::json!({"nameOverride": "acme-bot"});
        assert!(resolve_comms_values(Some(&no_slack), &[]).is_empty());
        // An empty string is what `comms --disconnect` writes; do not resurrect it.
        let disconnected = serde_json::json!({
            "dispatcher": {"slack": {"appToken": "", "botToken": ""}}
        });
        assert!(resolve_comms_values(Some(&disconnected), &[]).is_empty());
    }

    fn secret_for<'a>(opts: &'a UpOpts, key: &str) -> Option<&'a str> {
        opts.secrets
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    fn completed_dev_up(existing: Option<&serde_json::Value>, set: Vec<String>) -> UpOpts {
        complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                github_token: GithubTokenPlan::Untouched,
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                chart: "charts/curie".into(),
                secrets: vec![],
                dev: true,
                no_expose: true,
                set,
                set_string: vec![],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
            },
            existing,
            None,
            false,
            true,
        )
        .unwrap()
    }

    /// #1134 / #1125: `cluster up --dev` after `cluster comms` is a FULL
    /// upgrade. Helm reuse never engages because `--dev` always passes
    /// `--set security.allowDevDefaults=true`, so anything this path does not
    /// re-pass resets to the empty chart default.
    ///
    /// This is the wiring, not the family helpers: `resolve_comms_values` and
    /// `resolve_preserved_values` already return the keys. Deleting the `--dev`
    /// arm that copies them onto `opts.secrets` (or gating it behind
    /// `if !opts.dev`) is what this fails on -- the same class of hole that
    /// left #1256 green.
    #[test]
    fn a_dev_upgrade_re_supplies_recorded_comms_and_sealing_values() {
        let existing = serde_json::json!({
            "security": {"allowDevDefaults": true},
            "dispatcher": {"slack": {"appToken": "xapp-EXAMPLE", "botToken": "xoxb-EXAMPLE"}},
            "sealing": {"privateKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="},
            "api": {
                "githubAppId": "1234567",
                "githubAppPrivateKey": "FAKEKEY",
            }
        });
        let opts = completed_dev_up(Some(&existing), vec![]);
        assert_eq!(
            secret_for(&opts, "dispatcher.slack.appToken"),
            Some("xapp-EXAMPLE"),
            "a second `cluster up --dev` dropped the Slack app token (#1134): {:?}",
            opts.secrets
        );
        assert_eq!(
            secret_for(&opts, "dispatcher.slack.botToken"),
            Some("xoxb-EXAMPLE"),
            "a second `cluster up --dev` dropped the Slack bot token (#1134): {:?}",
            opts.secrets
        );
        assert_eq!(
            secret_for(&opts, crate::sealing::SEALING_PRIVATE_KEY),
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            "a second `cluster up --dev` dropped the recorded sealing key (#1134): {:?}",
            opts.secrets
        );
        assert_eq!(
            secret_for(&opts, "api.githubAppPrivateKey"),
            Some("FAKEKEY"),
            "a second `cluster up --dev` dropped the GitHub App on the --dev arm: {:?}",
            opts.secrets
        );
        for (key, _) in REQUIRED_SECRETS {
            assert!(
                secret_for(&opts, key).is_none(),
                "--dev must keep published chart credential defaults, not mint {key}: {:?}",
                opts.secrets
            );
        }

        let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(
            argv.contains("security.allowDevDefaults=true"),
            "dev mode must still opt into chart defaults: {argv}"
        );
        assert!(
            !argv.contains("--reuse-values"),
            "up must remain a full Helm upgrade: {argv}"
        );
        assert!(
            !argv.contains("xapp-EXAMPLE") && !argv.contains("xoxb-EXAMPLE"),
            "Slack tokens leaked into argv: {argv}"
        );
        let joined = secret_values_file_bodies(&materialized).join("\n");
        assert!(
            joined.contains("xapp-EXAMPLE") && joined.contains("xoxb-EXAMPLE"),
            "the --dev values file dropped the Slack tokens: {joined}"
        );
        assert!(
            joined.contains("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            "the --dev values file dropped the sealing key: {joined}"
        );
    }

    #[test]
    fn a_dev_upgrade_does_not_mint_a_sealing_key_when_none_is_recorded() {
        let existing = serde_json::json!({"security": {"allowDevDefaults": true}});
        let opts = completed_dev_up(Some(&existing), vec![]);
        assert!(
            secret_for(&opts, crate::sealing::SEALING_PRIVATE_KEY).is_none(),
            "--dev must not mint a sealing key on a release that never had one: {:?}",
            opts.secrets
        );
        assert!(
            opts.secrets.is_empty(),
            "a --dev rerun with nothing recorded must not invent secrets: {:?}",
            opts.secrets
        );
    }

    #[test]
    fn a_dev_upgrade_stays_disconnected_after_comms_disconnect() {
        let existing = serde_json::json!({
            "security": {"allowDevDefaults": true},
            "dispatcher": {"slack": {"appToken": "", "botToken": ""}}
        });
        let opts = completed_dev_up(Some(&existing), vec![]);
        assert!(
            secret_for(&opts, "dispatcher.slack.appToken").is_none(),
            "empty disconnect values must not be resurrected: {:?}",
            opts.secrets
        );
        assert!(secret_for(&opts, "dispatcher.slack.botToken").is_none());
    }

    #[test]
    fn a_dev_upgrade_lets_an_explicit_set_replace_a_recorded_comms_value() {
        let existing = serde_json::json!({
            "security": {"allowDevDefaults": true},
            "dispatcher": {"slack": {"appToken": "xapp-old", "botToken": "xoxb-old"}}
        });
        let opts = completed_dev_up(
            Some(&existing),
            vec!["dispatcher.slack.botToken=xoxb-new".into()],
        );
        assert_eq!(
            secret_for(&opts, "dispatcher.slack.appToken"),
            Some("xapp-old")
        );
        assert!(
            secret_for(&opts, "dispatcher.slack.botToken").is_none(),
            "an explicit --set must own the key, not ride alongside the recorded value: {:?}",
            opts.secrets
        );
    }

    #[test]
    fn up_injects_generated_secrets_via_values_file_not_argv() {
        // Success criterion: a missing secret's generated value lands in the
        // private -f values file, never in the executed argv / process table.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![
                ("api.apiKey".into(), "generated-api-key".into()),
                (
                    "langfuse.encryptionKey".into(),
                    "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef0".into(),
                ),
            ],
            dev: false,
            no_expose: true,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        // Printed form masks the values and shows the -f secret values file.
        let line = cmds[0].display();
        assert!(line.contains("-f '<secret values file:"), "{line}");
        assert!(line.contains("api.apiKey=generate***"), "{line}");
        assert!(!line.contains("generated-api-key"), "secret leaked: {line}");

        // Materialize the way the executor does: the secret must be in the file,
        // not in argv.
        let (materialized, guards) = cmds[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(
            !argv.contains("generated-api-key"),
            "leaked into argv: {argv}"
        );
        assert!(
            !argv.contains("api.apiKey="),
            "secret --set leaked into argv: {argv}"
        );
        let f_pos = materialized.argv().iter().position(|a| a == "-f").unwrap();
        let path = std::path::PathBuf::from(&materialized.argv()[f_pos + 1]);
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.contains("generated-api-key"), "{body}");
        assert!(
            body.contains("api") && body.contains("apiKey"),
            "values file is not the expected nested shape: {body}"
        );
        drop(guards);
    }

    #[test]
    fn up_without_generated_secrets_is_unchanged() {
        // The pure builder with no supplied secrets (the --dev path, and every
        // pre-#196 argv test) emits no secret values file.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: true,
            no_expose: true,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        assert!(!cmds[0].display().contains("secret values file"));
    }

    #[test]
    fn up_dev_emits_allow_dev_defaults_flag() {
        // Under --dev the operator opts into the deterministic published chart
        // credentials, so `up` must pass security.allowDevDefaults=true through
        // to helm (issue #195). Without it the sealed chart generates strong
        // random values and the dev/e2e stack would not match compose.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: true,
            no_expose: true,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(
            line.contains("security.allowDevDefaults=true"),
            "expected --dev to emit security.allowDevDefaults=true: {line}"
        );
    }

    #[test]
    fn up_without_dev_omits_allow_dev_defaults_flag() {
        // The default (non-dev) path must NOT opt into the published defaults;
        // the sealed chart generates strong per-release credentials there.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(
            !line.contains("security.allowDevDefaults"),
            "non-dev up must not emit security.allowDevDefaults: {line}"
        );
    }

    #[test]
    fn helm_get_values_reads_user_supplied_values_as_json() {
        let cmd = helm_get_values_cmd(&common());
        assert_eq!(cmd.display(), "helm get values curie -n curie -o json");
    }

    /// `--all` is the entire point of this helper, and its absence is invisible
    /// at runtime: helm answers happily either way, just without the chart
    /// defaults. That is exactly the bug #1950 reports -- an operator who never
    /// supplied a model has no user-supplied value to read, so the sibling
    /// command above returns nothing and `doctor` reports the floating chart
    /// default as "not applicable". A test that does not pin `--all` lets a
    /// refactor reintroduce that silently.
    #[test]
    fn helm_get_all_values_reads_computed_values_as_json() {
        let cmd = helm_get_all_values_cmd(&common());
        assert_eq!(
            cmd.display(),
            "helm get values curie -n curie --all -o json"
        );
    }

    #[test]
    fn up_emits_resolved_provider_cidrs_before_web_egress_contiguously() {
        // Resolved provider CIDRs take the first slots (in order), then declared
        // web destinations continue contiguously -- one array, no gaps.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            model: None,
            allow_egress_host: vec!["anthropic".into()],
            resolved_egress_cidrs: vec!["10.0.0.1/32".into(), "2001:db8::1/128".into()],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec!["203.0.113.0/24".into()],
            fake_model: false,
            credentials: Some("sk-ant-secretsecret".into()),
            local_model: None,
        });
        let line = cmds[0].display();
        // Provider CIDRs occupy [0] and [1], each with the shared TCP/443 shape.
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].cidr=10.0.0.1/32'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].ports[0].protocol=TCP'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].ports[0].port=443'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[1].cidr=2001:db8::1/128'"),
            "{line}"
        );
        // The declared web destination continues at the next index, not [0].
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[2].cidr=203.0.113.0/24'"),
            "{line}"
        );
        // The old unconditional Anthropic carve-out is gone.
        assert!(!line.contains("160.79.104.0/23"), "{line}");
    }

    #[test]
    fn up_credential_without_any_egress_emits_no_allowed_egress() {
        // A credential with neither a resolved provider CIDR nor a web egress
        // destination enables the real model but opens NO egress -- the old
        // unconditional Anthropic carve-out is removed entirely (#362). The
        // sandbox stays sealed and the model is unreachable by design.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            model: None,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: Some("sk-ant-secretsecret".into()),
            local_model: None,
        });
        let line = cmds[0].display();
        // Real model still enabled and the credential still delivered by file.
        assert!(
            line.contains("agentSandbox.runner.fakeModel=false"),
            "{line}"
        );
        assert!(line.contains("-f '<secret values file:"), "{line}");
        // But NO egress rule at all -- and specifically not the old Anthropic one.
        assert!(!line.contains("160.79.104.0/23"), "{line}");
        assert!(!line.contains("allowedEgress"), "{line}");
    }

    #[test]
    fn up_web_egress_alone_still_starts_at_index_zero() {
        // Existing behavior preserved: with no credential and no provider host,
        // a declared web destination still occupies index [0].
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            model: None,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec!["203.0.113.0/24".into()],
            fake_model: true,
            credentials: None,
            local_model: None,
        });
        let line = cmds[0].display();
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].cidr=203.0.113.0/24'"),
            "{line}"
        );
        assert!(!line.contains("allowedEgress[1]"), "{line}");
        assert!(!line.contains("160.79.104.0/23"), "{line}");
    }

    #[test]
    fn github_token_absent_from_every_rendered_form() {
        // AC2, all four renderings in ONE test so none can be silently forgotten:
        // the human preview (`display()`), the executed argv, the `--dry-run`
        // plan built the way `up()` builds it, and that plan's `--json` string.
        // The `--verbose` plumbing echo is the same `display()` (run_step's
        // `ui.plumbing("+ {display()}")`), so it is covered by the first form.
        //
        // AC2's fifth surface, "errors", is NOT covered here and has no test:
        // `run_step`'s failure line is built inline
        // (`ui.failure(&format!("`{}` failed: {reason}", cmd.program))`) and
        // written to a process-global `Ui` with no capture seam, so a unit test
        // cannot read it. Asserting on a hand-rebuilt copy of that string would
        // be a tautology that survives changing `cmd.program` to `cmd.argv()`,
        // so there is deliberately no such test. Closing it needs `run_step`'s
        // failure line extracted into a named pure function, a production change.
        use crate::ui::CliOutput;

        let cmds = up_with_github_token(GithubTokenPlan::Set(GH_SENTINEL.into()));

        // 1. Human preview.
        let line = cmds[0].display();
        assert!(!line.contains(GH_SENTINEL), "display leak: {line}");

        // 2. Executed argv, after the executor's materialization step.
        let (materialized, _guards) = cmds[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(!argv.contains(GH_SENTINEL), "argv leak: {argv}");

        // 3. The `--dry-run` plan, constructed exactly as `up()` constructs it.
        let plan = crate::ui::DryRunPlan {
            lines: cmds.iter().map(|cmd| cmd.display()).collect(),
        };
        for planned in &plan.lines {
            assert!(
                !planned.contains(GH_SENTINEL),
                "dry-run plan leak: {planned}"
            );
        }

        // 4. The `--dry-run --json` serialization of that same plan.
        let json = plan.to_json().to_string();
        assert!(!json.contains(GH_SENTINEL), "dry-run --json leak: {json}");

        // And the masked form IS present in each printed form, so an operator can
        // still see the credential is being applied rather than inferring it.
        assert!(
            line.contains(GH_MASKED),
            "masked form missing from display: {line}"
        );
        assert!(
            plan.lines.iter().any(|l| l.contains(GH_MASKED)),
            "masked form missing from the dry-run plan: {:?}",
            plan.lines
        );
        assert!(
            json.contains(GH_MASKED),
            "masked form missing from --json: {json}"
        );
    }

    #[test]
    fn preserved_github_token_never_reaches_argv_either() {
        // AC2 secondary path. A token arriving through the PRESERVE path (what a
        // plain `cluster up` takes) is exactly as live as one just typed, so it
        // gets the same four-form treatment. Armed through the resolver over a
        // recorded release, never through the flag.
        use crate::ui::CliOutput;

        let existing = serde_json::json!({"api": {"githubToken": GH_SENTINEL}});
        let plan_state = resolve_github_token(Some(&existing), &[], None, false);
        assert_eq!(plan_state, GithubTokenPlan::Set(GH_SENTINEL.to_string()));

        let cmds = up_with_github_token(plan_state);
        let line = cmds[0].display();
        assert!(!line.contains(GH_SENTINEL), "display leak: {line}");
        assert!(line.contains(GH_MASKED), "masked form missing: {line}");

        let (materialized, _guards) = cmds[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(!argv.contains(GH_SENTINEL), "argv leak: {argv}");

        let plan = crate::ui::DryRunPlan {
            lines: cmds.iter().map(|cmd| cmd.display()).collect(),
        };
        for planned in &plan.lines {
            assert!(
                !planned.contains(GH_SENTINEL),
                "dry-run plan leak: {planned}"
            );
        }
        let json = plan.to_json().to_string();
        assert!(!json.contains(GH_SENTINEL), "dry-run --json leak: {json}");
    }

    #[test]
    fn set_and_flag_together_is_rejected() {
        // AC2 guard. Letting `--set` quietly win would discard the operator's
        // protected input AND put the token in the process table, so supplying
        // both is a usage error rather than a precedence rule.
        let err = check_github_token_conflict(
            Some(GH_SENTINEL),
            false,
            &["api.githubToken=ghp-other".to_string()],
        )
        .expect_err("flag + --set must be rejected")
        .to_string();
        assert!(err.contains("--github-token"), "{err}");
        assert!(err.contains("--set"), "{err}");
        assert!(err.contains("api.githubToken"), "{err}");

        // The comma-joined form helm also accepts, which `operator_set_keys`
        // splits -- a guard that only matched the bare form would miss it.
        assert!(
            check_github_token_conflict(
                Some(GH_SENTINEL),
                false,
                &["worker.replicas=2,api.githubToken=ghp-other".to_string()],
            )
            .is_err(),
            "the comma-joined --set form must be rejected too"
        );

        // The clear is just as explicit an input as a value.
        assert!(
            check_github_token_conflict(None, true, &["api.githubToken=ghp-other".to_string()])
                .is_err(),
            "--clear-github-token + --set must be rejected"
        );

        // Legal: exactly one side supplied.
        assert!(check_github_token_conflict(Some(GH_SENTINEL), false, &[]).is_ok());
        assert!(check_github_token_conflict(None, true, &[]).is_ok());
        assert!(
            check_github_token_conflict(None, false, &["api.githubToken=ghp-other".to_string()])
                .is_ok(),
            "a lone --set stays legal (it is a verbatim pass-through by design)"
        );
        // An EMPTY flag value is not an explicit input (an exported-but-empty
        // CURIE_GITHUB_TOKEN is a routine shell accident), so it does not conflict.
        assert!(
            check_github_token_conflict(
                Some(""),
                false,
                &["api.githubToken=ghp-other".to_string()]
            )
            .is_ok(),
            "an empty --github-token is absence, not a competing input"
        );
    }

    #[test]
    fn plain_up_preserves_the_recorded_github_token() {
        // AC3. `up` does a FULL upgrade and drops whatever it does not re-pass,
        // so without this a later plain `cluster up` silently resets the
        // credential to the chart's empty default and private clones start
        // failing with nothing in the diff mentioning GitHub (#1067's shape).
        let existing = serde_json::json!({"api": {"githubToken": "ghp-recorded"}});
        assert_eq!(
            resolve_github_token(Some(&existing), &[], None, false),
            GithubTokenPlan::Set("ghp-recorded".to_string())
        );
    }

    #[test]
    fn fresh_install_or_never_set_preserves_nothing() {
        // AC3. Nothing recorded means nothing to keep, and an EMPTY record is
        // what `--clear-github-token` wrote -- resurrecting it would make the
        // clear a one-shot. Mirrors
        // `up_preserves_nothing_on_a_fresh_install_or_when_slack_was_never_set`.
        assert_eq!(
            resolve_github_token(None, &[], None, false),
            GithubTokenPlan::Untouched
        );
        let no_token = serde_json::json!({"nameOverride": "acme-bot"});
        assert_eq!(
            resolve_github_token(Some(&no_token), &[], None, false),
            GithubTokenPlan::Untouched
        );
        let cleared = serde_json::json!({"api": {"githubToken": ""}});
        assert_eq!(
            resolve_github_token(Some(&cleared), &[], None, false),
            GithubTokenPlan::Untouched
        );
    }

    #[test]
    fn github_token_is_not_in_the_generating_list() {
        // AC3, the CLI half of #1109's pin. `REQUIRED_SECRETS` GENERATES a random
        // for a key it finds absent, which is right for a credential the install
        // must have and wrong for this one: 32 characters of noise sent to GitHub
        // as a bearer token fails auth in a way that reads like a permissions
        // problem rather than a missing credential. It joins the preserve-only
        // semantics instead.
        assert!(
            !REQUIRED_SECRETS
                .iter()
                .any(|(k, _)| *k == "api.githubToken"),
            "api.githubToken must never join the generating list: {REQUIRED_SECRETS:?}"
        );
        let secrets = resolve_generated_secrets(None, &[]).unwrap();
        assert_eq!(secrets.len(), REQUIRED_SECRETS.len());
        assert!(
            !secrets.iter().any(|(k, _)| k == "api.githubToken"),
            "a fresh install minted a GitHub token: {secrets:?}"
        );
    }

    #[test]
    fn dev_never_skips_the_existing_values_read_only_dry_run_does() {
        // AC3, and the pin on the change that actually makes a `cluster up --dev`
        // preserve a recorded GitHub token: the existing-values read must happen
        // on the `--dev` path too. Before the hoist that read sat inside
        // `if !opts.dev`, so a `--dev` upgrade saw no record, resolved
        // `Untouched`, and the full helm upgrade reset the credential to the
        // chart's empty default with nothing in the output mentioning GitHub.
        //
        // All four combinations, so this states the whole rule rather than one
        // corner: `dry_run` is the ONLY input that may skip the read (`--dry-run`
        // stays fully offline), and `dev` must never be a term. Re-adding a `dev`
        // term is the specific regression this catches, and it fails here in the
        // `dev = true, dry_run = false` case regardless of which direction the
        // term is written.
        assert!(
            should_read_existing(false, false),
            "a plain live cluster up must read the release's existing values"
        );
        assert!(
            should_read_existing(true, false),
            "a live `cluster up --dev` must ALSO read them: --dev governs the chart's \
             dev-default secret VALUES, not whether an operator's recorded credential \
             is preserved (#1124)"
        );
        assert!(
            !should_read_existing(false, true),
            "--dry-run stays offline and never touches helm"
        );
        assert!(
            !should_read_existing(true, true),
            "--dev --dry-run stays offline too"
        );
    }

    #[test]
    fn dev_flag_does_not_suppress_the_github_token_values_file() {
        // AC3 secondary path, and the name states EXACTLY what it proves: the
        // `--dev` branch of `up_commands` adds only
        // `security.allowDevDefaults=true`, so a resolved credential still rides
        // the private values file beside it rather than being swallowed.
        //
        // The other half of `--dev` preservation, the decision to read the
        // release's existing values at all, is pinned by
        // `dev_never_skips_the_existing_values_read_only_dry_run_does`: that test
        // is what fails if someone re-adds a `dev` term to
        // `should_read_existing` and reinstates the pre-hoist `if !opts.dev` gate.
        //
        // Neither test covers the path end to end, which is worth stating rather
        // than leaving implied: `up()` is async and the read runs behind
        // `require_on_path("helm")` plus a live `helm get values`, so no unit test
        // exercises a real `--dev` upgrade actually re-supplying a recorded token.
        // Between them these two pin the read DECISION and the argv it feeds; the
        // live evidence is the E2E's `--dev` arm.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Set(GH_SENTINEL.into()),
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: true,
            no_expose: true,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(line.contains("security.allowDevDefaults=true"), "{line}");
        assert!(line.contains(GH_MASKED), "{line}");
        assert!(!line.contains(GH_SENTINEL), "display leak: {line}");

        let (materialized, _guards) = cmds[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(!argv.contains(GH_SENTINEL), "argv leak: {argv}");
        assert!(
            secret_values_file_bodies(&materialized)
                .iter()
                .any(|body| body.contains(GH_SENTINEL)),
            "the --dev install dropped the credential"
        );
    }

    /// #1145 / G1, G2: the guard's `dev == false` arm is unconditional -- a
    /// plain `cluster up` is never refused, whatever the release recorded.
    ///
    /// The reverse flip (a sealed run over a release that IS on dev defaults)
    /// is safe by construction, which is why this arm can be unconditional:
    /// `resolve_generated_secrets` re-supplies only what the release already
    /// recorded and never mints a value for an unrecorded key, so
    /// `curie.managedSecret` sees `value == default`, falls through to its
    /// `hasKey .existingData` branch, and preserves the dev values. If that
    /// ever became mint-on-missing, a plain `up` would rotate the credential
    /// and this arm would have to change with it.
    ///
    /// Inverting the `dev == false` arm fails here.
    #[test]
    fn a_non_dev_up_is_never_refused_whatever_the_release_recorded() {
        // The guard is a pure function precisely so it can be pinned here.
        // `run_prepared_up`, its single call site, is `async` and shells out to
        // helm, so no unit test drives that call site end to end -- worth
        // stating rather than leaving implied, exactly as the #1124 tests above
        // do. These tests pin the DECISION; the live evidence that a refused
        // `--dev` mutates nothing (`helm history` shows no new revision) is the
        // E2E arm's.
        let sealed = serde_json::json!({"api": {"githubToken": "gh"}});
        guard_dev_defaults_flip(false, Some(&sealed), &[])
            .expect("a plain `cluster up` over a sealed release must never be refused");

        let dev_release = serde_json::json!({"security": {"allowDevDefaults": true}});
        guard_dev_defaults_flip(false, Some(&dev_release), &[])
            .expect("a plain `cluster up` over a DEV release must never be refused either");

        guard_dev_defaults_flip(false, None, &[])
            .expect("a fresh non-dev install is never refused");
    }

    /// #1145 / G3, AC3: `--dev` on a fresh namespace is the supported case and
    /// the whole point of the flag. `existing == None` means helm positively
    /// reported "release: not found" -- and it is also exactly what `--dry-run`
    /// produces (`should_read_existing(_, true)` is `false`), so a
    /// `--dev --dry-run` plan is never refused either.
    #[test]
    fn dev_is_allowed_when_there_is_no_existing_release() {
        guard_dev_defaults_flip(true, None, &[])
            .expect("`--dev` on a fresh namespace is the supported case (AC3)");
    }

    /// #1145 / G4: a real release that recorded no user-supplied values yields
    /// `Some(Value::Null)`, not `None` -- `fetch_existing_values` returns
    /// `None` only when helm positively reports "release: not found". It is
    /// still an existing release whose Secret the flip would rewrite, so it
    /// must be refused. A guard written as a bare `existing.is_none()` check
    /// gets this exact case wrong.
    #[test]
    fn an_existing_release_with_no_recorded_values_is_still_an_existing_release() {
        let err = guard_dev_defaults_flip(true, Some(&serde_json::Value::Null), &[])
            .expect_err("Some(Value::Null) is an existing release and must be refused");
        assert_eq!(crate::exit::classify(&err).0, crate::exit::ExitClass::Usage);
    }

    /// #1145 / G5, G6: an idempotent `--dev` re-run over a release that is
    /// already on dev defaults is allowed, in BOTH shapes helm can record the
    /// key in -- the JSON boolean that `--set security.allowDevDefaults=true`
    /// produces, and the JSON string that `--set-string` or a quoted
    /// `curie.yaml` `set:` map produces (#1375). The boolean case is also the
    /// retry-after-a-failed-`--dev`-install case, since helm records the values
    /// of an install that failed partway.
    #[test]
    fn dev_reruns_over_a_release_already_on_dev_defaults_are_allowed() {
        let json_bool = serde_json::json!({"security": {"allowDevDefaults": true}});
        guard_dev_defaults_flip(true, Some(&json_bool), &[])
            .expect("an idempotent `--dev` re-run over a dev release must be allowed");

        let json_string = serde_json::json!({"security": {"allowDevDefaults": "true"}});
        guard_dev_defaults_flip(true, Some(&json_string), &[]).expect(
            "the `--set-string` / quoted `curie.yaml` spelling records the STRING \"true\" \
             and must read as dev-on too (#1375)",
        );
    }

    /// #1145 / G7-G10: every recorded shape the chart's
    /// `eq (toString .root.Values.security.allowDevDefaults) "true"` reads as
    /// OFF must be refused, and every refusal must be Usage class (exit 2) --
    /// a deterministic input error, not a runtime failure (AC5).
    ///
    /// `{}` with the key absent is the #1145 defect itself. The quoted
    /// `"false"` mirrors `charts/curie/ci/render-assertions.sh:247-258`, which
    /// asserts that spelling fails closed: the CLI must not read as dev-on what
    /// the chart reads as off. `"TRUE"`, a bare `security` map, a number and a
    /// null leaf pin the fail-closed default against a reader that treats any
    /// non-empty string (or any present key) as truthy.
    #[test]
    fn dev_refuses_every_recorded_shape_the_chart_reads_as_off() {
        for existing in [
            serde_json::json!({"security": {"allowDevDefaults": false}}),
            serde_json::json!({"security": {"allowDevDefaults": "false"}}),
            serde_json::json!({}),
            serde_json::json!({"security": {"allowDevDefaults": "TRUE"}}),
            serde_json::json!({"security": {}}),
            serde_json::json!({"security": {"allowDevDefaults": 1}}),
            serde_json::json!({"security": {"allowDevDefaults": null}}),
        ] {
            let refused = guard_dev_defaults_flip(true, Some(&existing), &[]);
            assert!(
                refused.is_err(),
                "`--dev` must refuse this recorded shape: {existing}"
            );
            let err = refused.unwrap_err();
            assert_eq!(
                crate::exit::classify(&err).0,
                crate::exit::ExitClass::Usage,
                "a refused `--dev` is a deterministic input error (exit 2), not a runtime \
                 failure: {existing}"
            );
        }
    }

    /// #1145 / AC2: the refusal has to SAY WHY, and the part an operator cannot
    /// recover from without being told is that the PVC-backed Postgres and
    /// RustFS data still holds the ORIGINAL generated credentials -- so the
    /// flip breaks authentication rather than merely reconfiguring. Asserted on
    /// the concept, case-insensitively, rather than by pinning a whole sentence
    /// that will churn as the wording improves.
    ///
    /// A fix hint must ride along (`classify(&err).1`), steering to a re-run
    /// without `--dev` or a teardown first.
    #[test]
    fn the_refusal_explains_the_pvc_held_original_credentials_and_carries_a_fix() {
        let existing = serde_json::json!({"api": {"githubToken": "gh"}});
        let err = guard_dev_defaults_flip(true, Some(&existing), &[])
            .expect_err("`--dev` over a sealed release is the #1145 defect and must be refused");

        let (class, fix) = crate::exit::classify(&err);
        assert_eq!(class, crate::exit::ExitClass::Usage);
        assert!(
            fix.is_some(),
            "a refused `--dev` must carry a fix hint: the operator has to be told what to \
             run instead: {err}"
        );

        let message = err.to_string().to_lowercase();
        assert!(
            message.contains("pvc"),
            "the refusal must name the PVC-backed store data as why the flip breaks: {err}"
        );
        assert!(
            message.contains("original"),
            "the refusal must say the store data still holds the ORIGINAL credentials: {err}"
        );
        assert!(
            message.contains("--dev"),
            "the refusal must name the flag it is refusing: {err}"
        );
    }

    /// #1145: `--set security.allowDevDefaults=true` stays a deliberately
    /// unguarded operator escape hatch, but on an existing release it is
    /// DESTRUCTIVE advice and must never be advertised in operator-facing text
    /// -- not in the message and not in the fix hint. It belongs in the guard's
    /// doc comment only. This fails loudly if someone "helpfully" adds it.
    #[test]
    fn the_refusal_never_advertises_the_dev_defaults_escape_hatch() {
        let existing = serde_json::json!({"security": {"allowDevDefaults": false}});
        let err = guard_dev_defaults_flip(true, Some(&existing), &[]).expect_err("must refuse");
        let (_, fix) = crate::exit::classify(&err);

        let operator_text = format!("{err}\n{}", fix.unwrap_or_default()).to_lowercase();
        assert!(
            !operator_text.contains("allowdevdefaults"),
            "the refusal leaked the escape hatch into operator-facing text: {operator_text}"
        );
    }

    /// #1145 follow-up: an operator who explicitly supplies
    /// `security.allowDevDefaults` through `--set` or `--set-string` OWNS the
    /// effective value, so the guard must not refuse the run.
    ///
    /// `up_value_plan` emits the CLI's own `security.allowDevDefaults=true`
    /// FIRST when `--dev` is set and appends the operator's own expressions
    /// AFTER it. Helm is last-wins, so `--dev --set
    /// security.allowDevDefaults=false` actually renders the chart with the
    /// flag OFF and preserves the credentials the release already recorded --
    /// a safe run the guard used to refuse, and refuse with a message that
    /// misdescribed what was about to happen. The `=true` spelling is the
    /// documented unguarded escape hatch and stays open too. Both match this
    /// file's standing rule that an operator `--set` always wins.
    ///
    /// Every lane `UpOpts::operator_sets` can deliver the key through is
    /// covered: repeated `--set`, helm's comma-joined `a=1,b=2` form (which
    /// `operator_set_keys` also parses), and `--set-string`.
    #[test]
    fn an_explicit_operator_allow_dev_defaults_set_is_never_refused() {
        // Non-empty and recording no `security.allowDevDefaults`: a SEALED
        // release, which is exactly the shape the guard refuses on its own.
        let sealed = serde_json::json!({"security": {"gvisor": {"mode": "off"}}});

        for sets in [
            vec!["security.allowDevDefaults=false".to_string()],
            vec!["security.allowDevDefaults=true".to_string()],
            // Helm's comma-joined form, with the key in a non-leading position.
            vec!["api.replicas=2,security.allowDevDefaults=false".to_string()],
            vec!["security.gvisor.mode=off,security.allowDevDefaults=true".to_string()],
            // An unrelated override ahead of the explicit one.
            vec![
                "api.githubToken=x".to_string(),
                "security.allowDevDefaults=false".to_string(),
            ],
        ] {
            guard_dev_defaults_flip(true, Some(&sealed), &sets).unwrap_or_else(|error| {
                panic!(
                    "an explicit operator `security.allowDevDefaults` override owns the \
                     effective value and must not be refused: {sets:?}: {error}"
                )
            });
        }

        // The `--set-string` lane, threaded through `UpOpts::operator_sets()`
        // itself rather than a hand-built Vec, so this pins the real wiring:
        // `operator_sets` chains `--set` THEN `--set-string`, and a key the
        // operator supplied only through the latter must exempt the run too.
        let opts = UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: true,
            no_expose: false,
            set: vec![],
            set_string: vec!["security.allowDevDefaults=false".into()],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        };
        guard_dev_defaults_flip(true, Some(&sealed), &opts.operator_sets()).expect(
            "an explicit `--set-string security.allowDevDefaults=false` must exempt the run \
             too: `operator_sets()` chains both lanes",
        );
    }

    /// #1145 follow-up, mutation resistance: the exemption is keyed to the ONE
    /// key the operator overrode, NOT to "any `--set` was passed". A run
    /// carrying only unrelated overrides still leaves the CLI's own
    /// `security.allowDevDefaults=true` as helm's last word, so it is still the
    /// destructive flip and must still be refused, Usage class (exit 2).
    ///
    /// Widening the exemption to `!operator_sets.is_empty()` -- the easy way to
    /// write it -- fails here, and would in practice disable the guard for
    /// almost every real `cluster up`, since those nearly always carry
    /// overrides. Near-miss keys are included so a substring or bare-leaf match
    /// cannot stand in for the real dotted key either.
    #[test]
    fn unrelated_operator_sets_do_not_exempt_a_dev_flip() {
        let sealed = serde_json::json!({"security": {"gvisor": {"mode": "off"}}});

        for sets in [
            vec!["api.githubToken=x".to_string()],
            vec!["security.gvisor.mode=off,api.replicas=2".to_string()],
            // Same prefix, different leaf.
            vec!["security.allowDevDefaultsExtra=false".to_string()],
            // The bare leaf name, without the `security.` path helm records.
            vec!["allowDevDefaults=false".to_string()],
        ] {
            let refused = guard_dev_defaults_flip(true, Some(&sealed), &sets);
            assert!(
                refused.is_err(),
                "only an explicit `security.allowDevDefaults` override exempts a `--dev` \
                 flip over a sealed release: {sets:?}"
            );
            let err = refused.unwrap_err();
            assert_eq!(
                crate::exit::classify(&err).0,
                crate::exit::ExitClass::Usage,
                "a refused `--dev` stays a deterministic input error (exit 2): {sets:?}"
            );
        }
    }

    /// #1145: the reader mirrors the chart's
    /// `eq (toString .root.Values.security.allowDevDefaults) "true"`
    /// (`charts/curie/templates/_helpers.tpl:453`) and the fail-closed contract
    /// `charts/curie/ci/render-assertions.sh:247-258` pins. ONLY the JSON
    /// boolean `true` and the JSON string `"true"` read as dev-on; every other
    /// shape -- including a plausible-looking `"TRUE"`, a number, an object, a
    /// null leaf, a missing leaf and a missing intermediate segment -- fails
    /// closed to dev-off, continuing #1375's rule that a gate-shaped value of
    /// ambiguous spelling must fail closed.
    #[test]
    fn lookup_dotted_flag_mirrors_the_charts_tostring_coercion() {
        const KEY: &str = "security.allowDevDefaults";

        for on in [
            serde_json::json!({"security": {"allowDevDefaults": true}}),
            serde_json::json!({"security": {"allowDevDefaults": "true"}}),
        ] {
            assert!(lookup_dotted_flag(&on, KEY), "must read as dev-on: {on}");
        }

        for off in [
            serde_json::json!({"security": {"allowDevDefaults": false}}),
            serde_json::json!({"security": {"allowDevDefaults": "false"}}),
            serde_json::json!({"security": {"allowDevDefaults": "TRUE"}}),
            serde_json::json!({"security": {"allowDevDefaults": ""}}),
            serde_json::json!({"security": {"allowDevDefaults": 1}}),
            serde_json::json!({"security": {"allowDevDefaults": {"enabled": true}}}),
            serde_json::json!({"security": {"allowDevDefaults": null}}),
            serde_json::json!({"security": {}}),
            serde_json::json!({}),
            serde_json::json!({"security": "true"}),
            serde_json::json!({"other": {"allowDevDefaults": true}}),
            serde_json::Value::Null,
        ] {
            assert!(!lookup_dotted_flag(&off, KEY), "must fail closed: {off}");
        }
    }

    /// #1145: WHY this sibling reader exists rather than reusing
    /// `lookup_dotted`. Helm records `--set security.allowDevDefaults=true` as
    /// a JSON BOOLEAN, and `lookup_dotted` ends in `as_str()`, so it returns
    /// `None` for exactly the shape this key normally has. "Simplifying" the
    /// guard back onto `lookup_dotted` would make every idempotent `--dev`
    /// re-run (G5) refuse, silently reintroducing #1145's class of surprise --
    /// so this test states the structural gap rather than trusting a comment.
    #[test]
    fn lookup_dotted_cannot_read_the_json_boolean_this_flag_reader_exists_for() {
        let doc = serde_json::json!({"security": {"allowDevDefaults": true}});
        assert_eq!(
            lookup_dotted(&doc, "security.allowDevDefaults"),
            None,
            "if this ever returns Some, re-check whether the sibling reader is still needed"
        );
        assert!(
            lookup_dotted_flag(&doc, "security.allowDevDefaults"),
            "the boolean-aware reader is the one that can see helm's recorded shape"
        );
    }

    #[test]
    fn github_token_and_generated_secrets_ride_separate_values_files_that_merge() {
        // The shape a real sealed (`!dev`) `cluster up` actually emits, which no
        // other test covers: TWO `SecretValuesFile` args whose bodies both carry
        // a top-level `api` map -- `{"api":{"githubToken":...}}` from the
        // credential arm and `{"api":{"apiKey":...}}` from the generated/preserved
        // chart secrets. That is only correct because helm's `mergeMaps`
        // DEEP-merges successive `-f` files rather than replacing them
        // ("Priority is given to the last (right-most) file specified",
        // `helm install --help`, helm v3; values files are merged, not
        // overwritten, per helm's Values Files docs). A change that flattened
        // these into one map or made a later file replace an earlier one would
        // silently drop either `api.apiKey` (breaking platform auth) or the
        // credential, so both keys must survive to their own file and neither
        // may reach argv.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Set(GH_SENTINEL.into()),
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![
                ("api.apiKey".into(), "generated-api-key".into()),
                (
                    "api.githubWebhookSecret".into(),
                    "generated-webhook-secret".into(),
                ),
            ],
            dev: false,
            no_expose: true,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });

        // Both credentials are masked in the printed form, neither is raw.
        let line = cmds[0].display();
        assert!(
            !line.contains(GH_SENTINEL),
            "token leaked into display: {line}"
        );
        assert!(
            !line.contains("generated-api-key"),
            "api key leaked into display: {line}"
        );
        assert!(line.contains(GH_MASKED), "{line}");
        assert!(line.contains("api.apiKey=generate***"), "{line}");

        let (materialized, _guards) = cmds[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(
            !argv.contains(GH_SENTINEL),
            "token leaked into argv: {argv}"
        );
        assert!(
            !argv.contains("generated-api-key"),
            "api key leaked into argv: {argv}"
        );

        // Two distinct `-f` files, not one: the token must not be folded into
        // the generated-secret map (that would put it in the same document helm
        // already had, losing the separation) and neither may be dropped.
        let bodies = secret_values_file_bodies(&materialized);
        assert_eq!(
            bodies.len(),
            2,
            "a sealed up with a credential must emit exactly two values files: {bodies:?}"
        );
        let token_file = bodies
            .iter()
            .find(|body| body.contains(GH_SENTINEL))
            .unwrap_or_else(|| panic!("no values file carries the GitHub credential: {bodies:?}"));
        let secrets_file = bodies
            .iter()
            .find(|body| body.contains("generated-api-key"))
            .unwrap_or_else(|| panic!("no values file carries the generated secrets: {bodies:?}"));
        assert_ne!(
            token_file, secrets_file,
            "the credential and the generated secrets must stay in separate files"
        );
        assert!(
            secrets_file.contains("generated-webhook-secret"),
            "the generated-secret file lost a key: {secrets_file}"
        );
        // Each nests under `api`, which is exactly why the deep merge matters.
        for body in [token_file, secrets_file] {
            let doc: serde_json::Value =
                serde_json::from_str(body).expect("each values file is a JSON document helm reads");
            assert!(
                doc.get("api").and_then(|v| v.as_object()).is_some(),
                "expected a top-level `api` map: {body}"
            );
        }
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(token_file).unwrap()["api"]["githubToken"],
            serde_json::Value::String(GH_SENTINEL.to_string())
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(secrets_file).unwrap()["api"]["apiKey"],
            serde_json::Value::String("generated-api-key".to_string())
        );
    }

    #[test]
    fn explicit_flag_replaces_the_recorded_value() {
        // AC4. An explicit value is a rotation; it must win over the record.
        let existing = serde_json::json!({"api": {"githubToken": "ghp-recorded"}});
        assert_eq!(
            resolve_github_token(Some(&existing), &[], Some("ghp-new"), false),
            GithubTokenPlan::Set("ghp-new".to_string())
        );
    }

    #[test]
    fn explicit_clear_writes_an_empty_value() {
        // AC4. The clear is the EMPTY string, which is not secret: it rides as a
        // visible plain `--set` (the shape `comms --disconnect` writes) so the
        // operator sees the removal in the preview instead of inferring it from
        // an absence, and so the release records the empty value.
        let existing = serde_json::json!({"api": {"githubToken": GH_SENTINEL}});
        assert_eq!(
            resolve_github_token(Some(&existing), &[], None, true),
            GithubTokenPlan::Clear
        );

        // Assert on argv, which is what helm actually receives.
        let cmds = up_with_github_token(GithubTokenPlan::Clear);
        let argv = cmds[0].argv();
        let pos = argv
            .iter()
            .position(|a| a == "api.githubToken=")
            .unwrap_or_else(|| panic!("no empty api.githubToken assignment in argv: {argv:?}"));
        assert!(pos > 0, "the assignment has no preceding flag: {argv:?}");
        assert_eq!(argv[pos - 1], "--set", "{argv:?}");
        assert!(
            !argv.join(" ").contains(GH_SENTINEL),
            "a clear must carry no token at all: {argv:?}"
        );
    }

    #[test]
    fn clear_then_plain_up_stays_cleared() {
        // AC4. The real state chain: a clear writes `""` into the release, and
        // the next plain `cluster up` reads that record back. If an empty record
        // preserved as a value the clear would be one-shot; if it were treated as
        // a credential the operator would see it re-applied.
        assert_eq!(
            resolve_github_token(
                Some(&serde_json::json!({"api": {"githubToken": GH_SENTINEL}})),
                &[],
                None,
                true
            ),
            GithubTokenPlan::Clear
        );
        assert_eq!(
            resolve_github_token(
                Some(&serde_json::json!({"api": {"githubToken": ""}})),
                &[],
                None,
                false
            ),
            GithubTokenPlan::Untouched
        );
    }

    #[test]
    fn empty_value_preserves_and_never_clears() {
        // AC4 negative, the destructive-ambiguity pin. `export
        // CURIE_GITHUB_TOKEN="$UNSET_VAR"` is a routine shell accident and clap
        // hands an exported-but-empty variable through as `Some("")`, not `None`.
        // An ambiguous signal must never trigger the sticky destructive state:
        // destroying a live credential requires the unambiguous
        // `--clear-github-token`.
        let existing = serde_json::json!({"api": {"githubToken": "ghp-recorded"}});
        assert_eq!(
            resolve_github_token(Some(&existing), &[], Some(""), false),
            GithubTokenPlan::Set("ghp-recorded".to_string())
        );
        // With nothing recorded an empty value is simply absence, not a clear.
        assert_eq!(
            resolve_github_token(None, &[], Some(""), false),
            GithubTokenPlan::Untouched
        );
    }

    #[test]
    fn set_passthrough_leaves_the_key_to_the_operator() {
        // AC4. A key the operator pinned through `--set` is theirs to own, and
        // the CLI supplies nothing for it -- matching
        // `operator_set_secret_is_left_to_the_operator`. (Passing BOTH is
        // rejected earlier by `check_github_token_conflict`.)
        let existing = serde_json::json!({"api": {"githubToken": "ghp-recorded"}});
        assert_eq!(
            resolve_github_token(
                Some(&existing),
                &["api.githubToken=ghp-operator".to_string()],
                None,
                false
            ),
            GithubTokenPlan::Untouched
        );
    }

    #[test]
    fn lone_set_passthrough_leak_is_detected() {
        // AC2's one mitigation on the surviving leak path. A lone `--set
        // api.githubToken=<value>` stays legal (the pass-through is verbatim by
        // design and breaking it would break existing operators), but it lands
        // the complete token in the process table, shell history and the printed
        // plan, so `up` steers the operator to the private input.
        //
        // Named for DETECTION, not for the warning: this pins the predicate
        // `up()` gates that `ui.warn` on, and nothing more. The `ui.warn` call
        // site itself is unguarded -- deleting it leaves this green -- because
        // `crate::ui::ui()` is a process-global `OnceLock<Ui>` writing straight
        // to `anstream::stderr()` with no capture seam, and the warning lives
        // inside async `up()` behind a live `helm get values`. Covering the call
        // site needs either a `Ui` capture seam or the note/warn set lifted into
        // a pure function returning the lines, both production changes.
        assert!(set_passthrough_leaks_github_token(&[
            "api.githubToken=ghp-operator".to_string()
        ]));
        // The comma-joined form helm also accepts leaks identically.
        assert!(set_passthrough_leaks_github_token(&[
            "worker.replicas=2,api.githubToken=ghp-operator".to_string()
        ]));
        // An empty assignment is the operator clearing the key by hand. Nothing
        // leaks, so warning about it would be noise on a correct command.
        assert!(!set_passthrough_leaks_github_token(&[
            "api.githubToken=".to_string()
        ]));
        // And an unrelated `--set` must stay silent.
        assert!(!set_passthrough_leaks_github_token(&[
            "worker.replicas=2".to_string()
        ]));
        assert!(!set_passthrough_leaks_github_token(&[]));
    }

    #[test]
    fn set_passthrough_shell_noise_still_leaks_but_a_respaced_key_is_a_different_key() {
        // The whitespace contract on the `--set` grammar, which decides both
        // which invocations warn AND (through the same
        // `operator_set_entries` parse) which ones
        // `check_github_token_conflict` rejects.
        //
        // The two trims are asymmetric ON PURPOSE, and this test exists so a
        // future reader does not "clean it up" into symmetry: the pair
        // reproduces, exactly, the behavior of the two hand-rolled parsers the
        // shared `operator_set_entries` replaced. Whitespace AROUND an
        // assignment is shell noise -- `--set " api.githubToken=ghp-x "` is the
        // same command an operator meant to type, and the token leaks either
        // way -- while whitespace INSIDE the key is not noise: helm reads
        // `api.githubToken ` as a differently-named value that this credential
        // does not ride on, so there is nothing to warn about.

        // Leading noise on the key is stripped: the token still leaks.
        assert!(set_passthrough_leaks_github_token(&[
            " api.githubToken=ghp-x".to_string()
        ]));
        // Noise on BOTH ends of the assignment, the shape a real shell produces.
        assert!(set_passthrough_leaks_github_token(&[
            " api.githubToken=ghp-x ".to_string()
        ]));
        // Trailing noise on a non-empty value does not make it empty: still a leak.
        assert!(set_passthrough_leaks_github_token(&[
            "api.githubToken=ghp-x   ".to_string()
        ]));
        // Whitespace BEFORE the `=` names a different helm key. Nothing this
        // credential rides on is being set, so nothing leaks. This is the case
        // that dies if the key's `trim_start` is widened to a `trim`.
        assert!(!set_passthrough_leaks_github_token(&[
            "api.githubToken =ghp-x".to_string()
        ]));
        // A whitespace-only value is an empty assignment, not a credential.
        assert!(!set_passthrough_leaks_github_token(&[
            "api.githubToken=   ".to_string()
        ]));

        // The sibling reader trims the key SYMMETRICALLY
        // (`operator_set_keys`), so `api.githubToken =x` is a key the CLI leaves
        // to the operator and a value the conflict guard rejects, even though it
        // raises no leak warning. Recorded here so the divergence between the two
        // consumers of one parse is deliberate rather than discovered later.
        assert!(
            operator_set_keys(&["api.githubToken =ghp-x".to_string()]).contains(GITHUB_TOKEN_KEY)
        );
        assert!(
            check_github_token_conflict(
                Some(GH_SENTINEL),
                false,
                &["api.githubToken =ghp-x".to_string()]
            )
            .is_err(),
            "the conflict guard matches the key trimmed on both ends"
        );
    }
}
