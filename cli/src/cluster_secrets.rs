//! Cluster-tier per-agent connector secret binding (#1488, ADR-0009).
//!
//! Values go to the per-agent Helm Secret through a private values file (never
//! argv). The agent record stores a names-only placeholder so the worker can
//! route claims to the per-agent pool without keeping the secret material in
//! Postgres. Rotation deletes SandboxClaims labeled for that agent; sandbox
//! pods are not Deployments, so there is no `rollout restart` of claimed
//! sandboxes.
//!
//! The same bind carries the agent's layered runner image (#3260): the digest
//! `connectors.lock.yaml` records lands in `agentSandbox.runnerImages.<agent>`,
//! and a bundle with no runner entry clears an earlier value.

#![deny(clippy::let_underscore_must_use, clippy::let_underscore_untyped)]

use std::collections::BTreeMap;

use anyhow::{bail, Result};

use crate::docker::CONNECTOR_AGENT_LABEL_KEY;
use crate::ops::{plain, require_on_path, run_step, CmdArg, CommonOpts, OpsCommand};

/// Placeholder stored on the agent record at the cluster tier. Non-empty so the
/// API validator accepts it; not the secret material. The k8s substrate strips
/// these keys off the claim; the template secretKeyRef delivers the real value.
pub const CLUSTER_SECRET_PLACEHOLDER: &str = "secretKeyRef";

/// Chart agent names match templates/agent-sandbox.yaml.
fn validate_agent_resource_name(agent: &str) -> Result<()> {
    // `self` is reserved for `admits` (ADR-0168 decision 7), where it means
    // the agent this bundle is deployed as. A real agent named `self` would
    // be indistinguishable from that sentinel wherever `admits` is resolved.
    if agent == "self" {
        bail!(
            "agent name \"self\" is not valid -- it is reserved for `admits`, where it means \
             the agent this bundle is deployed as"
        );
    }
    let valid = agent.len() <= 40
        && agent
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && agent.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
        && agent.ends_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit());
    if !valid {
        bail!(
            "agent name {agent:?} is not a valid per-agent Secret key (lowercase DNS label, max 40 characters)"
        );
    }
    Ok(())
}

/// Resolve `--secret` NAMES the same way `deploy` does: env, then the host vault.
pub fn resolve_named_secrets(names: &[String]) -> Result<BTreeMap<String, String>> {
    let mut secrets = BTreeMap::new();
    for name in names {
        let value = std::env::var(name)
            .ok()
            .filter(|v| !v.is_empty())
            .or(crate::secrets::get_value(name)?);
        match value {
            Some(v) => {
                secrets.insert(name.clone(), v);
            }
            None => {
                return Err(crate::exit::usage(format!(
                    "--secret {name}: not set in the environment and not saved in Curie \
                     storage; export it or run `curie secrets set {name}` first"
                )));
            }
        }
    }
    Ok(secrets)
}

/// Names-only placeholder map written to the agent record at the cluster tier,
/// built from NAMES that have no resolved value on this box.
///
/// The cluster tier's agent record is placeholders either way -- the value
/// lives in the per-agent Helm Secret, never in the record -- so a connector
/// secret whose value is resolved later, cluster-scoped (#1913), still belongs
/// in it. It has to: the worker keys `inject_connector_secrets` off this map,
/// and `sandbox.types` routes the claim to the per-agent pool only when the
/// marker is present. A record built from `--secret` alone routes the pod to
/// the generic pool with no connector secret env at all (#2503).
pub fn agent_record_secret_names<'a, I>(names: I) -> BTreeMap<String, String>
where
    I: IntoIterator<Item = &'a String>,
{
    names
        .into_iter()
        .map(|name| (name.clone(), CLUSTER_SECRET_PLACEHOLDER.to_string()))
        .collect()
}

/// Dotted helm keys for `agentSandbox.connectorSecrets.<agent>.<NAME>`.
/// Connector secret names that must never be bound into a sandbox.
///
/// `E2E_CLUSTER_KUBECONFIG` is the test cluster credential (ADR 0176). The
/// connector pod receives it from the connector Secret. The sandbox bind map
/// does not. The spelling is frozen in `tests/vectors/e2e-connector-sandbox.json`.
pub const SANDBOX_WITHHELD_CONNECTOR_SECRETS: &[&str] = &["E2E_CLUSTER_KUBECONFIG"];

pub fn sandbox_connector_secrets(secrets: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    secrets
        .iter()
        .filter(|(name, _)| !SANDBOX_WITHHELD_CONNECTOR_SECRETS.contains(&name.as_str()))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

pub fn helm_secret_pairs(
    agent: &str,
    secrets: &BTreeMap<String, String>,
) -> Result<Vec<(String, String)>> {
    validate_agent_resource_name(agent)?;
    Ok(secrets
        .iter()
        .map(|(name, value)| {
            (
                format!("agentSandbox.connectorSecrets.{agent}.{name}"),
                value.clone(),
            )
        })
        .collect())
}

pub struct BindOpts {
    pub common: CommonOpts,
    pub chart: String,
    pub agent: String,
    pub secrets: BTreeMap<String, String>,
    pub runner_image: RunnerImageUpdate,
}

/// What a bind does to `agentSandbox.runnerImages.<agent>` (#3260).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunnerImageUpdate {
    /// Leave the release's value as it is.
    Keep,
    /// Bind this locked digest.
    Set(String),
    /// Clear an earlier value: the bundle no longer declares a runner.
    Clear,
}

/// helm upgrade with a private values file for the secrets and a `--set` for
/// the runner image, then replace the agent's claimed sandboxes so
/// secretKeyRef env and the runner image are re-resolved at pod start.
pub fn bind_commands(opts: &BindOpts) -> Result<Vec<OpsCommand>> {
    let pairs = helm_secret_pairs(&opts.agent, &opts.secrets)?;
    let runner_set = match &opts.runner_image {
        RunnerImageUpdate::Keep => None,
        RunnerImageUpdate::Set(digest) => Some(digest.as_str()),
        RunnerImageUpdate::Clear => Some("null"),
    };
    if pairs.is_empty() && runner_set.is_none() {
        return Ok(Vec::new());
    }
    // --reset-then-reuse-values, not --reuse-values: on helm v3.20 a `null`
    // override under --reuse-values prunes the stored value but the manifest
    // keeps rendering the old key, so a cleared runner image never leaves.
    let mut args = vec![
        plain("upgrade"),
        plain(&opts.common.release),
        plain(&opts.chart),
        plain("-n"),
        plain(&opts.common.namespace),
        plain("--reset-then-reuse-values"),
    ];
    if !pairs.is_empty() {
        args.push(CmdArg::SecretValuesFile(pairs));
    }
    if let Some(value) = runner_set {
        args.push(plain("--set"));
        args.push(plain(format!(
            "agentSandbox.runnerImages.{}={value}",
            opts.agent
        )));
    }
    Ok(vec![
        OpsCommand::new("helm", args),
        retire_claims_command(&opts.common.namespace, &opts.agent),
    ])
}

/// Bind `agent` to `image` as `agentSandbox.runnerImages.<agent>`, creating
/// (or replacing a non-object) value, `agentSandbox`, and `runnerImages` maps.
fn set_runner_image_binding(values: &mut serde_json::Value, agent: &str, image: &str) {
    if !values.is_object() {
        *values = serde_json::json!({});
    }
    let sandbox = values
        .as_object_mut()
        .expect("an object")
        .entry("agentSandbox")
        .or_insert_with(|| serde_json::json!({}));
    if !sandbox.is_object() {
        *sandbox = serde_json::json!({});
    }
    let images = sandbox
        .as_object_mut()
        .expect("an object")
        .entry("runnerImages")
        .or_insert_with(|| serde_json::json!({}));
    if !images.is_object() {
        *images = serde_json::json!({});
    }
    images.as_object_mut().expect("an object").insert(
        agent.to_string(),
        serde_json::Value::String(image.to_string()),
    );
}

/// The release's supplied values with `agent`'s connector-secret binding
/// removed, its runner image updated as `runner_image` says (#3260), and
/// everything else untouched (#3021).
pub fn without_agent_binding(
    release_values: &serde_json::Value,
    agent: &str,
    runner_image: &RunnerImageUpdate,
) -> serde_json::Value {
    let mut values = release_values.clone();
    if let Some(all) = values
        .pointer_mut("/agentSandbox/connectorSecrets")
        .and_then(|all| all.as_object_mut())
    {
        all.remove(agent);
    }
    match runner_image {
        RunnerImageUpdate::Keep => {}
        RunnerImageUpdate::Clear => {
            if let Some(all) = values
                .pointer_mut("/agentSandbox/runnerImages")
                .and_then(|all| all.as_object_mut())
            {
                all.remove(agent);
            }
        }
        RunnerImageUpdate::Set(digest) => set_runner_image_binding(&mut values, agent, digest),
    }
    values
}

/// Remove the agent's binding from the release, then replace its claimed
/// sandboxes (#3021).
///
/// The upgrade replaces the supplied values with `release_values` minus the
/// agent's entry (`--reset-values` plus a private values file), so the chart
/// stops rendering the per-agent Secret and SandboxTemplate and Helm prunes
/// both. A `--reuse-values --set <agent>=null` upgrade is NOT equivalent: Helm
/// drops the key from the stored values but still renders the old objects, so
/// the credential would survive. Retiring the claims afterwards means no
/// running pod keeps the removed credential in its env.
pub fn clear_commands(
    common: &CommonOpts,
    chart: &str,
    agent: &str,
    release_values: &serde_json::Value,
    runner_image: &RunnerImageUpdate,
) -> Result<Vec<OpsCommand>> {
    validate_agent_resource_name(agent)?;
    Ok(vec![
        OpsCommand::new(
            "helm",
            vec![
                plain("upgrade"),
                plain(&common.release),
                plain(chart),
                plain("-n"),
                plain(&common.namespace),
                plain("--reset-values"),
                CmdArg::SecretValuesDocument(without_agent_binding(
                    release_values,
                    agent,
                    runner_image,
                )),
            ],
        ),
        retire_claims_command(&common.namespace, agent),
    ])
}

/// Replace the agent's claimed sandboxes so the next turn starts a fresh pod
/// with the newly deployed bundle and re-resolved secretKeyRef env.
pub(crate) fn retire_claims_command(namespace: &str, agent: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("delete"),
            plain("sandboxclaim"),
            plain("-l"),
            plain(format!("{CONNECTOR_AGENT_LABEL_KEY}={agent}")),
            plain("--wait=true"),
            plain("--ignore-not-found=true"),
        ],
    )
}

/// Whether a bind would change the release at all (#3082).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindNeed {
    /// Every requested name already holds the requested value in the
    /// release's supplied values, so a `helm upgrade` would only re-render the
    /// platform and restart its pods for nothing.
    Current,
    /// Something differs. `secrets` names the connector secrets that are
    /// missing or hold a different value (names only, never values);
    /// `runner_image` is true when the agent's runner image must be set to a
    /// new digest or cleared.
    Changed {
        secrets: Vec<String>,
        runner_image: bool,
    },
    /// The redeploy carries no connector secret for this agent but the release
    /// still binds some (#3021). Leaving them would keep a removed credential
    /// reachable by the agent's runner pods, so the binding is cleared.
    /// `runner_image` is true when the same upgrade must also set or clear the
    /// agent's runner image (#3260).
    Clear { runner_image: bool },
}

/// Pure over the JSON `helm get values -o json` returns for the release.
///
/// `--reuse-values` merges onto exactly these supplied values, so a name whose
/// value already matches here is a no-op for the bind. The runner image
/// changes when a locked digest differs from the release's value, or when no
/// digest is locked and the release still holds one for this agent.
pub fn bind_need(
    release_values: &serde_json::Value,
    agent: &str,
    secrets: &BTreeMap<String, String>,
    runner_image: Option<&str>,
) -> BindNeed {
    let bound = release_values
        .pointer("/agentSandbox/connectorSecrets")
        .and_then(|all| all.get(agent));
    let bound_runner = release_values
        .pointer("/agentSandbox/runnerImages")
        .and_then(|all| all.get(agent))
        .filter(|v| !v.is_null());
    let runner_changed = match runner_image {
        Some(digest) => bound_runner.and_then(|v| v.as_str()) != Some(digest),
        None => bound_runner.is_some(),
    };
    if secrets.is_empty() {
        let still_bound = bound
            .and_then(|b| b.as_object())
            .is_some_and(|b| !b.is_empty());
        return if still_bound {
            BindNeed::Clear {
                runner_image: runner_changed,
            }
        } else if runner_changed {
            BindNeed::Changed {
                secrets: Vec::new(),
                runner_image: true,
            }
        } else {
            BindNeed::Current
        };
    }
    let changed: Vec<String> = secrets
        .iter()
        .filter(|(name, value)| {
            bound
                .and_then(|b| b.get(name.as_str()))
                .and_then(|v| v.as_str())
                != Some(value.as_str())
        })
        .map(|(name, _)| name.clone())
        .collect();
    if changed.is_empty() && !runner_changed {
        BindNeed::Current
    } else {
        BindNeed::Changed {
            secrets: changed,
            runner_image: runner_changed,
        }
    }
}

fn helm_values_command(common: &CommonOpts) -> OpsCommand {
    OpsCommand::new(
        "helm",
        vec![
            plain("get"),
            plain("values"),
            plain(&common.release),
            plain("-n"),
            plain(&common.namespace),
            plain("-o"),
            plain("json"),
        ],
    )
}

/// The release's supplied values, or `None` when the read fails or does not
/// parse.
async fn read_release_values(common: &CommonOpts) -> Result<Option<serde_json::Value>> {
    require_on_path("helm")?;
    let (ok, stdout, _stderr) = crate::ops::run_capture(&helm_values_command(common)).await?;
    Ok(if ok {
        serde_json::from_str::<serde_json::Value>(&stdout).ok()
    } else {
        None
    })
}

/// Judge the bind against the values read, or against their absence. A values
/// read that fails or does not parse cannot prove the bind is a no-op, so
/// every name counts as changed and the caller upgrades exactly as it did
/// before this check existed. The runner image counts as changed too: with
/// none locked, an unread release could still hold an earlier image, and a
/// `null` override of an absent key is harmless, so the clear is sent rather
/// than silently skipped (#3260). With no secret to bind, an unreadable
/// release cannot prove a stale connector binding exists, so that binding is
/// left alone and the operator is told (#3021).
fn need_from_values(
    values: Option<&serde_json::Value>,
    common: &CommonOpts,
    agent: &str,
    secrets: &BTreeMap<String, String>,
    runner_image: Option<&str>,
) -> BindNeed {
    match values {
        Some(values) => bind_need(values, agent, secrets, runner_image),
        None => {
            if secrets.is_empty() {
                crate::ui::ui().note(&format!(
                    "could not read the values of release {}; any existing connector-secret \
                     binding for agent {agent} was left in place",
                    common.release
                ));
            }
            BindNeed::Changed {
                secrets: secrets.keys().cloned().collect(),
                runner_image: true,
            }
        }
    }
}

/// Read the release's supplied values and judge whether binding `secrets`
/// for `agent` changes anything.
pub async fn read_bind_need(
    common: &CommonOpts,
    agent: &str,
    secrets: &BTreeMap<String, String>,
    runner_image: Option<&str>,
) -> Result<BindNeed> {
    validate_agent_resource_name(agent)?;
    let values = read_release_values(common).await?;
    Ok(need_from_values(
        values.as_ref(),
        common,
        agent,
        secrets,
        runner_image,
    ))
}

/// Bind only when the release does not already hold these values (#3082).
///
/// A bundle deploy re-binds its connector secrets every time, and each bind
/// used to be a full `helm upgrade` of the platform release: it re-rendered
/// every template, restarted platform pods, and reverted any out-of-band
/// change. When nothing differs the release is left alone. When something
/// does, the operator is told the platform release is being upgraded and why.
/// `chart` is only awaited on that path, so an unchanged bind never resolves
/// or downloads a chart.
pub async fn bind_if_changed<F>(
    common: CommonOpts,
    agent: String,
    secrets: BTreeMap<String, String>,
    runner_image: Option<String>,
    chart: F,
) -> Result<BindNeed>
where
    F: std::future::Future<Output = Result<String>>,
{
    validate_agent_resource_name(&agent)?;
    let values = read_release_values(&common).await?;
    let need = need_from_values(
        values.as_ref(),
        &common,
        &agent,
        &secrets,
        runner_image.as_deref(),
    );
    let ui = crate::ui::ui();
    match &need {
        BindNeed::Current => {
            if !secrets.is_empty() || runner_image.is_some() {
                ui.note(&format!(
                    "connector secrets and runner image for agent {agent} are already current \
                     on release {}; the platform release was not upgraded",
                    common.release
                ));
                // Sandboxes are not platform pods: the agent's claims are
                // still retired so a running thread picks up the new bundle.
                require_on_path("kubectl")?;
                run_step(
                    &ui.checklist(),
                    &format!("replacing sandboxes for agent {agent}"),
                    "replaced",
                    &retire_claims_command(&common.namespace, &agent),
                )
                .await?;
            }
        }
        BindNeed::Changed {
            secrets: names,
            runner_image: runner_changed,
        } => {
            let mut what = Vec::new();
            if !names.is_empty() {
                what.push(format!("connector secret(s) {}", names.join(", ")));
            }
            let update = runner_image_update(runner_image.as_deref(), *runner_changed);
            match &update {
                RunnerImageUpdate::Set(digest) => {
                    what.push(format!("runner image digest {digest}"))
                }
                RunnerImageUpdate::Clear => what.push("removal of the runner image".to_string()),
                RunnerImageUpdate::Keep => {}
            }
            ui.note(&format!(
                "platform change required: {} for agent {agent} changed, so release {} \
                 is being helm-upgraded to bind it",
                what.join(" and "),
                common.release
            ));
            let chart = chart.await?;
            bind(BindOpts {
                common,
                chart,
                agent,
                secrets,
                runner_image: update,
            })
            .await?;
        }
        BindNeed::Clear {
            runner_image: runner_changed,
        } => {
            ui.note(&format!(
                "platform change required: agent {agent} no longer declares any connector \
                 secret, so release {} is being helm-upgraded to remove its binding",
                common.release
            ));
            let chart = chart.await?;
            require_on_path("kubectl")?;
            let cl = ui.checklist();
            let label = format!(
                "clearing connector secrets for agent {agent} on release {}",
                common.release
            );
            // `Clear` is only judged from values that were read.
            let values = values.unwrap_or_default();
            let update = runner_image_update(runner_image.as_deref(), *runner_changed);
            for cmd in &clear_commands(&common, &chart, &agent, &values, &update)? {
                run_step(&cl, &label, "cleared", cmd).await?;
            }
        }
    }
    Ok(need)
}

/// The runner image change a bind applies, from the locked digest and whether
/// the release's value differs from it (#3260).
fn runner_image_update(runner_image: Option<&str>, changed: bool) -> RunnerImageUpdate {
    match (runner_image, changed) {
        (Some(digest), true) => RunnerImageUpdate::Set(digest.to_string()),
        (None, true) => RunnerImageUpdate::Clear,
        (_, false) => RunnerImageUpdate::Keep,
    }
}

pub async fn bind(opts: BindOpts) -> Result<()> {
    let cmds = bind_commands(&opts)?;
    if cmds.is_empty() {
        return Ok(());
    }
    require_on_path("helm")?;
    require_on_path("kubectl")?;
    let ui = crate::ui::ui();
    let cl = ui.checklist();
    let label = format!(
        "binding sandbox values for agent {} on release {}",
        opts.agent, opts.common.release
    );
    for cmd in &cmds {
        run_step(&cl, &label, "bound", cmd).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The installation's runner and a layered runner's base (#3218, ADR 0173 d5)
// ---------------------------------------------------------------------------
//
// A layered runner is built on one exact platform runner, recorded in the
// bundle's lock as `runner.base`. The worker serves the runner its own release
// ships, so a layer on any other base is a runner the worker may not serve.
// Two checks keep them together, and both compare by the sha256 digest only,
// because the same image can be spelled with different repositories:
//
// - `curie cluster deploy` refuses a bundle whose recorded base is not the
//   installation's runner ([`check_layered_runner_base`]).
// - `curie cluster upgrade` names every agent in
//   `agentSandbox.runnerImages` whose layer will stop matching, and in the
//   same `helm upgrade` clears those entries (`=null`, as
//   [`RunnerImageUpdate::Clear`] does) so the agent runs the new platform
//   runner the worker serves until its owner rebuilds and redeploys
//   ([`layers_stopping_to_match`]). Leaving the old layer bound would run an
//   old runner under a new worker; refusing the upgrade would let one bundle
//   owner block every platform upgrade.
//   A binding to the project's own published dark factory layer is instead
//   rebound to the layer published for the target version when that layer's
//   base is the target runner ([`rebind_runner_image_bindings`], #4321), and
//   cleared with the stock remedy otherwise.
//
// When the installation's runner cannot be determined, deploy refuses and
// upgrade treats every layered agent as affected: neither passes silently.

/// The runner reference `agentSandbox.runner` renders, from the release's
/// computed values (`helm get values --all`) and the release chart's
/// appVersion. A digest wins over a tag, and an empty tag means the chart
/// appVersion, exactly as the chart's helper renders it. `None` when the
/// values name no image, or no tag and no appVersion is known.
pub fn effective_runner_ref(
    values: &serde_json::Value,
    app_version: Option<&str>,
) -> Option<String> {
    let runner = values.pointer("/agentSandbox/runner")?;
    let field = |name: &str| {
        runner
            .get(name)
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|v| !v.is_empty())
    };
    let image = field("image")?;
    if let Some(digest) = field("digest") {
        return Some(format!("{image}@{digest}"));
    }
    let tag = field("tag").or(app_version.map(str::trim).filter(|v| !v.is_empty()))?;
    Some(format!("{image}:{tag}"))
}

/// The `sha256:<hex>` a digest-pinned reference carries, if any.
pub fn reference_digest(reference: &str) -> Option<&str> {
    reference
        .split_once('@')
        .map(|(_, digest)| digest)
        .filter(|digest| digest.starts_with("sha256:"))
}

/// Whether two digest-pinned references name the same image. The repository
/// is ignored on purpose: a mirror or a renamed repo serves the same bytes.
/// A reference with no digest never matches, since it proves nothing.
pub fn same_runner(a: &str, b: &str) -> bool {
    matches!((reference_digest(a), reference_digest(b)), (Some(x), Some(y)) if x == y)
}

/// The release's layered runner bindings: every non-blank string
/// `agentSandbox.runnerImages.<agent>`, as `agent -> image` in name order.
pub fn layered_bindings(values: &serde_json::Value) -> BTreeMap<String, String> {
    values
        .pointer("/agentSandbox/runnerImages")
        .and_then(|all| all.as_object())
        .map(|all| {
            all.iter()
                .filter_map(|(agent, image)| {
                    let image = image.as_str().filter(|s| !s.trim().is_empty())?;
                    Some((agent.clone(), image.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The agents the release binds a layered runner to: every non-null
/// `agentSandbox.runnerImages.<agent>`, in name order.
pub fn layered_agents(values: &serde_json::Value) -> Vec<String> {
    layered_bindings(values).into_keys().collect()
}

/// The layered agents an upgrade from `current` to `target` leaves on a base
/// that is no longer the installation's runner. Both are digest-pinned
/// runner references, `None` when they could not be determined. Only a proven
/// digest match spares them: an unknown on either side affects every layer.
pub fn layers_stopping_to_match(
    layered: &[String],
    current: Option<&str>,
    target: Option<&str>,
) -> Vec<String> {
    match (current, target) {
        (Some(current), Some(target)) if same_runner(current, target) => Vec::new(),
        _ => layered.to_vec(),
    }
}

/// Remove layered runner bindings from retained upgrade values (#3849).
///
/// Deleting the key is the clear. A null entry is not: Helm 3.20 keeps the
/// previous digest when `--set key=null` follows a values file, and a nil
/// entry fails the chart digest guard before a revision is created. Other
/// agents, connector secrets, and unrelated settings stay.
pub fn omit_runner_image_bindings(values: &mut serde_json::Value, agents: &[String]) {
    let Some(images) = values
        .pointer_mut("/agentSandbox/runnerImages")
        .and_then(|node| node.as_object_mut())
    else {
        return;
    };
    for agent in agents {
        images.remove(agent);
    }
    if images.is_empty() {
        if let Some(sandbox) = values
            .pointer_mut("/agentSandbox")
            .and_then(|node| node.as_object_mut())
        {
            sandbox.remove("runnerImages");
        }
    }
}

/// Bind each `(agent, image)` as `agentSandbox.runnerImages.<agent>` in
/// retained upgrade values (#4321), creating the maps when absent. Other
/// agents and unrelated settings stay.
pub fn rebind_runner_image_bindings(values: &mut serde_json::Value, rebinds: &[(String, String)]) {
    for (agent, image) in rebinds {
        set_runner_image_binding(values, agent, image);
    }
}

/// The deploy-time decision: `Ok` when the lock's recorded base is the
/// installation's runner, else the refusal naming the `curie build` that
/// rebuilds the layer on it. `installed` is `(reference, pinned)`: the
/// reference an operator passes to `--runner-image`, and its digest-pinned
/// form, or the reason it could not be determined.
pub fn runner_base_verdict(
    plugin_dir: &std::path::Path,
    recorded_base: &str,
    installed: std::result::Result<(String, String), String>,
) -> Result<()> {
    let (reference, pinned) = match installed {
        Ok(found) => found,
        Err(reason) => {
            let fix = "confirm the release is healthy with `curie cluster status` and that \
                       its runner image resolves in its registry (or pin the runner by digest \
                       with the chart value `agentSandbox.runner.digest`), then redeploy";
            // The human presenter prints only the message, so the fix is
            // composed into it as well as carried for `--json` (#3423).
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(format!(
                    "this bundle's runner layer was built on {recorded_base}, but the \
                     installation's runner could not be determined ({reason}), so the deploy \
                     cannot prove the worker serves that base. Refusing rather than risk an \
                     old runner under a new worker; {fix}."
                ))
                .with_fix(fix),
            ));
        }
    };
    if same_runner(recorded_base, &pinned) {
        return Ok(());
    }
    let fix = format!(
        "run `curie build --plugin-dir {} --registry <ref> --runner-image {reference}` and \
         redeploy",
        plugin_dir.display()
    );
    Err(anyhow::Error::from(
        crate::exit::CliError::usage(format!(
            "this bundle's runner layer was built on {recorded_base}, but the installation \
             runs {pinned}. A layer on another base is a runner this worker may not serve; \
             {fix}."
        ))
        .with_fix(fix),
    ))
}

pub(crate) fn helm_get_json(
    common: &CommonOpts,
    what: &str,
    all: bool,
    revision: u32,
) -> OpsCommand {
    let mut args = vec![
        plain("get"),
        plain(what),
        plain(&common.release),
        plain("-n"),
        plain(&common.namespace),
        plain("--revision"),
        plain(revision.to_string()),
    ];
    if all {
        args.push(plain("--all"));
    }
    args.push(plain("-o"));
    args.push(plain("json"));
    OpsCommand::new("helm", args)
}

/// The revision the release is serving: the newest `deployed` row of
/// `helm history -o json`. A failed or pending upgrade leaves the previous
/// revision deployed, and that one is what the worker runs. With no deployed
/// revision at all, the reason names the newest record so the operator knows
/// which revision to roll back from.
pub(crate) fn serving_revision(
    history: &serde_json::Value,
    release: &str,
) -> std::result::Result<u32, String> {
    let rows = history
        .as_array()
        .ok_or_else(|| format!("`helm history {release}` did not return a list"))?;
    let revision = |row: &serde_json::Value| {
        row.get("revision")
            .and_then(|v| v.as_u64())
            .and_then(|v| u32::try_from(v).ok())
    };
    let status = |row: &serde_json::Value| {
        row.get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string()
    };
    if let Some(deployed) = rows
        .iter()
        .filter(|row| status(row) == "deployed")
        .filter_map(revision)
        .max()
    {
        return Ok(deployed);
    }
    match rows.iter().max_by_key(|row| revision(row)) {
        Some(newest) => Err(format!(
            "release {release} has no deployed revision; its newest, revision {}, is {}. \
             Roll back to a known-good revision with `curie cluster rollback --revision N`",
            revision(newest).map_or_else(|| "?".to_string(), |r| r.to_string()),
            status(newest)
        )),
        None => Err(format!("release {release} has no revisions")),
    }
}

/// Pin a runner reference to its registry manifest digest. A reference that
/// already carries one is returned as it is.
pub async fn pin_runner_reference(reference: &str) -> Result<String> {
    if reference_digest(reference).is_some() {
        return Ok(reference.to_string());
    }
    // Native first (#3503): an operator host with only kubectl and helm has
    // no docker, and an anonymous registry read needs none. Docker stays the
    // fallback for a registry that needs a docker login.
    let native = match crate::oci_registry::fetch_manifest(reference).await {
        Ok(manifest) => {
            return Ok(crate::connector_build::digest_pinned_ref(
                reference,
                &manifest.digest,
            ))
        }
        Err(err) => format!("{err:#}"),
    };
    if !crate::ops::on_path("docker") {
        bail!(
            "could not resolve {reference} to a digest in its registry ({native}), and `docker` \
             is not on PATH to ask with a registry login. Pin the release's runner by digest \
             with the chart value `agentSandbox.runner.digest`, or run from a host where \
             `docker buildx imagetools inspect {reference}` resolves"
        );
    }
    docker_runner_digest(reference)
        .await
        .map(|digest| crate::connector_build::digest_pinned_ref(reference, &digest))
        .map_err(|err| {
            anyhow::anyhow!(
                "could not resolve {reference} to a digest in its registry ({native}), nor with \
                 docker ({err:#})"
            )
        })
}

/// The top-level manifest digest `docker buildx imagetools inspect` reports
/// for `reference`, which honors the host's docker registry logins.
async fn docker_runner_digest(reference: &str) -> Result<String> {
    let inspect = OpsCommand::new(
        "docker",
        vec![
            plain("buildx"),
            plain("imagetools"),
            plain("inspect"),
            plain(reference),
            plain("--format"),
            plain("{{json .Manifest}}"),
        ],
    );
    let (ok, stdout, stderr) = crate::ops::run_capture(&inspect).await?;
    if !ok {
        bail!("{}", stderr.trim());
    }
    let manifest: serde_json::Value = serde_json::from_str(stdout.trim())
        .map_err(|err| anyhow::anyhow!("the manifest of {reference} is malformed: {err}"))?;
    manifest
        .get("digest")
        .and_then(|d| d.as_str())
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("the manifest of {reference} names no digest"))
}

/// The installation's runner as `(reference, pinned)`, read from the
/// release's computed values and chart metadata, or why it could not be.
pub async fn installed_runner(
    common: &CommonOpts,
) -> std::result::Result<(String, String), String> {
    let read = |cmd: OpsCommand| async move {
        match crate::ops::run_capture(&cmd).await {
            Ok((true, out, _)) => serde_json::from_str::<serde_json::Value>(&out)
                .map_err(|err| format!("`{}` returned malformed JSON: {err}", cmd.display())),
            Ok((false, _, err)) => Err(format!("`{}` failed: {}", cmd.display(), err.trim())),
            Err(err) => Err(format!("{err:#}")),
        }
    };
    // A bare `helm get` answers from the newest record, even a failed upgrade
    // whose runner never served (#3421), so read the deployed revision.
    let history = read(crate::ops::helm_history_cmd(common)).await?;
    let revision = serving_revision(&history, &common.release)?;
    let values = read(helm_get_json(common, "values", true, revision)).await?;
    let app_version = read(helm_get_json(common, "metadata", false, revision))
        .await
        .ok()
        .and_then(|m| {
            m.get("appVersion")
                .and_then(|v| v.as_str())
                .map(String::from)
        });
    let reference = effective_runner_ref(&values, app_version.as_deref()).ok_or_else(|| {
        format!(
            "release {} names no agentSandbox.runner image and tag",
            common.release
        )
    })?;
    let pinned = pin_runner_reference(&reference)
        .await
        .map_err(|err| format!("{err:#}"))?;
    Ok((reference, pinned))
}

/// Refuse a cluster deploy whose layered runner was built on anything but the
/// installation's runner (#3218). A bundle without a locked runner layer is
/// untouched and costs no helm read.
pub async fn check_layered_runner_base(
    common: &CommonOpts,
    plugin_dir: &std::path::Path,
) -> Result<()> {
    if crate::connector_build::load(plugin_dir)?.runner.is_none() {
        return Ok(());
    }
    let Some(entry) = crate::connector_build::load_lock(plugin_dir)?.and_then(|lock| lock.runner)
    else {
        return Ok(());
    };
    // A missing runner entry or a local-daemon one is `lock_preflight`'s to
    // refuse, with its own message; this check is about a registry base.
    if entry.delivery != crate::connector_build::Delivery::Registry {
        return Ok(());
    }
    require_on_path("helm")?;
    runner_base_verdict(plugin_dir, &entry.base, installed_runner(common).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secrets() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("GITHUB_PERSONAL_ACCESS_TOKEN".into(), "ghp_agent_a".into()),
            ("JIRA_TOKEN".into(), "jira-a".into()),
        ])
    }

    #[test]
    fn the_e2e_kubeconfig_is_withheld_from_the_sandbox_bind() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../tests/vectors/e2e-connector-sandbox.json");
        let raw = std::fs::read_to_string(path).expect("vector");
        let doc: serde_json::Value = serde_json::from_str(&raw).expect("vector json");
        let key = doc["kubeconfig_secret"]
            .as_str()
            .expect("kubeconfig_secret")
            .to_string();
        let mut values = secrets();
        values.insert(key.clone(), "kubeconfig-sentinel".into());
        let bound = sandbox_connector_secrets(&values);
        assert!(!bound.contains_key(&key));
        assert!(!bound.values().any(|value| value == "kubeconfig-sentinel"));
        assert_eq!(
            bound
                .get("GITHUB_PERSONAL_ACCESS_TOKEN")
                .map(String::as_str),
            Some("ghp_agent_a")
        );
    }

    #[test]
    fn agent_record_keeps_names_and_drops_values() {
        let stored = agent_record_secret_names(secrets().keys());
        assert_eq!(
            stored["GITHUB_PERSONAL_ACCESS_TOKEN"],
            CLUSTER_SECRET_PLACEHOLDER
        );
        assert_eq!(stored["JIRA_TOKEN"], CLUSTER_SECRET_PLACEHOLDER);
        assert!(!stored
            .values()
            .any(|v| v.contains("ghp_") || v.contains("jira-a")));
    }

    #[test]
    fn record_from_names_alone_is_the_same_placeholder_map() {
        // #2503: a connector secret whose value is resolved cluster-scoped
        // later has no value on this box, but the record still has to carry
        // its NAME -- the worker keys `inject_connector_secrets` (and the
        // per-agent sandbox pool routing) off this map. Names-only and
        // value-bearing inputs must produce byte-identical records for the
        // same key set, so the two deploy paths cannot diverge.
        let names: Vec<String> = secrets().keys().cloned().collect();
        let from_names = agent_record_secret_names(names.iter());
        assert_eq!(from_names, agent_record_secret_names(secrets().keys()));
        assert_eq!(
            from_names["GITHUB_PERSONAL_ACCESS_TOKEN"],
            CLUSTER_SECRET_PLACEHOLDER
        );
        assert_eq!(from_names["JIRA_TOKEN"], CLUSTER_SECRET_PLACEHOLDER);
    }

    #[test]
    fn record_from_names_carries_a_name_that_has_no_local_value() {
        // The #2503 case proper: the name reaches the record even though
        // nothing on this box ever resolved a value for it, and no value-like
        // material is invented for it.
        let stored = agent_record_secret_names(["CONNECTOR_ONLY".to_string()].iter());
        assert_eq!(stored.len(), 1);
        assert_eq!(stored["CONNECTOR_ONLY"], CLUSTER_SECRET_PLACEHOLDER);
    }

    #[test]
    fn helm_pairs_are_per_agent_and_keep_values_off_the_other_agent() {
        let a = helm_secret_pairs("acme-a", &secrets()).unwrap();
        let b = helm_secret_pairs(
            "acme-b",
            &BTreeMap::from([("GITHUB_PERSONAL_ACCESS_TOKEN".into(), "ghp_agent_b".into())]),
        )
        .unwrap();
        assert!(a.iter().any(|(k, v)| {
            k == "agentSandbox.connectorSecrets.acme-a.GITHUB_PERSONAL_ACCESS_TOKEN"
                && v == "ghp_agent_a"
        }));
        assert!(b.iter().any(|(k, v)| {
            k == "agentSandbox.connectorSecrets.acme-b.GITHUB_PERSONAL_ACCESS_TOKEN"
                && v == "ghp_agent_b"
        }));
        assert!(!a.iter().any(|(k, _)| k.contains("acme-b")));
        assert!(!b.iter().any(|(_, v)| v == "ghp_agent_a"));
    }

    #[test]
    fn bind_commands_use_a_values_file_and_never_argv_set() {
        let cmds = bind_commands(&BindOpts {
            common: CommonOpts {
                namespace: "curie".into(),
                release: "curie".into(),
                dry_run: false,
            },
            chart: "charts/curie".into(),
            agent: "acme-a".into(),
            secrets: secrets(),
            runner_image: RunnerImageUpdate::Keep,
        })
        .unwrap();
        let helm = cmds[0].display();
        assert!(helm.contains("helm upgrade"), "{helm}");
        // #3260: a cleared runner image under plain --reuse-values keeps
        // rendering the old key, so every bind resets then reuses.
        assert!(helm.contains("--reset-then-reuse-values"), "{helm}");
        assert!(helm.contains("-f"), "{helm}");
        assert!(
            !helm.contains("ghp_agent_a"),
            "secret leaked into argv: {helm}"
        );
        assert!(!helm.contains("--set"), "{helm}");
        let delete = cmds[1].display();
        assert!(delete.contains("delete sandboxclaim"), "{delete}");
        assert!(
            delete.contains(&format!("{CONNECTOR_AGENT_LABEL_KEY}=acme-a")),
            "{delete}"
        );
        assert!(delete.contains("--ignore-not-found=true"), "{delete}");
    }

    #[test]
    fn invalid_agent_name_is_rejected() {
        let err = helm_secret_pairs("Not_A_DNS", &secrets())
            .unwrap_err()
            .to_string();
        assert!(err.contains("Not_A_DNS"), "{err}");
    }

    #[test]
    fn the_agent_name_self_is_rejected_as_reserved() {
        // `self` is well-formed RFC 1123, so only a dedicated check catches
        // it. `admits:` reads `self` as the sentinel for "the deploying
        // agent"; a real agent named `self` would be indistinguishable from
        // it wherever `admits` is resolved.
        let err = helm_secret_pairs("self", &secrets())
            .unwrap_err()
            .to_string();
        assert!(err.contains("self"), "{err}");
        assert!(err.contains("admits"), "{err}");
    }

    #[test]
    fn bind_need_is_current_when_every_value_already_matches() {
        let values = serde_json::json!({"agentSandbox": {"connectorSecrets": {"acme-a": {
            "GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_agent_a",
            "JIRA_TOKEN": "jira-a",
            "OTHER": "kept"
        }}}});
        assert_eq!(
            bind_need(&values, "acme-a", &secrets(), None),
            BindNeed::Current
        );
    }

    #[test]
    fn bind_need_names_only_the_changed_or_missing_secrets() {
        let values = serde_json::json!({"agentSandbox": {"connectorSecrets": {
            "acme-a": {"GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_rotated"},
            "acme-b": {"JIRA_TOKEN": "jira-a"}
        }}});
        assert_eq!(
            bind_need(&values, "acme-a", &secrets(), None),
            BindNeed::Changed {
                secrets: vec!["GITHUB_PERSONAL_ACCESS_TOKEN".into(), "JIRA_TOKEN".into()],
                runner_image: false,
            }
        );
        assert_eq!(
            bind_need(&serde_json::json!({}), "acme-a", &secrets(), None),
            BindNeed::Changed {
                secrets: secrets().keys().cloned().collect(),
                runner_image: false,
            }
        );
    }

    /// Fake `helm`: `get values` answers from a file, `upgrade` logs its argv
    /// and bumps a revision counter the way a real upgrade bumps the release
    /// revision.
    const HELM_STUB: &str = r#"#!/bin/sh
case "$1 $2" in
  "get values") cat "$CURIE_TEST_BIND_DIR/values.json" ;;
  upgrade*) echo "$*" >> "$CURIE_TEST_BIND_DIR/helm.log"
    prev=; for a in "$@"; do [ "$prev" = -f ] && cat "$a" >> "$CURIE_TEST_BIND_DIR/helm-values.log"; prev=$a; done
    r=$(cat "$CURIE_TEST_BIND_DIR/revision"); echo $((r + 1)) > "$CURIE_TEST_BIND_DIR/revision" ;;
  *) echo "unexpected helm invocation: $*" >&2; exit 64 ;;
esac
"#;

    struct StubbedHelm {
        restore: Vec<(&'static str, Option<std::ffi::OsString>)>,
        dir: tempfile::TempDir,
    }

    impl StubbedHelm {
        fn install(values: &serde_json::Value) -> Self {
            use std::os::unix::fs::PermissionsExt;
            let dir = tempfile::tempdir().unwrap();
            for (name, body) in [
                ("helm", HELM_STUB),
                (
                    "kubectl",
                    "#!/bin/sh\necho \"$*\" >> \"$CURIE_TEST_BIND_DIR/kubectl.log\"\n",
                ),
            ] {
                let path = dir.path().join(name);
                std::fs::write(&path, body).unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
            std::fs::write(dir.path().join("values.json"), values.to_string()).unwrap();
            std::fs::write(dir.path().join("revision"), "7\n").unwrap();
            let mut entries = vec![dir.path().to_path_buf()];
            entries.extend(std::env::split_paths(
                &std::env::var_os("PATH").unwrap_or_default(),
            ));
            let path = std::env::join_paths(entries).unwrap();
            let restore = vec![
                ("PATH", std::env::var_os("PATH")),
                (
                    "CURIE_TEST_BIND_DIR",
                    std::env::var_os("CURIE_TEST_BIND_DIR"),
                ),
            ];
            std::env::set_var("PATH", path);
            std::env::set_var("CURIE_TEST_BIND_DIR", dir.path());
            Self { restore, dir }
        }

        fn revision(&self) -> u32 {
            std::fs::read_to_string(self.dir.path().join("revision"))
                .unwrap()
                .trim()
                .parse()
                .unwrap()
        }
    }

    impl StubbedHelm {
        /// Every `helm upgrade` argv, one per line; empty when none ran.
        fn helm_log(&self) -> String {
            std::fs::read_to_string(self.dir.path().join("helm.log")).unwrap_or_default()
        }

        fn kubectl_log(&self) -> Option<String> {
            std::fs::read_to_string(self.dir.path().join("kubectl.log")).ok()
        }
    }

    impl Drop for StubbedHelm {
        fn drop(&mut self) {
            for (name, value) in &self.restore {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }

    fn common() -> CommonOpts {
        CommonOpts {
            namespace: "curie".into(),
            release: "curie".into(),
            dry_run: false,
        }
    }

    #[tokio::test]
    async fn bundle_only_deploy_leaves_the_release_revision_unchanged() {
        let _env = crate::PROCESS_ENV_LOCK.lock().await;
        let helm = StubbedHelm::install(&serde_json::json!({"agentSandbox": {
            "connectorSecrets": {"acme-a": {
                "GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_agent_a",
                "JIRA_TOKEN": "jira-a"
            }}
        }}));
        let need = bind_if_changed(common(), "acme-a".into(), secrets(), None, async {
            panic!("an unchanged bind must not resolve a chart")
        })
        .await
        .unwrap();
        assert_eq!(need, BindNeed::Current);
        assert_eq!(helm.revision(), 7, "release was upgraded for a no-op bind");
        let kubectl = std::fs::read_to_string(helm.dir.path().join("kubectl.log")).unwrap();
        assert!(
            kubectl.contains("delete sandboxclaim"),
            "claims must still be retired so the new bundle loads: {kubectl}"
        );
    }

    #[tokio::test]
    async fn changed_secret_upgrades_the_release() {
        let _env = crate::PROCESS_ENV_LOCK.lock().await;
        let helm = StubbedHelm::install(&serde_json::json!({"agentSandbox": {
            "connectorSecrets": {"acme-a": {
                "GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_old",
                "JIRA_TOKEN": "jira-a"
            }}
        }}));
        let need = bind_if_changed(common(), "acme-a".into(), secrets(), None, async {
            Ok("charts/curie".to_string())
        })
        .await
        .unwrap();
        assert_eq!(
            need,
            BindNeed::Changed {
                secrets: vec!["GITHUB_PERSONAL_ACCESS_TOKEN".into()],
                runner_image: false,
            }
        );
        assert_eq!(helm.revision(), 8);
    }

    const DIGEST: &str = "ghcr.io/acme-corp/acme-bot-runner@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const EARLIER: &str = "ghcr.io/acme-corp/acme-bot-runner@sha256:9999999999999999999999999999999999999999999999999999999999999999";

    fn opts(secrets: BTreeMap<String, String>, runner_image: RunnerImageUpdate) -> BindOpts {
        BindOpts {
            common: common(),
            chart: "charts/curie".into(),
            agent: "acme-a".into(),
            secrets,
            runner_image,
        }
    }

    #[test]
    fn bind_need_sees_a_new_or_differing_runner_image() {
        let none = serde_json::json!({});
        assert_eq!(
            bind_need(&none, "acme-a", &BTreeMap::new(), Some(DIGEST)),
            BindNeed::Changed {
                secrets: vec![],
                runner_image: true
            }
        );
        let earlier = serde_json::json!({"agentSandbox": {"runnerImages": {"acme-a": EARLIER}}});
        assert_eq!(
            bind_need(&earlier, "acme-a", &BTreeMap::new(), Some(DIGEST)),
            BindNeed::Changed {
                secrets: vec![],
                runner_image: true
            }
        );
        let same = serde_json::json!({"agentSandbox": {"runnerImages": {"acme-a": DIGEST}}});
        assert_eq!(
            bind_need(&same, "acme-a", &BTreeMap::new(), Some(DIGEST)),
            BindNeed::Current
        );
    }

    #[test]
    fn bind_need_clears_an_earlier_runner_image_when_none_is_locked() {
        // #3260 AC2: a bundle with no runner entry clears the agent's value.
        let earlier = serde_json::json!({"agentSandbox": {"runnerImages": {"acme-a": EARLIER}}});
        assert_eq!(
            bind_need(&earlier, "acme-a", &BTreeMap::new(), None),
            BindNeed::Changed {
                secrets: vec![],
                runner_image: true
            }
        );
        let nulled = serde_json::json!({"agentSandbox": {"runnerImages": {"acme-a": null}}});
        assert_eq!(
            bind_need(&nulled, "acme-a", &BTreeMap::new(), None),
            BindNeed::Current
        );
    }

    #[test]
    fn bind_need_ignores_another_agents_runner_image() {
        let values = serde_json::json!({"agentSandbox": {"runnerImages": {"acme-b": EARLIER}}});
        assert_eq!(
            bind_need(&values, "acme-a", &BTreeMap::new(), None),
            BindNeed::Current
        );
        let values = serde_json::json!({"agentSandbox": {"runnerImages": {
            "acme-a": DIGEST,
            "acme-b": EARLIER
        }}});
        assert_eq!(
            bind_need(&values, "acme-a", &BTreeMap::new(), Some(DIGEST)),
            BindNeed::Current
        );
    }

    #[test]
    fn bind_commands_set_the_runner_digest_without_a_secret_file() {
        let cmds = bind_commands(&opts(
            BTreeMap::new(),
            RunnerImageUpdate::Set(DIGEST.into()),
        ))
        .unwrap();
        let helm = cmds[0].display();
        assert!(
            helm.contains("helm upgrade curie charts/curie -n curie"),
            "{helm}"
        );
        assert!(helm.contains("--reset-then-reuse-values"), "{helm}");
        assert!(!helm.contains(" --reuse-values"), "{helm}");
        assert!(
            helm.contains(&format!("--set agentSandbox.runnerImages.acme-a={DIGEST}")),
            "{helm}"
        );
        assert!(
            !helm.contains("-f "),
            "no secrets means no values file: {helm}"
        );
        let delete = cmds.last().unwrap().display();
        assert!(delete.contains("delete sandboxclaim"), "{delete}");
        assert!(
            delete.contains(&format!("{CONNECTOR_AGENT_LABEL_KEY}=acme-a")),
            "{delete}"
        );
    }

    #[test]
    fn bind_commands_clear_the_runner_image_with_null() {
        let cmds = bind_commands(&opts(secrets(), RunnerImageUpdate::Clear)).unwrap();
        let helm = cmds[0].display();
        assert!(helm.contains("--reset-then-reuse-values"), "{helm}");
        assert!(helm.contains("-f"), "{helm}");
        assert!(
            helm.contains("--set agentSandbox.runnerImages.acme-a=null"),
            "{helm}"
        );
        assert!(
            !helm.contains("ghp_agent_a"),
            "secret leaked into argv: {helm}"
        );
        assert!(cmds
            .last()
            .unwrap()
            .display()
            .contains("delete sandboxclaim"));
    }

    #[test]
    fn bind_commands_are_empty_with_no_secrets_and_the_runner_kept() {
        assert!(
            bind_commands(&opts(BTreeMap::new(), RunnerImageUpdate::Keep))
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn locked_runner_digest_upgrades_the_release_and_retires_claims() {
        // #3260 AC1: the recorded digest reaches the release and the agent's
        // claimed sandboxes are rolled onto it.
        let _env = crate::PROCESS_ENV_LOCK.lock().await;
        let helm = StubbedHelm::install(&serde_json::json!({"agentSandbox": {
            "runnerImages": {"acme-a": EARLIER}
        }}));
        let need = bind_if_changed(
            common(),
            "acme-a".into(),
            BTreeMap::new(),
            Some(DIGEST.to_string()),
            async { Ok("charts/curie".to_string()) },
        )
        .await
        .unwrap();
        assert_eq!(
            need,
            BindNeed::Changed {
                secrets: vec![],
                runner_image: true
            }
        );
        assert_eq!(helm.revision(), 8);
        let log = helm.helm_log();
        assert!(
            log.contains(&format!("agentSandbox.runnerImages.acme-a={DIGEST}")),
            "{log}"
        );
        assert!(log.contains("@sha256:"), "{log}");
        assert!(log.contains("--reset-then-reuse-values"), "{log}");
        let kubectl = helm.kubectl_log().expect("claims must be retired");
        assert!(kubectl.contains("delete sandboxclaim"), "{kubectl}");
        assert!(
            kubectl.contains(&format!("{CONNECTOR_AGENT_LABEL_KEY}=acme-a")),
            "{kubectl}"
        );
    }

    #[tokio::test]
    async fn no_runner_entry_clears_an_earlier_runner_image() {
        // #3260 AC2.
        let _env = crate::PROCESS_ENV_LOCK.lock().await;
        let helm = StubbedHelm::install(&serde_json::json!({"agentSandbox": {
            "runnerImages": {"acme-a": EARLIER}
        }}));
        let need = bind_if_changed(common(), "acme-a".into(), BTreeMap::new(), None, async {
            Ok("charts/curie".to_string())
        })
        .await
        .unwrap();
        assert_eq!(
            need,
            BindNeed::Changed {
                secrets: vec![],
                runner_image: true
            }
        );
        assert_eq!(helm.revision(), 8);
        let log = helm.helm_log();
        assert!(
            log.contains("agentSandbox.runnerImages.acme-a=null"),
            "{log}"
        );
        assert!(log.contains("--reset-then-reuse-values"), "{log}");
        let kubectl = helm.kubectl_log().expect("claims must be retired");
        assert!(kubectl.contains("delete sandboxclaim"), "{kubectl}");
    }

    #[tokio::test]
    async fn unchanged_runner_digest_leaves_the_release_but_retires_claims() {
        let _env = crate::PROCESS_ENV_LOCK.lock().await;
        let helm = StubbedHelm::install(&serde_json::json!({"agentSandbox": {
            "runnerImages": {"acme-a": DIGEST}
        }}));
        let need = bind_if_changed(
            common(),
            "acme-a".into(),
            BTreeMap::new(),
            Some(DIGEST.to_string()),
            async { panic!("an unchanged bind must not resolve a chart") },
        )
        .await
        .unwrap();
        assert_eq!(need, BindNeed::Current);
        assert_eq!(helm.revision(), 7, "release was upgraded for a no-op bind");
        assert_eq!(helm.helm_log(), "");
        let kubectl = helm.kubectl_log().expect("claims must be retired");
        assert!(
            kubectl.contains("delete sandboxclaim"),
            "claims must still be retired so the runner layer loads: {kubectl}"
        );
    }

    #[tokio::test]
    async fn no_runner_no_secrets_and_no_earlier_value_does_nothing() {
        let _env = crate::PROCESS_ENV_LOCK.lock().await;
        let helm = StubbedHelm::install(&serde_json::json!({"agentSandbox": {
            "runnerImages": {"acme-b": EARLIER}
        }}));
        let need = bind_if_changed(common(), "acme-a".into(), BTreeMap::new(), None, async {
            panic!("a no-op bind must not resolve a chart")
        })
        .await
        .unwrap();
        assert_eq!(need, BindNeed::Current);
        assert_eq!(helm.revision(), 7);
        assert_eq!(helm.helm_log(), "");
        assert_eq!(helm.kubectl_log(), None, "no sandbox should be touched");
    }

    #[tokio::test]
    async fn failed_values_read_with_no_runner_still_clears_the_runner_image() {
        let _env = crate::PROCESS_ENV_LOCK.lock().await;
        let helm = StubbedHelm::install(&serde_json::json!({}));
        // `cat` of a missing file fails, so `helm get values` exits non-zero.
        // An unread release could still hold an earlier image (AC2), so the
        // clear is sent rather than assumed unnecessary.
        std::fs::remove_file(helm.dir.path().join("values.json")).unwrap();
        let need = bind_if_changed(common(), "acme-a".into(), BTreeMap::new(), None, async {
            Ok("charts/curie".to_string())
        })
        .await
        .unwrap();
        assert_eq!(
            need,
            BindNeed::Changed {
                secrets: Vec::new(),
                runner_image: true
            }
        );
        assert!(
            helm.helm_log()
                .contains("agentSandbox.runnerImages.acme-a=null"),
            "{}",
            helm.helm_log()
        );
    }

    // --- #3218: the installation's runner and a layered runner's base ---

    const RUNNER_A: &str =
        "ghcr.io/curie-eng/curie-runner@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const RUNNER_A_MIRROR: &str =
        "mirror.example/curie-runner@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const RUNNER_B: &str =
        "ghcr.io/curie-eng/curie-runner@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn runner_values(image: &str, tag: &str, digest: &str) -> serde_json::Value {
        serde_json::json!({"agentSandbox": {"runner": {"image": image, "tag": tag, "digest": digest}}})
    }

    #[test]
    fn effective_runner_ref_prefers_digest_then_tag_then_app_version() {
        let img = "ghcr.io/curie-eng/curie-runner";
        assert_eq!(
            effective_runner_ref(&runner_values(img, "0.9.0", "sha256:abc"), Some("0.10.0")),
            Some(format!("{img}@sha256:abc"))
        );
        assert_eq!(
            effective_runner_ref(&runner_values(img, "0.9.0", ""), Some("0.10.0")),
            Some(format!("{img}:0.9.0"))
        );
        assert_eq!(
            effective_runner_ref(&runner_values(img, "", ""), Some("0.10.0")),
            Some(format!("{img}:0.10.0"))
        );
        assert_eq!(
            effective_runner_ref(&runner_values(img, "", ""), None),
            None
        );
        assert_eq!(
            effective_runner_ref(&serde_json::json!({}), Some("0.10.0")),
            None
        );
    }

    #[test]
    fn same_runner_compares_digests_only() {
        assert!(same_runner(RUNNER_A, RUNNER_A_MIRROR));
        assert!(!same_runner(RUNNER_A, RUNNER_B));
        assert!(!same_runner(
            "ghcr.io/curie-eng/curie-runner:0.10.0",
            "ghcr.io/curie-eng/curie-runner:0.10.0"
        ));
    }

    #[test]
    fn layered_agents_skip_null_and_empty_entries() {
        let values = serde_json::json!({"agentSandbox": {"runnerImages": {
            "zeta": RUNNER_B, "alpha": RUNNER_A, "gone": null, "blank": ""
        }}});
        assert_eq!(layered_agents(&values), vec!["alpha", "zeta"]);
        assert!(layered_agents(&serde_json::json!({})).is_empty());
    }

    #[test]
    fn layers_stop_matching_unless_the_digest_is_proven_equal() {
        let layered = vec!["factory".to_string()];
        assert!(
            layers_stopping_to_match(&layered, Some(RUNNER_A), Some(RUNNER_A_MIRROR)).is_empty()
        );
        assert_eq!(
            layers_stopping_to_match(&layered, Some(RUNNER_A), Some(RUNNER_B)),
            layered
        );
        assert_eq!(
            layers_stopping_to_match(&layered, None, Some(RUNNER_B)),
            layered
        );
        assert_eq!(
            layers_stopping_to_match(&layered, Some(RUNNER_A), None),
            layered
        );
        assert!(layers_stopping_to_match(&[], Some(RUNNER_A), Some(RUNNER_B)).is_empty());
        let mut values = serde_json::json!({"agentSandbox": {
            "runnerImages": {"factory": RUNNER_A, "acme-other": RUNNER_B},
            "connectorSecrets": {"factory": {"API_TOKEN": "acme-secret"}}
        }, "api": {"existingSecret": "acme-api-credentials"}});
        omit_runner_image_bindings(&mut values, &layered);
        assert!(values
            .pointer("/agentSandbox/runnerImages/factory")
            .is_none());
        assert_eq!(
            values
                .pointer("/agentSandbox/runnerImages/acme-other")
                .and_then(|v| v.as_str()),
            Some(RUNNER_B)
        );
        assert_eq!(
            values
                .pointer("/agentSandbox/connectorSecrets/factory/API_TOKEN")
                .and_then(|v| v.as_str()),
            Some("acme-secret")
        );
        assert_eq!(
            values
                .pointer("/api/existingSecret")
                .and_then(|v| v.as_str()),
            Some("acme-api-credentials")
        );
        assert!(!values.to_string().contains("null"));
    }

    #[test]
    fn serving_revision_skips_a_newer_failed_upgrade() {
        // #3421: revision 2 serves, revision 3 failed in its pre-upgrade hook.
        let history = serde_json::json!([
            {"revision": 1, "status": "superseded"},
            {"revision": 2, "status": "deployed"},
            {"revision": 3, "status": "failed"},
        ]);
        assert_eq!(serving_revision(&history, "curie"), Ok(2));
        // After `cluster rollback --revision 2` the new revision 4 serves.
        let history = serde_json::json!([
            {"revision": 2, "status": "superseded"},
            {"revision": 3, "status": "failed"},
            {"revision": 4, "status": "deployed"},
        ]);
        assert_eq!(serving_revision(&history, "curie"), Ok(4));
    }

    #[test]
    fn serving_revision_refuses_naming_the_newest_when_none_is_deployed() {
        let history = serde_json::json!([
            {"revision": 1, "status": "superseded"},
            {"revision": 2, "status": "failed"},
            {"revision": 3, "status": "pending-upgrade"},
        ]);
        let err = serving_revision(&history, "curie").unwrap_err();
        assert!(err.contains("no deployed revision"), "{err}");
        assert!(err.contains("revision 3, is pending-upgrade"), "{err}");
        assert!(serving_revision(&serde_json::json!([]), "curie").is_err());
    }

    #[test]
    fn helm_reads_pin_the_serving_revision() {
        let common = CommonOpts {
            release: "curie".into(),
            namespace: "curie".into(),
            dry_run: false,
        };
        let shown = helm_get_json(&common, "values", true, 2).display();
        assert!(shown.contains("--revision 2"), "{shown}");
    }

    #[test]
    fn runner_base_verdict_passes_a_matching_base_across_repo_spellings() {
        let dir = std::path::Path::new("/bundles/sre-bot");
        runner_base_verdict(
            dir,
            RUNNER_A_MIRROR,
            Ok((
                "ghcr.io/curie-eng/curie-runner:0.10.0".into(),
                RUNNER_A.into(),
            )),
        )
        .expect("same digest");
    }

    #[test]
    fn runner_base_verdict_refuses_another_base_with_the_build_command() {
        let dir = std::path::Path::new("/bundles/sre-bot");
        let err = runner_base_verdict(
            dir,
            RUNNER_B,
            Ok((
                "ghcr.io/curie-eng/curie-runner:0.10.0".into(),
                RUNNER_A.into(),
            )),
        )
        .expect_err("another base is refused");
        let cli = err
            .downcast_ref::<crate::exit::CliError>()
            .expect("a CliError");
        assert_eq!(cli.class, crate::exit::ExitClass::Usage);
        let (message, fix) = (cli.message.clone(), cli.fix.clone());
        assert!(
            message.contains(RUNNER_B) && message.contains(RUNNER_A),
            "{message}"
        );
        // #3423: the human presenter shows only the message, so the rebuild
        // command must be part of it, not only of the `--json` fix.
        let (human, _) = crate::exit::present_error(&err);
        assert!(
            human.contains(
                "curie build --plugin-dir /bundles/sre-bot --registry <ref> --runner-image \
                 ghcr.io/curie-eng/curie-runner:0.10.0"
            ),
            "{human}"
        );
        assert_eq!(
            fix.as_deref(),
            Some(
                "run `curie build --plugin-dir /bundles/sre-bot --registry <ref> --runner-image \
                 ghcr.io/curie-eng/curie-runner:0.10.0` and redeploy"
            )
        );
    }

    #[test]
    fn runner_base_verdict_refuses_when_the_installation_runner_is_unknown() {
        let err = runner_base_verdict(
            std::path::Path::new("/b"),
            RUNNER_A,
            Err("`helm get values` failed: boom".into()),
        )
        .expect_err("an unknown installation runner never passes");
        let (message, _) = crate::exit::present_error(&err);
        assert!(
            message.contains("could not be determined") && message.contains("boom"),
            "{message}"
        );
        // #3423: the remedy reaches human output, not only `--json`.
        assert!(message.contains("curie cluster status"), "{message}");
    }

    #[test]
    fn bind_need_clears_a_binding_the_redeploy_no_longer_carries() {
        let bound = serde_json::json!({"agentSandbox": {"connectorSecrets": {
            "acme-a": {"GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_agent_a"},
            "acme-b": {"JIRA_TOKEN": "jira-b"}
        }}});
        assert_eq!(
            bind_need(&bound, "acme-a", &BTreeMap::new(), None),
            BindNeed::Clear {
                runner_image: false
            }
        );
        assert_eq!(
            bind_need(&bound, "acme-c", &BTreeMap::new(), None),
            BindNeed::Current
        );
        assert_eq!(
            bind_need(&serde_json::json!({}), "acme-a", &BTreeMap::new(), None),
            BindNeed::Current
        );
    }

    #[tokio::test]
    async fn dropping_every_secret_clears_the_binding_then_retires_claims() {
        let _env = crate::PROCESS_ENV_LOCK.lock().await;
        let helm = StubbedHelm::install(&serde_json::json!({"agentSandbox": {
            "connectorSecrets": {
                "acme-a": {"GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_agent_a"},
                "acme-b": {"JIRA_TOKEN": "jira-b"}
            }
        }, "worker": {"replicas": 2}}));
        let need = bind_if_changed(common(), "acme-a".into(), BTreeMap::new(), None, async {
            Ok("charts/curie".to_string())
        })
        .await
        .unwrap();
        assert_eq!(
            need,
            BindNeed::Clear {
                runner_image: false
            }
        );
        assert_eq!(
            helm.revision(),
            8,
            "the stale binding was left in the release"
        );
        let upgrade = std::fs::read_to_string(helm.dir.path().join("helm.log")).unwrap();
        // `--reuse-values --set <agent>=null` drops the key from the stored
        // values but still renders the old Secret, so the supplied values
        // must be replaced, not merged.
        assert!(upgrade.contains("--reset-values"), "{upgrade}");
        assert!(!upgrade.contains("--reuse-values"), "{upgrade}");
        let supplied: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(helm.dir.path().join("helm-values.log")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            supplied,
            serde_json::json!({"agentSandbox": {"connectorSecrets": {
                "acme-b": {"JIRA_TOKEN": "jira-b"}
            }}, "worker": {"replicas": 2}}),
            "only the agent's binding may be removed from the supplied values"
        );
        let kubectl = std::fs::read_to_string(helm.dir.path().join("kubectl.log")).unwrap();
        assert!(
            kubectl.contains("delete sandboxclaim") && kubectl.contains("=acme-a"),
            "claims must be refreshed so no pod keeps the removed credential: {kubectl}"
        );
    }

    #[tokio::test]
    async fn no_secrets_and_no_binding_leaves_the_release_alone() {
        let _env = crate::PROCESS_ENV_LOCK.lock().await;
        let helm = StubbedHelm::install(&serde_json::json!({}));
        let need = bind_if_changed(common(), "acme-a".into(), BTreeMap::new(), None, async {
            panic!("nothing to clear must not resolve a chart")
        })
        .await
        .unwrap();
        assert_eq!(need, BindNeed::Current);
        assert_eq!(helm.revision(), 7);
        assert!(!helm.dir.path().join("kubectl.log").exists());
    }
}
