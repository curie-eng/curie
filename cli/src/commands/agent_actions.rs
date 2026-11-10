//! Agent actions shared by `local` and `cluster`: hooks, kill, resume, budget, reset-thread, delete, versions, and memory.

use super::*;

/// Shared flags for the agent-lifecycle verbs (`cluster kill|resume|budget|delete`).
/// Like `deploy`, these speak the committed platform-API contract through the
/// existing `ApiClient` (no second HTTP client).
pub struct AgentActionOpts {
    pub api_url: String,
    pub api_key: String,
    /// Agent name or id to act on. Resolved to the API's `{agent_id}` via the
    /// same name lookup `deploy` uses (`ApiClient::find_agent`).
    pub agent: String,
    pub dry_run: bool,
}

/// One success result for both local and cluster hook operations. The secret
/// variant deliberately has no Debug implementation.
pub enum HookOutput {
    DryRun(crate::ui::DryRunPlan),
    Config {
        id: String,
        agent: String,
        hook_partitions: Option<BTreeMap<String, serde_json::Value>>,
        source_bindings: Option<BTreeMap<String, serde_json::Value>>,
    },
    Secret {
        secret: String,
    },
}

impl crate::ui::CliOutput for HookOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            Self::DryRun(plan) => plan.to_json(),
            Self::Config {
                id,
                agent,
                hook_partitions,
                source_bindings,
            } => serde_json::json!({
                "id": id,
                "agent": agent,
                "hook_partitions": hook_partitions,
                "source_bindings": source_bindings,
            }),
            Self::Secret { secret } => serde_json::json!({"secret": secret}),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            Self::DryRun(plan) => plan.render(ui),
            Self::Config { .. } => ui.payload_plain(
                &serde_json::to_string_pretty(&self.to_json()).expect("hook config is JSON"),
            ),
            Self::Secret { secret } => ui.payload_plain(secret),
        }
    }
}

pub(super) fn hook_config_output(agent: crate::api::Agent) -> HookOutput {
    HookOutput::Config {
        id: agent.id,
        agent: agent.name,
        hook_partitions: agent.hook_partitions,
        source_bindings: agent.source_bindings,
    }
}

pub async fn hooks_show(opts: AgentActionOpts) -> Result<HookOutput> {
    if opts.dry_run {
        return Ok(HookOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "GET /agents  (would find agent {:?} and show its hook configuration)",
                opts.agent
            )],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    Ok(hook_config_output(client.find_agent(&opts.agent).await?))
}

pub async fn hooks_configure(opts: AgentActionOpts, file: &Path) -> Result<HookOutput> {
    let raw = std::fs::read_to_string(file)
        .with_context(|| format!("reading hook configuration from {}", file.display()))?;
    let config: crate::api::HookConfigInput = serde_json::from_str(&raw)
        .with_context(|| format!("decoding hook configuration from {}", file.display()))?;
    if config.is_empty() {
        bail!("hook configuration must contain hook_partitions or source_bindings");
    }
    let body = config.into_update();
    if opts.dry_run {
        return Ok(HookOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "PATCH /agents/<id>  (would resolve agent {:?} and update hook configuration)",
                opts.agent
            )],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    Ok(hook_config_output(
        client.update_agent(&agent.id, &body).await?,
    ))
}

pub async fn hooks_secret(opts: AgentActionOpts) -> Result<HookOutput> {
    if opts.dry_run {
        return Ok(HookOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "GET /agents/<id>/hook-secret  (would resolve agent {:?} first; secret redacted)",
                opts.agent
            )],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let secret = client.hook_secret(&agent.id).await?;
    Ok(HookOutput::Secret { secret })
}

/// Output of `<tier> kill <agent>`: the dry-run plan, or the resulting kill
/// state. Owns its data (agent name) so `to_json` / `render` outlive the
/// `ApiClient`. The json-vs-human choice is made once, in `Ui::emit` (#456).
#[derive(Debug)]
pub enum KillOutput {
    DryRun(crate::ui::DryRunPlan),
    Done { agent: String, killed: bool },
}

impl crate::ui::CliOutput for KillOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            KillOutput::DryRun(plan) => plan.to_json(),
            KillOutput::Done { agent, killed } => {
                serde_json::json!({"agent": agent, "killed": killed})
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            KillOutput::DryRun(plan) => plan.render(ui),
            KillOutput::Done { agent, killed } => {
                ui.payload(&format!("agent {agent} killed (killed={killed})"));
                ui.note("Run `curie cluster resume <agent>` to bring it back.");
            }
        }
    }
}

/// `curie cluster kill <agent> --yes`: flip the agent kill switch on
/// (`POST /agents/{id}/kill`). Destructive (it stops the agent's runs), so it
/// refuses without `--yes`, mirroring `cluster down`. `--dry-run` returns the
/// plan and makes no request.
pub async fn kill(opts: AgentActionOpts, yes: bool) -> Result<KillOutput> {
    let ui = crate::ui::ui();
    if opts.dry_run {
        return Ok(KillOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "POST {}/agents/<id>/kill  (would resolve agent {:?} first)",
                opts.api_url, opts.agent
            )],
        }));
    }
    if !yes {
        return Err(crate::exit::CliError::usage(format!(
            "`curie cluster kill {}` stops the agent's runs; re-run with --yes to confirm",
            opts.agent
        ))
        .with_fix("re-run with --yes")
        .into());
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let cl = ui.checklist();
    let step = cl.step(&format!("killing {}", agent.name));
    let state = match client.kill_agent(&agent.id).await {
        Ok(state) => {
            step.done("killed");
            state
        }
        Err(err) => {
            step.fail("failed");
            return Err(err);
        }
    };
    Ok(KillOutput::Done {
        agent: agent.name,
        killed: state.killed,
    })
}

/// Output of `<tier> resume <agent>`: the dry-run plan, or the resulting kill
/// state. Owns its data so it outlives the `ApiClient`.
#[derive(Debug)]
pub enum ResumeOutput {
    DryRun(crate::ui::DryRunPlan),
    Done { agent: String, killed: bool },
}

impl crate::ui::CliOutput for ResumeOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ResumeOutput::DryRun(plan) => plan.to_json(),
            ResumeOutput::Done { agent, killed } => {
                serde_json::json!({"agent": agent, "killed": killed})
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ResumeOutput::DryRun(plan) => plan.render(ui),
            ResumeOutput::Done { agent, killed } => {
                ui.payload(&format!("agent {agent} resumed (killed={killed})"));
            }
        }
    }
}

/// `curie cluster resume <agent>`: flip the agent kill switch off
/// (`POST /agents/{id}/resume`). Non-destructive, so no `--yes` gate.
/// `--dry-run` returns the plan and makes no request.
pub async fn resume(opts: AgentActionOpts) -> Result<ResumeOutput> {
    let ui = crate::ui::ui();
    if opts.dry_run {
        return Ok(ResumeOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "POST {}/agents/<id>/resume  (would resolve agent {:?} first)",
                opts.api_url, opts.agent
            )],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let cl = ui.checklist();
    let step = cl.step(&format!("resuming {}", agent.name));
    let state = match client.resume_agent(&agent.id).await {
        Ok(state) => {
            step.done("resumed");
            state
        }
        Err(err) => {
            step.fail("failed");
            return Err(err);
        }
    };
    Ok(ResumeOutput::Done {
        agent: agent.name,
        killed: state.killed,
    })
}

/// Output of `<tier> budget <agent>`: the dry-run plan, or the saved budget.
/// Each limit is `None` when the platform default applies. Owns its data so it
/// outlives the `ApiClient`.
#[derive(Debug)]
pub enum BudgetOutput {
    DryRun(crate::ui::DryRunPlan),
    Done {
        agent: String,
        max_usd_per_day: Option<f64>,
        max_output_tokens_per_run: Option<u64>,
    },
}

impl crate::ui::CliOutput for BudgetOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            BudgetOutput::DryRun(plan) => plan.to_json(),
            BudgetOutput::Done {
                agent,
                max_usd_per_day,
                max_output_tokens_per_run,
            } => serde_json::json!({
                "agent": agent,
                "max_usd_per_day": max_usd_per_day,
                "max_output_tokens_per_run": max_output_tokens_per_run,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            BudgetOutput::DryRun(plan) => plan.render(ui),
            BudgetOutput::Done {
                agent,
                max_usd_per_day,
                max_output_tokens_per_run,
            } => {
                let usd = max_usd_per_day
                    .map(|v| format!("${v}/day"))
                    .unwrap_or_else(|| "platform default".to_string());
                let tokens = max_output_tokens_per_run
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "platform default".to_string());
                ui.payload(&format!(
                    "budget for {agent} set: max output tokens/run {tokens}, max $/day {usd}"
                ));
            }
        }
    }
}

/// Update the agent budget through `GET /agents/{id}/budget` followed by
/// `PUT /agents/{id}/budget`. `--limit` sets `max_usd_per_day` and
/// `--output-tokens` sets `max_output_tokens_per_run`. At least one is required;
/// the current value of each unspecified field is preserved.
/// `--dry-run` returns the plan and makes no request.
pub async fn budget(
    opts: AgentActionOpts,
    limit: Option<f64>,
    output_tokens: Option<u64>,
) -> Result<BudgetOutput> {
    let ui = crate::ui::ui();
    // Validated before the dry-run early return (#3710): a dry run shows what
    // the real command would do, so it must refuse a --limit the real command
    // would refuse instead of printing a plan for it.
    if limit.is_none() && output_tokens.is_none() {
        return Err(crate::exit::usage(
            "at least one of --limit or --output-tokens is required",
        ));
    }
    if let Some(limit) = limit {
        if !limit.is_finite() || limit <= 0.0 {
            return Err(crate::exit::usage(format!(
                "--limit must be a finite value greater than 0 (got {limit})"
            )));
        }
    }
    if output_tokens == Some(0) {
        return Err(crate::exit::usage(
            "--output-tokens must be greater than 0 (got 0)",
        ));
    }
    if opts.dry_run {
        let mut selected = serde_json::Map::new();
        if let Some(limit) = limit {
            selected.insert("max_usd_per_day".to_string(), serde_json::json!(limit));
        }
        if let Some(output_tokens) = output_tokens {
            selected.insert(
                "max_output_tokens_per_run".to_string(),
                serde_json::json!(output_tokens),
            );
        }
        return Ok(BudgetOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![
                format!(
                    "GET {}/agents/<id>/budget  (would resolve agent {:?} first; read current fields to preserve unspecified limits)",
                    opts.api_url, opts.agent
                ),
                format!(
                    "PUT {}/agents/<id>/budget  {}  (update selected fields and preserve all unspecified current values, including platform defaults)",
                    opts.api_url,
                    serde_json::Value::Object(selected)
                ),
            ],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let mut cfg = client.get_budget(&agent.id).await?;
    if let Some(limit) = limit {
        cfg.max_usd_per_day = Some(limit);
    }
    if let Some(output_tokens) = output_tokens {
        cfg.max_output_tokens_per_run = Some(output_tokens);
    }
    let cl = ui.checklist();
    let step = cl.step(&format!("setting budget for {}", agent.name));
    let saved = match client.set_budget(&agent.id, &cfg).await {
        Ok(saved) => {
            step.done("updated");
            saved
        }
        Err(err) => {
            step.fail("failed");
            return Err(err);
        }
    };
    Ok(BudgetOutput::Done {
        agent: agent.name,
        max_usd_per_day: saved.max_usd_per_day,
        max_output_tokens_per_run: saved.max_output_tokens_per_run,
    })
}

/// Output of `<tier> reset-thread <agent> --thread-key <key>`: the dry-run
/// plan, or the resulting reset-request state. Owns its data so it outlives
/// the `ApiClient`.
#[derive(Debug)]
pub enum ResetThreadOutput {
    DryRun(crate::ui::DryRunPlan),
    Done {
        agent: String,
        thread_key: String,
        /// The reset was accepted and queued (always true on the `Done` path).
        requested: bool,
        /// The CLI polled `GET .../reset` and observed the worker actually
        /// release the sandbox within the wait window (#735). False means the
        /// release is still pending -- queued, but not yet confirmed drained.
        released: bool,
        /// The API reported that the drained reset found a route and released
        /// it (#3699). `None` when it reported nothing (an older API, or the
        /// outcome expired, or the release is still pending). A reset that
        /// matched no route is an error, never a `Done`.
        route_existed: Option<bool>,
    },
}

impl crate::ui::CliOutput for ResetThreadOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ResetThreadOutput::DryRun(plan) => plan.to_json(),
            ResetThreadOutput::Done {
                agent,
                thread_key,
                requested,
                released,
                route_existed,
            } => {
                let mut out = serde_json::json!({"agent": agent, "thread_key": thread_key, "requested": requested, "released": released});
                // Present only when the API reported the outcome, so an older API
                // leaves the payload exactly as it was.
                if let Some(route_existed) = route_existed {
                    out["route_existed"] = serde_json::json!(route_existed);
                }
                out
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ResetThreadOutput::DryRun(plan) => plan.render(ui),
            ResetThreadOutput::Done {
                agent,
                thread_key,
                released,
                ..
            } => {
                if *released {
                    ui.payload(&format!(
                        "thread {thread_key} on agent {agent}: reset complete, sandbox released"
                    ));
                    ui.note("The next message on this thread cold-creates a fresh sandbox.");
                } else {
                    ui.payload(&format!(
                        "thread {thread_key} on agent {agent}: reset queued, release still pending"
                    ));
                    ui.note(
                        "The worker did not confirm the release within the wait window; its next maintenance tick will release the sandbox. Poll GET .../reset (or re-run) to confirm before the next message.",
                    );
                }
            }
        }
    }
}

/// How long `reset-thread` waits for the worker to actually release the sandbox
/// before it reports the release as still pending (#735). Comfortably covers the
/// worker's default 30s `reclaim_interval_s` drain tick.
pub(super) const RESET_RELEASE_TIMEOUT: Duration = Duration::from_secs(45);
/// How often `reset-thread` re-polls `GET .../reset` while waiting (#735).
pub(super) const RESET_RELEASE_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// `curie <tier> reset-thread <agent> --thread-key <key> --yes`: force the
/// thread's sandbox to be released (`POST
/// /agents/{id}/threads/{thread_key}/reset`, #737). Interrupts a live turn on
/// the thread first, so it refuses without `--yes`, mirroring `kill`.
/// `--dry-run` returns the plan and makes no request.
pub async fn reset_thread(
    opts: AgentActionOpts,
    thread_key: String,
    yes: bool,
) -> Result<ResetThreadOutput> {
    let ui = crate::ui::ui();
    if opts.dry_run {
        return Ok(ResetThreadOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "POST {}/agents/<id>/threads/{}/reset  (would resolve agent {:?} first)",
                opts.api_url, thread_key, opts.agent
            )],
        }));
    }
    if !yes {
        return Err(crate::exit::CliError::usage(format!(
            "`curie ... reset-thread {} --thread-key {}` interrupts any live turn on the thread; re-run with --yes to confirm",
            opts.agent, thread_key
        ))
        .with_fix("re-run with --yes")
        .into());
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let cl = ui.checklist();
    let step = cl.step(&format!("resetting thread {thread_key} on {}", agent.name));
    if let Err(err) = client.reset_thread(&agent.id, &thread_key).await {
        step.fail("failed");
        return Err(err);
    }
    step.done("reset requested");

    // The POST only *queues* the release; the worker drains it on its next
    // maintenance tick (up to `reclaim_interval_s`, 30s by default). Poll the
    // reset state to completion so this command -- and therefore the operator --
    // does not move on until the sandbox has actually been released, closing the
    // window where the next message adopts the pre-reset sandbox (#735). A poll
    // failure never fails an already-accepted reset: it degrades to "release
    // unconfirmed, still pending".
    let wait = cl.step("waiting for the sandbox to be released");
    let deadline = Instant::now() + RESET_RELEASE_TIMEOUT;
    let mut route_existed = None;
    let released = loop {
        match client.thread_reset_state(&agent.id, &thread_key).await {
            Ok(state) if !state.requested => {
                route_existed = state.route_existed;
                break true;
            }
            Ok(_) => {}
            Err(_) => break false,
        }
        if Instant::now() >= deadline {
            break false;
        }
        tokio::time::sleep(RESET_RELEASE_POLL_INTERVAL).await;
    };
    // The worker drained the reset but the key matched no route, so nothing was
    // released and the sandbox the operator meant to free is still claimed
    // (#3699). Say so rather than print "released".
    if released && route_existed == Some(false) {
        wait.fail("no route matched");
        return Err(crate::exit::CliError::usage(
            "no route matched this thread key; nothing was released",
        )
        .with_fix(
            "check the thread key; a named bot's key carries its identity as its own segment (kind:identity:channel:conversation)",
        )
        .into());
    }
    if released {
        wait.done("released");
    } else {
        wait.done("still pending");
    }

    Ok(ResetThreadOutput::Done {
        agent: agent.name,
        thread_key,
        requested: true,
        released,
        route_existed,
    })
}

/// Output of `<tier> delete <agent>`: the dry-run plan, or the deleted agent's
/// name. Owns its data so it outlives the `ApiClient`.
#[derive(Debug)]
pub enum DeleteOutput {
    DryRun(crate::ui::DryRunPlan),
    Done { agent: String },
}

impl crate::ui::CliOutput for DeleteOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            DeleteOutput::DryRun(plan) => plan.to_json(),
            DeleteOutput::Done { agent } => serde_json::json!({"agent": agent, "deleted": true}),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            DeleteOutput::DryRun(plan) => plan.render(ui),
            DeleteOutput::Done { agent } => ui.payload(&format!("agent {agent} deleted")),
        }
    }
}

/// `curie <tier> delete <agent> --yes`: end active deployments, then delete the
/// agent. Destructive and irreversible, so it refuses without `--yes`.
/// `--dry-run` returns the plan and makes no request.
pub async fn delete(opts: AgentActionOpts, yes: bool) -> Result<DeleteOutput> {
    let ui = crate::ui::ui();
    if opts.dry_run {
        return Ok(DeleteOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![
                format!(
                    "GET {}/agents  (would resolve agent {:?})",
                    opts.api_url, opts.agent
                ),
                format!("GET {}/deployments?agent_id=<id>", opts.api_url),
                format!(
                    "DELETE {}/deployments/<id>  (for each active deployment)",
                    opts.api_url
                ),
                format!("DELETE {}/agents/<id>", opts.api_url),
            ],
        }));
    }
    if !yes {
        return Err(crate::exit::CliError::usage(format!(
            "`curie ... delete {}` permanently deletes the agent; re-run with --yes to confirm",
            opts.agent
        ))
        .with_fix("re-run with --yes")
        .into());
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let cl = ui.checklist();
    for deployment in client.list_deployments(&agent.id).await? {
        if deployment.status == "active" {
            let step = cl.step(&format!("ending deployment {}", deployment.id));
            if let Err(err) = client.end_deployment(&deployment.id).await {
                step.fail("failed");
                return Err(err);
            }
            step.done("ended");
        }
    }
    let step = cl.step(&format!("deleting {}", agent.name));
    match client.delete_agent(&agent.id).await {
        Ok(()) => step.done("deleted"),
        Err(err) => {
            step.fail("failed");
            return Err(err);
        }
    }
    Ok(DeleteOutput::Done { agent: agent.name })
}

/// Output of `<tier> versions <agent>`: the dry-run plan, the empty case, or the
/// version list. Owns its data (agent name + cloned versions) so `to_json` /
/// `render` outlive the `ApiClient`. The json-vs-human choice is made once, in
/// `Ui::emit` (issue #456).
pub enum VersionsOutput {
    DryRun(crate::ui::DryRunPlan),
    Empty {
        agent: String,
    },
    /// `versions` is held **newest-first**, normalized once by the `versions`
    /// handler (the API returns them oldest-first). Both `to_json` and `render`
    /// iterate it plainly, so any future constructor must preserve that order or
    /// the two paths silently diverge.
    List {
        agent: String,
        versions: Vec<crate::api::Version>,
    },
}

impl crate::ui::CliOutput for VersionsOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            VersionsOutput::DryRun(plan) => plan.to_json(),
            VersionsOutput::Empty { agent } => {
                serde_json::json!({"agent": agent, "versions": []})
            }
            VersionsOutput::List { agent, versions } => {
                let versions: Vec<serde_json::Value> = versions
                    .iter()
                    .map(|v| {
                        serde_json::json!({
                            "id": v.id,
                            "version_label": v.version_label,
                            "commit_sha": v.commit_sha,
                            "bundle_sha256": v.bundle_sha256,
                            "created_by": v.created_by,
                            "created_at": v.created_at,
                            "bundle_ref": v.bundle_ref,
                        })
                    })
                    .collect();
                serde_json::json!({"agent": agent, "versions": versions})
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            VersionsOutput::DryRun(plan) => plan.render(ui),
            VersionsOutput::Empty { agent } => {
                ui.payload(&format!("{agent} has no versions yet (deploy it first)"));
            }
            VersionsOutput::List { agent, versions } => {
                ui.payload(&format!(
                    "{agent} — {} version(s), newest first:",
                    versions.len()
                ));
                for v in versions.iter() {
                    let commit = v.commit_sha.as_deref().unwrap_or("-");
                    let by = v.created_by.as_deref().unwrap_or("-");
                    let at = v.created_at.as_deref().unwrap_or("-");
                    // Show the bundle hash consistently across tiers (#548): it is
                    // the parity evidence, so a human-readable listing must carry it
                    // too, not just `cluster deploy`'s printout.
                    let sha = v.bundle_sha256.as_deref().unwrap_or("-");
                    ui.kv(
                        &v.version_label,
                        &format!("sha256 {sha}  commit {commit}  by {by}  at {at}"),
                    );
                }
            }
        }
    }
}

/// `<tier> versions <agent>`: list the agent's immutable versions (newest first).
pub async fn versions(opts: AgentActionOpts) -> Result<VersionsOutput> {
    if opts.dry_run {
        return Ok(VersionsOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "GET {}/agents/<id>/versions  (would resolve agent {:?} first)",
                opts.api_url, opts.agent
            )],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let versions = client.list_versions(&agent.id).await?;
    if versions.is_empty() {
        return Ok(VersionsOutput::Empty { agent: agent.name });
    }
    // The API returns versions oldest-first; normalize to the documented
    // newest-first order HERE, once, so the json and human paths cannot diverge.
    Ok(VersionsOutput::List {
        agent: agent.name,
        versions: versions.into_iter().rev().collect(),
    })
}

/// Next command after operator `--add`. `--continue` would reuse a sandbox
/// that booted without this entry.
pub fn memory_add_next_command(message_verb: &str) -> String {
    format!(r#"curie {message_verb} message "...""#)
}

/// First human line after operator `--add`: the entry is not in the current
/// sandbox's boot memory.
pub fn memory_add_fresh_session_required_line() -> &'static str {
    "A fresh session is required before this entry is injected at boot."
}

/// Human follow-up after operator `--add`.
pub fn memory_add_fresh_session_line(message_verb: &str) -> String {
    format!(
        "Start a new thread with `{}` (omit --continue).",
        memory_add_next_command(message_verb)
    )
}

/// The exact channel pair owning a memory fact.
#[derive(Debug, Clone, Serialize)]
pub struct MemoryChannel {
    pub kind: String,
    pub address: String,
}

impl MemoryChannel {
    fn pair(&self) -> (&str, &str) {
        (&self.kind, &self.address)
    }
}

/// An individual fact with the attribution stored by its producer.
#[derive(Debug, Clone, Serialize)]
pub struct MemoryFact {
    pub id: String,
    pub scope: String,
    pub channel: Option<MemoryChannel>,
    pub statement: String,
    pub author: String,
    pub stated_at: String,
}

/// Output of `<tier> memory <agent>`: a plan, log and fact listing, deletion,
/// or an operator seeded add. Owns its data so it outlives the `ApiClient`.
#[derive(Debug)]
pub enum MemoryOutput {
    DryRun(crate::ui::DryRunPlan),
    List {
        agent: String,
        entries: Vec<crate::api::MemoryEntry>,
        facts: Vec<MemoryFact>,
    },
    Deleted {
        agent: String,
        id: String,
        scope: String,
        channel: Option<MemoryChannel>,
    },
    Added {
        agent: String,
        index: u64,
        content: String,
        source: String,
        fresh_session_required: bool,
        message_verb: String,
    },
}

impl crate::ui::CliOutput for MemoryOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            MemoryOutput::DryRun(plan) => plan.to_json(),
            MemoryOutput::List {
                agent,
                entries,
                facts,
            } => {
                let entries: Vec<serde_json::Value> = entries
                    .iter()
                    .map(|e| serde_json::json!({"index": e.index, "content": e.content}))
                    .collect();
                serde_json::json!({"agent": agent, "entries": entries, "facts": facts})
            }
            MemoryOutput::Deleted {
                agent,
                id,
                scope,
                channel,
            } => serde_json::json!({
                "agent": agent,
                "id": id,
                "scope": scope,
                "channel": channel,
                "deleted": true,
            }),
            MemoryOutput::Added {
                agent,
                index,
                content,
                source,
                fresh_session_required,
                message_verb,
            } => serde_json::json!({
                "agent": agent,
                "index": index,
                "content": content,
                "source": source,
                "fresh_session_required": fresh_session_required,
                "next_command": memory_add_next_command(message_verb),
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            MemoryOutput::DryRun(plan) => plan.render(ui),
            MemoryOutput::List {
                agent,
                entries,
                facts,
            } => {
                if entries.is_empty() && facts.is_empty() {
                    ui.payload(&format!("{agent} has no learned memory yet"));
                    return;
                }
                ui.payload(&format!(
                    "{agent}: {} memory log entries and {} facts:",
                    entries.len(),
                    facts.len()
                ));
                for e in entries {
                    ui.kv(&format!("#{}", e.index), &e.content);
                }
                for fact in facts {
                    let scope = memory_scope_label(fact.channel.as_ref());
                    let date = fact.stated_at.split('T').next().unwrap_or(&fact.stated_at);
                    ui.payload(&format!(
                        "{} ({scope}): {} on {date} stated: {}",
                        fact.id, fact.author, fact.statement
                    ));
                }
            }
            MemoryOutput::Deleted {
                agent, id, channel, ..
            } => {
                ui.payload(&format!(
                    "{agent}: deleted fact {id} from {} memory",
                    memory_scope_label(channel.as_ref())
                ));
            }
            MemoryOutput::Added {
                agent,
                index,
                content,
                source,
                fresh_session_required,
                message_verb,
            } => {
                ui.payload(&format!("{agent}: added memory #{index} ({source})"));
                ui.kv("content", content);
                if *fresh_session_required {
                    ui.payload(memory_add_fresh_session_required_line());
                    ui.payload(&memory_add_fresh_session_line(message_verb));
                }
            }
        }
    }
}

fn memory_scope_label(channel: Option<&MemoryChannel>) -> String {
    match channel {
        Some(channel) => format!("channel {}={}", channel.kind, channel.address),
        None => "agent".to_string(),
    }
}

fn is_memory_fact_id(id: &str) -> bool {
    id.len() == 37
        && id.starts_with("fact-")
        && id.as_bytes()[5..]
            .iter()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn memory_channel(channel: Option<String>) -> Result<Option<MemoryChannel>> {
    channel
        .map(|selection| {
            let (kind, address) = selection.split_once('=').ok_or_else(|| {
                crate::exit::usage("channel must be KIND=ADDRESS for an exact bound pair")
            })?;
            if kind.is_empty() || address.is_empty() || selection.chars().any(char::is_whitespace) {
                return Err(crate::exit::usage(
                    "channel must have a nonempty kind and address without whitespace",
                ));
            }
            Ok(MemoryChannel {
                kind: kind.to_string(),
                address: address.to_string(),
            })
        })
        .transpose()
}

fn require_memory_channel(agent: &crate::api::Agent, channel: &MemoryChannel) -> Result<()> {
    if !agent
        .channels
        .iter()
        .any(|binding| binding.kind == channel.kind && binding.address == channel.address)
    {
        return Err(crate::exit::usage(format!(
            "channel {}={} is not bound to agent {:?}",
            channel.kind, channel.address, agent.name
        )));
    }
    Ok(())
}

fn memory_plan_url(
    api_url: &str,
    channel: Option<(&str, &str)>,
    id: Option<&str>,
) -> Result<String> {
    let url = crate::api::memory_state_url(api_url, "<id>", channel, id)?;
    Ok(url
        .as_str()
        .replacen("/agents/%3Cid%3E/state", "/agents/<id>/state", 1))
}

async fn read_memory_facts(
    client: &ApiClient,
    agent_id: &str,
    channel: Option<&MemoryChannel>,
) -> Result<Vec<MemoryFact>> {
    let rows = client
        .list_memory_facts(agent_id, channel.map(MemoryChannel::pair))
        .await?;
    rows.into_iter()
        .filter(|row| is_memory_fact_id(&row.key))
        .map(|row| {
            let field = |name: &str| -> Result<String> {
                row.value
                    .get(name)
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "memory fact {} in {} memory has malformed {name}: expected a nonempty stored string",
                            row.key,
                            memory_scope_label(channel)
                        )
                    })
            };
            Ok(MemoryFact {
                id: row.key.clone(),
                scope: if channel.is_some() { "channel" } else { "agent" }.to_string(),
                channel: channel.cloned(),
                statement: field("statement")?,
                author: field("author")?,
                stated_at: field("stated_at")?,
            })
        })
        .collect()
}

/// List the retained memory log and agent and bound channel facts. A channel
/// selection reads only that binding's fact namespace.
pub async fn memory(opts: AgentActionOpts, channel: Option<String>) -> Result<MemoryOutput> {
    let channel = memory_channel(channel)?;
    if opts.dry_run {
        let lines = if let Some(channel) = &channel {
            vec![format!(
                "GET {}  (would resolve agent {:?} and verify the bound channel first)",
                memory_plan_url(&opts.api_url, Some(channel.pair()), None)?,
                opts.agent
            )]
        } else {
            vec![
                format!(
                    "GET {}/agents/<id>/memory  (would resolve agent {:?} first)",
                    opts.api_url.trim_end_matches('/'),
                    opts.agent
                ),
                format!("GET {}", memory_plan_url(&opts.api_url, None, None)?),
                format!(
                    "GET {}/agents/<id>/state/bindings/<kind>/<address>/memory  (for every distinct bound kind and address pair)",
                    opts.api_url.trim_end_matches('/')
                ),
            ]
        };
        return Ok(MemoryOutput::DryRun(crate::ui::DryRunPlan { lines }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let (entries, facts) = if let Some(channel) = &channel {
        require_memory_channel(&agent, channel)?;
        (
            vec![],
            read_memory_facts(&client, &agent.id, Some(channel)).await?,
        )
    } else {
        let entries = client.list_memory(&agent.id).await?;
        let mut facts = read_memory_facts(&client, &agent.id, None).await?;
        let channels: BTreeSet<_> = agent
            .channels
            .iter()
            .map(|binding| (binding.kind.clone(), binding.address.clone()))
            .collect();
        for (kind, address) in channels {
            let channel = MemoryChannel { kind, address };
            facts.extend(read_memory_facts(&client, &agent.id, Some(&channel)).await?);
        }
        (entries, facts)
    };
    Ok(MemoryOutput::List {
        agent: agent.name,
        entries,
        facts,
    })
}

/// Delete a canonical fact using the current row's version. Its value need not
/// be well formed so operators can remove a corrupt fact.
pub async fn memory_delete(
    opts: AgentActionOpts,
    id: String,
    channel: Option<String>,
) -> Result<MemoryOutput> {
    if !is_memory_fact_id(&id) {
        return Err(crate::exit::usage(
            "fact id must be fact- followed by exactly 32 lowercase hexadecimal characters",
        ));
    }
    let channel = memory_channel(channel)?;
    if opts.dry_run {
        let url = memory_plan_url(
            &opts.api_url,
            channel.as_ref().map(MemoryChannel::pair),
            Some(&id),
        )?;
        return Ok(MemoryOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![
                format!(
                    "GET {url}  (would resolve agent {:?} and verify any selected bound channel first)",
                    opts.agent
                ),
                format!("DELETE {url}?expected_version=<current-version>  (using the version from that GET)"),
            ],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    if let Some(channel) = &channel {
        require_memory_channel(&agent, channel)?;
    }
    let pair = channel.as_ref().map(MemoryChannel::pair);
    let row = client.get_memory_fact(&agent.id, pair, &id).await?;
    client
        .delete_memory_fact(&agent.id, pair, &id, row.version)
        .await?;
    Ok(MemoryOutput::Deleted {
        agent: agent.name,
        id,
        scope: if channel.is_some() {
            "channel"
        } else {
            "agent"
        }
        .to_string(),
        channel,
    })
}

/// `<tier> memory <agent> --add <content>`: append an operator-authored record.
pub async fn memory_add(
    opts: AgentActionOpts,
    content: String,
    message_verb: &str,
) -> Result<MemoryOutput> {
    let content = content.trim().to_string();
    if content.is_empty() {
        return Err(crate::exit::usage(
            "memory content must not be empty. Pass the durable lesson with --add.",
        ));
    }
    if opts.dry_run {
        return Ok(MemoryOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "POST {}/agents/<id>/memory  (would resolve agent {:?} first)",
                opts.api_url, opts.agent
            )],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let entry = client.create_memory(&agent.id, &content).await?;
    Ok(MemoryOutput::Added {
        agent: agent.name,
        index: entry.index,
        content: entry.content,
        source: entry
            .provenance
            .source
            .unwrap_or_else(|| "operator".to_string()),
        fresh_session_required: true,
        message_verb: message_verb.to_string(),
    })
}

/// Which guidance operation `<tier> memory <agent>` was asked for (#1461).
#[derive(Debug, Clone)]
pub enum MemoryGuidanceAction {
    /// `--guidance`: read the effective guidance.
    Show,
    /// `--guidance-from <file>`: store this file's text as operator guidance.
    SetFrom(std::path::PathBuf),
    /// `--reset-guidance`: remove operator guidance.
    Reset,
}

/// The result of [`memory_guidance`]: a dry-run plan (emitted through the
/// memory verb's own `MemoryOutput::DryRun`), or the effective guidance.
#[derive(Debug)]
pub enum MemoryGuidanceResult {
    DryRun(crate::ui::DryRunPlan),
    Shown(MemoryGuidanceOutput),
}

/// Output of `<tier> memory <agent> --guidance|--guidance-from|--reset-guidance`:
/// the guidance the agent gets beside its memory tools after this invocation,
/// its source (`default` or `operator`), and whether this invocation wrote.
#[derive(Debug)]
pub struct MemoryGuidanceOutput {
    pub agent: String,
    pub text: String,
    pub source: String,
    pub changed: bool,
}

impl crate::ui::CliOutput for MemoryGuidanceOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "agent": self.agent,
            "text": self.text,
            "source": self.source,
            "changed": self.changed,
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        let verb = if self.changed { " now" } else { "" };
        ui.payload(&format!(
            "{} memory guidance{verb} ({}):",
            self.agent, self.source
        ));
        ui.payload(&self.text);
    }
}

/// `<tier> memory <agent> --guidance|--guidance-from <file>|--reset-guidance`.
///
/// The file is read, and an empty or whitespace-only one refused, before
/// anything else, dry run included, so a plan is never printed for a write that
/// would be refused. The text is sent verbatim. After a write, the result is
/// the effective guidance the API reports, so the operator sees what the agent
/// will get rather than what was intended.
pub async fn memory_guidance(
    opts: AgentActionOpts,
    action: MemoryGuidanceAction,
) -> Result<MemoryGuidanceResult> {
    let text = match &action {
        MemoryGuidanceAction::SetFrom(path) => {
            let text = std::fs::read_to_string(path).map_err(|e| {
                crate::exit::usage(format!(
                    "cannot read --guidance-from {}: {e}",
                    path.display()
                ))
            })?;
            if text.trim().is_empty() {
                return Err(crate::exit::usage(format!(
                    "--guidance-from {} is empty. Put the guidance text in the file, \
                     or pass --reset-guidance to go back to the platform default",
                    path.display()
                )));
            }
            Some(text)
        }
        _ => None,
    };
    if opts.dry_run {
        let url = format!("{}/agents/<id>/memory/guidance", opts.api_url);
        let line = match (&action, &text) {
            (MemoryGuidanceAction::SetFrom(path), Some(text)) => format!(
                "PUT {url}  {{\"text\": <{} bytes from {}>}}  (would resolve agent {:?} first)",
                text.len(),
                path.display(),
                opts.agent
            ),
            (MemoryGuidanceAction::Reset, _) => format!(
                "DELETE {url}  (would resolve agent {:?} first; the platform default applies after)",
                opts.agent
            ),
            _ => format!(
                "GET {url}  (read-only: would resolve agent {:?} first)",
                opts.agent
            ),
        };
        return Ok(MemoryGuidanceResult::DryRun(crate::ui::DryRunPlan {
            lines: vec![line],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let (guidance, changed) = match (&action, text) {
        (MemoryGuidanceAction::SetFrom(_), Some(text)) => {
            (client.put_memory_guidance(&agent.id, &text).await?, true)
        }
        (MemoryGuidanceAction::Reset, _) => {
            client.delete_memory_guidance(&agent.id).await?;
            (client.get_memory_guidance(&agent.id).await?, true)
        }
        _ => (client.get_memory_guidance(&agent.id).await?, false),
    };
    Ok(MemoryGuidanceResult::Shown(MemoryGuidanceOutput {
        agent: agent.name,
        text: guidance.text,
        source: guidance.source,
        changed,
    }))
}
