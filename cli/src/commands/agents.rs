//! `list-agents` and `deploy-local`: the folder-named agent shortcuts.

use super::*;

/// `curie list-agents`: list the plugin bundles under `agents/`, a personal,
/// gitignored directory (sibling of `examples/`) for in-progress agent
/// projects ready to hand to `curie deploy-local <folder>`. A release binary has
/// no checkout to scan, so this errors clearly outside one, same as `dev_script`.
pub async fn list_agents() -> Result<()> {
    let root = find_repo_root().context(
        "runner/Dockerfile not found here or in any parent directory. Run `curie list-agents` \
         from a curie source checkout.",
    )?;
    let bundles = crate::discover::discover_bundles(&root.join("agents"))?;
    crate::ui::ui().emit(&ListAgentsOutput {
        agents: bundles
            .into_iter()
            .map(|b| LocalAgentSummary {
                name: b.name,
                description: b.description,
                directory: b.directory.display().to_string(),
            })
            .collect(),
    });
    Ok(())
}

pub struct LocalAgentSummary {
    pub name: String,
    pub description: String,
    pub directory: String,
}

/// Output of `list-agents`. Routes through the one `Ui::emit` point rather
/// than an inline `if json()` branch (mirrors `secrets list`'s
/// `SecretsListOutput`). Public so the schema contract test (#634) can build one
/// and validate `to_json` against `cli/schema/list-agents.schema.json`.
pub struct ListAgentsOutput {
    pub agents: Vec<LocalAgentSummary>,
}

impl crate::ui::CliOutput for ListAgentsOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "agents": self.agents.iter().map(|a| serde_json::json!({
                "name": a.name,
                "description": a.description,
                "directory": a.directory,
            })).collect::<Vec<_>>(),
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        if self.agents.is_empty() {
            ui.note("no local agents under agents/ (none found, or the directory doesn't exist)");
        } else {
            let lines: Vec<String> = self
                .agents
                .iter()
                .map(|a| format!("{} -- {} ({})", a.name, a.description, a.directory))
                .collect();
            ui.payload_plain(&lines.join("\n"));
        }
    }
}

/// Resolve `agents/<folder>` under the repo root to a bundle directory for
/// `curie deploy-local <folder>`. Errors with the available folder names (from
/// `discover::discover_bundles`) when `folder` doesn't match one, so a typo
/// doesn't dead-end without a next step.
pub(super) fn resolve_agent_folder(folder: &str) -> Result<std::path::PathBuf> {
    let root = find_repo_root().context(
        "runner/Dockerfile not found here or in any parent directory. Run `curie deploy-local` from \
         a curie source checkout.",
    )?;
    let agents_root = root.join("agents");
    let dir = agents_root.join(folder);
    if dir.join(".claude-plugin/plugin.json").is_file() {
        return Ok(dir);
    }
    let available = crate::discover::discover_bundles(&agents_root)?;
    if available.is_empty() {
        bail!(
            "no agent bundle named {folder:?} under agents/ (the directory has no bundles yet -- \
             create one with `curie init` inside agents/{folder})"
        );
    }
    let names: Vec<String> = available
        .iter()
        .filter_map(|b| b.directory.file_name())
        .map(|n| n.to_string_lossy().into_owned())
        .collect();
    bail!(
        "no agent bundle named {folder:?} under agents/. Available: {}",
        names.join(", ")
    );
}

/// `curie deploy-local <folder>`: shorthand for
/// `curie local deploy --plugin-dir agents/<folder>` -- same underlying
/// `deploy()` call, just resolved by name instead of a hand-typed path. Local
/// tier only: cluster deploy's API-key discovery and port-forward
/// self-plumbing (`main.rs`'s `ClusterAction::Deploy` arm) is not duplicated
/// here; use `curie cluster deploy --plugin-dir agents/<folder>` directly
/// for that tier.
pub async fn deploy_named(folder: &str, opts: DeployNamedOpts) -> Result<DeployOutput> {
    let plugin_dir = resolve_agent_folder(folder)?;
    let api_url = opts.api_url.clone();
    let result = deploy(DeployOpts {
        // Local tier: there is no release to read, so delivery is not assessed.
        delivery: None,
        plugin_dir,
        // deploy_named is the multi-agent-folder path: the folder IS the agent
        // identity there, so there is nothing to override.
        agent: None,
        target: None,
        identity: None,
        api_url,
        api_key: opts.api_key,
        slack_channel: opts.slack_channel,
        repo: opts.repo,
        workspace: WorkspaceIntent::Preserve,
        tier: DeployTier::Local,
        env: Some(opts.env),
        label: opts.label,
        secret: opts.secret,
        secret_binding_supported: true,
        connect_hint: "the platform API is unreachable.".to_string(),
    })
    .await;
    crate::local::with_deploy_unreachable_hint(result, &opts.api_url).await
}

/// The `curie deploy-local <folder>` flags, mirroring `local deploy`'s minus
/// `plugin_dir` (resolved from `folder` instead).
pub struct DeployNamedOpts {
    pub api_url: String,
    pub api_key: String,
    pub slack_channel: Option<String>,
    /// `owner/name` binding the repository whose pushes deploy this agent
    /// (ADR-0014). Bound when the agent is created, or on a later deploy if the
    /// agent has no binding yet (#1194). An agent already bound to a DIFFERENT
    /// repository is left alone and warned about, because a deploy does not
    /// reroute an existing binding.
    pub repo: Option<String>,
    pub env: DeployEnv,
    pub label: Option<String>,
    pub secret: Vec<String>,
}
