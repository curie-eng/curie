//! `<tier> deploy`: prepare a bundle and register it with the platform API.

use super::*;

pub struct DeployOpts {
    /// What was observed about this release's push-delivery paths (#2496).
    ///
    /// `None` means delivery was NOT assessed -- the local tier, which has no
    /// release to read -- and prints nothing at all. Deliberately distinct from
    /// `Some(Delivery::Unknown)`, which means assessed and undetermined and
    /// still speaks: collapsing the two would recreate the very conflation this
    /// ticket exists to remove.
    pub delivery: Option<crate::delivery::Delivery>,
    /// Deploy under this agent name instead of the manifest's.
    ///
    /// One repository is otherwise structurally one agent (#1166): the name
    /// comes from `plugin.json`, so a bundle cannot run as both a dev and a
    /// prod agent. Overriding at deploy keeps the ARTIFACT identical -- the
    /// bundle bytes are untouched, only the binding differs -- which is the
    /// property that lets prod promote exactly what dev validated.
    pub agent: Option<String>,
    /// Resolve agent/env/channel from a `deploy.yaml` target (ADR-0089).
    pub target: Option<String>,
    /// The identity the Slack binding this deploy writes speaks through
    /// (ADR-0168 decision 8). `None` takes the target's, else `default`, which
    /// is written exactly as before.
    pub identity: Option<String>,
    pub plugin_dir: PathBuf,
    pub api_url: String,
    pub api_key: String,
    /// Explicit `--slack-channel`; None when the flag was omitted so a redeploy
    /// leaves an existing agent's channel untouched instead of masking intent
    /// with a default.
    pub slack_channel: Option<String>,
    /// `owner/name` binding the repository whose pushes deploy this agent
    /// (ADR-0014). Bound when the agent is created, or on a later deploy if the
    /// agent has no binding yet (#1194). An agent already bound to a DIFFERENT
    /// repository is left alone and warned about, because a deploy does not
    /// reroute an existing binding.
    pub repo: Option<String>,
    /// Deployment-level managed workspace intent. Preserve deliberately omits
    /// the workspace field so the server can carry the previous value forward.
    pub workspace: WorkspaceIntent,
    /// None means the caller did not pass --env, so a declared target may
    /// supply it. Without a target, cluster deploy infers the sole active
    /// environment. An explicit flag still wins (ADR-0089).
    pub env: Option<DeployEnv>,
    pub label: Option<String>,
    /// Per-agent connector secret NAMES to bind on deploy (ADR-0009, #429). Each
    /// value is resolved from the caller's env or the host secret vault and sent
    /// to the platform API, which stores it on the agent for the worker to
    /// forward into the sandbox. From `deploy --secret <NAME>`.
    pub secret: Vec<String>,
    /// Whether this tier offers `--secret` binding, gating the declared-secrets
    /// policy check (#464): true when the tier can bind a declared secret
    /// (local by value, cluster via the per-agent Helm Secret). When false the
    /// gate is skipped.
    pub secret_binding_supported: bool,
    /// Actionable remediation line printed when the platform API connection
    /// fails (e.g. the kubectl port-forward command for cluster, or
    /// `curie local up` for local). Naming the fix turns a raw
    /// "Connection refused" into something the operator can act on.
    pub connect_hint: String,
    /// Which tier this deploy targets. Explicit and without a `Default` impl on
    /// purpose: `local deploy`, `cluster deploy` and `deploy_named` all reach
    /// one `deploy()`, so the cluster-only connector checks have nothing to
    /// select on without it, and a new caller must not silently inherit
    /// `Local` and skip them.
    pub tier: DeployTier,
}

/// The tier a deploy targets. Two variants, no default: the compiler enumerates
/// every construction site when the field is added.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeployTier {
    Local,
    Cluster,
}

pub use crate::api::WorkspaceIntent;

pub const EMPTY_GITHUB_REPO_ALLOWLIST_WARNING: &str =
    "api.githubRepoAllowlist is empty, so runtime workspace selection denies every repository. \
     Allow `owner/repo` or `owner/*` in the chart values, for example \
     `curie cluster up --set 'api.githubRepoAllowlist[0]=owner/repo'`";

pub fn github_repo_allowlist_is_empty(values: &serde_json::Value) -> bool {
    match values.pointer("/api/githubRepoAllowlist") {
        None | Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::Array(items)) => items.iter().all(|item| {
            item.as_str()
                .map(|entry| entry.trim().is_empty())
                .unwrap_or(true)
        }),
        Some(serde_json::Value::String(raw)) => {
            let trimmed = raw.trim();
            trimmed.is_empty() || trimmed == "[]"
        }
        Some(_) => false,
    }
}

/// Best-effort warning when `--workspace` is passed against an empty chart allowlist.
pub async fn warn_if_empty_github_repo_allowlist(namespace: &str, release: &str) {
    let opts = crate::ops::CommonOpts {
        namespace: namespace.to_string(),
        release: release.to_string(),
        dry_run: false,
    };
    let Ok(Some(values)) = crate::ops::fetch_release_computed_values(&opts).await else {
        return;
    };
    if github_repo_allowlist_is_empty(&values) {
        crate::ui::ui().warn(EMPTY_GITHUB_REPO_ALLOWLIST_WARNING);
    }
}

/// The declared connector-secret NAMES not present in the operator's bound
/// `--secret` set (#464). A non-empty result is a deploy-time gap: the bundle
/// expects a secret nothing will bind, which would surface at runtime as an
/// auth failure (#429).
///
/// Only WELL-FORMED, bindable names count as a gap: a declared name is diffed
/// only when it passes `crate::secrets::validate_name`, the same env-var-syntax
/// check (`^[A-Z_][A-Z0-9_]*$`) used for `curie secrets set`. Malformed or
/// reserved names are the plugin-format validator's responsibility (server-side
/// on upload); the gate excludes them so it never preempts that real validation
/// error with a misleading "bind `--secret <NAME>`" message. The reserved-name
/// list is deliberately NOT mirrored into Rust (drift risk) -- the regex filter
/// is the intended scope.
pub(super) fn unbound_declared_secrets(declared: &[String], bound: &[String]) -> Vec<String> {
    declared
        .iter()
        .filter(|name| {
            crate::secrets::validate_name(name).is_ok() && !bound.iter().any(|b| b == *name)
        })
        .cloned()
        .collect()
}

/// True when a cluster verb SELF-PLUMBS its API transport: no explicit
/// `--api-url`/`CURIE_API_URL` was given, so the release's api Service is
/// reached over a loopback kubectl port-forward rather than direct-dialed.
///
/// The single statement of that discriminant (#1533). [`deploy_port_forward`]
/// and [`deploy_api_tunnel`] both key on this one predicate, so "is a tunnel in
/// play?" cannot answer differently in the two places a call site consults it.
pub fn deploy_self_plumbs(api_url: Option<&str>) -> bool {
    api_url.is_none()
}

/// The kubectl port-forward the auto `cluster deploy` path opens to the
/// release's api service (ADR-0057, superseding ADR-0024's deploy transport).
/// When no `--api-url` is given, deploy self-plumbs this loopback tunnel and
/// posts to `localhost:<local>`, so the discovered strong release key travels
/// only in the X-API-Key header over the tunnel, never over the cleartext UI
/// `/api` NodePort proxy. An explicit `--api-url` direct-dials the given URL, so
/// no tunnel is built (`None`).
pub fn deploy_port_forward(
    api_url: Option<&str>,
    namespace: &str,
    fullname: &crate::ops::ReleaseFullname,
    local_port: u16,
    remote_port: u16,
) -> Option<crate::ops::OpsCommand> {
    if !deploy_self_plumbs(api_url) {
        return None;
    }
    Some(crate::message::port_forward_command(
        namespace,
        fullname,
        "api",
        local_port,
        remote_port,
    ))
}

/// The self-plumbed API tunnel for a cluster verb: the release's RESOLVED
/// [`crate::ops::ReleaseFullname`] and the port-forward command that reaches its
/// api Service, returned TOGETHER. `None` when an explicit
/// `--api-url`/`CURIE_API_URL` was given, since that path direct-dials the URL
/// and builds no tunnel.
///
/// The fullname is resolved LAZILY, on the self-plumbed branch only: the
/// explicit `--api-url` path names no Service, and
/// `cli/tests/cluster_connection_transport.rs` pins a fully explicit connection
/// as never invoking kubectl at all, so resolving eagerly would fire kubectl on
/// a path proven not to.
///
/// Paired rather than handed back as two independent `Option`s (#1533): the
/// fullname the tunnel forwards to is the same one the caller needs for its
/// `svc/<name>` diagnostics and unreachable hints. Carrying them apart left
/// every call site re-deriving the self-plumbed discriminant for itself and
/// asserting at runtime that the two `Option`s agreed.
pub async fn deploy_api_tunnel(
    api_url: Option<&str>,
    namespace: &str,
    release: &str,
    local_port: u16,
    remote_port: u16,
) -> Option<(crate::ops::ReleaseFullname, crate::ops::OpsCommand)> {
    if !deploy_self_plumbs(api_url) {
        return None;
    }
    let fullname = crate::ops::release_fullname(namespace, release).await;
    let command =
        crate::message::port_forward_command(namespace, &fullname, "api", local_port, remote_port);
    Some((fullname, command))
}

/// True when `cluster deploy` must auto-discover the release Secret key: no
/// explicit `--api-key`/`CURIE_API_KEY` was given. An explicit key wins and
/// skips discovery (ADR-0057).
pub fn deploy_needs_key_discovery(explicit_api_key: Option<&str>) -> bool {
    explicit_api_key.is_none()
}

/// An empty `--api-key`/`CURIE_API_KEY=""` is absent, not a key: normalize
/// `Some("")` (after trim) to `None` so a blank value triggers discovery like
/// an omitted flag instead of posting an empty key (401). Same empty-credential
/// rule settled in `message::api_key_or_default` and
/// `ops::resolve_up_credentials`.
pub fn normalize_deploy_api_key(api_key: Option<String>) -> Option<String> {
    api_key.filter(|k| !k.trim().is_empty())
}

pub struct PreparedDeploy {
    /// See [`DeployOpts::delivery`]. Carried, not re-observed: the assessment
    /// belongs to the caller that has a namespace/release.
    delivery: Option<crate::delivery::Delivery>,
    client: ApiClient,
    outcome: crate::api::PreparedDeployOutcome,
    plugin_name: String,
    /// Declarations from the packed manifest, carried so activation never reads
    /// a source manifest that may have changed since upload.
    cron_triggers: Vec<CronTriggerReceipt>,
    label: String,
    env: String,
    requested_repo: Option<String>,
    /// The bundle's `deploy.yaml` TEXT, or None when the bundle has no such
    /// file. Carried rather than re-read so the routing check (#1221) asks
    /// about the file that was actually packed, and unparsed because ADR-0089
    /// keeps exactly one parser for this format and it is not in this binary.
    deploy_targets_yaml: Option<String>,
    connect_hint: String,
    step: crate::ui::Step,
    tier: DeployTier,
    plugin_dir: PathBuf,
    /// The resolved target's connector allowlist (ADR-0168 decision 8); `None`
    /// when no target was resolved or it lists none, which runs every one.
    connector_allowlist: Option<Vec<String>>,
}

pub(super) fn is_documentation_placeholder_channel(channel: &str) -> bool {
    channel.strip_prefix("C0EXAMPLE").is_some_and(|suffix| {
        !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
    })
}

/// Refuse an `--identity` that is not a deploy.yaml identity name, before any
/// request. @spec ADR-0168 d8. The rule is `plugin_format.deploy_targets`'
/// target-name rule; which names exist is the platform's to say.
pub fn validate_identity_name(name: &str) -> Result<()> {
    let bytes = name.as_bytes();
    let alnum = |byte: &u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    let valid = matches!((bytes.first(), bytes.last()), (Some(first), Some(last)) if alnum(first) && alnum(last))
        && bytes.len() <= 40
        && bytes.iter().all(|byte| alnum(byte) || *byte == b'-');
    if valid {
        return Ok(());
    }
    Err(crate::exit::CliError::usage(format!(
        "--identity `{name}` is not an identity name: lowercase letters, digits and dashes, \
         starting and ending with a letter or digit, at most 40 characters"
    ))
    .with_fix("pass the identity name the installation declares, for example --identity ops-bot")
    .into())
}

pub(super) fn reject_documentation_placeholder_target(
    target_name: &str,
    target: &crate::api::ResolvedTarget,
) -> Result<()> {
    let Some(channel) = target.slack_channel.as_deref() else {
        return Ok(());
    };
    if !is_documentation_placeholder_channel(channel) {
        return Ok(());
    }

    Err(crate::exit::CliError::usage(format!(
        "deploy target `{target_name}` uses documentation placeholder Slack channel `{channel}`; \
         refusing before creating or changing an agent, version, or deployment"
    ))
    .with_fix(
        "replace the target's slack_channel with a real Slack channel ID, or remove \
         slack_channel from deploy.yaml",
    )
    .into())
}

impl PreparedDeploy {
    pub fn agent_id(&self) -> &str {
        &self.outcome.agent.id
    }

    pub fn agent_name(&self) -> &str {
        &self.outcome.agent.name
    }

    pub fn version_id(&self) -> &str {
        &self.outcome.version.id
    }
}

pub async fn prepare_deploy(opts: DeployOpts) -> Result<PreparedDeploy> {
    prepare_deploy_with_commit_sha(opts, None).await
}

/// Prepare a deploy with commit provenance supplied by an installer binary.
/// Ordinary deploys pass no override and continue discovering the clean bundle
/// checkout's HEAD. The override is deliberately crate-private: it is not a
/// user-facing way to claim arbitrary provenance.
pub(super) async fn prepare_deploy_with_commit_sha(
    opts: DeployOpts,
    installer_commit_sha: Option<&str>,
) -> Result<PreparedDeploy> {
    let plugin_dir = opts
        .plugin_dir
        .canonicalize()
        .with_context(|| format!("plugin dir not found: {}", opts.plugin_dir.display()))?;
    let (plugin_name, manifest_version) = read_manifest(&plugin_dir)?;

    // Connector lock preflight (ADR 0113), before the bundle is packed so the
    // failure names the operator's own directory rather than a temp path, and
    // long before anything is applied. `opts.plugin_dir` (not the canonicalized
    // copy) is the path they typed.
    let connector_decl = crate::connector_build::load(&plugin_dir)?;
    if let Some(warning) =
        crate::connector_build::undeclared_runner_dockerfile(&opts.plugin_dir, &connector_decl)
    {
        crate::ui::ui().warn(&warning);
    }
    if opts.tier == DeployTier::Local
        && connector_decl
            .connectors
            .values()
            .any(|spec| spec.url.is_none() && spec.unhosted_url.is_none())
    {
        connector_start_timeout_from_env()?;
    }
    {
        let decl = &connector_decl;
        if decl.runner.is_some() || decl.connectors.values().any(|spec| spec.build.is_some()) {
            let recomputed = recompute_source_digests(&plugin_dir, decl)?;
            let lock = crate::connector_build::load_lock(&plugin_dir)?;
            lock_preflight(
                &opts.plugin_dir,
                decl,
                lock.as_ref(),
                &recomputed,
                opts.tier,
            )?;
            // What the REGISTRY answers for the locked digest, not what the lock
            // claims about it, is what a node has to pull (see
            // `registry_preflight`). Cluster only: no local deploy pulls this.
            if opts.tier == DeployTier::Cluster {
                run_registry_preflight(decl, lock.as_ref()).await?;
            }
        }
    }

    let label = opts
        .label
        .unwrap_or_else(|| format!("{manifest_version}-{}", unix_now()));
    let created_by = std::env::var("USER").unwrap_or_else(|_| "curie-cli".to_string());

    // Deploy-time secrets-policy gate (#464 / ADR-0009): every NAME the bundle's
    // manifest `secrets` policy declares must be in the operator's bound
    // `--secret` set, else deploy FAILS naming the gap. Decision is fail-loud per
    // the ticket -- a missing binding otherwise surfaces later as a runtime auth
    // failure (#429). This runs in the shared deploy() path, pre-network, so it
    // covers BOTH `local deploy` and `cluster deploy`. It is gated on
    // `secret_binding_supported` (AC2): skip only on a tier that still cannot
    // bind. Cluster binding is #1488. It
    // runs first, before the archive is even packed: the check is a pure
    // name-set diff on `opts.secret` (the bound NAME set) and needs no packed
    // bundle or resolved values, so a declared-but-unbound policy fails fast
    // without doing any of that work.
    // The NAMES this deploy will actually bind into the agent sandbox: the
    // explicit `--secret` flags, plus the Bearer secret each hosted connector
    // names (#2503, #2559). Curie resolves those itself, so a
    // plugin.json-declared name that connectors.yaml already auto-binds is not
    // a gap and must not be diffed against the flags alone -- the #464 gate
    // below would otherwise refuse a deploy that needs no flag. A manifest
    // secret nothing binds still fails, unchanged. Extra `secrets:` names stay
    // on the connector pod.
    let connector_env_secret_names =
        crate::connector_build::hosted_env_secret_names(&connector_decl);
    // Before any value is read: local resolves these names from the operator's
    // env and vault onto the agent record (#2518), and `unhosted_url`
    // connectors escape `refuse_reserved_secret_names`.
    for name in &connector_env_secret_names {
        crate::connector_build::refuse_reserved_env_secret_name(name)?;
    }
    // Both tiers auto-bind the connectors.yaml Bearer names (#2503 cluster,
    // #2518 local). Local resolves their values below and delivers them on the
    // agent record, exactly like an explicit `--secret`.
    let effective_secret_names = merge_secret_env(opts.secret.clone(), &connector_env_secret_names);

    if opts.secret_binding_supported {
        let declared = read_declared_secrets(&plugin_dir)?;
        let unbound = unbound_declared_secrets(&declared, &effective_secret_names);
        if !unbound.is_empty() {
            return Err(crate::exit::usage(format!(
                "{plugin_name} declares connector secret(s) that were not bound on deploy: {}. \
                 Bind each with `--secret <NAME>` (value read from the environment or from \
                 `curie secrets set <NAME>`).",
                unbound.join(", ")
            )));
        }
    }

    let ui = crate::ui::ui();
    if let Some(channel) = opts.slack_channel.as_deref() {
        validate_channel_binding("slack", channel)?;
    }
    if let Some(identity) = opts.identity.as_deref() {
        validate_identity_name(identity)?;
    }
    let archive = pack_tar_gz(&plugin_dir)?;
    let packed_manifest = read_packed_bundle_manifest(&archive);
    // #2448: the bundle's declared approval routes, read from the PACKED
    // archive -- the exact bytes `pack_tar_gz` just produced and that
    // `deploy_prepared` uploads -- rather than the source tree. A manifest
    // excluded from the archive (a root `.curieignore`, or one of the packer's
    // own built-in exclusions) must never drive this judgment, and a manifest
    // the archive packs from a different location than the naive source read
    // would find must be the one judged instead. Fail-open: an unreadable or
    // absent packed policy only warns, and the API's own fail-closed refusal
    // decides.
    let packed_gates: Result<Vec<(String, String)>> = match &packed_manifest {
        Ok((location, body)) => {
            parse_manifest_gates(body, &format!("packed bundle manifest ({location})"))
        }
        Err(err) => Err(anyhow::anyhow!("{err:#}")),
    };
    let declared_routes: Option<BTreeSet<String>> = match packed_gates {
        Ok(gates) => Some(declared_approval_routes(&gates)),
        Err(err) => {
            ui.warn(&format!(
                "could not read the bundle's approvalPolicy from the packed archive ({err:#}); \
                 skipping the approval-route pre-check. The platform API still checks declared \
                 routes on deploy"
            ));
            None
        }
    };
    let commit_sha = match installer_commit_sha {
        Some(commit_sha) => Some(commit_sha.to_string()),
        None => {
            let git_prefix = tokio::process::Command::new("git")
                .args(["rev-parse", "--show-prefix"])
                .current_dir(&plugin_dir)
                .env_remove("GIT_DIR")
                .env_remove("GIT_WORK_TREE")
                .output()
                .await
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| String::from_utf8(output.stdout).ok())
                .map(|prefix| PathBuf::from(prefix.trim_end_matches(['\r', '\n'])));
            let git_status = tokio::process::Command::new("git")
                .args([
                    "status",
                    "--porcelain=v1",
                    "-z",
                    "--no-renames",
                    "--untracked-files=all",
                    "--ignored=matching",
                    "--",
                    ".",
                ])
                .current_dir(&plugin_dir)
                .env_remove("GIT_DIR")
                .env_remove("GIT_WORK_TREE")
                .output()
                .await
                .ok();
            match (git_prefix, git_status) {
                (Some(prefix), Some(output))
                    if output.status.success()
                        && git_status_is_clean_for_pack(&plugin_dir, &prefix, &output.stdout) =>
                {
                    tokio::process::Command::new("git")
                        .args(["rev-parse", "HEAD"])
                        .current_dir(&plugin_dir)
                        .env_remove("GIT_DIR")
                        .env_remove("GIT_WORK_TREE")
                        .output()
                        .await
                        .ok()
                        .filter(|output| output.status.success())
                        .and_then(|output| String::from_utf8(output.stdout).ok())
                        .map(|sha| sha.trim().to_string())
                        .filter(|sha| !sha.is_empty())
                }
                _ => None,
            }
        }
    };
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    // Resolve a declared target, if one was named (ADR-0089). The file is sent
    // as TEXT and parsed server-side: one parser means the CLI and the
    // validator cannot disagree about where this deploy lands.
    // Read ONCE, for two consumers: `--target` resolution below and the
    // post-deploy routing check (#1221). Absence is the ordinary case, not an
    // error -- most bundles declare no targets -- so the read is kept as a
    // Result only so `--target` can still name the io failure precisely.
    let deploy_targets_path = plugin_dir.join("deploy.yaml");
    let deploy_targets_read = std::fs::read_to_string(&deploy_targets_path);
    let deploy_targets_yaml = deploy_targets_read.as_ref().ok().cloned();
    let resolved = match &opts.target {
        Some(name) => {
            let content = deploy_targets_read.as_ref().map_err(|err| {
                crate::exit::usage(format!(
                    "--target {name} needs a deploy.yaml in the bundle, but {} could not be \
                     read: {err}",
                    deploy_targets_path.display()
                ))
            })?;
            let target = client.resolve_deploy_target(content, name).await?;
            reject_documentation_placeholder_target(name, &target)?;
            Some(target)
        }
        None => None,
    };

    // A target states its environment. An explicit flag still wins, and an
    // omitted environment is resolved after the agent lookup below (#3504).
    let requested_env = opts
        .env
        .map(|e| e.as_str().to_string())
        .or_else(|| resolved.as_ref().map(|r| r.env.clone()));

    // Resolve each --secret NAME to a value (env wins, else the host vault) so
    // the connector secret is bound on the agent for the worker to forward into
    // the sandbox (ADR-0009, #429). The value never appears in argv.
    // On Local the auto-bound connector Bearer names are resolved here too
    // (#2518): the record is local's only sandbox delivery path. Cluster keeps
    // `opts.secret` alone; its connector values are resolved cluster-scoped.
    // @spec ADR-0168 d8. Only a resolved target narrows the CLI's own work; the
    // API render and the runner narrow every deploy's pods regardless.
    let connector_allowlist = resolved.as_ref().and_then(|r| r.connectors.clone());
    // The explicit `--secret` names plus the Bearer names of the connectors
    // this target keeps. Local resolves their values here; an excluded
    // connector never reaches `bring_up_local`, so its secret is not this
    // deploy's to require.
    let target_secret_names = merge_secret_env(
        opts.secret.clone(),
        &crate::connector_build::hosted_env_secret_names(&crate::connector_build::restrict_to(
            &connector_decl,
            connector_allowlist.as_deref(),
        )),
    );
    let resolved_names: &[String] = match opts.tier {
        DeployTier::Local => &target_secret_names,
        DeployTier::Cluster => &opts.secret,
    };
    let mut secrets: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    for name in resolved_names {
        let value = crate::secrets::resolve_env_or_saved(name)?;
        match value {
            Some(v) => {
                secrets.insert(name.clone(), v);
            }
            None if opts.secret.contains(name) => {
                return Err(crate::exit::usage(format!(
                    "--secret {name}: not set in the environment and not saved in Curie \
                     storage; export it or run `curie secrets set {name}` first"
                )));
            }
            None => {
                return Err(crate::exit::usage(format!(
                    "connector secret {name} (the Bearer secret connectors.yaml declares): not \
                     set in the environment and not saved in Curie storage; export it or run \
                     `curie secrets set {name}` first"
                )));
            }
        }
    }
    if !secrets.is_empty() {
        ui.note(&format!(
            "binding {} connector secret(s): {}",
            secrets.len(),
            secrets.keys().cloned().collect::<Vec<_>>().join(", ")
        ));
    }

    // The manifest name is the default, not the law (#1166). `plugin_name`
    // stays the DISPLAY name so the log still says which bundle was deployed;
    // `agent_name` is what the platform binds. Explicit flags beat the target,
    // so a one-off deploy never requires editing a committed file.
    let agent_name = opts
        .agent
        .clone()
        .or_else(|| resolved.as_ref().and_then(|r| r.agent.clone()))
        .unwrap_or_else(|| plugin_name.clone());
    let slack_channel = opts
        .slack_channel
        .as_deref()
        .or_else(|| resolved.as_ref().and_then(|r| r.slack_channel.as_deref()));
    // @spec ADR-0168 d8. An explicit flag beats the target, as every field does.
    let identity = opts
        .identity
        .as_deref()
        .or_else(|| resolved.as_ref().map(|r| r.identity.as_str()))
        .filter(|name| *name != crate::api::DEFAULT_SLACK_IDENTITY);
    if let (Some(identity), None) = (identity, slack_channel) {
        return Err(crate::exit::CliError::usage(format!(
            "identity `{identity}` names the Slack binding this deploy writes, and this deploy \
             writes none: no --slack-channel was passed and no target slack_channel applies"
        ))
        .with_fix("pass --slack-channel <id>, or add slack_channel to the deploy.yaml target")
        .into());
    }
    let record_secret_names = if opts.tier == DeployTier::Cluster {
        target_secret_names.clone()
    } else {
        opts.secret.clone()
    };
    let record_secrets = match opts.tier {
        DeployTier::Local => secrets.clone(),
        // Names-only placeholders over the EFFECTIVE set, not just
        // the resolved `--secret` values: the worker keys
        // `inject_connector_secrets` -- and with it the per-agent
        // sandbox pool routing -- off this map, so a connector secret
        // missing from it lands the claim on the generic pool with no
        // connector env at all (#2503). The connector's own value is
        // resolved cluster-scoped later (#1913) and reaches the pod
        // through the per-agent Helm Secret, never through the record.
        DeployTier::Cluster => {
            crate::cluster_secrets::agent_record_secret_names(&record_secret_names)
        }
    };
    let cl = ui.checklist();
    let step = cl.step(&format!("deploying {plugin_name} as {agent_name}"));
    // Resolve the agent, judge its bound approval routes against the bundle's
    // declared ones (#2448), then send secrets, version, and bundle. One async
    // block so every failure, the local refusal included, reaches the single
    // error arm below.
    let prepared = async {
        let (agent, channel, repo_note) = client
            .resolve_agent_as(&agent_name, slack_channel, opts.repo.as_deref(), identity)
            .await?;
        check_deploy_routes_bound(
            declared_routes.as_ref(),
            &plugin_name,
            &agent,
            &channel,
            opts.tier,
        )?;
        let env = if opts.tier == DeployTier::Cluster && (opts.env.is_some() || resolved.is_none())
        {
            let deployments = client.list_deployments(&agent.id).await?;
            let active_environments: BTreeSet<&str> = deployments
                .iter()
                .filter(|deployment| deployment.status == "active")
                .map(|deployment| deployment.environment.as_str())
                .collect();
            match requested_env {
                Some(env) => {
                    if !active_environments.is_empty()
                        && !active_environments.contains(env.as_str())
                    {
                        let current = active_environments
                            .iter()
                            .copied()
                            .collect::<Vec<_>>()
                            .join(", ");
                        let additional = if active_environments.len() == 1 {
                            "a second environment"
                        } else {
                            "another environment"
                        };
                        ui.note(&format!(
                            "explicit environment {env} differs from the current active \
                             environment {current} of agent {agent_name}; creating {additional}"
                        ));
                    }
                    env
                }
                None => match active_environments.len() {
                    0 => "dev".to_string(),
                    1 => {
                        let env = active_environments
                            .first()
                            .expect("one active environment")
                            .to_string();
                        ui.note(&format!(
                            "inferred environment {env} from the active deployments of agent \
                             {agent_name}"
                        ));
                        env
                    }
                    _ => {
                        return Err(crate::exit::usage(format!(
                            "agent {agent_name} has active deployments in multiple environments \
                             ({}); pass --env or --target to choose the deployment environment",
                            active_environments
                                .iter()
                                .copied()
                                .collect::<Vec<_>>()
                                .join(", ")
                        )));
                    }
                },
            }
        } else {
            requested_env.unwrap_or_else(|| "dev".to_string())
        };
        ui.note(&format!(
            "deploying {plugin_name} ({} bytes) to {} [{env}]",
            archive.len(),
            opts.api_url,
        ));
        client
            .prepare_deploy(
                agent,
                channel,
                repo_note,
                &label,
                &created_by,
                archive,
                &record_secrets,
                commit_sha.as_deref(),
                opts.workspace,
            )
            .await
            .map(|outcome| (outcome, env))
    }
    .await;
    let (outcome, env) = match prepared {
        Ok(prepared) => prepared,
        Err(err) => {
            step.fail("failed");
            if crate::exit::is_transient_reqwest(&err) {
                return Err(crate::exit::operator_context(err, opts.connect_hint, None));
            }
            return Err(err);
        }
    };
    // Successful upload establishes the platform's manifest validation. Read
    // receipts only now, from those packed bytes, without another preflight gate
    // or an empty-receipt fallback for an unreadable manifest.
    let (_, manifest_body) = packed_manifest.context("reading the uploaded bundle's manifest")?;
    let cron_triggers = deploy_cron_triggers_from_manifest(&manifest_body)?;

    Ok(PreparedDeploy {
        delivery: opts.delivery,
        client,
        outcome,
        plugin_name,
        cron_triggers,
        label,
        env,
        requested_repo: opts.repo,
        deploy_targets_yaml,
        connect_hint: opts.connect_hint,
        step,
        tier: opts.tier,
        plugin_dir,
        connector_allowlist,
    })
}

/// The operator-facing text for a repository whose pushes no longer route (#1221).
///
/// Pure, so the wording is unit-testable without a platform. Three things it
/// must do and one it must not:
///
/// - name the repository and every agent now bound to it, because the operator
///   who just deployed one of them cannot otherwise tell who else is affected;
/// - carry the resolver's own `message` VERBATIM -- paraphrasing it would put a
///   second statement of the routing rule in the client, free to drift from the
///   one `gitflow.py` enforces on a push (the drift #1212 exists to correct);
/// - say plainly that the affected pushes break for EVERY agent bound to the
///   repository, including ones this deploy never touched, since the surprise
///   is that a working agent breaks without being deployed to.
///
/// What it must NOT do is widen the damage past what the resolver reported. A
/// bundle can declare a valid `dev` target and a `prod` target naming an agent
/// that does not exist: the answer comes back unresolvable carrying ONLY the
/// prod problem, while dev pushes still deploy exactly as before. Claiming
/// every push is rejected would be false there, and appending a fixed "declare
/// a target" remedy would be worse than false -- targets already exist, and the
/// real remedy is the one the resolver named in its own `message`. So the
/// affected environments are read off `unresolvable`, and no generic remedy is
/// appended at all: each problem's own text carries the fix for its own code
/// (`deploy.no_targets` already ends with "Declare a target (ADR-0089)."), and
/// a hardcoded second remedy could only contradict it.
pub(super) fn routing_warning(check: &RoutingCheck) -> String {
    let agents = if check.agents.is_empty() {
        "(none)".to_string()
    } else {
        check.agents.join(", ")
    };
    // Environments as the resolver named them, deduplicated in the order it
    // sent them. A problem with a blank `environment` (the field is
    // `serde(default)`) contributes no name rather than an empty one.
    let mut envs: Vec<&str> = Vec::new();
    for problem in &check.unresolvable {
        let env = problem.environment.trim();
        if !env.is_empty() && !envs.contains(&env) {
            envs.push(env);
        }
    }
    // With no environment named, the response says routing is broken without
    // saying where. Staying vague is the honest rendering: inventing a list
    // would be the same overstatement this function exists to avoid.
    let scope = if envs.is_empty() {
        "some pushes to this repository no longer deploy anything".to_string()
    } else {
        format!(
            "pushes that deploy the {} environment{} no longer deploy anything",
            envs.join(", "),
            if envs.len() == 1 { "" } else { "s" }
        )
    };
    let mut lines = vec![format!(
        "git-flow routing for {} is broken: {} agents are bound to it ({agents}), and {scope} \
         -- for every one of those agents, including agents this deploy did not touch.",
        check.repo_full_name, check.agent_count
    )];
    for problem in &check.unresolvable {
        lines.push(format!(
            "  {} ({}): {}",
            problem.environment, problem.code, problem.message
        ));
    }
    lines.join("\n")
}

pub async fn deploy_prepared(prepared: PreparedDeploy) -> Result<DeployOutput> {
    let ui = crate::ui::ui();
    let PreparedDeploy {
        delivery,
        client,
        outcome,
        plugin_name,
        cron_triggers,
        label,
        env,
        requested_repo,
        deploy_targets_yaml,
        connect_hint,
        step,
        tier,
        plugin_dir,
        connector_allowlist,
    } = prepared;
    let outcome = match client.activate_deploy(outcome, &env).await {
        Ok(outcome) => {
            step.done(&env);
            outcome
        }
        Err(err) => {
            step.fail("failed");
            if crate::exit::is_transient_reqwest(&err) {
                return Err(crate::exit::operator_context(err, connect_hint, None));
            }
            return Err(err);
        }
    };

    let warnings = deploy_cron_target_warnings(&cron_triggers, &outcome.agent);

    // A declined --repo is otherwise silent: the deploy succeeds, the agent
    // looks fine, and the operator believes the rebind took (#1064, #1212).
    if let Some(note) = &outcome.repo_note {
        ui.warn(note);
    }

    // An APPLIED --repo was equally silent: writing the field that decides
    // which repository's pushes deploy this agent produced no output at all.
    // The value read here is the one the API stored, not the one asked for, so
    // a bind the platform dropped falls to the warning above instead (#1212).
    let bound_repo = requested_repo
        .as_deref()
        .filter(|want| outcome.agent.repo_full_name.as_deref() == Some(*want));
    if let Some(repo) = bound_repo {
        // A binding FACT, not a delivery promise (#2496). Binding is observed;
        // whether a push reaches this install is a separate, unobserved fact
        // that the delivery line below answers from the release's own values.
        ui.note(&crate::delivery::bind_note(repo));
        // ...unless they no longer route anywhere (#1221). Migration 0018
        // (ADR-0091) dropped the unique index on `repo_full_name`, so a SECOND
        // agent may bind the same repository -- and with no declared targets
        // that silently flips every future push for the agent that was ALREADY
        // bound from "deploys" to "rejected". This is the one point BOTH
        // binding paths converge on: an agent bound at creation and one bound
        // by a later PATCH (#1212) both arrive here.
        //
        // The API answers, because the API owns the resolver. Asking it is what
        // keeps this warning from becoming a second copy of the routing rule,
        // free to drift from the one a push actually enforces. It is advisory
        // in every direction: an older platform, an unreachable one, or an
        // undecodable answer all print nothing rather than souring a deploy
        // that already succeeded.
        if let Ok(Some(check)) = client
            .check_git_flow_routing(repo, deploy_targets_yaml.as_deref())
            .await
        {
            if !check.resolvable {
                ui.warn(&routing_warning(&check));
            }
        }

        // Delivery last, because it is the operator's action item: binding fact,
        // then routing (#1221), then whether a push has any observed path here.
        // Inside the `bound_repo` block on purpose -- a declined `--repo` binds
        // nothing, so there is no binding to make a delivery claim about.
        //
        // Advisory in every direction, exactly as #1221's check above: it states
        // only what was observed locally, never mutates helm, and never changes
        // the exit code. A deploy that already succeeded is not soured by it.
        if let Some(line) = crate::delivery::render_optional(delivery.as_ref()) {
            if line.warning {
                ui.warn(&line.text);
            } else {
                ui.note(&line.text);
            }
        }
    }

    let channel = match &outcome.channel {
        ChannelOutcome::Created(channel) => channel.clone(),
        ChannelOutcome::Added { address } => format!("added {address}"),
        ChannelOutcome::Unchanged { channels, passed } => {
            let bound = channels.join(", ");
            if *passed {
                format!("unchanged ({bound})")
            } else {
                format!("unchanged ({bound}); pass --slack-channel to bind another")
            }
        }
    };
    // The local tier runs the bundle's connectors itself (ADR 0113). Reached by
    // both local callers -- `local deploy` and `deploy-local` -- because both
    // arrive here, so the shorthand cannot upload a source-built bundle and
    // start no connector. The identity is the one the deploy RESOLVED, never
    // re-read from plugin.json: `--agent`/`--target` can override it, and an
    // alias built from the manifest name is one the runner never dials.
    if tier == DeployTier::Local {
        let lock = crate::connector_build::load_lock(&plugin_dir)?.unwrap_or_else(|| {
            crate::connector_build::ConnectorLockFileDecl {
                version: crate::connector_build::LOCK_VERSION,
                connectors: std::collections::BTreeMap::new(),
                runner: None,
            }
        });
        let identity = crate::connector_build::ConnectorScope {
            release: "curie".to_string(),
            agent: outcome.agent.name.clone(),
            namespace: "default".to_string(),
        };
        let project = crate::local::current_resources()?.project;
        let decl = crate::connector_build::restrict_to(
            &crate::connector_build::load(&plugin_dir)?,
            connector_allowlist.as_deref(),
        );
        bring_up_local(&plugin_dir, &decl, &lock, &identity, &project).await?;
    }

    Ok(DeployOutput {
        plugin_name,
        label,
        env,
        agent_name: outcome.agent.name,
        agent_id: outcome.agent.id,
        version_label: outcome.version.version_label,
        version_id: outcome.version.id,
        channel,
        bundle_ref: outcome.bundle.bundle_ref,
        bundle_sha256: outcome.bundle.bundle_sha256,
        bundle_size_bytes: outcome.bundle.size_bytes,
        deployment_id: outcome.deployment.id,
        deployment_environment: outcome.deployment.environment,
        deployment_status: outcome.deployment.status,
        cron_triggers,
        warnings,
    })
}

pub async fn deploy(opts: DeployOpts) -> Result<DeployOutput> {
    deploy_with_commit_sha(opts, None).await
}

/// Installer-only deploy entry point. A stamped binary commit takes precedence
/// over bundle-directory Git discovery; `None` preserves ordinary deploy
/// behavior for no-Git builds.
pub(crate) async fn deploy_with_commit_sha(
    opts: DeployOpts,
    installer_commit_sha: Option<&str>,
) -> Result<DeployOutput> {
    let prepared = prepare_deploy_with_commit_sha(opts, installer_commit_sha).await?;
    deploy_prepared(prepared).await
}

/// One cron declaration from the immutable bundle uploaded by a deploy.
#[derive(Debug, Serialize)]
pub struct CronTriggerReceipt {
    pub name: String,
    pub schedule: String,
    pub zone: String,
    pub target: Option<String>,
}

/// Output of `<tier> deploy`: the deployed agent/version/channel/bundle/deployment
/// summary. Owns its data so `to_json`/`render` outlive the `ApiClient`; the
/// json-vs-human choice is made once in `Ui::emit` (#456, #485). Without this the
/// real-path success emitted only `payload`/`kv`, which suppress under `--json`,
/// so `deploy --json` exited 0 with empty stdout.
#[derive(Debug)]
pub struct DeployOutput {
    pub plugin_name: String,
    pub label: String,
    pub env: String,
    pub agent_name: String,
    pub agent_id: String,
    pub version_label: String,
    pub version_id: String,
    pub channel: String,
    pub bundle_ref: String,
    pub bundle_sha256: String,
    pub bundle_size_bytes: u64,
    pub deployment_id: String,
    pub deployment_environment: String,
    pub deployment_status: String,
    pub cron_triggers: Vec<CronTriggerReceipt>,
    pub warnings: Vec<String>,
}

impl DeployOutput {
    fn render_cron(&self, ui: &crate::ui::Ui) {
        for trigger in &self.cron_triggers {
            ui.kv(
                "cron",
                &format!(
                    "{}: {} [{}] -> {} ({})",
                    trigger.name,
                    trigger.schedule,
                    trigger.zone,
                    trigger.target.as_deref().unwrap_or("targetless"),
                    self.agent_name,
                ),
            );
        }
        for warning in &self.warnings {
            ui.warn(warning);
        }
    }
}

/// One completed entry in a deploy across every declared target, retaining the
/// target name beside the full deploy result.
#[derive(Debug)]
pub struct AllTargetsDeployResult {
    pub target: String,
    pub result: DeployOutput,
}

impl AllTargetsDeployResult {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "target": self.target,
            "result": <DeployOutput as crate::ui::CliOutput>::to_json(&self.result),
        })
    }
}

/// Ordered result of a cluster deploy across every declared target.
#[derive(Debug)]
pub struct AllTargetsDeployOutput {
    pub results: Vec<AllTargetsDeployResult>,
}

impl crate::ui::CliOutput for AllTargetsDeployOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "results": self
                .results
                .iter()
                .map(AllTargetsDeployResult::to_json)
                .collect::<Vec<_>>(),
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        if let Some((last, earlier)) = self.results.split_last() {
            for entry in earlier {
                entry.result.render_cron(ui);
            }
            <DeployOutput as crate::ui::CliOutput>::render(&last.result, ui);
        }
    }
}

/// Build the reconciliation payload for a failed cluster deploy across every
/// target. A connector failure includes the completed deploy result.
pub fn all_targets_deploy_failure_json(
    failed_target: &str,
    completed: &[AllTargetsDeployResult],
    failed_result: Option<&DeployOutput>,
    err: &anyhow::Error,
) -> serde_json::Value {
    let completed = completed
        .iter()
        .map(AllTargetsDeployResult::to_json)
        .collect::<Vec<_>>();
    let fix = crate::exit::classify(err).1;

    match failed_result {
        Some(result) => serde_json::json!({
            "failed_target": failed_target,
            "stage": "connector_sync",
            "completed": completed,
            "failed_result": <DeployOutput as crate::ui::CliOutput>::to_json(result),
            "error": format!("{err:#}"),
            "fix": fix,
        }),
        None => serde_json::json!({
            "failed_target": failed_target,
            "stage": "deploy",
            "completed": completed,
            "error": format!("{err:#}"),
            "fix": fix,
        }),
    }
}

impl crate::ui::CliOutput for DeployOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "plugin": self.plugin_name,
            "label": self.label,
            "environment": self.env,
            "agent": {"name": self.agent_name, "id": self.agent_id},
            "version": {"label": self.version_label, "id": self.version_id},
            "channel": self.channel,
            "bundle": {
                "ref": self.bundle_ref,
                "sha256": self.bundle_sha256,
                "size_bytes": self.bundle_size_bytes,
            },
            "deployment": {
                "id": self.deployment_id,
                "environment": self.deployment_environment,
                "status": self.deployment_status,
            },
            "cron_triggers": self.cron_triggers,
            "warnings": self.warnings,
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        ui.payload(&format!(
            "deployed {} {} -> {}",
            self.plugin_name, self.label, self.env
        ));
        ui.kv(
            "agent",
            &format!("{} ({})", self.agent_name, ui.url(&self.agent_id)),
        );
        ui.kv(
            "version",
            &format!("{} ({})", self.version_label, ui.url(&self.version_id)),
        );
        ui.kv("channel", &self.channel);
        ui.kv(
            "bundle",
            &format!(
                "{} sha256:{} {} bytes",
                self.bundle_ref, self.bundle_sha256, self.bundle_size_bytes
            ),
        );
        ui.kv(
            "deployment",
            &format!(
                "{} [{}] {}",
                self.deployment_id, self.deployment_environment, self.deployment_status
            ),
        );
        self.render_cron(ui);
    }
}

/// Read receipts without validating declarations again: the platform's bundle
/// validator decides whether the uploaded manifest can be activated.
fn deploy_cron_triggers_from_manifest(body: &str) -> Result<Vec<CronTriggerReceipt>> {
    let manifest: serde_json::Value =
        serde_json::from_str(body).context("plugin manifest is not valid JSON")?;
    let Some(triggers) = manifest.get("triggers").and_then(|value| value.as_array()) else {
        return Ok(Vec::new());
    };
    Ok(triggers
        .iter()
        .filter_map(|trigger| {
            if trigger.get("type").and_then(|value| value.as_str()) != Some("cron") {
                return None;
            }
            Some(CronTriggerReceipt {
                name: trigger.get("name")?.as_str()?.to_string(),
                schedule: trigger.get("schedule")?.as_str()?.to_string(),
                zone: trigger
                    .get("timezone")
                    .and_then(|value| value.as_str())
                    .unwrap_or("UTC")
                    .to_string(),
                target: trigger
                    .get("target")
                    .and_then(|value| value.as_str())
                    .map(str::to_string),
            })
        })
        .collect())
}

/// Match the selected hook-fire rule, using the resolved agent's API-normalized
/// bindings. The worker's existing cron loop does not trim targets; changing
/// that behavior is outside this deploy advisory (#4009).
fn deploy_cron_target_warnings(
    triggers: &[CronTriggerReceipt],
    agent: &crate::api::Agent,
) -> Vec<String> {
    triggers
        .iter()
        .filter_map(|trigger| {
            let target = trigger.target.as_deref()?.trim();
            let candidates: Vec<_> = agent
                .channels
                .iter()
                .filter(|binding| binding.address == target)
                .collect();
            // apps/api/src/curie_api/routers/hook_fire.py::fire_hook narrows only
            // multiple all-Slack matches to the default identity, then requires
            // exactly one. Mixed kinds and duplicate defaults stay ambiguous.
            let matches = if candidates.len() > 1
                && candidates.iter().all(|binding| binding.kind == "slack")
            {
                candidates
                    .iter()
                    .filter(|binding| binding.named_adapter().is_none())
                    .count()
            } else {
                candidates.len()
            };
            if matches == 1 {
                return None;
            }
            Some(format!(
                "cron trigger `{}` targets `{target}`, which matches no single channel bound to \
                 `{}`; every slot records failed until that address is bound.",
                trigger.name, agent.name,
            ))
        })
        .collect()
}
