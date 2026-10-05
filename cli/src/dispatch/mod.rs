//! Route a parsed [`Command`] to its handler. [`run`] matches the top-level
//! command; each tier's subcommands dispatch from their own submodule.

use std::collections::BTreeMap;

use anyhow::{bail, Result};
use clap::parser::ValueSource;
use curie::api;
use curie::artifacts;
use curie::channel_token as crate_channel_token;
use curie::commands::{self, AgentActionOpts, DeployOpts, StartOpts};
use curie::comms::{self, CommsOpts, LocalCommsOpts};
use curie::github_app as crate_github_app;
use curie::local::{self, LocalDownOpts, LocalOpts};
use curie::message::{self, MessageOpts};
use curie::ops::{self, CommonOpts, DownOpts, RollbackOpts, UpOpts, UpgradeChart, UpgradeOpts};
use curie::secrets;
use curie::state::{apply_continue, load_turn, CliTurnArgs, TurnVerb};
use curie::ui;

use crate::args::*;

mod cluster_action;
mod cluster_target;
mod dev_action;
mod local_action;
mod skill_action;

pub(crate) use cluster_target::*;

/// Stand up the connectors this version declares, and prune what it dropped.
///
/// Split across the two components on purpose (ADR-0086): the API RENDERS the
/// Kubernetes objects -- a pure function of the bundle, so it needs no cluster
/// access and keeps its read-only RBAC on the service that receives internet
/// webhooks -- and the CLI APPLIES them under the operator's own kubectl
/// credentials, where cluster-write authority already lived.
///
/// A bundle with no `connectors.yaml` still reaches the prune: that is the case
/// where a connector was REMOVED, and leaving it running with a credential
/// mounted and nothing referencing it is the leak nobody notices.
///
/// This half only RESOLVES: it discovers the cluster binding and renders the
/// objects, so the caller can read the owned secret names off the prepared sync
/// (#2503) before handing it to [`apply_connectors`].
pub(crate) async fn prepare_cluster_connectors(
    api_url: &str,
    api_key: &str,
    namespace: &str,
    release: &str,
    agent_id: &str,
    agent_name: &str,
    version_id: &str,
) -> anyhow::Result<curie::connectors::PreparedConnectorSync> {
    let target = curie::connectors::bind_current_cluster(namespace, release).await?;
    let app_name = curie::connectors::discover_app_name(&target).await?;
    let connector_version = ConnectorVersion {
        agent_id,
        agent_name,
        version_id,
    };
    prepare_connectors(
        api_url,
        api_key,
        namespace,
        release,
        &app_name,
        connector_version,
        target,
    )
    .await
}

pub(crate) struct ConnectorVersion<'a> {
    pub(crate) agent_id: &'a str,
    pub(crate) agent_name: &'a str,
    pub(crate) version_id: &'a str,
}

pub(crate) async fn prepare_connectors(
    api_url: &str,
    api_key: &str,
    namespace: &str,
    release: &str,
    app_name: &str,
    connector_version: ConnectorVersion<'_>,
    target: curie::connectors::ClusterTarget,
) -> anyhow::Result<curie::connectors::PreparedConnectorSync> {
    let client = curie::api::ApiClient::new(api_url, api_key)?;
    let rendered = client
        .version_connectors(
            connector_version.agent_id,
            connector_version.version_id,
            release,
            namespace,
            app_name,
        )
        .await?;
    curie::connectors::prepare(
        &rendered.manifests,
        &rendered.mcp_entries,
        &rendered.owned_secret_name,
        &rendered.owned_secret_keys,
        &target.scope,
        connector_version.agent_name,
        &BTreeMap::new(),
    )?
    .bind_target(target)
}

pub(crate) async fn apply_connectors(
    prepared: curie::connectors::PreparedConnectorSync,
) -> anyhow::Result<()> {
    let synced = curie::connectors::sync(prepared).await?;
    curie::connectors::report_connector_sync(&synced);
    Ok(())
}

/// Resolve a cluster verb's API URL, key, and optional loopback tunnel. Explicit
/// values win. An omitted URL self-plumbs the release API service, while an
/// omitted key is read from the release Secret.
pub(crate) async fn resolve_cluster_conn(
    conn: ClusterConn,
    dry_run: bool,
) -> anyhow::Result<(String, String, Option<tokio::process::Child>)> {
    let ClusterConn {
        api_url,
        api_key,
        namespace,
        release,
    } = conn;
    let api_key = commands::normalize_deploy_api_key(api_key);
    let key_auto_discovered = api_key.is_none();
    let local_port = 0;
    if dry_run {
        return Ok((
            api_url.unwrap_or_else(|| format!("http://localhost:{local_port}")),
            api_key.unwrap_or_default(),
            None,
        ));
    }
    let api_key = match api_key {
        Some(key) => key,
        None => ops::discover_api_key(&namespace, &release).await?,
    };
    let tunnel = commands::deploy_api_tunnel(
        api_url.as_deref(),
        &namespace,
        &release,
        local_port,
        message::API_REMOTE_PORT,
    )
    .await;
    let (api_url, port_forward) = match tunnel {
        Some((_fullname, pf_cmd)) => {
            let (child, effective_port) =
                message::start_port_forward(&pf_cmd, local_port, "cluster api").await?;
            (format!("http://127.0.0.1:{effective_port}"), Some(child))
        }
        None => {
            let url = api_url.expect("explicit url when no port-forward");
            if key_auto_discovered && api::is_insecure_endpoint(&url) {
                bail!(
                    "refusing to send the auto-discovered release key over cleartext \
                     HTTP to {url}: the strong key would leak on the wire. Pass \
                     --api-key explicitly to acknowledge, use an https:// URL, or omit \
                     --api-url to reach the release over the loopback port-forward."
                );
            }
            (url, None)
        }
    };
    Ok((api_url, api_key, port_forward))
}

pub(crate) async fn run_local_observability_query(
    action: LocalObservabilityQuery,
) -> Result<Box<dyn curie::ui::CliOutput>> {
    let (conn, query) = match action {
        LocalObservabilityQuery::Runs { query, conn } => (
            conn,
            curie::observability::ObservabilityQuery::Runs {
                limit: query.limit,
                agent_id: query.agent_id,
            },
        ),
        LocalObservabilityQuery::Run { query, conn } => (
            conn,
            curie::observability::ObservabilityQuery::Run {
                trace_id: query.trace_id,
            },
        ),
        LocalObservabilityQuery::Metrics { query, conn } => (conn, query.into_query()?),
    };
    curie::observability::query("local", &conn.api_url, &conn.api_key, query).await
}

pub(crate) async fn run_cluster_observability_query(
    action: ClusterObservabilityQuery,
    namespace: String,
    release: String,
) -> Result<Box<dyn curie::ui::CliOutput>> {
    let (conn, query) = match action {
        ClusterObservabilityQuery::Runs { query, conn } => (
            conn,
            curie::observability::ObservabilityQuery::Runs {
                limit: query.limit,
                agent_id: query.agent_id,
            },
        ),
        ClusterObservabilityQuery::Run { query, conn } => (
            conn,
            curie::observability::ObservabilityQuery::Run {
                trace_id: query.trace_id,
            },
        ),
        ClusterObservabilityQuery::Metrics { query, conn } => (conn, query.into_query()?),
    };
    if conn.api_url.is_some() && conn.api_key.is_none() {
        return Err(anyhow::Error::from(
            curie::exit::CliError::usage(
                "--api-url requires --api-key for cluster observability queries",
            )
            .with_fix(
                "pass the matching --api-key explicitly, or omit --api-url to use release discovery over a loopback port-forward",
            ),
        ));
    }
    let api_key = match conn.api_key {
        Some(key) => key,
        None => ops::discover_api_key(&namespace, &release).await?,
    };
    let local_port = 0;
    let _port_forward;
    let tunnel = commands::deploy_api_tunnel(
        conn.api_url.as_deref(),
        &namespace,
        &release,
        local_port,
        message::API_REMOTE_PORT,
    )
    .await;
    let api_url = match tunnel {
        Some((_fullname, command)) => {
            let (child, effective_port) =
                message::start_port_forward(&command, local_port, "observability api").await?;
            _port_forward = Some(child);
            format!("http://127.0.0.1:{effective_port}")
        }
        None => {
            _port_forward = None;
            conn.api_url
                .expect("explicit URL present when no port-forward is planned")
        }
    };
    curie::observability::query("cluster", &api_url, &api_key, query).await
}

/// Resolve, and materialize, the compose file for a local verb.
///
/// `local up` does not call this: it inlines the same two steps so the
/// `--build` channel guard can run between them (#1926).
pub(crate) async fn bind_local_opts(
    project: Option<String>,
    files: Vec<String>,
    dry_run: bool,
    build: bool,
) -> Result<LocalOpts> {
    let isolated = project
        .as_deref()
        .is_some_and(|value| value != local::COMPOSE_PROJECT)
        || files.len() > 1
        || std::env::var("COMPOSE_FILE")
            .ok()
            .is_some_and(|value| !value.is_empty());
    if isolated {
        let resources = local::resolve_local_resources(project, files, String::new(), build)?;
        if build {
            let resolved = artifacts::resolve_compose(
                resources.compose_files.first().map(String::as_str),
                artifacts::Channel::current(),
                artifacts::version(),
                artifacts::cache_root,
                std::path::Path::new(local::DEFAULT_COMPOSE_FILE).exists(),
            )?;
            let _ = local::ensure_build_reaches_the_stack(&resolved)?;
        }
        let mut opts = LocalOpts::for_file(
            resources
                .compose_files
                .first()
                .cloned()
                .unwrap_or_else(|| local::DEFAULT_COMPOSE_FILE.to_string()),
        );
        opts.resources = resources;
        opts.dry_run = dry_run;
        opts.build = build.then_some(local::BuildReach::Substitutes);
        return Ok(opts);
    }
    let resolved = artifacts::resolve_compose(
        files.first().map(String::as_str),
        artifacts::Channel::current(),
        artifacts::version(),
        artifacts::cache_root,
        std::path::Path::new(local::DEFAULT_COMPOSE_FILE).exists(),
    )?;
    let reach = if build {
        Some(local::ensure_build_reaches_the_stack(&resolved)?)
    } else {
        None
    };
    let file = materialize_artifact(resolved, dry_run, "compose").await?;
    let mut opts = LocalOpts::for_file(file);
    opts.dry_run = dry_run;
    opts.build = reach;
    Ok(opts)
}

/// The sandbox connector-secret bind map for a cluster deploy (#2503).
///
/// Two sources, deliberately resolved differently:
/// - an explicit `--secret NAME` keeps its existing semantics -- env first,
///   then unscoped host storage, and a hard error when neither has it;
/// - a hosted connector's declared name reuses the value the connector plan
///   ALREADY resolved for this cluster scope. It is never re-resolved here: a
///   scoped-only credential would not resolve at all, and a stale environment
///   value would put a different credential in the sandbox than the connector
///   pod holds. On overlap the scoped connector value therefore wins, because
///   that is the credential the connector itself authenticates with and #1913's
///   guarantee is one cluster, one credential.
pub(crate) fn cluster_connector_bind_values(
    explicit: &[String],
    connector_names: &[String],
    connector_values: &std::collections::BTreeMap<String, String>,
) -> Result<std::collections::BTreeMap<String, String>> {
    let mut values = curie::cluster_secrets::resolve_named_secrets(explicit)?;
    for name in connector_names {
        if let Some(value) = connector_values.get(name) {
            values.insert(name.clone(), value.clone());
        }
    }
    Ok(curie::cluster_secrets::sandbox_connector_secrets(&values))
}

fn parse_runner_image_binding(raw: &str) -> Result<curie::factory_intake::RunnerImageBinding> {
    let (agent, image) = raw.split_once('=').ok_or_else(|| {
        curie::exit::CliError::usage("--runner-image must be AGENT=IMAGE")
            .with_fix("pass dark-factory=ghcr.io/example/runner@sha256:<64 hex digits>")
    })?;
    let digest = image
        .split_once("@sha256:")
        .map(|(_, digest)| digest)
        .filter(|digest| digest.len() == 64 && digest.chars().all(|c| c.is_ascii_hexdigit()));
    if agent.is_empty()
        || !agent.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        || digest.is_none()
    {
        return Err(curie::exit::CliError::usage(
            "--runner-image must name an agent and a digest-pinned image",
        )
        .with_fix("pass dark-factory=ghcr.io/example/runner@sha256:<64 hex digits>")
        .into());
    }
    Ok(curie::factory_intake::RunnerImageBinding {
        agent: agent.to_string(),
        image: image.to_string(),
    })
}

pub(crate) async fn bind_cluster_connector_secrets(
    namespace: &str,
    release: &str,
    chart: Option<&str>,
    agent_name: &str,
    secrets: std::collections::BTreeMap<String, String>,
    runner_image: Option<String>,
) -> Result<()> {
    // No early return on empty secrets: a redeploy that drops every connector
    // secret must clear the agent's stale binding (#3021), and a runner image
    // to set or an earlier one to clear (#3260) is decided by bind_if_changed.
    //
    // #3082: a bundle deploy whose connector secrets already match the
    // release must not helm-upgrade the platform, so the chart is resolved
    // only when the bind actually changes something.
    let chart = async {
        let resolved = artifacts::resolve_chart(
            chart,
            artifacts::Channel::current(),
            artifacts::version(),
            artifacts::cache_root,
            std::path::Path::new("charts/curie").is_dir(),
        )?;
        materialize_artifact(resolved, false, "chart").await
    };
    curie::cluster_secrets::bind_if_changed(
        CommonOpts {
            namespace: namespace.to_string(),
            release: release.to_string(),
            dry_run: false,
        },
        agent_name.to_string(),
        secrets,
        runner_image,
        chart,
    )
    .await?;
    Ok(())
}

/// Bind the sandbox connector secrets for one deployed agent and then apply its
/// prepared connector objects (#2503). The union of explicit `--secret` names
/// and the bundle's connector-owned names is resolved against the values the
/// connector plan already resolved for this cluster scope, bound into the
/// per-agent Helm Secret, and only then are the connector objects applied --
/// the one order both the single-target and `--all-targets` cluster deploy
/// paths use. The same bind sets or clears the agent's locked runner image
/// (#3260).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn bind_and_apply_cluster_connectors(
    namespace: &str,
    release: &str,
    chart: Option<&str>,
    agent_name: &str,
    explicit_secrets: &[String],
    connector_env_secret_names: &[String],
    runner_image: Option<String>,
    prepared: curie::connectors::PreparedConnectorSync,
) -> Result<()> {
    let bind_values = cluster_connector_bind_values(
        explicit_secrets,
        connector_env_secret_names,
        prepared.owned_secret_values(),
    )?;
    bind_cluster_connector_secrets(
        namespace,
        release,
        chart,
        agent_name,
        bind_values,
        runner_image,
    )
    .await?;
    apply_connectors(prepared).await
}

pub(crate) async fn materialize_artifact(
    resolved: artifacts::Resolved,
    dry_run: bool,
    label: &str,
) -> Result<String> {
    if dry_run {
        if let artifacts::Resolved::Fetch { url, .. } = &resolved {
            ui::ui().note(&format!("{label} source: {}", ui::ui().url(url)));
        }
        Ok(resolved.planned_target().display().to_string())
    } else {
        Ok(artifacts::ensure_cached(&resolved)
            .await?
            .display()
            .to_string())
    }
}

/// Resolve the upgrade chart without erasing whether a release dry-run still
/// needs the network-free target checks. Explicit operands retain the existing
/// rule: an available path is local, while any other operand is Helm-resolved.
pub(crate) async fn materialize_upgrade_chart(
    resolved: artifacts::Resolved,
    dry_run: bool,
) -> Result<UpgradeChart> {
    let pending_source = match &resolved {
        artifacts::Resolved::Fetch { url, cache_path } if dry_run && !cache_path.exists() => {
            Some(url.clone())
        }
        _ => None,
    };
    let helm_reference = matches!(&resolved, artifacts::Resolved::Local(path) if !path.exists());
    let operand = materialize_artifact(resolved, dry_run, "chart").await?;

    if let Some(source_url) = pending_source {
        Ok(UpgradeChart::PendingRelease {
            source_url,
            cache_path: operand,
        })
    } else if helm_reference {
        Ok(UpgradeChart::HelmReference(operand))
    } else {
        Ok(UpgradeChart::AvailableLocal(operand))
    }
}

/// Run a `<tier> memory --guidance*` action and emit its result: a dry-run
/// plan through the memory verb's own `MemoryOutput::DryRun`, otherwise the
/// effective guidance.
pub(crate) async fn emit_memory_guidance(
    opts: AgentActionOpts,
    action: commands::MemoryGuidanceAction,
) -> Result<()> {
    match commands::memory_guidance(opts, action).await? {
        commands::MemoryGuidanceResult::DryRun(plan) => emit(commands::MemoryOutput::DryRun(plan)),
        commands::MemoryGuidanceResult::Shown(out) => emit(out),
    }
}

/// Route a handler's structured output through the one success-path emit
/// (`Ui::emit`), mirroring the centralized error emit in `main`. The read verbs
/// return a `CliOutput` instead of touching stdout themselves, so the
/// json-vs-human decision is made in exactly one place (issue #456).
pub(crate) fn emit<T: curie::ui::CliOutput>(out: T) -> Result<()> {
    ui::ui().emit(&out);
    Ok(())
}

/// Trait-object counterpart used by the shared observability query handler,
/// whose three leaves intentionally return different concrete output types.
pub(crate) fn emit_boxed(out: Box<dyn curie::ui::CliOutput>) -> Result<()> {
    ui::ui().emit(out.as_ref());
    Ok(())
}

/// Select the runner image declared by a bundle.
pub(crate) fn skill_runner_image(
    plugin_dir: &std::path::Path,
    image: Option<&str>,
) -> Result<String> {
    if let Some(image) = image {
        return Ok(image.to_string());
    }

    let declaration = curie::connector_build::load(plugin_dir)
        .map_err(|err| curie::exit::usage(format!("{err:#}")))?;
    if declaration.runner.is_some() {
        let lock = curie::connector_build::load_lock(plugin_dir)
            .map_err(|err| curie::exit::usage(format!("{err:#}")))?;
        return lock
            .and_then(|lock| lock.runner)
            .map(|runner| runner.image)
            .ok_or_else(|| {
                curie::exit::CliError::usage(format!(
                    "{} declares a runner layer, but {} records no runner image for it. Run `curie build --plugin-dir {}` first.",
                    curie::connector_build::CONNECTORS_FILE,
                    curie::connector_build::CONNECTOR_LOCK_FILE,
                    plugin_dir.display(),
                ))
                .with_fix(format!(
                    "Run `curie build --plugin-dir {}` first.",
                    plugin_dir.display(),
                ))
                .into()
            });
    }

    Ok(artifacts::resolve_image(
        None,
        artifacts::Channel::current(),
        artifacts::version(),
    ))
}

/// Dispatch one parsed command. No subcommand opens the interactive terminal,
/// matching `curie interactive` / `curie ui`. Returns the command's
/// `Result`; `main`
/// classifies any error into a semantic exit code (see `curie::exit`).
pub(crate) async fn run(command: Option<Command>) -> Result<()> {
    match command {
        None => curie::interactive::run().await,
        Some(Command::Try { keep }) => {
            let image =
                artifacts::resolve_image(None, artifacts::Channel::current(), artifacts::version());
            emit(commands::try_first_run(keep, image).await?)
        }
        Some(Command::Init {
            name,
            dir,
            from_spec,
            adopt,
        }) => commands::init(name, dir, from_spec, adopt),
        Some(Command::Example {
            action:
                ExampleAction::DarkFactory {
                    action: DarkFactoryAction::Render { out },
                },
            ..
        }) => emit(
            curie::examples::render_dark_factory(curie::examples::DarkFactoryRenderOpts { out })
                .await?,
        ),
        Some(Command::Example {
            action: ExampleAction::SreBot { action },
            context,
        }) => {
            let target = curie::kube_context::pin_for_cluster_command(context.as_deref())?;
            if let Some(target) = &target {
                ui::ui().note(&format!(
                    "Kubernetes context: {} (cluster {})",
                    target.context, target.cluster
                ));
            }
            match action {
                SreBotAction::Install {
                    observability,
                    observability_only,
                    dry_run,
                    slack_channel,
                    platform_upgrade,
                    namespace,
                    release,
                    observability_namespace,
                    workspace_repo,
                    approvers,
                } => match curie::examples::install_sre_bot(curie::examples::SreBotInstallOpts {
                    observability,
                    observability_only,
                    dry_run,
                    slack_channel,
                    platform_upgrade,
                    namespace,
                    release,
                    observability_namespace,
                    workspace_repo,
                    approvers,
                })
                .await?
                {
                    curie::examples::SreBotInstallResult::DryRun(plan) => emit(plan),
                    curie::examples::SreBotInstallResult::Installed(deployed) => emit(*deployed),
                    curie::examples::SreBotInstallResult::ObservabilityInstalled(ready) => {
                        emit(ready)
                    }
                },
                SreBotAction::Render {
                    out,
                    platform_upgrade,
                    namespace,
                    release,
                    observability_namespace,
                } => emit(
                    curie::examples::render_sre_bot(curie::examples::SreBotRenderOpts {
                        out,
                        platform_upgrade,
                        namespace,
                        release,
                        observability_namespace,
                    })
                    .await?,
                ),
                SreBotAction::ProvisionObservability {
                    namespace,
                    release,
                    observability_namespace,
                    chart,
                    dry_run,
                } => match curie::examples::provision_observability(
                    curie::examples::ObservabilityProvisionOpts {
                        namespace,
                        release,
                        observability_namespace,
                        chart,
                        dry_run,
                    },
                )
                .await?
                {
                    curie::examples::ObservabilityProvisionResult::DryRun(plan) => emit(plan),
                    curie::examples::ObservabilityProvisionResult::Ready(ready) => emit(ready),
                },
            }
        }
        Some(Command::Build {
            tag,
            plugin_dir,
            registry,
            runner_image,
            force,
            platform,
        }) => match plugin_dir {
            Some(plugin_dir) => emit(
                commands::build_connectors(commands::ConnectorBuildOpts {
                    plugin_dir,
                    registry,
                    runner_image,
                    force,
                    platforms: platform,
                })
                .await?,
            ),
            None => commands::build(&tag).await,
        },
        Some(Command::Install { update }) => commands::install(update).await,
        Some(Command::Update { image }) => commands::update(image).await,
        Some(Command::Interactive) => curie::interactive::run().await,
        Some(Command::Secrets { action }) => match action {
            SecretsAction::Set {
                name,
                from_env,
                cluster_identity,
                release,
                namespace,
                expected_version,
            } => secrets::set(secrets::SetSecretOpts {
                name,
                from_env,
                cluster_identity,
                namespace,
                release,
                expected_version,
            }),
            SecretsAction::List => secrets::list(),
            SecretsAction::Unset {
                name,
                cluster_identity,
                release,
                namespace,
            } => secrets::unset(secrets::UnsetSecretOpts {
                name,
                cluster_identity,
                namespace,
                release,
            }),
        },
        Some(Command::Dev { action }) => dev_action::run(action).await,
        Some(Command::Skill { action }) => skill_action::run(action).await,
        Some(Command::Local { action }) => local_action::run(action).await,
        Some(Command::Factory { action }) => match action {
            FactoryAction::Quickstart {
                repo,
                app_id,
                private_key_file,
                context,
                namespace,
                release,
                org,
                kind_name,
                model,
                execution_deadline,
                budget,
                chart,
                dry_run,
            } => {
                let resolved = artifacts::resolve_chart(
                    chart.as_deref(),
                    artifacts::Channel::current(),
                    artifacts::version(),
                    artifacts::cache_root,
                    std::path::Path::new("charts/curie").is_dir(),
                )?;
                let chart = materialize_artifact(resolved, dry_run, "chart").await?;
                emit(
                    curie::factory_quickstart::quickstart(
                        curie::factory_quickstart::QuickstartOpts {
                            repo,
                            app_id,
                            private_key_file,
                            context,
                            namespace,
                            release,
                            org,
                            kind_name,
                            model,
                            execution_deadline_seconds: execution_deadline,
                            budget_usd: budget,
                            chart,
                            dry_run,
                        },
                    )
                    .await?,
                )
            }
        },
        Some(Command::Cluster { action, context }) => cluster_action::run(action, context).await,
        Some(Command::ListAgents) => commands::list_agents().await,
        Some(Command::DeployLocal {
            folder,
            api_url,
            api_key,
            slack_channel,
            repo,
            env,
            label,
            secret,
        }) => emit(
            commands::deploy_named(
                &folder,
                commands::DeployNamedOpts {
                    api_url,
                    api_key,
                    slack_channel,
                    repo,
                    // deploy_named has no --target, so the historical default
                    // applies here rather than deferring to a declared one.
                    env: env.unwrap_or(commands::DeployEnv::Dev),
                    label,
                    secret,
                },
            )
            .await?,
        ),
        Some(Command::Schema) => {
            use clap::CommandFactory;
            print!("{}", curie::schema::manifest_json(&Cli::command()));
            Ok(())
        }
        Some(Command::SchemaIndex { name }) => {
            match name {
                None => print!("{}", curie::schemas::index()),
                Some(name) => {
                    let body = curie::schemas::schema(&name).ok_or_else(|| {
                        curie::exit::CliError::usage(format!(
                            "no result schema named {name:?}; run `curie schema-index` for the inventory of {} schemas",
                            curie::schemas::names().len()
                        ))
                    })?;
                    print!("{body}");
                }
            }
            Ok(())
        }
        Some(Command::Guide) => curie::guide::run(),
        Some(Command::Apply {
            file,
            init,
            context,
            dry_run,
            chart,
            migrate_store,
            allow_stateful_removal,
        }) => {
            if init {
                curie::installation::write_starter(&file)?;
                return emit(curie::installation::ApplyOutput::WroteStarter {
                    path: file.display().to_string(),
                });
            }
            let cfg = curie::installation::Installation::load(&file)?;
            if let Some(target) =
                curie::kube_context::pin_for_cluster_command(curie::installation::resolve_context(
                    context.as_deref(),
                    cfg.install.context.as_deref(),
                ))?
            {
                ui::ui().note(&format!(
                    "Kubernetes context: {} (cluster {})",
                    target.context, target.cluster
                ));
            }
            let local = curie::installation::plan_installation(cfg, dry_run)?;
            let resolved = artifacts::resolve_chart(
                chart.as_deref(),
                artifacts::Channel::current(),
                artifacts::version(),
                artifacts::cache_root,
                std::path::Path::new("charts/curie").is_dir(),
            )?;
            let chart = materialize_artifact(resolved, dry_run, "chart").await?;
            emit(
                curie::installation::apply(curie::installation::ApplyOpts {
                    local,
                    chart,
                    allow_stateful_removal,
                    migrate_store,
                })
                .await?,
            )
        }
        Some(Command::Seal {
            connector,
            env_name,
            namespace,
            release,
            public_key,
            from_env,
        }) => emit(
            curie::seal::seal(curie::seal::SealOpts {
                connector,
                env_name,
                namespace,
                release,
                public_key,
                from_env,
            })
            .await?,
        ),
        Some(Command::Doctor {
            context,
            namespace,
            release,
            api_url,
            api_key,
        }) => {
            // Read-only, and its whole job is to report: a `curie.yaml` it
            // cannot parse must narrow what doctor knows, never stop it
            // answering, so the load is best-effort and never propagates. Saying
            // so out loud matters -- silence would make doctor look like it
            // ignored the operator's file. But an explicit --namespace/--release
            // always wins over the file (see resolve_target's precedence), so
            // only warn about whichever of the two the operator did NOT supply
            // -- that's the only part an unreadable file could actually change.
            let declared_path = std::path::Path::new("curie.yaml");
            let declared = curie::installation::Installation::load(declared_path).ok();
            if declared.is_none() && declared_path.exists() {
                // The three sayable cases differ only in which defaults are
                // being fallen back to, so only that noun phrase varies.
                let defaults = match (namespace.is_some(), release.is_some()) {
                    (true, true) => None,
                    (false, false) => Some("--namespace and --release defaults"),
                    (true, false) => Some("--release default"),
                    (false, true) => Some("--namespace default"),
                };
                if let Some(defaults) = defaults {
                    ui::ui().note(&format!(
                        "a curie.yaml is here but could not be read; \
                         falling back to the {defaults}"
                    ));
                }
            }
            let selected_context = curie::kube_context::pin_for_cluster_command(
                curie::installation::resolve_context(
                    context.as_deref(),
                    declared
                        .as_ref()
                        .and_then(|cfg| cfg.install.context.as_deref()),
                ),
            )?;
            if let Some(target) = &selected_context {
                ui::ui().note(&format!(
                    "Kubernetes context: {} (cluster {})",
                    target.context, target.cluster
                ));
            }
            let target = curie::doctor::resolve_target(
                namespace.as_deref(),
                release.as_deref(),
                declared
                    .as_ref()
                    .map(|c| (c.install.namespace.as_str(), c.install.release.as_str())),
            );
            // On stderr, via `note`: `--json` owns stdout, and a machine
            // consumer parses it whole. The resolution is a fact about how
            // doctor was TARGETED, not about what it observed, so it also does
            // not belong in a schema-validated check payload.
            if let Some(announcement) = &target.announcement {
                ui::ui().note(announcement);
            }
            // Discover independently. `zip` required both flags, so a bare
            // `curie doctor` never reached the platform API (#1367). Errors
            // are discarded inside `doctor`: gather is failure-tolerant.
            let matching_file = declared.as_ref().filter(|config| {
                config.install.namespace == target.namespace
                    && config.install.release == target.release
            });
            let apply_context = matching_file.and_then(|config| {
                selected_context.as_ref().and_then(|selected| {
                    (curie::installation::resolve_context(None, config.install.context.as_deref())
                        != Some(selected.context.as_str()))
                    .then_some(selected.context.as_str())
                })
            });
            let out = curie::doctor::doctor(
                &target.namespace,
                &target.release,
                api_url.as_deref(),
                api_key.as_deref(),
                matching_file.is_some(),
                apply_context,
            )
            .await;
            if out.release_not_serving() {
                let release = out
                    .checks
                    .iter()
                    .find(|check| check.id == "release")
                    .expect("release_not_serving requires a release check");
                let fix = release.fix.clone().unwrap_or_else(|| {
                    format!(
                        "curie cluster status --namespace {} --release {}",
                        target.namespace, target.release
                    )
                });
                return Err(ui::ui().failed_report(
                    &out,
                    curie::exit::CliError::failure(release.detail.clone())
                        .with_fix(fix)
                        .into(),
                ));
            }
            emit(out)
        }
        Some(Command::Diff {
            file,
            context,
            chart,
        }) => {
            let cfg = curie::installation::Installation::load(&file)?;
            let cluster = match curie::kube_context::pin_for_cluster_command(
                curie::installation::resolve_context(
                    context.as_deref(),
                    cfg.install.context.as_deref(),
                ),
            )? {
                Some(target) => {
                    ui::ui().note(&format!(
                        "Kubernetes context: {} (cluster {})",
                        target.context, target.cluster
                    ));
                    Some(target.cluster).filter(|name| !name.is_empty())
                }
                None => None,
            };
            // Lenient on purpose: `diff` mutates nothing, so a credential it
            // cannot resolve must not withhold the answer. See
            // installation::resolve_credentials_lenient.
            let (local, missing) = curie::installation::plan_installation_lenient(cfg)?;
            // Resolved exactly as `Apply` does, so both verbs answer about the
            // same chart. `resolve_chart` errors on the Dev channel with no
            // `charts/curie` in cwd and its remedy text literally says "pass
            // --chart", so the flag has to exist on this verb too (#1352).
            let overridden = chart.is_some();
            let resolved = artifacts::resolve_chart(
                chart.as_deref(),
                artifacts::Channel::current(),
                artifacts::version(),
                artifacts::cache_root,
                std::path::Path::new("charts/curie").is_dir(),
            )?;
            // `false`, never `dry_run`-style `true`: `true` returns a
            // `planned_target()` path that may not exist, and diff must actually
            // render the chart rather than plan a fetch of it.
            let chart = materialize_artifact(resolved, false, "chart").await?;
            // The version REPORTED has to be the version RENDERED. Under
            // `--chart` those are different charts, and reporting this CLI's
            // own package version there would compare the deployed release
            // against a chart the probe never looked at -- raising a false
            // CHART VERSION MISMATCH, or suppressing a real one. That is the
            // same two-sources-of-truth defect #1352 is about, in a new place.
            // The default path keeps `artifacts::version()` exactly as before,
            // so it makes no extra helm call.
            let chart_target = if overridden {
                curie::ops::chart_version(&chart).await?
            } else {
                artifacts::version().to_string()
            };
            emit(
                curie::installation::diff(curie::installation::DiffOpts {
                    local,
                    unresolved_credentials: missing,
                    cluster,
                    chart,
                    chart_target,
                })
                .await?,
            )
        }
    }
}
