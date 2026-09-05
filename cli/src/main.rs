//! The `curie` binary: `init`, `skill <up|down|status|message|eval|approvals>`
//! for a local runner, `local <up|down|status|message|deploy>` for the compose stack,
//! and `cluster <up|down|status|comms|message|deploy>` for Kubernetes and the
//! platform API. Task I1; contracts are frozen in packages/aci-protocol and
//! packages/plugin-format.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{bail, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use curie::api;
use curie::artifacts;
use curie::commands::{
    self, AgentActionOpts, DeployEnv, DeployOpts, SendType, StartOpts, DEFAULT_PORT,
};
use curie::comms::{self, CommsOpts, LocalCommsOpts};
use curie::docker;
use curie::github_app as crate_github_app;
use curie::local::{self, LocalDownOpts, LocalOpts};
use curie::message::{self, MessageOpts};
use curie::ops::{self, CommonOpts, DownOpts, RollbackOpts, UpOpts, UpgradeOpts};
use curie::secrets;
use curie::state::{apply_continue, load_turn, CliTurnArgs, TurnVerb};
use curie::ui::{self, ColorFlag, Ui};

/// Per-tier defaults for the flags shared by the agent-target verbs. The only
/// thing that differs between `local` and `cluster` is where the platform API
/// listens, so that is the single const each tier supplies.
trait TierDefaults: Clone + Send + Sync + std::fmt::Debug + 'static {
    const API_URL: &'static str;
}

#[derive(Clone, Debug)]
struct LocalTier;

impl TierDefaults for LocalTier {
    const API_URL: &'static str = "http://localhost:28000";
}

/// The flags a LOCAL agent-target verb (`versions`, `memory`, `approvals`) takes.
/// The local tier correctly defaults to the compose stack on localhost; the
/// cluster tier discovers its connection from the release instead (see
/// [`ClusterAgentTarget`] / [`ClusterConn`], #524), so it no longer shares this.
#[derive(Args, Debug, Clone)]
struct AgentTarget<T: TierDefaults> {
    /// Agent name or id.
    agent: String,
    #[arg(long, default_value = T::API_URL, env = "CURIE_API_URL")]
    api_url: String,
    #[arg(long, default_value = "curie-dev-key", env = "CURIE_API_KEY", value_parser = message::api_key_or_default)]
    api_key: String,
    #[arg(long)]
    dry_run: bool,
    #[arg(skip)]
    _tier: std::marker::PhantomData<T>,
}

impl<T: TierDefaults> From<AgentTarget<T>> for AgentActionOpts {
    fn from(target: AgentTarget<T>) -> Self {
        AgentActionOpts {
            api_url: target.api_url,
            api_key: target.api_key,
            agent: target.agent,
            dry_run: target.dry_run,
        }
    }
}

/// The connection surface for a cluster governance verb (#524). Unlike the local
/// tier, an omitted URL self-plumbs a loopback tunnel to the release API and an
/// omitted key is read from the release Secret. An explicit `--api-url` or
/// `--api-key` value still wins.
#[derive(Args, Debug, Clone)]
struct ClusterConn {
    /// Platform API base URL. Omit to self-plumb a loopback tunnel to the release API.
    #[arg(long, env = "CURIE_API_URL")]
    api_url: Option<String>,
    /// Platform API key. Omit to read the release's `api.apiKey` from its Secret.
    #[arg(long, env = "CURIE_API_KEY")]
    api_key: Option<String>,
    /// Kubernetes namespace of the release. Default: curie.
    #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
    namespace: String,
    /// Helm release name. Default: curie.
    #[arg(long, default_value = "curie")]
    release: String,
}

/// Local API connection flags shared by every observability query leaf. They
/// intentionally live on the leaves: bare `local observability` remains the
/// existing URL printer and does not grow a transport contract.
#[derive(Args, Debug, Clone)]
struct LocalObservabilityConn {
    /// Platform API base URL.
    #[arg(
        long,
        default_value = message::DEFAULT_LOCAL_API_URL,
        env = "CURIE_API_URL"
    )]
    api_url: String,
    /// Platform API key.
    #[arg(long, default_value = message::DEFAULT_API_KEY, env = "CURIE_API_KEY", hide_env_values = true, value_parser = message::api_key_or_default)]
    api_key: String,
}

/// Explicit cluster API overrides shared by every observability query leaf.
/// Namespace and release stay on the parent `cluster observability` command so
/// self-plumbing and the bare surface report always target the same release.
#[derive(Args, Debug, Clone)]
struct ClusterObservabilityConn {
    /// Platform API base URL. Omit to self-plumb the release API over loopback.
    #[arg(long, env = "CURIE_API_URL")]
    api_url: Option<String>,
    /// Platform API key. Required with --api-url; omit both for release discovery.
    #[arg(long, env = "CURIE_API_KEY", hide_env_values = true)]
    api_key: Option<String>,
}

fn parse_observability_limit(raw: &str) -> std::result::Result<usize, String> {
    let limit = raw
        .parse::<usize>()
        .map_err(|_| "limit must be an integer from 1 through 100".to_string())?;
    if (1..=100).contains(&limit) {
        Ok(limit)
    } else {
        Err("limit must be from 1 through 100".to_string())
    }
}

/// Shared list filters and defaults for the local and cluster sibling leaves.
#[derive(Args, Debug, Clone)]
struct ObservabilityRunsArgs {
    /// Maximum newest-first trace rows to return (1-100).
    #[arg(long, default_value = "20", value_parser = parse_observability_limit)]
    limit: usize,
    /// Restrict traces to one agent id.
    #[arg(long)]
    agent_id: Option<String>,
}

/// Shared detail selector for the local and cluster sibling leaves.
#[derive(Args, Debug, Clone)]
struct ObservabilityRunArgs {
    /// Trace id previously returned by `observability runs` or a completed turn.
    #[arg(value_parser = api::parse_trace_id)]
    trace_id: String,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum ObservabilityMetric {
    Runs,
    #[value(name = "latency_p95_ms")]
    LatencyP95Ms,
    Tokens,
    #[value(name = "cost_usd")]
    CostUsd,
    #[value(name = "error_rate")]
    ErrorRate,
}

impl ObservabilityMetric {
    fn as_str(self) -> &'static str {
        match self {
            Self::Runs => "runs",
            Self::LatencyP95Ms => "latency_p95_ms",
            Self::Tokens => "tokens",
            Self::CostUsd => "cost_usd",
            Self::ErrorRate => "error_rate",
        }
    }
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum ObservabilityGranularity {
    Hour,
    Day,
    Week,
}

impl ObservabilityGranularity {
    fn as_str(self) -> &'static str {
        match self {
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Week => "week",
        }
    }
}

/// Shared metrics filters for both tiers. `--granularity` applies only to a
/// series; omitting it with `--metric` resolves to `day` before the API call.
#[derive(Args, Debug, Clone)]
struct ObservabilityMetricsArgs {
    /// Return a time series for this metric; omit for the scalar summary.
    #[arg(long, value_enum)]
    metric: Option<ObservabilityMetric>,
    /// Series bucket size. Defaults to day when --metric is present.
    #[arg(long, value_enum)]
    granularity: Option<ObservabilityGranularity>,
    /// Optional metrics-window start accepted by the platform API.
    #[arg(long)]
    start: Option<String>,
    /// Optional metrics-window end accepted by the platform API.
    #[arg(long)]
    end: Option<String>,
    /// Restrict metrics to one deployment environment.
    #[arg(long)]
    environment: Option<String>,
    /// Restrict metrics to one agent name.
    #[arg(long)]
    agent: Option<String>,
}

impl ObservabilityMetricsArgs {
    fn into_query(self) -> Result<curie::observability::ObservabilityQuery> {
        if self.metric.is_none() && self.granularity.is_some() {
            return Err(curie::exit::usage(
                "--granularity requires --metric because summaries are not bucketed",
            ));
        }
        let granularity = self.granularity.unwrap_or(ObservabilityGranularity::Day);
        Ok(curie::observability::ObservabilityQuery::Metrics {
            metric: self.metric.map(|metric| metric.as_str().to_string()),
            granularity: granularity.as_str().to_string(),
            start: self.start,
            end: self.end,
            environment: self.environment,
            agent: self.agent,
        })
    }
}

/// Local query grammar. Only the connection block differs from the cluster
/// enum below; every behavioral flag is one of the shared argument structs.
#[derive(Subcommand, Debug, Clone)]
enum LocalObservabilityQuery {
    /// List recent runs, newest first.
    Runs {
        #[command(flatten)]
        query: ObservabilityRunsArgs,
        #[command(flatten)]
        conn: LocalObservabilityConn,
    },
    /// Read one complete run by trace id.
    Run {
        #[command(flatten)]
        query: ObservabilityRunArgs,
        #[command(flatten)]
        conn: LocalObservabilityConn,
    },
    /// Read the metrics summary or one bounded metric series.
    Metrics {
        #[command(flatten)]
        query: ObservabilityMetricsArgs,
        #[command(flatten)]
        conn: LocalObservabilityConn,
    },
}

/// Cluster query grammar. Explicit URL/key values bypass discovery; omitted
/// values use the same namespace/release discovery as other cluster reads.
#[derive(Subcommand, Debug, Clone)]
enum ClusterObservabilityQuery {
    /// List recent runs, newest first.
    Runs {
        #[command(flatten)]
        query: ObservabilityRunsArgs,
        #[command(flatten)]
        conn: ClusterObservabilityConn,
    },
    /// Read one complete run by trace id.
    Run {
        #[command(flatten)]
        query: ObservabilityRunArgs,
        #[command(flatten)]
        conn: ClusterObservabilityConn,
    },
    /// Read the metrics summary or one bounded metric series.
    Metrics {
        #[command(flatten)]
        query: ObservabilityMetricsArgs,
        #[command(flatten)]
        conn: ClusterObservabilityConn,
    },
}

/// Skill-tier query grammar. The leaves deliberately accept the same query
/// selectors as the platform tiers so a caller gets the tier-capability answer
/// (exit 4) instead of an "unknown command" usage error. They never execute a
/// query: the skill tier has no platform API to read from.
#[derive(Subcommand, Debug, Clone)]
enum SkillObservabilityQuery {
    /// Explain why recent runs cannot be queried at the skill tier.
    Runs {
        #[command(flatten)]
        _query: ObservabilityRunsArgs,
    },
    /// Explain why a run cannot be queried by trace id at the skill tier.
    Run {
        #[command(flatten)]
        _query: ObservabilityRunArgs,
    },
    /// Explain why metrics cannot be queried at the skill tier.
    Metrics {
        #[command(flatten)]
        _query: ObservabilityMetricsArgs,
    },
}

/// An agent-target cluster verb (`versions`/`memory`/`approvals`): the agent plus
/// the discoverable [`ClusterConn`] and a `--dry-run`. The cluster analogue of
/// `AgentTarget<LocalTier>`, which keeps its localhost defaults for the local tier.
#[derive(Args, Debug, Clone)]
struct ClusterAgentTarget {
    /// Agent name or id.
    agent: String,
    #[command(flatten)]
    conn: ClusterConn,
    #[arg(long)]
    dry_run: bool,
}

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
async fn sync_connectors(
    api_url: &str,
    api_key: &str,
    namespace: &str,
    release: &str,
    agent_id: &str,
    agent_name: &str,
    version_id: &str,
) -> anyhow::Result<()> {
    let target = curie::connectors::bind_current_cluster(namespace, release).await?;
    let app_name = curie::connectors::discover_app_name(&target).await?;
    let connector_version = ConnectorVersion {
        agent_id,
        agent_name,
        version_id,
    };
    let prepared = prepare_connectors(
        api_url,
        api_key,
        namespace,
        release,
        &app_name,
        connector_version,
        target,
    )
    .await?;
    apply_connectors(prepared).await
}

struct ConnectorVersion<'a> {
    agent_id: &'a str,
    agent_name: &'a str,
    version_id: &'a str,
}

async fn prepare_connectors(
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

async fn apply_connectors(
    prepared: curie::connectors::PreparedConnectorSync,
) -> anyhow::Result<()> {
    let synced = curie::connectors::sync(prepared).await?;
    let ui = curie::ui::ui();
    for (name, url) in &synced.urls {
        ui.note(&format!("connector {name}: {url}"));
    }
    Ok(())
}

/// Resolve a cluster verb's API URL, key, and optional loopback tunnel. Explicit
/// values win. An omitted URL self-plumbs the release API service, while an
/// omitted key is read from the release Secret.
async fn resolve_cluster_conn(
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

async fn run_local_observability_query(
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

async fn run_cluster_observability_query(
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

/// clap `value_parser` for every `--local-model` (#1254). All four sites carry the
/// same value and hand it to the same downstream consumers, so validating one and
/// not the others is the sibling-path drift this repo keeps getting bitten by.
fn parse_model_ref(raw: &str) -> Result<String, String> {
    curie::docker::validate_model_ref(raw).map_err(|e| e.to_string())?;
    Ok(raw.to_string())
}

#[derive(Parser)]
#[command(
    name = "curie",
    version,
    about = "Curie CLI: run `curie` for the interactive terminal, or pass a subcommand for scripts"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    /// Show verbose plumbing (helm/kubectl/rollout/port-forward).
    #[arg(
        long,
        global = true,
        help = "Show verbose plumbing (helm/kubectl/rollout/port-forward)"
    )]
    debug: bool,
    /// Payload only; suppress progress and diagnostics.
    #[arg(
        short = 'q',
        long,
        global = true,
        help = "Payload only; suppress progress and diagnostics"
    )]
    quiet: bool,
    /// Colorize output.
    #[arg(
        long,
        global = true,
        value_enum,
        default_value_t = ColorFlag::Auto,
        help = "Colorize output"
    )]
    color: ColorFlag,
    /// Machine-readable JSON to stdout; human/log text to stderr.
    #[arg(
        long,
        global = true,
        help = "Machine-readable JSON to stdout; human/log text to stderr"
    )]
    json: bool,
}

#[derive(Subcommand)]
enum Command {
    /// Scaffold a keyless first reply, using a saved or environment model credential when available.
    Try {
        /// Keep the standard scaffold in ./curie-demo for normal skill commands.
        #[arg(long)]
        keep: bool,
    },
    /// Scaffold a new plugin bundle (Claude Code plugin shape).
    Init {
        /// Kebab-case plugin name (e.g. deal-desk). Omit when using --from-spec.
        name: Option<String>,
        /// Target directory; defaults to ./<name>.
        #[arg(long)]
        dir: Option<PathBuf>,
        /// Scaffold non-interactively from an agent-authored spec file (JSON). The bundle name comes from the spec.
        #[arg(long, value_name = "PATH")]
        from_spec: Option<PathBuf>,
        /// Adopt an existing non-plugin directory: scaffold the plugin skeleton INTO it (alongside your code, never overwriting existing files), deriving the name from the directory unless a NAME is given. The on-ramp for a pre-plugin (agent-ss-template) bundle; port the logic by hand afterward (docs/adopting-a-bundle.md, #745).
        #[arg(
            long,
            value_name = "DIR",
            conflicts_with = "from_spec",
            conflicts_with = "dir"
        )]
        adopt: Option<PathBuf>,
    },
    /// Work with the runner only tier for a plugin bundle. `skill` names that tier, not a
    /// bundle skill artifact at `skills/<name>/SKILL.md`. Subcommands:
    /// `skill <up|down|status|message|eval|approvals>`. `versions` and `memory`
    /// are answered here too, reporting that this tier has neither.
    Skill {
        #[command(subcommand)]
        action: SkillAction,
    },
    /// Work with the local compose stack and local platform API.
    Local {
        #[command(subcommand)]
        action: LocalAction,
    },
    /// Work with the deployed cluster release and platform API.
    Cluster {
        #[command(subcommand)]
        action: ClusterAction,
    },
    /// Install a complete first party example workflow.
    Example {
        #[command(subcommand)]
        action: ExampleAction,
    },
    /// List locally-authored agent bundles under `agents/` (source checkout
    /// only) -- a personal, gitignored directory (sibling of `examples/`) for
    /// in-progress agent projects ready to hand to `deploy-local`. Empty, not
    /// an error, when the directory doesn't exist.
    ListAgents,
    /// Deploy an `agents/<folder>` bundle to the local platform by name --
    /// shorthand for `local deploy --plugin-dir agents/<folder>` (same
    /// underlying operation, just resolved by name). Local tier only; use
    /// `cluster deploy --plugin-dir agents/<folder>` for the cluster tier.
    /// (Bare `deploy` is a retired pre-tier-split verb name -- see
    /// `retired.rs` -- so this spells out the tier it targets.)
    DeployLocal {
        /// Bundle folder name under `agents/`.
        folder: String,
        /// Platform API base URL.
        #[arg(
            long,
            default_value = message::DEFAULT_LOCAL_API_URL,
            env = "CURIE_API_URL"
        )]
        api_url: String,
        /// Platform API key.
        #[arg(long, default_value = "curie-dev-key", env = "CURIE_API_KEY", value_parser = message::api_key_or_default)]
        api_key: String,
        /// Slack channel to bind the agent to. On first create it defaults to
        /// C0LOCALDEV; on redeploy the channel is ADDED when the agent is not
        /// already bound to it, never moved and never removed, so omitting the
        /// flag leaves the deployed agent's binding set untouched.
        #[arg(long)]
        slack_channel: Option<String>,
        /// Bind this agent to a GitHub repository (`owner/name`) so pushes to
        /// its dev/prod branches deploy it (ADR-0014).
        ///
        /// A new agent is created bound to it, and an existing agent with no
        /// binding is bound now (#1194 made `repo_full_name` PATCHable and
        /// ADR-0091 dropped the uniqueness, so one repository can build several
        /// agents). An agent already bound to a DIFFERENT repository is NOT
        /// moved, because that would reroute which repository's pushes deploy
        /// it; a warning names the binding it kept.
        #[arg(long = "repo", value_name = "OWNER/NAME")]
        repo: Option<String>,
        /// Target environment. Defaults to dev; a `--target` supplies it
        /// instead, and an explicit value here still wins over the target.
        #[arg(long, value_enum)]
        env: Option<DeployEnv>,
        /// Version label; defaults to <manifest version>-<unix time>.
        #[arg(long)]
        label: Option<String>,
        /// Bind a per-agent connector secret by NAME (ADR-0009, #429). The value
        /// is resolved from your environment or the host secret vault (`curie
        /// secrets set <NAME>`) and sent to the platform. Repeatable.
        #[arg(long = "secret", value_name = "NAME")]
        secret: Vec<String>,
    },
    /// Build the runner image, or an agent bundle's declared connectors.
    ///
    /// With no flags it runs `docker build -f runner/Dockerfile -t <tag> .` from
    /// the repo root (source checkout only; a release binary pulls the pinned
    /// runner image from GHCR automatically and never needs this).
    ///
    /// With `--plugin-dir <PATH>` it builds every connector that bundle's
    /// `connectors.yaml` declares from source and writes `connectors.lock.yaml`
    /// beside it. With `--registry <REF>` it builds every declared platform,
    /// pushes, and records the registry manifest digest, which is what a cluster
    /// deploy requires. Without `--registry` it builds the host platform only
    /// into the local Docker daemon and records the local image id, which is
    /// usable at the skill and local tiers and refused at cluster.
    Build {
        /// Image tag to build.
        #[arg(long, default_value = docker::RUNNER_IMAGE, conflicts_with = "plugin_dir")]
        tag: String,
        /// Build the connectors this agent bundle declares.
        #[arg(long, value_name = "PATH")]
        plugin_dir: Option<PathBuf>,
        /// Push a multi-platform index to this registry (e.g. ghcr.io/acme-corp).
        #[arg(long, value_name = "REF", requires = "plugin_dir")]
        registry: Option<String>,
        /// Replace a registry lock with a local-daemon one deliberately.
        #[arg(long, requires = "plugin_dir")]
        force: bool,
    },
    /// Bootstrap or update a dev checkout: install deps and build, start nothing (source checkout only).
    ///
    /// From the repo root, runs (each idempotent, streaming output): copy
    /// `.env.example` to `.env` if missing, `uv sync`, `pnpm install` in
    /// `apps/ui`, `cargo install --path cli` (builds AND puts `curie` on PATH,
    /// so re-running install refreshes the live CLI), then builds the runner
    /// image. With `--update`, already-present heavyweight artifacts like the
    /// runner image are reused. `curie update` is the fast CLI-only subset. A
    /// release binary has no source tree to install and errors clearly; a
    /// missing tool (uv/pnpm/cargo/docker) prints a pointer and stops.
    #[command(alias = "i")]
    Install {
        /// Reuse already-present artifacts while refreshing dependencies and builds.
        #[arg(long)]
        update: bool,
    },
    /// Rebuild this CLI from the source checkout and reinstall it on PATH (source checkout only).
    ///
    /// The fast per-change refresh: runs `cargo install --path cli --force` from
    /// the repo root so a code change to the CLI is live on the next `curie`
    /// invocation, without re-running the bootstrap script. Pass `--image` to
    /// also rebuild the local runner image (for `runner/` changes). A release
    /// binary cannot rebuild itself and errors clearly.
    #[command(alias = "u")]
    Update {
        /// Also rebuild the local runner image (for runner/ changes).
        #[arg(long)]
        image: bool,
    },
    /// Open the interactive terminal interface.
    ///
    /// A keyboard-driven terminal UI for humans: browse targets and actions,
    /// preview exact commands, fill required values, and run workflows without
    /// memorizing the full command surface.
    #[command(alias = "ui", alias = "tui")]
    Interactive,
    /// Store and manage local secrets in Curie private storage.
    Secrets {
        #[command(subcommand)]
        action: SecretsAction,
    },
    /// Run a repo dev script (contracts, chart-check, e2e) -- source checkout only.
    ///
    /// Thin wrappers over the repo's dev scripts so contributors get a unified
    /// `curie <command>` surface; the scripts stay the implementation. A
    /// release binary has no scripts and errors clearly.
    Dev {
        #[command(subcommand)]
        action: DevAction,
    },
    /// Print the machine-readable command manifest (JSON) to stdout.
    ///
    /// Hidden, developer-facing: regenerates `cli/command-manifest.json`, which
    /// a CI drift gate keeps in lockstep with the CLI grammar. Also reachable as
    /// `dump-commands`.
    #[command(hide = true, alias = "dump-commands")]
    Schema,
    /// Print the committed, versioned JSON Schemas for the `--json` result outputs.
    ///
    /// With no NAME, emits the schema inventory index (`cli/schema/index.json`):
    /// every agent-facing result family, the schema file it maps to, and its
    /// version. With a NAME (e.g. `kill`, or `kill.schema.json`), emits that
    /// schema. The schemas are embedded in the binary, so this works from a
    /// released `curie` with no source checkout (issue #634).
    SchemaIndex {
        /// The schema to print (short name like `kill`, or `kill.schema.json`).
        /// Omit to print the inventory index of all result schemas.
        name: Option<String>,
    },
    /// Print a self-contained primer for a coding agent driving the harness (ADR-0021).
    ///
    /// Ordered by what the agent needs first (roughly 100 lines), carrying only
    /// non-discoverable knowledge: the parity ladder, when/which decision logic,
    /// the landmines, and verify-first. Human-readable Markdown by default;
    /// The global `--json` emits a structured variant (data on stdout, human
    /// text on stderr).
    Guide,

    /// Converge a cluster to a `curie.yaml` installation file (ADR-0097).
    ///
    /// The file states the whole intent, so `apply` never has to be told what
    /// it was told last time -- the gap behind the dropped-settings failures
    /// the `--set`/`--reuse-values` shape kept producing.
    ///
    /// A worked common installation is available at `examples/curie.yaml` in
    /// the Curie repository.
    Apply {
        /// Path to the installation file.
        #[arg(short = 'f', long = "file", default_value = "curie.yaml")]
        file: std::path::PathBuf,
        /// Print the plan without touching the cluster.
        #[arg(long)]
        dry_run: bool,
        /// Chart reference override, as `cluster up` takes.
        #[arg(long)]
        chart: Option<String>,
        /// Carry the object store's contents across a chart that renames it,
        /// instead of refusing. Apply then stages every object, upgrades, loads
        /// them back, and verifies per object -- one command, no separate
        /// procedure and no safety override.
        ///
        /// Opt-in rather than automatic because the migration has a window
        /// where the store is empty and the bot cannot answer: an apply that
        /// changes a log level must never silently start moving data.
        #[arg(long)]
        migrate_store: bool,
        /// Proceed even when the upgrade would DELETE a stateful component the
        /// release is running, WITHOUT its data. Refused by default. Prefer
        /// --migrate-store, which keeps the data; this flag is for a store you
        /// genuinely intend to discard.
        #[arg(long, conflicts_with = "migrate_store")]
        allow_stateful_removal: bool,
    },

    /// Encrypt a connector credential to a cluster (ADR-0094).
    ///
    /// The blob is safe to commit: only a cluster holding the matching private
    /// key can read it. The value is never taken as an argument -- it comes
    /// from a hidden prompt, a pipe, or `--from-env` -- so it cannot land in a
    /// shell history or the process table.
    Seal {
        /// The connector in connectors.yaml that reads this value.
        #[arg(long)]
        connector: String,
        /// The environment variable name the connector reads it as.
        env_name: String,
        #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        #[arg(long, default_value = "curie")]
        release: String,
        /// Seal against this public key instead of reading one from a cluster,
        /// so an author with no cluster access can still seal.
        #[arg(long)]
        public_key: Option<String>,
        /// Read the value from this environment variable instead of prompting.
        #[arg(long)]
        from_env: Option<String>,
    },

    /// Report what is set up, what is missing, and the command that fixes it.
    ///
    /// The required inputs are otherwise learned one failure at a time -- boot
    /// succeeds and the next command fails on a credential; a deploy works and
    /// the next push silently does nothing. This states the whole list, and
    /// reports only what is actually observable rather than what a doc claims.
    ///
    /// Read-only. Safe to run anywhere, including against production.
    Doctor {
        /// Kubernetes namespace to inspect. Defaults to `curie.yaml`'s `install:`
        /// block when one is present in this directory, otherwise `curie`.
        #[arg(long)]
        namespace: Option<String>,
        /// Helm release to inspect. Defaults to `curie.yaml`'s `install:` block
        /// when one is present in this directory, otherwise `curie`.
        #[arg(long)]
        release: Option<String>,
        /// Platform API, to include the repo-binding check. Optional: omitted
        /// values are discovered from the release, same as sibling cluster verbs.
        #[arg(long, env = "CURIE_API_URL")]
        api_url: Option<String>,
        /// API key for `--api-url`. Optional: discovered from the release Secret
        /// when omitted.
        #[arg(long, env = "CURIE_API_KEY")]
        api_key: Option<String>,
    },

    /// Show what `curie apply` would change about the live release (ADR-0097).
    ///
    /// Read-only, and resolves no credential: "what would change?" is most
    /// urgent while an install is still incomplete. A value the release carries
    /// that the file does not declare is reported as preserved or as a reset
    /// according to what `up` actually does with it, never guessed.
    Diff {
        /// Path to the installation file.
        #[arg(short = 'f', long = "file", default_value = "curie.yaml")]
        file: std::path::PathBuf,
        /// Chart reference override, as `cluster up` takes. Diff RENDERS this
        /// chart to detect stateful components the apply would delete, so point
        /// it at the same chart `curie apply --chart` would use.
        #[arg(long)]
        chart: Option<String>,
    },
}

#[derive(Subcommand)]
enum ExampleAction {
    /// Install the self referential SRE bot example.
    SreBot {
        #[command(subcommand)]
        action: SreBotAction,
    },
}

#[derive(Subcommand)]
enum SreBotAction {
    /// Install Curie, its observability stack, and the SRE bot bundle.
    Install {
        /// Install the fixed self referential Grafana, Loki, Alloy, Tempo, and Prometheus stack.
        #[arg(long, required = true)]
        observability: bool,
        /// Print the ordered plan without mutating the cluster.
        #[arg(long)]
        dry_run: bool,
        /// Bind the installed bot to this Slack channel.
        #[arg(long, value_name = "CHANNEL")]
        slack_channel: Option<String>,
        /// Install the upgrade path: the self-upgrade connector, the platform
        /// upgrade Job, and the two identities behind them.
        ///
        /// CREATES A NAMESPACE-ADMIN-EQUIVALENT IDENTITY for the Job that runs
        /// `helm upgrade`. Read examples/sre-bot/manifests/platform-upgrade-role.yaml
        /// before using this: it enumerates exactly what that grant covers and
        /// what does and does not bound it. Omit the flag and nothing about the
        /// install changes.
        #[arg(long)]
        platform_upgrade: bool,
        /// Kubernetes namespace of the Curie release. Default: curie.
        #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        /// Helm release name of the Curie install. Default: curie.
        #[arg(long, default_value = "curie")]
        release: String,
        /// Kubernetes namespace of the retained observability stack. Default: observability.
        #[arg(long, default_value = "observability")]
        observability_namespace: String,
        /// Allow this GitHub repository, or `owner/*`, for runtime workspace
        /// selection. Repeatable. Sets `api.githubRepoAllowlist` on the Curie
        /// install.
        #[arg(long = "workspace-repo", value_name = "OWNER/REPO")]
        workspace_repo: Vec<String>,
    },
}

#[derive(Subcommand)]
enum DevAction {
    /// Check the frozen contracts (`bash scripts/check-contracts.sh`).
    Contracts,
    /// Render-assert the Helm chart: discover and run every executable assertion
    /// script in `charts/curie/ci/`, the same set helm-ci runs on a
    /// `charts/curie/**` change (#1481). Reports per-script pass or fail and exits
    /// non-zero if any failed, so one run surfaces every problem.
    ChartCheck,
    /// Prove a changed test fails when only the change's product hunks are reversed.
    VerifyFixPin {
        /// Commit or pull request to verify.
        change: String,
        /// Changed test selector to run before and after reversal.
        selector: String,
    },
    /// Run the scripted CLI end-to-end test (`bash cli/scripts/e2e.sh`).
    E2e,
    /// Run the cold-start parity ladder across the skill, local, and cluster
    /// tiers, fake model by default (#690, `bash cli/scripts/e2e-ladder.sh`).
    E2eLadder,
    /// Nightly SRE demo e2e: six assertions on kind with the pinned Kubernetes
    /// MCP server, a CI-only Socket Mode Slack app, a live provider, and an
    /// allowlisted throwaway repo (#2246, `bash cli/scripts/sre-demo-e2e.sh`).
    /// Missing those CI secrets skip with the reason in the run summary.
    SreDemoE2e,
    /// Select the end to end tiers CI would run for paths or revisions.
    E2eCiSelection {
        /// Changed path. Repeat for every path in the candidate change.
        #[arg(
            long,
            required_unless_present_any = ["base", "push"],
            conflicts_with_all = ["base", "head", "push"]
        )]
        path: Vec<PathBuf>,
        /// Base revision for a local branch comparison.
        #[arg(long, requires = "head", conflicts_with = "push")]
        base: Option<String>,
        /// Head revision for a local branch comparison.
        #[arg(long, requires = "base", conflicts_with = "push")]
        head: Option<String>,
        /// Select every tier, matching a push event.
        #[arg(long, conflicts_with_all = ["path", "base", "head"])]
        push: bool,
    },
    /// Runtime E2E the Helm chart on a local cluster: install a trimmed slice,
    /// seed a bundle into RustFS, run the sandbox bundle-fetch init pair, and
    /// exec-assert the runner's view -- the one-command way to satisfy a
    /// chart/sandbox runtime acceptance criterion static checks cannot (#199,
    /// `bash scripts/chart-runtime-e2e.sh`).
    ChartRuntimeE2e {
        /// Allow running against a kube context other than `k8scratch`.
        ///
        /// The script refuses a non-`k8scratch` context and names `--force` as
        /// the override. Without this passthrough that override was unreachable
        /// from the `curie` surface, so a contributor whose scratch context has
        /// another name could not run this gate the documented way at all.
        #[arg(long)]
        force: bool,
    },
    /// Lint the interface catalog docs (`bash scripts/check-docs.sh`).
    DocsLint,
    /// Validate every `examples/` bundle against Claude Code (`bash scripts/check-plugin-compat.sh`).
    PluginCompat,
    /// Validate every Curie-owned skill against the Agent Skills reference
    /// validator, pinned to `skills-ref==0.1.1` so the gate is deterministic
    /// (`bash scripts/check-agent-skills.sh`). The inbound spec-conformance twin
    /// of `plugin-compat`: that one proves our bundles are accepted by Claude
    /// Code, this one proves our skills satisfy the published spec.
    AgentSkills,
    /// Run the committed eval suites through the fake model and assert every case
    /// goes RED -- the falsifiability gate's real-path negative control (#619,
    /// `bash cli/scripts/eval-falsifiability.sh`). Offline, no credential.
    EvalFalsifiability,
    /// Assert every `Deserialize` struct in `cli/src/api.rs` is declared in
    /// `cli/api-mirrors.json` and covers its API model's fields (#691,
    /// `bash cli/scripts/check-field-parity.sh`). Offline, no credential.
    FieldParity,
    /// Assert every declared `emits` projection in `cli/api-mirrors.json` --
    /// a `CliOutput::to_json` that hand-projects a mirror struct into a
    /// `json!` literal -- covers that struct's fields (#699, one hop
    /// downstream of `field-parity`, `bash cli/scripts/check-emit-parity.sh`).
    /// Offline, no credential.
    EmitParity,
    /// Assert sibling CLI verbs expose matching conversation controls across
    /// the skill, local, and cluster tiers (#1666,
    /// `bash cli/scripts/check-verb-parity.sh`). Offline, no credential.
    VerbParity,
    /// Refresh the ADR-0101 schema compatibility baseline (cli/schema/baseline/).
    /// Refuses when a schema changed shape without a version bump.
    SchemaBaseline,
    /// Assert Rail 1 (ADR-0067) actually ENFORCES on the cluster kubectl points
    /// at, not merely that its NetworkPolicies are applied (#1153,
    /// `bash scripts/check-netpol-enforcement.sh`). Structured as a
    /// non-vacuity check: it proves a DENIED direction is genuinely blocked
    /// before trusting any allowed one, so a CNI that ignores NetworkPolicy
    /// (kindnet, minikube's default) FAILS rather than passing green.
    NetpolCheck,
    /// Assert the release-coupled versions agree: cli/Cargo.toml, Chart.yaml
    /// version, and appVersion (`bash scripts/check-version-consistency.sh`).
    VersionCheck,
    /// Assert every direct `ClassName.model_validate*(...)` call on an
    /// `_AciModel` subclass threads `READER_CONTEXT` or is a declared
    /// exception in `tools/wire-tolerance-gate/allowlist.json` (#625, following
    /// #492's forgotten-context bug, `bash scripts/check-wire-tolerance.sh`).
    /// Offline, no credential.
    WireTolerance,
    /// Set the release version across cli/Cargo.toml + Chart.yaml
    /// version/appVersion (and refresh the CLI lockfile) so a release cut can't
    /// leave the three out of sync. Does not commit or tag.
    BumpVersion {
        /// The new release version: semver `X.Y.Z` or `X.Y.Z-rc.N`.
        version: String,
        /// Print the planned edits without writing anything.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
enum SecretsAction {
    /// Save a secret in Curie private storage. Prompts with hidden input by default.
    Set {
        /// Environment-variable-style secret name, e.g. GITHUB_PERSONAL_ACCESS_TOKEN.
        name: String,
        /// Read the value from another environment variable instead of prompting.
        #[arg(long)]
        from_env: Option<String>,
        /// Cluster identity fingerprint from `kubectl config view`. Required with
        /// --release and --namespace to scope a connector secret to one cluster.
        #[arg(long)]
        cluster_identity: Option<String>,
        /// Helm release the secret may be injected into.
        #[arg(long)]
        release: Option<String>,
        /// Kubernetes namespace the secret may be injected into.
        #[arg(long)]
        namespace: Option<String>,
        /// Compare-and-set version from `curie secrets list --json`. Required to
        /// replace an existing cluster-scoped secret.
        #[arg(long)]
        expected_version: Option<u64>,
    },
    /// List saved Curie secret names. Values are never printed.
    List,
    /// Remove a saved secret.
    Unset {
        /// Environment-variable-style secret name.
        name: String,
        /// Cluster identity fingerprint. Required with --release and --namespace
        /// to remove one scoped entry without deleting the unscoped value.
        #[arg(long)]
        cluster_identity: Option<String>,
        /// Helm release of the scoped entry to remove.
        #[arg(long)]
        release: Option<String>,
        /// Kubernetes namespace of the scoped entry to remove.
        #[arg(long)]
        namespace: Option<String>,
    },
}

/// Shared `--samples` / `--aggregation` / `--pass-at-k` flags for every eval
/// tier (#1907). Default n=1 majority is documented rather than silent.
#[derive(Args, Debug, Clone)]
struct EvalSamplingArgs {
    /// Independent samples per case for live-model grading. Default: 1. A
    /// single sample is not proof of tier drift; raise this to distinguish
    /// variance from a real miss. Same policy on skill, local, and cluster.
    #[arg(long, default_value_t = 1, env = "CURIE_EVAL_SAMPLES", value_parser = clap::value_parser!(u32).range(1..))]
    samples: u32,
    /// How to reduce N sample verdicts. Default: majority.
    #[arg(long, default_value_t = curie::eval_sampling::AggregationPolicy::Majority, env = "CURIE_EVAL_AGGREGATION")]
    aggregation: curie::eval_sampling::AggregationPolicy,
    /// Pass@k threshold when --aggregation pass_at_k. Default: 1.
    #[arg(long = "pass-at-k", default_value_t = 1, env = "CURIE_EVAL_PASS_AT_K", value_parser = clap::value_parser!(u32).range(1..))]
    pass_at_k: u32,
}

impl EvalSamplingArgs {
    fn config(self) -> anyhow::Result<curie::eval_sampling::SampleConfig> {
        curie::eval_sampling::SampleConfig::new(self.samples, self.aggregation, self.pass_at_k)
    }
}

#[derive(Subcommand)]
enum SkillAction {
    /// Boot a local runner container for the bundle and print the env summary.
    Up {
        /// Plugin bundle directory.
        #[arg(long, default_value = ".")]
        plugin_dir: PathBuf,
        /// Runner image. Default: version-pinned `ghcr.io/curie-eng/curie-runner:<version>` on release builds; local `curie-runner` on dev builds. Pass to override.
        #[arg(long)]
        image: Option<String>,
        /// Host port for the local bot.
        #[arg(long, default_value_t = DEFAULT_PORT)]
        port: u16,
        /// Container name.
        #[arg(long, default_value = docker::RUNNER_CONTAINER_LOCAL)]
        name: String,
        /// Use the runner's scripted fake model (offline; no credential).
        #[arg(long)]
        fake_model: bool,
        /// Docker network to join (e.g. curie_default for the dev stack).
        #[arg(long)]
        network: Option<String>,
        /// OTLP endpoint for traces (e.g. http://otel-collector:4318).
        #[arg(long)]
        otel_endpoint: Option<String>,
        /// ACI budget JSON for the session.
        #[arg(long, default_value = commands::DEFAULT_BUDGET)]
        budget: String,
        /// Model id, forwarded as CURIE_MODEL. Omit for the SDK default.
        /// Setting it makes token usage attributable in Langfuse traces.
        #[arg(long)]
        model: Option<String>,
        /// Run the named model through local Ollama.
        #[arg(
            long,
            num_args = 0..=1,
            default_missing_value = commands::DEFAULT_LOCAL_MODEL,
            value_parser = parse_model_ref,
            conflicts_with = "fake_model",
            conflicts_with = "model"
        )]
        local_model: Option<String>,
        /// Allow --local-model to DOWNLOAD its assets on this run. Without it,
        /// `up` refuses when the pinned Ollama image (~8.9 GB) or the requested
        /// model is not already on the machine, rather than fetching them
        /// implicitly (ADR 0093).
        #[arg(long, requires = "local_model")]
        pull_model: bool,
        /// Forward an environment variable BY NAME into the runner sandbox, so a
        /// bundle's authed MCP server can read a secret (e.g. an API token) the
        /// same way model credentials are forwarded: the value is read from your
        /// environment by docker and never placed in argv. Repeatable. Example:
        /// `--secret GITHUB_PERSONAL_ACCESS_TOKEN` with that var exported.
        #[arg(long = "secret", value_name = "NAME")]
        secret: Vec<String>,
        /// Opt-in: read a bundle-local `.env` (any dotenv path) as the LOWEST-
        /// priority model-credential source, so the bundle boots live with no
        /// `set -a; source .env` step. Precedence: shell env > stored secret
        /// (`curie secrets set`) > this file. Only CURIE_CREDENTIALS,
        /// CLAUDE_CODE_OAUTH_TOKEN, and ANTHROPIC_API_KEY are read; every other
        /// key in the file is ignored (#749).
        #[arg(long = "env-file", value_name = "PATH")]
        env_file: Option<PathBuf>,
        /// Remove a leftover container of the same name before booting, instead
        /// of failing on the conflict.
        #[arg(long)]
        replace: bool,
    },
    /// Check that the bundle's MCP servers load in an offline runner container.
    Check {
        /// Plugin bundle directory.
        #[arg(long, default_value = ".")]
        plugin_dir: PathBuf,
        /// Runner image. Defaults to the same image resolution as `skill up`.
        #[arg(long)]
        image: Option<String>,
        /// Check deadline in seconds, forwarded to the runner container.
        #[arg(long, default_value_t = 30)]
        timeout: u64,
    },
    /// View the bundle's declared approval gates, or print the env assignment
    /// that sets or clears the runner's override (nothing is mutated).
    Approvals {
        /// Plugin bundle directory.
        #[arg(long, default_value = ".")]
        plugin_dir: PathBuf,
        /// Tool name to gate. Repeatable. Omit (with no --clear) to view the
        /// bundle's declared gates.
        #[arg(long = "gate", value_name = "TOOL")]
        gate: Vec<String>,
        /// Print the assignment that clears the env override.
        #[arg(long)]
        clear: bool,
        /// List pending approval RECORDS (not gate config). Accepted so it can be
        /// DECLINED with a reason at this tier rather than error like a typo: the
        /// skill tier's local runner keeps no durable approval store (ADR-0063,
        /// ADR-0077). Use `curie local/cluster approvals --list`.
        #[arg(long)]
        list: bool,
        /// Resolve the approval with this id. Not available at the skill tier for
        /// the same reason as --list; declined with that reason.
        #[arg(long, value_name = "APPROVAL_ID")]
        resolve: Option<String>,
        /// Reject instead of approve (paired with --resolve). Accepted only to be
        /// declined cleanly at this tier.
        #[arg(long)]
        reject: bool,
        /// Bind a route's verified Slack resolution card. Accepted so it can be
        /// DECLINED with a reason: the skill tier has no platform agent record.
        #[arg(long = "route-resolution", value_name = "NAME=CHANNEL")]
        route_resolution: Vec<String>,
        /// Narrow a route's approvers. Declined at this tier for the same reason
        /// as --route-resolution.
        #[arg(long = "route-approvers", value_name = "NAME=KIND:VALUES")]
        route_approvers: Vec<String>,
        /// Read the complete route map, including optional notifications, from
        /// JSON. Declined at this tier for the same reason as --route-resolution.
        #[arg(long = "routes-from", value_name = "FILE")]
        routes_from: Option<PathBuf>,
        /// Show the agent's route bindings. Declined at this tier for the same
        /// reason as --route-resolution.
        #[arg(long)]
        list_routes: bool,
        /// Remove every route binding. Declined at this tier for the same reason
        /// as --route-resolution.
        #[arg(long)]
        clear_routes: bool,
    },
    // The about text is composed from the same consts the runtime `{error, fix}`
    // payload uses, so the discovery surface cannot drift from the answer
    // (issue #459, ADR-0041).
    #[command(about = format!(
        "Not available at this tier: {}; {}",
        commands::VERSIONS_REASON, commands::VERSIONS_ALT,
    ))]
    Versions,
    #[command(about = format!(
        "Not available at this tier: {}; {}",
        commands::MEMORY_REASON, commands::MEMORY_ALT,
    ))]
    Memory,
    #[command(about = format!(
        "Not available at this tier: {}; {}",
        commands::OBSERVABILITY_REASON, commands::OBSERVABILITY_ALT,
    ))]
    Observability {
        /// Optional so the bare form (no leaf) reaches the exit-4 capability
        /// refusal below instead of dying as a clap usage error (issue
        /// #1955, ADR-0041).
        #[command(subcommand)]
        _query: Option<SkillObservabilityQuery>,
    },
    /// Stop and remove the local runner container.
    Down {
        /// Container name to remove. Defaults to the recorded runner, then to
        /// `curie-runner-local`. Pass it to clear a leftover container from a
        /// directory with no `.curie/runner.json`.
        #[arg(long)]
        name: Option<String>,
    },
    /// Show the local runner's session status.
    Status {
        /// Runner base URL (defaults to the started runner, then localhost).
        #[arg(long)]
        url: Option<String>,
    },
    /// Send a synthetic event to the local runner and stream the reply.
    Message {
        /// The message text.
        text: String,
        /// Synthetic Slack user id.
        #[arg(long, default_value = "U-local")]
        user: String,
        /// ACI event type.
        #[arg(long, value_enum, default_value_t = SendType::Message)]
        event_type: SendType,
        /// Runner base URL (defaults to the started runner, then localhost).
        #[arg(long)]
        url: Option<String>,
        /// Reuse the runner's current conversation instead of starting fresh.
        #[arg(long = "continue")]
        r#continue: bool,
    },
    /// Run the bundle's eval cases through the local runner.
    Eval {
        /// Eval case file (default: evals/cases.json here, then the running
        /// bundle's).
        #[arg(long)]
        cases: Option<PathBuf>,
        /// Run only the case(s) with these ids; repeat to select several.
        /// Omit to run the whole suite. A value that matches no case in the
        /// suite exits 2 (usage), so a mistyped selector fails the gate instead
        /// of greening an empty run.
        #[arg(long = "case-id", value_name = "ID")]
        case_id: Vec<String>,
        /// Runner base URL (defaults to the started runner, then localhost).
        #[arg(long)]
        url: Option<String>,
        /// Run the suite against this model in a throwaway runner instead of the
        /// already-running one. Repeatable: pass it N times to sweep N models and
        /// report pass-rate per model (#526). Needs a model credential + Docker.
        #[arg(long = "model", value_name = "MODEL")]
        model: Vec<String>,
        /// Forward a connector secret BY NAME into each sweep runner (as
        /// `skill up --secret`), so an authed-MCP bundle can run under `--model`.
        #[arg(long = "secret", value_name = "NAME")]
        secret: Vec<String>,
        /// Runner image for the sweep runners. Defaults to the same image
        /// resolution as `skill up`.
        #[arg(long)]
        image: Option<String>,
        #[command(flatten)]
        sampling: EvalSamplingArgs,
    },
    /// Interview to generate a starter `evals/cases.json` (guided eval generation).
    EvalInit {
        /// Where to write the suite (default: evals/cases.json).
        #[arg(long, default_value = "evals/cases.json")]
        out: PathBuf,
        /// Overwrite an existing suite file instead of refusing.
        #[arg(long)]
        force: bool,
    },
}

/// Subcommands of `curie <tier> console`.
///
/// Generic over the tier so `local` and `cluster` share one surface and one
/// handler, the way `AgentTarget` does: the connection flags live on the leaf
/// verb, because `curie local console login --dry-run` is what an operator will
/// type and clap only accepts parent flags BEFORE the subcommand.
#[derive(Subcommand)]
enum ConsoleAction<T: TierDefaults + Clone + Send + Sync + 'static> {
    /// Mint a single-use login code for the web console (ADR-0083).
    ///
    /// The console authenticates with a revocable session cookie, not the
    /// platform key. This command is the only place the key is handled, it
    /// happens here rather than in a browser, and what you carry away is a
    /// short-lived code that authorizes one browser and nothing else.
    Login {
        #[arg(long, default_value = T::API_URL, env = "CURIE_API_URL")]
        api_url: String,
        #[arg(long, default_value = "curie-dev-key", env = "CURIE_API_KEY", value_parser = message::api_key_or_default)]
        api_key: String,
        /// Who the session is for. Bound to the code at mint time (ADR-0106),
        /// so the console session it becomes carries an identity rather than
        /// being anonymous.
        #[arg(long, value_name = "SUBJECT")]
        subject: String,
        /// Where the console is served, for the printed instruction.
        #[arg(long, value_name = "URL")]
        console_url: Option<String>,
        #[arg(long)]
        dry_run: bool,
        /// The tier only supplies the default API URL above; nothing reads it.
        #[arg(skip)]
        _tier: std::marker::PhantomData<T>,
    },
}

/// `curie cluster console`. Separate from the local surface because the cluster
/// connection self-plumbs a tunnel and discovers its key, which is a different
/// set of flags rather than different defaults for the same ones.
#[derive(Subcommand)]
enum ClusterConsoleAction {
    /// Mint a single-use login code for the cluster's web console (ADR-0083).
    Login {
        #[command(flatten)]
        conn: ClusterConn,
        /// Who the session is for. Bound to the code at mint time (ADR-0106).
        #[arg(long, value_name = "SUBJECT")]
        subject: String,
        /// Where the console is served, for the printed instruction.
        #[arg(long, value_name = "URL")]
        console_url: Option<String>,
        #[arg(long)]
        dry_run: bool,
    },
}

/// Subcommands of `curie local`.
#[derive(Subcommand)]
enum LocalAction {
    /// Bring the dev stack up (`core` with `--minimal`, else `full`) and print URLs. Add `--slack` for the optional dispatcher.
    ///
    /// Model parity with `curie skill up`: `local up` runs the real model when a
    /// model credential is present in the shell, and the offline fake model
    /// otherwise. Providers are first-class beyond Anthropic: an Anthropic key
    /// (`ANTHROPIC_API_KEY` / `CLAUDE_CODE_OAUTH_TOKEN`) OR the provider-agnostic
    /// `CURIE_CREDENTIALS` (with `ANTHROPIC_BASE_URL` for an OpenAI-compatible
    /// endpoint such as OpenRouter). Set `CURIE_FAKE_MODEL=1` to force the fake
    /// even with a credential; set `CURIE_FAKE_MODEL=0` (or provide a
    /// credential) to go live.
    Up {
        /// Compose file. Default: version-pinned `compose.release.yaml` from the remote on release builds; local `compose.dev.yaml` on dev builds. Pass to override.
        #[arg(short = 'f', long)]
        file: Option<String>,
        /// Print the docker compose command and exit without executing.
        #[arg(long)]
        dry_run: bool,
        /// Bring up only the 7 core services (skip Langfuse/ClickHouse/OTel/UI).
        #[arg(long)]
        minimal: bool,
        /// Model id, forwarded as CURIE_MODEL. Omit for the SDK default.
        /// Setting it makes token usage attributable in Langfuse traces.
        #[arg(long, conflicts_with = "local_model")]
        model: Option<String>,
        /// Run the named model through local Ollama.
        #[arg(
            long,
            num_args = 0..=1,
            default_missing_value = commands::DEFAULT_LOCAL_MODEL,
            value_parser = parse_model_ref
        )]
        local_model: Option<String>,
        /// Allow --local-model to DOWNLOAD its assets on this run. Without it,
        /// `up` refuses when the pinned Ollama image (~8.9 GB) or the requested
        /// model is not already on the machine, rather than fetching them
        /// implicitly (ADR 0093).
        #[arg(long, requires = "local_model")]
        pull_model: bool,
        /// Also start the optional Slack dispatcher (adds --profile slack).
        #[arg(long)]
        slack: bool,
        /// Opt-in: read a bundle-local `.env` (any dotenv path) as the LOWEST-
        /// priority model-credential source, so the compose stack boots live
        /// with no `set -a; source .env` step. Precedence: shell env > this
        /// file. Only CURIE_CREDENTIALS, CLAUDE_CODE_OAUTH_TOKEN, and
        /// ANTHROPIC_API_KEY are read; every other key in the file is ignored,
        /// and the value never reaches argv or logs (#749).
        #[arg(long = "env-file", value_name = "PATH")]
        env_file: Option<PathBuf>,
        /// Build the stack's images from THIS checkout instead of pulling the
        /// published ones, and run them (#1915).
        ///
        /// `curie update` refreshes the CLI and the runner image; nothing
        /// refreshed api, worker, ui or dispatcher, so a contributor on a feature
        /// branch ran a source-built CLI against whatever the registry last
        /// published. The skew does not announce itself: it surfaces as a serde
        /// error about a field name, or `No module named` from inside a
        /// container. Builds only what the selected profiles run.
        ///
        /// Requires a compose file that substitutes the image tags, so a
        /// release-channel curie must pass `-f compose.dev.yaml` (#1926).
        ///
        /// The tag survives the command: `rebuild`, `comms` and a later plain
        /// `up` read it back off the running api container, so they recreate
        /// services onto what this built rather than silently re-resolving every
        /// image to `:latest` (#1925).
        #[arg(long)]
        build: bool,
    },
    /// Rebuild + recreate ONE compose service (e.g. after a code change) without
    /// losing the stack's already-resolved credential/model-mode wiring.
    ///
    /// A raw `docker compose up --no-deps <service>` silently reverts that one
    /// service to compose's fake-model/dev-stub defaults, because compose's
    /// `${VAR-default}` substitution reads THIS invocation's shell, not what the
    /// rest of the stack is running with -- export the same credential /
    /// CURIE_FAKE_MODEL you want, same as `local up`.
    ///
    /// The image tag is the exception: it is read back off the running api
    /// container rather than the shell, so a service rebuilt against a stack
    /// started with `local up --build` comes back on that build's tag (#1925).
    Rebuild {
        /// The compose service to rebuild, e.g. `curie-worker`.
        service: String,
        /// Compose file. Default: version-pinned `compose.release.yaml` from the remote on release builds; local `compose.dev.yaml` on dev builds. Pass to override.
        #[arg(short = 'f', long)]
        file: Option<String>,
        /// Print the docker compose command and exit without executing.
        #[arg(long)]
        dry_run: bool,
        /// Match how `local up` brought the stack up (core-only vs full).
        #[arg(long)]
        minimal: bool,
        /// Model id, forwarded as CURIE_MODEL. Omit for the SDK default.
        /// Match the explicit model used by `local up`.
        #[arg(long, conflicts_with = "local_model")]
        model: Option<String>,
        /// Match how `local up` brought the stack up (--local-model, if used).
        #[arg(
            long,
            num_args = 0..=1,
            default_missing_value = commands::DEFAULT_LOCAL_MODEL,
            value_parser = parse_model_ref
        )]
        local_model: Option<String>,
        /// Match how `local up` brought the stack up (--slack, if used).
        #[arg(long)]
        slack: bool,
        /// Match how `local up` brought the stack up (--env-file, if used):
        /// read a bundle-local `.env` as the LOWEST-priority model-credential
        /// source so the rebuilt service comes back on the SAME model as the
        /// rest of the stack, instead of reverting to compose's fake default
        /// (#853). Precedence (shell env > this file), the recognized keys, and
        /// the never-in-argv/logs masking are identical to `local up --env-file`.
        #[arg(long = "env-file", value_name = "PATH")]
        env_file: Option<PathBuf>,
    },
    /// Stop the dev stack (docker compose down), keeping volumes.
    Down {
        /// Compose file. Default: version-pinned `compose.release.yaml` from the remote on release builds; local `compose.dev.yaml` on dev builds. Pass to override.
        #[arg(short = 'f', long)]
        file: Option<String>,
        /// Also destroy volumes (adds -v). Prompts for confirmation unless --yes.
        #[arg(long)]
        wipe: bool,
        /// Skip the --wipe confirmation prompt.
        #[arg(long)]
        yes: bool,
        /// Print the docker compose command and exit without executing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Show the dev stack's service status (docker compose ps).
    Status {
        /// Compose file. Default: version-pinned `compose.release.yaml` from the remote on release builds; local `compose.dev.yaml` on dev builds. Pass to override.
        #[arg(short = 'f', long)]
        file: Option<String>,
        /// Print the docker compose command and exit without executing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Connect or disconnect the local compose stack from a real Slack workspace.
    Comms {
        /// Chat surface to configure. Required until the CLI grows more than
        /// one comms target.
        #[arg(long)]
        slack: bool,
        /// Clear Slack from the local stack instead of connecting it.
        #[arg(long)]
        disconnect: bool,
        /// The stack runs only the 7 core services (skip Langfuse/ClickHouse/OTel/UI). Must match how `local up` brought it up.
        #[arg(long)]
        minimal: bool,
        /// Match the explicit model used by `local up`. Defaults from CURIE_MODEL.
        #[arg(long, env = "CURIE_MODEL")]
        model: Option<String>,
        /// Slack app token. Defaults from SLACK_APP_TOKEN.
        #[arg(
            long,
            env = "SLACK_APP_TOKEN",
            hide_env_values = true,
            default_value = ""
        )]
        app_token: String,
        /// Slack bot token. Defaults from SLACK_BOT_TOKEN.
        #[arg(
            long,
            env = "SLACK_BOT_TOKEN",
            hide_env_values = true,
            default_value = ""
        )]
        bot_token: String,
        /// Compose file. Default: version-pinned `compose.release.yaml` from the remote on release builds; local `compose.dev.yaml` on dev builds. Pass to override.
        #[arg(short = 'f', long)]
        file: Option<String>,
        /// Print the docker compose command(s) that would run and exit without executing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Drive the local compose stack end to end with zero Slack contact.
    Message {
        /// The user message text.
        text: String,
        /// Slack channel id to send as; must match one of the target agent's
        /// channels. Omit when exactly one channel is bound across all
        /// deployed agents (errors on zero or several).
        #[arg(long)]
        channel: Option<String>,
        /// Existing thread ts to continue a conversation; omit to start a new
        /// thread. Pair with --channel to keep multi-turn context.
        #[arg(long)]
        thread: Option<String>,
        /// Reuse the last turn's context (channel, thread, transport) recorded
        /// in .curie/last-turn.json in the working directory; type only the
        /// new message text.
        #[arg(long = "continue")]
        r#continue: bool,
        /// Valkey password (compose default `valkeypass`). Prefer the
        /// CURIE_VALKEY_PASSWORD env var over passing a real secret on the
        /// command line, where it leaks via `ps` and shell history.
        #[arg(
            long,
            env = "CURIE_VALKEY_PASSWORD",
            hide_env_values = true,
            default_value = message::DEFAULT_VALKEY_PASSWORD
        )]
        valkey_password: String,
        /// Local mode only: platform API base URL for the channel lookup.
        #[arg(long)]
        api_url: Option<String>,
        /// Platform API key for the default-channel lookup.
        #[arg(long, env = "CURIE_API_KEY", default_value = message::DEFAULT_API_KEY, value_parser = message::api_key_or_default)]
        api_key: String,
        /// Synthetic Slack user id for the enqueued event.
        #[arg(long, default_value = message::DEFAULT_USER)]
        user: String,
        /// Stream the dispatcher enqueues onto.
        #[arg(long, env = "CURIE_STREAM", default_value = message::DEFAULT_STREAM)]
        stream: String,
        /// How long to wait for the worker's reply before printing diagnostics.
        /// Default: 300 seconds.
        #[arg(long)]
        timeout_secs: Option<u64>,
        /// Print the queue and stub plan that a real run would produce, and exit.
        #[arg(long)]
        dry_run: bool,
    },
    /// Run the bundle's `evals/cases.json` through the local tier and grade with
    /// the same grader `skill eval` uses (the per-tier parity gate).
    Eval {
        /// Eval case file (default: `evals/cases.json` here, then the recorded
        /// bundle's).
        #[arg(long)]
        cases: Option<PathBuf>,
        /// Run only the case(s) with these ids; repeat to select several.
        /// Omit to run the whole suite. A value that matches no case in the
        /// suite exits 2 (usage), so a mistyped selector fails the gate instead
        /// of greening an empty run.
        #[arg(long = "case-id", value_name = "ID")]
        case_id: Vec<String>,
        /// Slack channel id to send as; must match one of the target agent's
        /// channels. Omit when exactly one channel is bound across all
        /// deployed agents.
        #[arg(long)]
        channel: Option<String>,
        /// Valkey password (compose default `valkeypass`). Prefer the
        /// CURIE_VALKEY_PASSWORD env var over passing a real secret on the
        /// command line, where it leaks via `ps` and shell history.
        #[arg(
            long,
            env = "CURIE_VALKEY_PASSWORD",
            hide_env_values = true,
            default_value = message::DEFAULT_VALKEY_PASSWORD
        )]
        valkey_password: String,
        /// Platform API base URL for the channel lookup.
        #[arg(long)]
        api_url: Option<String>,
        /// Platform API key for the default-channel lookup.
        #[arg(long, env = "CURIE_API_KEY", default_value = message::DEFAULT_API_KEY, value_parser = message::api_key_or_default)]
        api_key: String,
        /// Synthetic Slack user id for the enqueued events.
        #[arg(long, default_value = message::DEFAULT_USER)]
        user: String,
        /// Stream the dispatcher enqueues onto.
        #[arg(long, env = "CURIE_STREAM", default_value = message::DEFAULT_STREAM)]
        stream: String,
        /// How long to wait for each case's reply. Default: 300 seconds.
        #[arg(long, default_value_t = message::DEFAULT_TIMEOUT_SECS)]
        timeout_secs: u64,
        /// Evaluate under this model instead of the deployed one; repeat to sweep
        /// several models in one run (#526). A sweep triggers a platform eval per
        /// model and reports the per-model pass-rate from `GET /evals/matrix`.
        #[arg(long = "model")]
        model: Vec<String>,
        /// Number of eval cases to run concurrently. Sequential (1) is the only
        /// supported value today; parallel dispatch is tracked in #709, so any
        /// value above 1 is refused rather than silently run sequentially.
        #[arg(long, default_value_t = 1)]
        concurrency: usize,
        #[command(flatten)]
        sampling: EvalSamplingArgs,
        /// Print the plan that a real run would produce, and exit.
        #[arg(long)]
        dry_run: bool,
    },
    /// Push the bundle to the local platform API and deploy it.
    Deploy {
        /// Resolve the agent, environment, and channel from a target declared
        /// in the bundle's `deploy.yaml` (ADR-0089).
        ///
        /// Routing lives in the repository and is a reviewable diff, instead of
        /// flags scattered across whatever invoked this command. Explicit
        /// --agent/--env/--slack-channel still win, so a one-off deploy needs
        /// no committed file.
        #[arg(long, conflicts_with = "agent")]
        target: Option<String>,
        /// Deploy under this agent name instead of the manifest's `name`.
        ///
        /// The bundle is unchanged -- only which agent it binds to. This is how
        /// one repository serves a dev agent and a prod agent from the same
        /// artifact, so prod promotes exactly what dev validated (#1166).
        #[arg(long)]
        agent: Option<String>,
        /// Plugin bundle directory.
        #[arg(long, default_value = ".")]
        plugin_dir: PathBuf,
        /// Platform API base URL.
        #[arg(
            long,
            default_value = message::DEFAULT_LOCAL_API_URL,
            env = "CURIE_API_URL"
        )]
        api_url: String,
        /// Platform API key.
        #[arg(long, default_value = "curie-dev-key", env = "CURIE_API_KEY", value_parser = message::api_key_or_default)]
        api_key: String,
        /// Slack channel to bind the agent to. On first create it defaults to
        /// C0LOCALDEV; on redeploy the channel is ADDED when the agent is not
        /// already bound to it, never moved and never removed, so omitting the
        /// flag leaves the deployed agent's binding set untouched.
        #[arg(long)]
        slack_channel: Option<String>,
        /// Bind this agent to a GitHub repository (`owner/name`) so pushes to
        /// its dev/prod branches deploy it (ADR-0014).
        ///
        /// A new agent is created bound to it, and an existing agent with no
        /// binding is bound now (#1194 made `repo_full_name` PATCHable and
        /// ADR-0091 dropped the uniqueness, so one repository can build several
        /// agents). An agent already bound to a DIFFERENT repository is NOT
        /// moved, because that would reroute which repository's pushes deploy
        /// it; a warning names the binding it kept.
        #[arg(long = "repo", value_name = "OWNER/NAME")]
        repo: Option<String>,
        /// Deprecated compatibility no-op: coding tools are built in, and an
        /// allowed root GitHub URL in the opening message drives managed
        /// /workspace acquisition.
        #[arg(long, conflicts_with = "no_workspace")]
        workspace: bool,
        /// Deprecated compatibility no-op: coding tools are built in, and an
        /// allowed root GitHub URL in the opening message drives managed
        /// /workspace acquisition.
        #[arg(long, conflicts_with = "workspace")]
        no_workspace: bool,
        /// Target environment. Defaults to dev; a `--target` supplies it
        /// instead, and an explicit value here still wins over the target.
        #[arg(long, value_enum)]
        env: Option<DeployEnv>,
        /// Version label; defaults to <manifest version>-<unix time>.
        #[arg(long)]
        label: Option<String>,
        /// Bind a per-agent connector secret by NAME (ADR-0009, #429). The value
        /// is resolved from your environment or the host secret vault (`curie
        /// secrets set <NAME>`) and sent to the platform, which stores it on the
        /// agent so the worker forwards it into the sandbox for a bundle's authed
        /// MCP server. The value never appears in argv. Repeatable.
        #[arg(long = "secret", value_name = "NAME")]
        secret: Vec<String>,
    },
    /// Sign in to the local web console.
    Console {
        #[command(subcommand)]
        action: ConsoleAction<LocalTier>,
    },
    /// List an agent's immutable versions (`GET /agents/{id}/versions`).
    Versions {
        #[command(flatten)]
        target: AgentTarget<LocalTier>,
    },
    /// Show what an agent has learned (its memory log; `GET /agents/{id}/memory`).
    /// `--add <content>` seeds an operator-authored record; a fresh session is
    /// required before it is injected at boot.
    Memory {
        #[command(flatten)]
        target: AgentTarget<LocalTier>,
        /// Append this content as an operator-authored memory record.
        #[arg(long, value_name = "CONTENT")]
        add: Option<String>,
    },
    /// The human-in-the-loop plane: list and resolve pending approval records,
    /// and view or set the tools whose calls require approval. Which channel an
    /// approval posts to, and who may resolve it, come from the agent's approval
    /// route bindings; `curie guide` explains the whole plane.
    Approvals {
        #[command(flatten)]
        target: AgentTarget<LocalTier>,
        /// Tool name to gate behind approval (repeatable). Omit to show current gates.
        #[arg(long = "gate", value_name = "TOOL")]
        gate: Vec<String>,
        /// Clear all approval gates on the agent.
        #[arg(long)]
        clear: bool,
        /// List the agent's pending approval records instead of the gate config.
        #[arg(long)]
        list: bool,
        /// Resolve the approval with this id (approve by default). Authentication
        /// comes from CURIE_APPROVAL_PRINCIPAL_TOKEN.
        #[arg(long, value_name = "APPROVAL_ID")]
        resolve: Option<String>,
        /// Reject instead of approve (with --resolve).
        #[arg(long)]
        reject: bool,
        /// Optional note recorded with the resolution (with --resolve).
        #[arg(long)]
        note: Option<String>,
        /// Administratively mint a reusable, subject-bound operator principal.
        /// The token is delivered once; export it as
        /// CURIE_APPROVAL_PRINCIPAL_TOKEN before resolving.
        #[arg(long, value_name = "SUBJECT")]
        mint_operator_principal: Option<String>,
        /// Administratively mint a single-use, subject-bound Console login code.
        #[arg(long, value_name = "SUBJECT")]
        mint_console_login_code: Option<String>,
        /// Bind a manifest route's verified Slack resolution card, as
        /// NAME=CHANNEL (e.g. deal_desk=C0123ABCD). Repeatable. A write REPLACES
        /// the whole route map, like --gate does for tool gates.
        #[arg(long = "route-resolution", value_name = "NAME=CHANNEL")]
        route_resolution: Vec<String>,
        /// Narrow WHO may resolve a route, independently of its resolution target,
        /// as NAME=users:U1,U2 or NAME=group:S1. Repeatable. Omit to leave the
        /// resolution card's channel members as the approvers.
        #[arg(long = "route-approvers", value_name = "NAME=KIND:VALUES")]
        route_approvers: Vec<String>,
        /// Read the whole route map from a JSON file, e.g.
        /// {"deal_desk":{"resolution":{"kind":"slack","address":"C0123ABCD"}}}.
        /// Notifications, including endpoint+adapter transport, are declared in
        /// this strict map. The repeatable override flags apply on top of it.
        #[arg(long = "routes-from", value_name = "FILE")]
        routes_from: Option<PathBuf>,
        /// Show the agent's approval route bindings instead of its tool gates.
        #[arg(long)]
        list_routes: bool,
        /// Remove every approval route binding on the agent.
        #[arg(long)]
        clear_routes: bool,
    },
    /// Show the local observability surfaces (Curie Console + Langfuse traces/cost + API base).
    Observability {
        /// Query platform observability data through the Curie API. Omit to
        /// preserve the existing URL/surface report.
        #[command(subcommand)]
        query: Option<LocalObservabilityQuery>,
        /// Also open the browsable surfaces in a browser. Off by default: the URLs
        /// are printed and nothing is opened unless --open is passed, and --json
        /// never opens a browser.
        #[arg(long)]
        open: bool,
    },
    /// Read or change an agent's model and thinking overrides (`PATCH /agents/{id}`).
    ///
    /// With no flags this inspects. Both fields are nullable operator
    /// overrides of a platform default, so clearing is `--clear-<field>`
    /// (which sends JSON null) and never an empty value, which would skip the
    /// platform default rather than restore it.
    Overrides {
        /// Agent name or id.
        agent: String,
        /// Pin this model for the agent (forwarded as CURIE_MODEL at boot).
        #[arg(long)]
        model: Option<String>,
        /// Clear the model override back to the platform default.
        #[arg(long)]
        clear_model: bool,
        /// Pin this thinking depth (e.g. `disabled`, `adaptive`, `enabled:2000`).
        #[arg(long)]
        thinking: Option<String>,
        /// Clear the thinking override back to the platform default.
        #[arg(long)]
        clear_thinking: bool,
        #[arg(long, default_value = "http://localhost:28000", env = "CURIE_API_URL")]
        api_url: String,
        #[arg(long, default_value = "curie-dev-key", env = "CURIE_API_KEY", value_parser = message::api_key_or_default)]
        api_key: String,
        #[arg(long)]
        dry_run: bool,
    },
    /// List, add, or remove an agent's surfaces
    /// (`/agents/{id}/channels`).
    ///
    /// With no flags this lists. An agent holds one or more bindings
    /// (ADR-0118), so exactly one `--add` OR one `--remove` is applied per
    /// invocation: the API has no batch endpoint, and a half-applied batch
    /// would leave the operator guessing what took.
    Surfaces {
        #[command(flatten)]
        target: AgentTarget<LocalTier>,
        /// Add this surface, as KIND=ADDRESS (e.g. slack=C0EXAMPLE1).
        #[arg(long, value_name = "KIND=ADDRESS")]
        add: Option<String>,
        /// Reply HTTP endpoint for a non-Slack adapter. Requires --adapter.
        #[arg(long, requires_all = ["add", "adapter"])]
        endpoint: Option<String>,
        /// Worker credential selector for the reply adapter. Requires --endpoint.
        #[arg(long, requires_all = ["add", "endpoint"])]
        adapter: Option<String>,
        /// Remove this surface, as KIND=ADDRESS. The API refuses to remove an
        /// agent's final surface.
        #[arg(long, value_name = "KIND=ADDRESS", conflicts_with = "add")]
        remove: Option<String>,
    },
    /// Set an agent's daily budget (`PUT /agents/{id}/budget`).
    Budget {
        /// Agent name or id.
        agent: String,
        /// Daily spend cap in USD. Must be > 0.
        #[arg(long)]
        limit: f64,
        #[arg(long, default_value = "http://localhost:28000", env = "CURIE_API_URL")]
        api_url: String,
        #[arg(long, default_value = "curie-dev-key", env = "CURIE_API_KEY", value_parser = message::api_key_or_default)]
        api_key: String,
        #[arg(long)]
        dry_run: bool,
    },
    /// Kill an agent (stop its runs; `POST /agents/{id}/kill`).
    Kill {
        /// Agent name or id.
        agent: String,
        #[arg(long, default_value = "http://localhost:28000", env = "CURIE_API_URL")]
        api_url: String,
        #[arg(long, default_value = "curie-dev-key", env = "CURIE_API_KEY", value_parser = message::api_key_or_default)]
        api_key: String,
        /// Confirm the action.
        #[arg(long)]
        yes: bool,
        #[arg(long)]
        dry_run: bool,
    },
    /// Resume a killed agent (`POST /agents/{id}/resume`).
    Resume {
        /// Agent name or id.
        agent: String,
        #[arg(long, default_value = "http://localhost:28000", env = "CURIE_API_URL")]
        api_url: String,
        #[arg(long, default_value = "curie-dev-key", env = "CURIE_API_KEY", value_parser = message::api_key_or_default)]
        api_key: String,
        #[arg(long)]
        dry_run: bool,
    },
    /// Force a stuck thread's sandbox to be released (`POST
    /// /agents/{id}/threads/{thread_key}/reset`, #737). The worker's next
    /// maintenance tick deletes the thread's claim and route, so its next
    /// message cold-creates a fresh sandbox instead of adopting one that may be
    /// running stale env. Interrupts a live turn on the thread first, so it
    /// requires --yes; does not delete conversation history.
    ResetThread {
        /// Agent name or id (scopes the action; the release is thread-keyed).
        agent: String,
        /// The worker's composed key: kind:channel:thread-ts (e.g.
        /// slack:C0EXAMPLE1:1700000000.000100).
        #[arg(long, value_name = "THREAD_KEY")]
        thread_key: String,
        #[arg(long, default_value = "http://localhost:28000", env = "CURIE_API_URL")]
        api_url: String,
        #[arg(long, default_value = "curie-dev-key", env = "CURIE_API_KEY", value_parser = message::api_key_or_default)]
        api_key: String,
        /// Confirm the action; it interrupts any live turn on the thread.
        #[arg(long)]
        yes: bool,
        #[arg(long)]
        dry_run: bool,
    },
    /// Delete an agent via the local platform API.
    Delete {
        /// Agent name or id to delete.
        agent: String,
        #[arg(
            long,
            default_value = message::DEFAULT_LOCAL_API_URL,
            env = "CURIE_API_URL"
        )]
        api_url: String,
        #[arg(long, default_value = message::DEFAULT_API_KEY, env = "CURIE_API_KEY", value_parser = message::api_key_or_default)]
        api_key: String,
        /// Confirm this destructive action.
        #[arg(long)]
        yes: bool,
        /// Print what would be done and exit without making a request.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
enum ClusterAction {
    /// Install or upgrade the Curie release via Helm (helm upgrade --install).
    /// By default it puts the UI and Langfuse on node ports for tailnet/LAN
    /// access; pass --no-expose to keep them ClusterIP-only. Set
    /// CURIE_CREDENTIALS to a supported model provider credential
    /// (CURIE_MODEL_CREDENTIALS is a deprecated alias) to install with the real
    /// model. A fresh install without it uses fake mode. A rerun preserves the
    /// recorded model configuration. Use --fake-model to explicitly downgrade
    /// to fake mode. An sk-ant- or sk-or- credential infers its provider egress
    /// when --allow-egress-host is absent. Other credential shapes remain sealed
    /// until their provider or a raw range is explicit. Existing singleton
    /// resources are reused only from complete Helm ownership metadata. An exact
    /// admission result that the gvisor RuntimeClass is absent applies
    /// security.gvisor.mode=off and retries once. Every inferred value is printed.
    Up {
        /// Kubernetes namespace.
        #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        /// Helm release name.
        #[arg(long, default_value = "curie")]
        release: String,
        /// Helm chart. Default: the version-pinned chart release asset on release builds; local `charts/curie` on dev builds. Pass a path or ref to override.
        #[arg(long)]
        chart: Option<String>,
        /// Keep the UI and Langfuse services ClusterIP instead of NodePort.
        #[arg(long)]
        no_expose: bool,
        /// Force the sealed fake-model install even when CURIE_CREDENTIALS
        /// is set (dev/CI escape hatch); suppresses the fake-model warning.
        #[arg(long)]
        fake_model: bool,
        /// Model id, forwarded as CURIE_MODEL. Omit for the SDK default.
        /// Setting it makes token usage attributable in Langfuse traces.
        #[arg(long, conflicts_with = "local_model")]
        model: Option<String>,
        /// Run the named model through the chart inference deployment.
        #[arg(
            long,
            num_args = 0..=1,
            default_missing_value = commands::DEFAULT_LOCAL_MODEL,
            value_parser = parse_model_ref,
            conflicts_with = "fake_model"
        )]
        local_model: Option<String>,
        /// Explicitly open runner egress to a named model provider's API host(s),
        /// resolved to narrow host routes at install time (repeatable). An sk-ant-
        /// or sk-or- credential infers anthropic or openrouter when this flag is
        /// absent. An explicit list that omits the detected provider is an error.
        /// One of: anthropic,
        /// openrouter, zhipu, moonshot, deepseek. Provider native Zhipu,
        /// Moonshot, and DeepSeek also need their matching worker runtime base
        /// URL; a credential plus egress alone does not reach them. For a raw
        /// CIDR, use --allow-web-egress.
        #[arg(long = "allow-egress-host", value_name = "PROVIDER")]
        allow_egress_host: Vec<String>,
        /// Open runner egress to a declared destination for skill web access,
        /// repeatable CIDR, TCP 443. Additive to provider egress. Omit to keep
        /// general web egress sealed.
        #[arg(long = "allow-web-egress", value_name = "CIDR")]
        allow_web_egress: Vec<String>,
        /// GitHub credential the API uses for the git-flow bundle clone and the
        /// eval commit status, needed for private repositories. Passed to helm
        /// through a private 0600 values file, not as an argument, so it stays
        /// out of the helm command line and the printed plan. Supply it through
        /// CURIE_GITHUB_TOKEN to also keep it out of your shell history and the
        /// process table: a value typed after this flag is in curie's own argv.
        /// Omit it and a later cluster up preserves whatever was recorded.
        #[arg(
            long = "github-token",
            value_name = "TOKEN",
            env = "CURIE_GITHUB_TOKEN",
            hide_env_values = true,
            conflicts_with = "clear_github_token"
        )]
        github_token: Option<String>,
        /// Remove the recorded GitHub credential from the release. An empty
        /// --github-token does NOT clear it; this flag is the only way, so an
        /// empty environment variable cannot destroy a working credential.
        #[arg(long = "clear-github-token")]
        clear_github_token: bool,
        /// Extra `--set KEY=VAL` passed through to helm verbatim (repeatable).
        #[arg(long = "set", value_name = "KEY=VAL")]
        set: Vec<String>,
        /// Install with the chart's built-in dev-default secrets instead of
        /// generating strong per-release randoms. Deterministic, for local dev
        /// and CI only -- these defaults are published in the public repo.
        /// Applies to a fresh install or a release already on dev defaults; it
        /// is refused against a release installed without it, since switching
        /// an existing release onto the published defaults breaks
        /// authentication against the credentials its PVCs still hold.
        #[arg(long)]
        dev: bool,
        /// Print the helm command that would run and exit without executing.
        #[arg(long)]
        dry_run: bool,
        /// Apply contract or irreversible schema migrations. Without this flag
        /// the upgrade Job refuses those migrations before mutation so a patch
        /// rollback window stays intact (#2300).
        #[arg(long = "forward-only")]
        forward_only: bool,
    },
    /// Uninstall the release and sweep its runtime namespaces, running helm
    /// uninstall followed by kubectl delete namespace. The namespace delete
    /// is scoped to namespaces this release created, matched by both its
    /// release name and install namespace, so another release's namespaces
    /// on the same cluster are never touched. Pre-existing namespaces and
    /// the agents.x-k8s.io CRDs are left in place.
    Down {
        /// Kubernetes namespace.
        #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        /// Helm release name.
        #[arg(long, default_value = "curie")]
        release: String,
        /// Skip the interactive confirmation prompt.
        #[arg(long)]
        yes: bool,
        /// Print the commands that would run and exit without executing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Roll the release back to the newest revision that is actually known good.
    ///
    /// A bare `helm rollback` targets the immediately preceding revision. On a
    /// cluster with no `runsc` RuntimeClass that is the wrong one: `cluster up`
    /// records a FAILED revision before its successful gVisor-off retry, so the
    /// history alternates failed/superseded and the preceding revision is a
    /// failed one -- a manifest helm never finished applying.
    ///
    /// This verb skips every revision whose status is not `deployed` or
    /// `superseded` and rolls back to the newest one below the current revision
    /// that is, printing which revisions it passed over. See issue #1899.
    Rollback {
        /// Roll back to this exact revision instead of the newest safe one. A
        /// revision that is not `deployed` or `superseded` is refused unless
        /// --allow-failed-revision is also passed.
        #[arg(long)]
        revision: Option<u32>,
        /// Permit --revision to name a revision helm never finished applying
        /// (`failed`, `pending-*`, `uninstalling`). Off by default. Requires
        /// --revision -- auto-select never chooses an ineligible revision, so
        /// this flag alone would otherwise be a silent no-op.
        #[arg(long, requires = "revision")]
        allow_failed_revision: bool,
        /// Kubernetes namespace.
        #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        /// Helm release name.
        #[arg(long, default_value = "curie")]
        release: String,
        /// Skip the interactive confirmation prompt.
        #[arg(long)]
        yes: bool,
        /// Print the commands that would run and exit without executing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Run the resumable cluster upgrade lifecycle to a target version.
    ///
    /// Plans, validates, drains accepted work, checkpoints, migrates, applies,
    /// proves exact convergence, runs a target-version canary, and records the
    /// new known-good revision. The operator does not pass Helm merge flags.
    /// A failed attempt either leaves the previous known-good version serving
    /// or returns one fail-forward command. See issue #2301.
    Upgrade {
        /// Target Curie version (chart/app version) to upgrade to.
        #[arg(long = "to", value_name = "VERSION")]
        to: String,
        /// Kubernetes namespace.
        #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        /// Helm release name.
        #[arg(long, default_value = "curie")]
        release: String,
        /// Helm chart. Default: the version-pinned chart for `--to` on release
        /// builds; local `charts/curie` on dev builds.
        #[arg(long)]
        chart: Option<String>,
        /// Skip the interactive confirmation prompt.
        #[arg(long)]
        yes: bool,
        /// Print the redacted upgrade plan and exit without mutating.
        #[arg(long)]
        dry_run: bool,
    },
    /// Carry bundle objects across a chart upgrade that renames the object
    /// store (issue #1324).
    ///
    /// Chart 0.6.0 renamed the in-cluster store from `minio` to `rustfs`. Helm
    /// does a full upgrade, so the old StatefulSet -- and the bundles in it --
    /// are deleted. Every sandbox downloads its bundle from that store at
    /// start, so an empty one stops the bot answering rather than merely
    /// breaking rollbacks.
    ///
    /// One command does the whole thing:
    ///
    ///   curie cluster migrate-store
    ///
    /// It stages every object into a pod Helm does not own, upgrades the
    /// release, loads them into the new store, and verifies per object -- a
    /// concurrent push can legitimately add one mid-migration, and only a
    /// per-object diff tells that from data loss.
    ///
    /// Running it as one operation is the safe default because the halfway
    /// state is an empty store, which stops the bot answering. It also means no
    /// `--allow-stateful-removal`: that override exists so a human confirms the
    /// data is safe, and here the command staged it itself moments earlier.
    ///
    /// `--phase export` / `--phase import` run a single half, for recovery when
    /// an upgrade already happened or a run was interrupted.
    MigrateStore {
        /// Run only one half. Omit for the whole migration -- export, upgrade,
        /// import and verify -- which is the safe default: the halfway state is
        /// an empty store, and that stops the bot answering. The split phases
        /// exist for recovery, when an upgrade already happened or a run was
        /// interrupted.
        #[arg(long, value_parser = ["export", "import"])]
        phase: Option<String>,
        /// Kubernetes namespace.
        #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        /// Helm release name.
        #[arg(long, default_value = "curie")]
        release: String,
        /// Chart reference, used by `export` to see which store the upgrade
        /// would render.
        #[arg(long)]
        chart: Option<String>,
        /// Bundle bucket name, matching the platform's BUNDLE_BUCKET.
        #[arg(long, default_value = "curie-bundles")]
        bucket: String,
        /// Keep the staging pod after a successful import, instead of deleting
        /// it. The staged copy is the only thing standing between a failed
        /// import and an empty store, so keep it until you have verified a turn.
        #[arg(long)]
        keep_staging: bool,
        /// Print the commands that would run and exit without executing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Report release health and access URLs (read-only: helm status + kubectl).
    Status {
        /// Kubernetes namespace.
        #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        /// Helm release name.
        #[arg(long, default_value = "curie")]
        release: String,
        /// Print the read-only commands that would run and exit.
        #[arg(long)]
        dry_run: bool,
    },
    /// Show the release's observability surfaces (Curie Console + Langfuse traces/cost + API base).
    Observability {
        /// Query platform observability data through the Curie API. Omit to
        /// preserve the existing URL/surface report.
        #[command(subcommand)]
        query: Option<ClusterObservabilityQuery>,
        /// Kubernetes namespace.
        #[arg(long, global = true, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        /// Helm release name.
        #[arg(long, global = true, default_value = "curie")]
        release: String,
        /// Print the read-only discovery commands that would run and exit.
        #[arg(long)]
        dry_run: bool,
        /// Also open the browsable surfaces in a browser. Off by default: the URLs
        /// are printed and nothing is opened unless --open is passed, and --json
        /// never opens a browser.
        #[arg(long)]
        open: bool,
    },
    /// Connect or disconnect the cluster release from a real Slack workspace.
    Comms {
        /// Chat surface to configure. Required until the CLI grows more than
        /// one comms target.
        #[arg(long)]
        slack: bool,
        /// Clear the Slack tokens from the release instead of setting them.
        #[arg(long)]
        disconnect: bool,
        /// Slack app token. Defaults from SLACK_APP_TOKEN.
        #[arg(
            long,
            env = "SLACK_APP_TOKEN",
            hide_env_values = true,
            default_value = ""
        )]
        app_token: String,
        /// Slack bot token. Defaults from SLACK_BOT_TOKEN.
        #[arg(
            long,
            env = "SLACK_BOT_TOKEN",
            hide_env_values = true,
            default_value = ""
        )]
        bot_token: String,
        /// Kubernetes namespace.
        #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        /// Helm release name.
        #[arg(long, default_value = "curie")]
        release: String,
        /// Helm chart. Default: the version-pinned chart release asset on release builds; local `charts/curie` on dev builds. Pass a path or ref to override.
        #[arg(long)]
        chart: Option<String>,
        /// Print the helm command that would run and exit without executing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Give the platform its own GitHub identity so agent repos need no deploy workflow (ADR-0092).
    GithubApp {
        /// The App's numeric id, from its GitHub settings page.
        #[arg(long, default_value = "")]
        app_id: String,
        /// Path to the App's PEM private key file. The path is passed to helm
        /// with --set-file, so the key's contents never enter argv.
        #[arg(long, default_value = "")]
        private_key: String,
        /// Name of a Secret you manage that holds the App's PEM. The chart only
        /// references it, so the key never enters helm release history. The
        /// recommended path; mutually exclusive with --private-key.
        #[arg(long, default_value = "")]
        existing_secret: String,
        /// Data key inside --existing-secret holding the PEM.
        #[arg(long, default_value = crate_github_app::DEFAULT_APP_KEY_DATA_KEY)]
        existing_secret_key: String,
        /// Where the platform clones from. Change only for GitHub Enterprise.
        #[arg(long, default_value = crate_github_app::DEFAULT_CLONE_BASE)]
        clone_base: String,
        /// Clear the App credentials, falling back to api.githubToken.
        #[arg(long)]
        disconnect: bool,
        /// Kubernetes namespace.
        #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        /// Helm release name.
        #[arg(long, default_value = "curie")]
        release: String,
        /// Helm chart. Default: the version-pinned chart release asset on release builds; local `charts/curie` on dev builds. Pass a path or ref to override.
        #[arg(long)]
        chart: Option<String>,
        /// Print the helm command that would run and exit without executing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Drive the deployed Kubernetes release end to end with zero Slack contact.
    Message {
        /// The user message text.
        text: String,
        /// Slack channel id to send as; must match one of the target agent's
        /// channels. Omit when exactly one channel is bound across all
        /// deployed agents (errors on zero or several).
        #[arg(long)]
        channel: Option<String>,
        /// Existing thread ts to continue a conversation; omit to start a new
        /// thread. Pair with --channel to keep multi-turn context.
        #[arg(long)]
        thread: Option<String>,
        /// Reuse the last turn's context (channel, thread, transport) recorded
        /// in .curie/last-turn.json in the working directory; type only the
        /// new message text.
        #[arg(long = "continue")]
        r#continue: bool,
        /// Kubernetes namespace of the release. Default: curie.
        #[arg(long, env = "CURIE_NAMESPACE")]
        namespace: Option<String>,
        /// Helm release name. Default: curie.
        #[arg(long)]
        release: Option<String>,
        /// Helm chart. Default: the version-pinned chart release asset on release builds; local `charts/curie` on dev builds. Pass a path or ref to override.
        #[arg(long)]
        chart: Option<String>,
        /// Host the in-cluster worker uses to reach the stub. Omit to auto-detect
        /// the local IP the kernel would use to reach the cluster.
        #[arg(long)]
        listen_host: Option<String>,
        /// Port the stub binds (0.0.0.0); the worker posts here.
        #[arg(long, default_value_t = 0)]
        listen_port: u16,
        /// Local port the Valkey port-forward binds.
        #[arg(long, default_value_t = 0)]
        valkey_local_port: u16,
        /// Valkey password. Omit to read the release's own password from its
        /// chart Secret. Prefer the CURIE_VALKEY_PASSWORD env var over passing
        /// a real secret on the command line, where it leaks via `ps` and shell
        /// history.
        #[arg(
            long,
            env = "CURIE_VALKEY_PASSWORD",
            hide_env_values = true,
            value_parser = message::cluster_valkey_password
        )]
        valkey_password: Option<String>,
        /// Local port the API port-forward binds (default-channel lookup).
        #[arg(long, default_value_t = 0)]
        api_local_port: u16,
        /// Platform API key for the default-channel lookup. Omit to read the
        /// release's own key from its chart Secret.
        #[arg(long, env = "CURIE_API_KEY", value_parser = message::cluster_api_key)]
        api_key: Option<String>,
        /// Synthetic Slack user id for the enqueued event.
        #[arg(long, default_value = message::DEFAULT_USER)]
        user: String,
        /// Stream the dispatcher enqueues onto.
        #[arg(long, env = "CURIE_STREAM", default_value = message::DEFAULT_STREAM)]
        stream: String,
        /// How long to wait for the worker's reply before printing diagnostics.
        /// Defaults high because the worker kernel can retry a run up to 3 times
        /// with a 90s sandbox-claim timeout each (worst case near 270s of claim
        /// waits alone), so a shorter ceiling can time out while it is still working.
        /// Default: 300 seconds.
        #[arg(long)]
        timeout_secs: Option<u64>,
        /// Print the kubectl commands, stub URL, and enqueue description that a
        /// real run would produce, and exit without executing anything.
        #[arg(long)]
        dry_run: bool,
    },
    /// Run the bundle's `evals/cases.json` through the deployed Kubernetes
    /// release and grade with the same grader `skill eval` uses (the per-tier
    /// parity gate).
    Eval {
        /// Eval case file (default: `evals/cases.json` here, then the recorded
        /// bundle's).
        #[arg(long)]
        cases: Option<PathBuf>,
        /// Run only the case(s) with these ids; repeat to select several.
        /// Omit to run the whole suite. A value that matches no case in the
        /// suite exits 2 (usage), so a mistyped selector fails the gate instead
        /// of greening an empty run.
        #[arg(long = "case-id", value_name = "ID")]
        case_id: Vec<String>,
        /// Slack channel id to send as; must match one of the target agent's
        /// channels. Omit when exactly one channel is bound across all
        /// deployed agents.
        #[arg(long)]
        channel: Option<String>,
        /// Kubernetes namespace of the release. Default: curie.
        #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        /// Helm release name. Default: curie.
        #[arg(long, default_value = "curie")]
        release: String,
        /// Host the in-cluster worker uses to reach the stub. Omit to auto-detect
        /// the local IP the kernel would use to reach the cluster.
        #[arg(long)]
        listen_host: Option<String>,
        /// Port the stub binds (0.0.0.0); the worker posts here.
        /// Default 0 lets the kernel assign an ephemeral port.
        #[arg(long, default_value_t = 0)]
        listen_port: u16,
        /// Local port the Valkey port-forward binds.
        /// Default 0 lets kubectl assign an ephemeral local port.
        #[arg(long, default_value_t = 0)]
        valkey_local_port: u16,
        /// Valkey password. Omit to read the release's own password from its
        /// chart Secret. Prefer the CURIE_VALKEY_PASSWORD env var over passing
        /// a real secret on the command line, where it leaks via `ps` and shell
        /// history.
        #[arg(
            long,
            env = "CURIE_VALKEY_PASSWORD",
            hide_env_values = true,
            value_parser = message::cluster_valkey_password
        )]
        valkey_password: Option<String>,
        /// Local port the API port-forward binds (default-channel lookup).
        /// Default 0 is kernel-assigned, matching `cluster message`.
        #[arg(long, default_value_t = 0)]
        api_local_port: u16,
        /// Platform API key for the default-channel lookup. Omit to read the
        /// release's own key from its chart Secret.
        #[arg(long, env = "CURIE_API_KEY", value_parser = message::cluster_api_key)]
        api_key: Option<String>,
        /// Synthetic Slack user id for the enqueued events.
        #[arg(long, default_value = message::DEFAULT_USER)]
        user: String,
        /// Stream the dispatcher enqueues onto.
        #[arg(long, env = "CURIE_STREAM", default_value = message::DEFAULT_STREAM)]
        stream: String,
        /// How long to wait for each case's reply. Default: 300 seconds.
        #[arg(long, default_value_t = message::DEFAULT_TIMEOUT_SECS)]
        timeout_secs: u64,
        /// Evaluate under this model instead of the deployed one; repeat to sweep
        /// several models in one run (#526). A sweep triggers a platform eval per
        /// model and reports the per-model pass-rate from `GET /evals/matrix`.
        #[arg(long = "model")]
        model: Vec<String>,
        /// Number of eval cases to run concurrently. Sequential (1) is the only
        /// supported value today; parallel dispatch is tracked in #709, so any
        /// value above 1 is refused rather than silently run sequentially.
        #[arg(long, default_value_t = 1)]
        concurrency: usize,
        #[command(flatten)]
        sampling: EvalSamplingArgs,
        /// Print the kubectl commands, stub URL, and enqueue description that a
        /// real run would produce, and exit without executing anything.
        #[arg(long)]
        dry_run: bool,
    },
    /// Push the bundle to the platform API and deploy it.
    Deploy {
        /// Resolve the agent, environment, and channel from a target declared
        /// in the bundle's `deploy.yaml` (ADR-0089).
        ///
        /// Routing lives in the repository and is a reviewable diff, instead of
        /// flags scattered across whatever invoked this command. Explicit
        /// --agent/--env/--slack-channel still win, so a one-off deploy needs
        /// no committed file.
        #[arg(long, conflicts_with = "agent")]
        target: Option<String>,
        /// Deploy EVERY target `deploy.yaml` declares, dev before prod.
        ///
        /// Onboarding a repository otherwise means one invocation per target,
        /// and forgetting one leaves an agent that exists and never updates.
        /// Ordered dev-first so a run that fails part-way leaves prod on its
        /// previous version rather than ahead of a dev that never landed.
        #[arg(long, conflicts_with_all = ["target", "agent", "env", "slack_channel"])]
        all_targets: bool,
        /// Deploy under this agent name instead of the manifest's `name`.
        ///
        /// The bundle is unchanged -- only which agent it binds to. This is how
        /// one repository serves a dev agent and a prod agent from the same
        /// artifact, so prod promotes exactly what dev validated (#1166).
        #[arg(long)]
        agent: Option<String>,
        /// Plugin bundle directory.
        #[arg(long, default_value = ".")]
        plugin_dir: PathBuf,
        /// Platform API base URL. Omit to self-plumb a kubectl port-forward to
        /// the release's api service (a loopback tunnel); CURIE_API_URL or an
        /// explicit value direct-dials the given URL with no tunnel.
        #[arg(long, env = "CURIE_API_URL")]
        api_url: Option<String>,
        /// Kubernetes namespace of the release (for the port-forward + key discovery). Default: curie.
        #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        /// Helm release name (for the port-forward + key discovery). Default: curie.
        #[arg(long, default_value = "curie")]
        release: String,
        /// Helm chart used to write per-agent connector Secrets. Default: the
        /// version-pinned chart release asset on release builds; local
        /// `charts/curie` on dev builds.
        #[arg(long)]
        chart: Option<String>,
        /// Platform API key. Omit to auto-discover the release Secret key
        /// (`<release>-secrets`); the discovered key travels only in the
        /// X-API-Key header over the loopback tunnel, never over the cleartext
        /// NodePort proxy (ADR-0057). An explicit value wins.
        #[arg(long, env = "CURIE_API_KEY", hide_env_values = true)]
        api_key: Option<String>,
        /// Slack channel to bind the agent to. On first create it defaults to
        /// C0LOCALDEV; on redeploy the channel is ADDED when the agent is not
        /// already bound to it, never moved and never removed, so omitting the
        /// flag leaves the deployed agent's binding set untouched.
        #[arg(long)]
        slack_channel: Option<String>,
        /// Bind this agent to a GitHub repository (`owner/name`) so pushes to
        /// its dev/prod branches deploy it (ADR-0014).
        ///
        /// A new agent is created bound to it, and an existing agent with no
        /// binding is bound now (#1194 made `repo_full_name` PATCHable and
        /// ADR-0091 dropped the uniqueness, so one repository can build several
        /// agents). An agent already bound to a DIFFERENT repository is NOT
        /// moved, because that would reroute which repository's pushes deploy
        /// it; a warning names the binding it kept.
        #[arg(long = "repo", value_name = "OWNER/NAME")]
        repo: Option<String>,
        /// Deprecated compatibility no-op: coding tools are built in, and an
        /// allowed root GitHub URL in the opening message drives managed
        /// /workspace acquisition.
        #[arg(long, conflicts_with = "no_workspace")]
        workspace: bool,
        /// Deprecated compatibility no-op: coding tools are built in, and an
        /// allowed root GitHub URL in the opening message drives managed
        /// /workspace acquisition.
        #[arg(long, conflicts_with = "workspace")]
        no_workspace: bool,
        /// Target environment. Defaults to dev; a `--target` supplies it
        /// instead, and an explicit value here still wins over the target.
        #[arg(long, value_enum)]
        env: Option<DeployEnv>,
        /// Version label; defaults to <manifest version>-<unix time>.
        #[arg(long)]
        label: Option<String>,
        /// Per-agent connector secrets: values are written to the agent's Helm
        /// Secret through a private values file (never argv). The SandboxClaim
        /// stays names-only. See ADR-0009 / #1488.
        #[arg(long = "secret")]
        secret: Vec<String>,
        /// Local port the self-plumbed API port-forward binds.
        /// Default 0 lets the kernel assign an ephemeral port, matching
        /// `cluster message` and `cluster eval` (#1652 / #1740), so concurrent
        /// deploys cannot collide and a squatted port cannot be inherited.
        #[arg(long, default_value_t = 0)]
        api_local_port: u16,
    },
    // Agent-lifecycle verbs (kill/resume/budget/delete) speak the platform API
    // like `deploy` does. Design decision (#149): extend the existing `cluster`
    // target rather than introduce a new top-level `agent` noun -- these act on a
    // deployed release's agents, so they belong beside `cluster deploy`/`message`
    // and reuse its `--api-url`/`--api-key` surface and agent resolution.
    /// Kill an agent (stop its runs) via the platform API (`POST /agents/{id}/kill`).
    Kill {
        /// Agent name or id to kill.
        agent: String,
        #[command(flatten)]
        conn: ClusterConn,
        /// Confirm this destructive action (required; it stops the agent's runs).
        #[arg(long)]
        yes: bool,
        /// Print what would be done and exit without making a request.
        #[arg(long)]
        dry_run: bool,
    },
    /// Resume a killed agent via the platform API (`POST /agents/{id}/resume`).
    Resume {
        /// Agent name or id to resume.
        agent: String,
        #[command(flatten)]
        conn: ClusterConn,
        /// Print what would be done and exit without making a request.
        #[arg(long)]
        dry_run: bool,
    },
    /// Read or change an agent's model and thinking overrides (`PATCH /agents/{id}`).
    ///
    /// With no flags this inspects. Both fields are nullable operator
    /// overrides of a platform default, so clearing is `--clear-<field>`
    /// (which sends JSON null) and never an empty value, which would skip the
    /// platform default rather than restore it.
    Overrides {
        /// Agent name or id.
        agent: String,
        /// Pin this model for the agent (forwarded as CURIE_MODEL at boot).
        #[arg(long)]
        model: Option<String>,
        /// Clear the model override back to the platform default.
        #[arg(long)]
        clear_model: bool,
        /// Pin this thinking depth (e.g. `disabled`, `adaptive`, `enabled:2000`).
        #[arg(long)]
        thinking: Option<String>,
        /// Clear the thinking override back to the platform default.
        #[arg(long)]
        clear_thinking: bool,
        #[command(flatten)]
        conn: ClusterConn,
        /// Print what would be done and exit without making a request.
        #[arg(long)]
        dry_run: bool,
    },
    /// List, add, or remove an agent's surfaces
    /// (`/agents/{id}/channels`).
    ///
    /// With no flags this lists. An agent holds one or more bindings
    /// (ADR-0118), so exactly one `--add` OR one `--remove` is applied per
    /// invocation: the API has no batch endpoint, and a half-applied batch
    /// would leave the operator guessing what took.
    Surfaces {
        /// Agent name or id.
        agent: String,
        /// Add this surface, as KIND=ADDRESS (e.g. slack=C0EXAMPLE1).
        #[arg(long, value_name = "KIND=ADDRESS")]
        add: Option<String>,
        /// Reply HTTP endpoint for a non-Slack adapter. Requires --adapter.
        #[arg(long, requires_all = ["add", "adapter"])]
        endpoint: Option<String>,
        /// Worker credential selector for the reply adapter. Requires --endpoint.
        #[arg(long, requires_all = ["add", "endpoint"])]
        adapter: Option<String>,
        /// Remove this surface, as KIND=ADDRESS. The API refuses to remove an
        /// agent's final surface.
        #[arg(long, value_name = "KIND=ADDRESS", conflicts_with = "add")]
        remove: Option<String>,
        #[command(flatten)]
        conn: ClusterConn,
        /// Print what would be done and exit without making a request.
        #[arg(long)]
        dry_run: bool,
    },
    /// Set an agent's budget via the platform API (`PUT /agents/{id}/budget`).
    Budget {
        /// Agent name or id.
        agent: String,
        /// Daily spend cap in USD (BudgetConfig.max_usd_per_day). Must be > 0.
        #[arg(long)]
        limit: f64,
        #[command(flatten)]
        conn: ClusterConn,
        /// Print what would be done and exit without making a request.
        #[arg(long)]
        dry_run: bool,
    },
    /// Force a stuck thread's sandbox to be released via the platform API
    /// (`POST /agents/{id}/threads/{thread_key}/reset`, #737). The worker's
    /// next maintenance tick deletes the thread's claim and route, so its next
    /// message cold-creates a fresh sandbox instead of adopting one that may be
    /// running stale env. Interrupts a live turn on the thread first, so it
    /// requires --yes; does not delete conversation history.
    ResetThread {
        /// Agent name or id (scopes the action; the release is thread-keyed).
        agent: String,
        /// The worker's composed key: kind:channel:thread-ts (e.g.
        /// slack:C0EXAMPLE1:1700000000.000100).
        #[arg(long, value_name = "THREAD_KEY")]
        thread_key: String,
        #[command(flatten)]
        conn: ClusterConn,
        /// Confirm the action; it interrupts any live turn on the thread.
        #[arg(long)]
        yes: bool,
        /// Print what would be done and exit without making a request.
        #[arg(long)]
        dry_run: bool,
    },
    /// Sign in to the cluster's web console.
    ///
    /// The key is read from the release Secret and flows straight into the
    /// request header, never through your shell or your screen; what you copy
    /// is the code.
    Console {
        #[command(subcommand)]
        action: ClusterConsoleAction,
    },
    /// Delete an agent via the platform API (`DELETE /agents/{id}`).
    Delete {
        /// Agent name or id to delete.
        agent: String,
        #[command(flatten)]
        conn: ClusterConn,
        /// Confirm this destructive action (required; it permanently deletes the agent).
        #[arg(long)]
        yes: bool,
        /// Print what would be done and exit without making a request.
        #[arg(long)]
        dry_run: bool,
    },
    /// List an agent's immutable versions (`GET /agents/{id}/versions`).
    Versions {
        #[command(flatten)]
        target: ClusterAgentTarget,
    },
    /// Show what an agent has learned (its memory log; `GET /agents/{id}/memory`).
    /// `--add <content>` seeds an operator-authored record; a fresh session is
    /// required before it is injected at boot.
    Memory {
        #[command(flatten)]
        target: ClusterAgentTarget,
        /// Append this content as an operator-authored memory record.
        #[arg(long, value_name = "CONTENT")]
        add: Option<String>,
    },
    /// The human-in-the-loop plane: list and resolve pending approval records,
    /// and view or set the tools whose calls require approval. Which channel an
    /// approval posts to, and who may resolve it, come from the agent's approval
    /// route bindings; `curie guide` explains the whole plane.
    Approvals {
        #[command(flatten)]
        target: ClusterAgentTarget,
        /// Tool name to gate behind approval (repeatable). Omit to show current gates.
        #[arg(long = "gate", value_name = "TOOL")]
        gate: Vec<String>,
        /// Clear all approval gates on the agent.
        #[arg(long)]
        clear: bool,
        /// List the agent's pending approval records instead of the gate config.
        #[arg(long)]
        list: bool,
        /// Resolve the approval with this id (approve by default). Authentication
        /// comes from CURIE_APPROVAL_PRINCIPAL_TOKEN.
        #[arg(long, value_name = "APPROVAL_ID")]
        resolve: Option<String>,
        /// Reject instead of approve (with --resolve).
        #[arg(long)]
        reject: bool,
        /// Optional note recorded with the resolution (with --resolve).
        #[arg(long)]
        note: Option<String>,
        /// Administratively mint a reusable, subject-bound operator principal.
        /// The token is delivered once; export it as
        /// CURIE_APPROVAL_PRINCIPAL_TOKEN before resolving.
        #[arg(long, value_name = "SUBJECT")]
        mint_operator_principal: Option<String>,
        /// Administratively mint a single-use, subject-bound Console login code.
        #[arg(long, value_name = "SUBJECT")]
        mint_console_login_code: Option<String>,
        /// Bind a manifest route's verified Slack resolution card, as
        /// NAME=CHANNEL (e.g. deal_desk=C0123ABCD). Repeatable. A write REPLACES
        /// the whole route map, like --gate does for tool gates.
        #[arg(long = "route-resolution", value_name = "NAME=CHANNEL")]
        route_resolution: Vec<String>,
        /// Narrow WHO may resolve a route, independently of its resolution target,
        /// as NAME=users:U1,U2 or NAME=group:S1. Repeatable. Omit to leave the
        /// resolution card's channel members as the approvers.
        #[arg(long = "route-approvers", value_name = "NAME=KIND:VALUES")]
        route_approvers: Vec<String>,
        /// Read the whole route map from a JSON file, e.g.
        /// {"deal_desk":{"resolution":{"kind":"slack","address":"C0123ABCD"}}}.
        /// Notifications, including endpoint+adapter transport, are declared in
        /// this strict map. The repeatable override flags apply on top of it.
        #[arg(long = "routes-from", value_name = "FILE")]
        routes_from: Option<PathBuf>,
        /// Show the agent's approval route bindings instead of its tool gates.
        #[arg(long)]
        list_routes: bool,
        /// Remove every approval route binding on the agent.
        #[arg(long)]
        clear_routes: bool,
    },
}

/// Resolve, and materialize, the compose file for a local verb.
///
/// `local up` does not call this: it inlines the same two steps so the
/// `--build` channel guard can run between them (#1926).
async fn resolve_compose_file(file: Option<String>, dry_run: bool) -> Result<String> {
    let resolved = artifacts::resolve_compose(
        file.as_deref(),
        artifacts::Channel::current(),
        artifacts::version(),
        artifacts::cache_root,
        std::path::Path::new(local::DEFAULT_COMPOSE_FILE).exists(),
    )?;
    materialize_artifact(resolved, dry_run, "compose").await
}

async fn bind_cluster_connector_secrets(
    namespace: &str,
    release: &str,
    chart: Option<&str>,
    agent_name: &str,
    secret_names: &[String],
) -> Result<()> {
    let secrets = curie::cluster_secrets::resolve_named_secrets(secret_names)?;
    if secrets.is_empty() {
        return Ok(());
    }
    let resolved = artifacts::resolve_chart(
        chart,
        artifacts::Channel::current(),
        artifacts::version(),
        artifacts::cache_root,
        std::path::Path::new("charts/curie").is_dir(),
    )?;
    let chart = materialize_artifact(resolved, false, "chart").await?;
    curie::cluster_secrets::bind(curie::cluster_secrets::BindOpts {
        common: CommonOpts {
            namespace: namespace.to_string(),
            release: release.to_string(),
            dry_run: false,
        },
        chart,
        agent: agent_name.to_string(),
        secrets,
    })
    .await
}

async fn materialize_artifact(
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

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Some(hint) = curie::retired_hint(&args) {
        eprintln!("{hint}");
        std::process::exit(curie::exit::ExitClass::Usage.code());
    }

    let cli = Cli::parse();
    ui::init(Ui::from_process(cli.color, cli.debug, cli.quiet, cli.json));
    // main never returns Err (which would give anyhow's default exit 1 and skip
    // classification). Run the command, then map any error to a semantic exit
    // code: the JSON payload goes to stdout under --json. Otherwise the human
    // presentation goes to stderr, debug receives the full cause chain, and the
    // class picks the exit code.
    if let Err(err) = run(cli.command).await {
        let (class, _fix) = curie::exit::classify(&err);
        if ui::ui().json() {
            let payload = curie::exit::wrapped_json_payload(&err)
                .unwrap_or_else(|| curie::exit::error_json(&err));
            ui::ui().emit_json(&payload);
        } else {
            let (message, remedy) = curie::exit::present_error(&err);
            eprintln!("Error: {message}");
            if let Some(remedy) = remedy {
                eprintln!("Fix: {remedy}");
            }
            ui::ui().plumbing(&format!("{err:#}"));
        }
        std::process::exit(class.code());
    }
}

/// Route a handler's structured output through the one success-path emit
/// (`Ui::emit`), mirroring the centralized error emit in `main`. The read verbs
/// return a `CliOutput` instead of touching stdout themselves, so the
/// json-vs-human decision is made in exactly one place (issue #456).
fn emit<T: curie::ui::CliOutput>(out: T) -> Result<()> {
    ui::ui().emit(&out);
    Ok(())
}

/// Trait-object counterpart used by the shared observability query handler,
/// whose three leaves intentionally return different concrete output types.
fn emit_boxed(out: Box<dyn curie::ui::CliOutput>) -> Result<()> {
    ui::ui().emit(out.as_ref());
    Ok(())
}

/// Dispatch one parsed command. No subcommand opens the interactive terminal,
/// matching `curie interactive` / `curie ui`. Returns the command's
/// `Result`; `main`
/// classifies any error into a semantic exit code (see `curie::exit`).
async fn run(command: Option<Command>) -> Result<()> {
    match command {
        None => curie::interactive::run().await,
        Some(Command::Try { keep }) => {
            let image =
                artifacts::resolve_image(None, artifacts::Channel::current(), artifacts::version());
            commands::try_first_run(keep, image).await
        }
        Some(Command::Init {
            name,
            dir,
            from_spec,
            adopt,
        }) => commands::init(name, dir, from_spec, adopt),
        Some(Command::Example {
            action:
                ExampleAction::SreBot {
                    action:
                        SreBotAction::Install {
                            observability,
                            dry_run,
                            slack_channel,
                            platform_upgrade,
                            namespace,
                            release,
                            observability_namespace,
                            workspace_repo,
                        },
                },
        }) => match curie::examples::install_sre_bot(curie::examples::SreBotInstallOpts {
            observability,
            dry_run,
            slack_channel,
            platform_upgrade,
            namespace,
            release,
            observability_namespace,
            workspace_repo,
        })
        .await?
        {
            curie::examples::SreBotInstallResult::DryRun(plan) => emit(plan),
            curie::examples::SreBotInstallResult::Installed(deployed) => emit(*deployed),
        },
        Some(Command::Build {
            tag,
            plugin_dir,
            registry,
            force,
        }) => match plugin_dir {
            Some(plugin_dir) => emit(
                commands::build_connectors(commands::ConnectorBuildOpts {
                    plugin_dir,
                    registry,
                    force,
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
        Some(Command::Dev { action }) => match action {
            DevAction::Contracts => commands::dev_script("scripts/check-contracts.sh", &[]).await,
            DevAction::ChartCheck => commands::dev_chart_check().await,
            DevAction::VerifyFixPin { change, selector } => {
                commands::dev_script(
                    "cli/scripts/verify-fix-pin.sh",
                    &[change.as_str(), selector.as_str()],
                )
                .await
            }
            DevAction::E2e => commands::dev_script("cli/scripts/e2e.sh", &[]).await,
            DevAction::E2eLadder => commands::dev_script("cli/scripts/e2e-ladder.sh", &[]).await,
            DevAction::SreDemoE2e => commands::dev_script("cli/scripts/sre-demo-e2e.sh", &[]).await,
            DevAction::E2eCiSelection {
                path,
                base,
                head,
                push,
            } => {
                commands::dev_e2e_ci_selection(&path, base.as_deref(), head.as_deref(), push).await
            }
            DevAction::ChartRuntimeE2e { force } => {
                let args: &[&str] = if force { &["--force"] } else { &[] };
                commands::dev_script("scripts/chart-runtime-e2e.sh", args).await
            }
            DevAction::DocsLint => commands::dev_script("scripts/check-docs.sh", &[]).await,
            DevAction::PluginCompat => {
                commands::dev_script("scripts/check-plugin-compat.sh", &[]).await
            }
            DevAction::AgentSkills => {
                commands::dev_script("scripts/check-agent-skills.sh", &[]).await
            }
            DevAction::EvalFalsifiability => {
                commands::dev_script("cli/scripts/eval-falsifiability.sh", &[]).await
            }
            DevAction::FieldParity => {
                commands::dev_script("cli/scripts/check-field-parity.sh", &[]).await
            }
            DevAction::EmitParity => {
                commands::dev_script("cli/scripts/check-emit-parity.sh", &[]).await
            }
            DevAction::VerbParity => {
                commands::dev_script("cli/scripts/check-verb-parity.sh", &[]).await
            }
            DevAction::SchemaBaseline => {
                commands::dev_script("cli/scripts/refresh-schema-baseline.sh", &[]).await
            }
            DevAction::NetpolCheck => {
                commands::dev_script("scripts/check-netpol-enforcement.sh", &[]).await
            }
            DevAction::VersionCheck => {
                commands::dev_script("scripts/check-version-consistency.sh", &[]).await
            }
            DevAction::WireTolerance => {
                commands::dev_script("scripts/check-wire-tolerance.sh", &[]).await
            }
            DevAction::BumpVersion { version, dry_run } => {
                commands::bump_version(&version, dry_run).await
            }
        },
        Some(Command::Skill { action }) => match action {
            SkillAction::Up {
                plugin_dir,
                image,
                port,
                name,
                fake_model,
                network,
                otel_endpoint,
                budget,
                model,
                local_model,
                pull_model,
                secret,
                env_file,
                replace,
            } => {
                let image = artifacts::resolve_image(
                    image.as_deref(),
                    artifacts::Channel::current(),
                    artifacts::version(),
                );
                commands::start(StartOpts {
                    plugin_dir,
                    image,
                    port,
                    name,
                    fake_model,
                    network,
                    otel_endpoint,
                    budget,
                    model,
                    local_model,
                    pull_model,
                    secret,
                    env_file,
                    replace,
                })
                .await
            }
            SkillAction::Check {
                plugin_dir,
                image,
                timeout,
            } => {
                let image = artifacts::resolve_image(
                    image.as_deref(),
                    artifacts::Channel::current(),
                    artifacts::version(),
                );
                commands::check(plugin_dir, image, timeout).await
            }
            SkillAction::Approvals {
                plugin_dir,
                gate,
                clear,
                list,
                resolve,
                route_resolution,
                route_approvers,
                routes_from,
                list_routes,
                clear_routes,
                ..
            } => {
                // Answered, not absent (ADR-0041, ADR-0077): the durable
                // list/resolve the local+cluster tiers have does not exist here,
                // so the verb reports why and exits 4 rather than erroring like a
                // typo. Gate config (view/set/clear) is unchanged.
                //
                // The route flags decline with their OWN reason (#1052): a pending
                // record is absent because this tier keeps no durable store, but a
                // route binding is absent because it is per-agent platform config
                // and this tier has no agent. Same wrong answer, different fix.
                let routes_asked = !route_resolution.is_empty()
                    || !route_approvers.is_empty()
                    || routes_from.is_some()
                    || list_routes
                    || clear_routes;
                if routes_asked {
                    Err(commands::skill_approval_routes_unavailable())
                } else if list || resolve.is_some() {
                    Err(commands::skill_approvals_list_unavailable())
                } else {
                    emit(commands::skill_approvals(plugin_dir, gate, clear).await?)
                }
            }
            // Answered, not absent: the concept does not exist at this tier, so
            // the verb reports why and exits 4 (issue #459, ADR-0041).
            SkillAction::Versions => Err(commands::skill_versions_unavailable()),
            SkillAction::Memory => Err(commands::skill_memory_unavailable()),
            SkillAction::Observability { .. } => Err(commands::skill_observability_unavailable()),
            SkillAction::Down { name } => commands::stop(name, std::path::Path::new(".")).await,
            SkillAction::Status { url } => commands::status(url).await,
            SkillAction::Message {
                text,
                user,
                event_type,
                url,
                r#continue,
            } => {
                let classified_failure =
                    commands::send(&text, &user, event_type.into(), url, r#continue).await?;
                if classified_failure {
                    std::process::exit(1);
                }
                Ok(())
            }
            SkillAction::Eval {
                cases,
                case_id,
                url,
                model,
                secret,
                image,
                sampling,
            } => {
                let image = artifacts::resolve_image(
                    image.as_deref(),
                    artifacts::Channel::current(),
                    artifacts::version(),
                );
                commands::eval(
                    cases,
                    case_id,
                    url,
                    model,
                    secret,
                    image,
                    sampling.config()?,
                )
                .await
            }
            SkillAction::EvalInit { out, force } => {
                curie::eval_init::run(curie::eval_init::EvalInitOpts { out, force })
            }
        },
        Some(Command::Local { action }) => match action {
            LocalAction::Up {
                file,
                dry_run,
                minimal,
                model,
                local_model,
                pull_model,
                slack,
                env_file,
                build,
            } => {
                // The `--build` channel guard runs between the resolve and
                // the materialize (#1926), so a refused run never downloads the
                // release compose, never prints the compose-source note, and
                // never emits a dry-run plan. That ordering is why these three
                // steps are inlined here instead of going through
                // `resolve_compose_file`.
                let resolved = artifacts::resolve_compose(
                    file.as_deref(),
                    artifacts::Channel::current(),
                    artifacts::version(),
                    artifacts::cache_root,
                    std::path::Path::new(local::DEFAULT_COMPOSE_FILE).exists(),
                )?;
                let build = if build {
                    Some(local::ensure_build_reaches_the_stack(&resolved)?)
                } else {
                    None
                };
                let file = materialize_artifact(resolved, dry_run, "compose").await?;
                emit(
                    local::up(
                        LocalOpts {
                            file,
                            dry_run,
                            minimal,
                            local_model,
                            pull_model,
                            slack,
                            model_mode: local::model_mode_from_env(),
                            env_file,
                            build,
                            stack_image_env: Vec::new(),
                        },
                        model,
                    )
                    .await?,
                )
            }
            LocalAction::Rebuild {
                service,
                file,
                dry_run,
                minimal,
                model,
                local_model,
                slack,
                env_file,
            } => {
                let file = resolve_compose_file(file, dry_run).await?;
                emit(
                    local::rebuild(local::LocalRebuildOpts {
                        common: LocalOpts {
                            file,
                            dry_run,
                            minimal,
                            local_model,
                            pull_model: false,
                            slack,
                            model_mode: local::model_mode_from_env(),
                            env_file,
                            // `local rebuild` recreates ONE service against the
                            // stack already running; it never re-tags images.
                            // The tag it recreates ONTO still has to match that
                            // stack, which is `resolve_stack_image_env` below,
                            // not this flag (#1925).
                            build: None,
                            stack_image_env: Vec::new(),
                        },
                        service,
                        model,
                    })
                    .await?,
                )
            }
            LocalAction::Down {
                file,
                wipe,
                yes,
                dry_run,
            } => {
                let file = resolve_compose_file(file, dry_run).await?;
                emit(
                    local::down(LocalDownOpts {
                        common: LocalOpts {
                            file,
                            dry_run,
                            minimal: false,
                            local_model: None,
                            pull_model: false,
                            slack: false,
                            model_mode: local::ModelMode::DefaultFake,
                            env_file: None,
                            build: None,
                            stack_image_env: Vec::new(),
                        },
                        wipe,
                        yes,
                    })
                    .await?,
                )
            }
            LocalAction::Status { file, dry_run } => {
                let file = resolve_compose_file(file, dry_run).await?;
                emit(
                    local::status(LocalOpts {
                        file,
                        dry_run,
                        minimal: false,
                        local_model: None,
                        pull_model: false,
                        slack: false,
                        model_mode: local::ModelMode::DefaultFake,
                        env_file: None,
                        build: None,
                        stack_image_env: Vec::new(),
                    })
                    .await?,
                )
            }
            LocalAction::Comms {
                slack,
                disconnect,
                minimal,
                model,
                app_token,
                bot_token,
                file,
                dry_run,
            } => {
                comms::require_provider(slack)?;
                let resolved_file = resolve_compose_file(file, dry_run).await?;
                // #749: fall back to Slack tokens persisted via `curie secrets
                // set` when neither a flag nor an env var supplied one, so
                // `--slack` needs no per-session re-export. Precedence: flag/env
                // > saved vault.
                let app_token =
                    comms::resolve_local_slack_token("SLACK_APP_TOKEN", &app_token, disconnect)?;
                let bot_token =
                    comms::resolve_local_slack_token("SLACK_BOT_TOKEN", &bot_token, disconnect)?;
                let mut model_opts = LocalOpts {
                    file: resolved_file.clone(),
                    dry_run,
                    minimal,
                    local_model: None,
                    pull_model: false,
                    slack: true,
                    model_mode: local::model_mode_from_env(),
                    env_file: None,
                    build: None,
                    stack_image_env: Vec::new(),
                };
                let model_credentials =
                    local::apply_credential_plan(&mut model_opts, crate::ui::ui())?;
                // #1925: `comms connect` recreates the worker and dispatcher --
                // and, via `depends_on`, the api and migrate behind them. Derive
                // the running stack's tag here, alongside the credential plan
                // this same throwaway `LocalOpts` already exists to resolve.
                local::resolve_stack_image_env(&mut model_opts).await;
                emit(
                    comms::local_comms(LocalCommsOpts {
                        file: resolved_file,
                        dry_run,
                        app_token,
                        bot_token,
                        disconnect,
                        model_mode: model_opts.model_mode,
                        model_credentials,
                        model,
                        minimal,
                        stack_image_env: model_opts.stack_image_env,
                    })
                    .await?,
                )
            }
            LocalAction::Message {
                text,
                channel,
                thread,
                r#continue,
                valkey_password,
                api_url,
                api_key,
                user,
                stream,
                timeout_secs,
                dry_run,
            } => {
                let state = if r#continue {
                    match load_turn(&std::env::current_dir()?)? {
                        Some(state) => Some(state),
                        None => anyhow::bail!(
                            "no previous turn recorded in .curie/last-turn.json; run a message without --continue first"
                        ),
                    }
                } else {
                    None
                };
                let resolved = apply_continue(
                    TurnVerb::Local,
                    CliTurnArgs {
                        channel,
                        thread,
                        namespace: None,
                        release: None,
                        chart: None,
                        listen_host: None,
                        timeout_secs,
                        api_url,
                        api_key,
                    },
                    state,
                    // Empty is unset (#540), so the recorded-env bail below still
                    // fires when $CURIE_API_KEY is exported blank.
                    std::env::var("CURIE_API_KEY")
                        .ok()
                        .filter(|v| !v.is_empty()),
                )?;
                message::message(MessageOpts {
                    text,
                    channel: resolved.channel,
                    thread: resolved.thread,
                    namespace: "curie".into(),
                    release: "curie".into(),
                    chart: "charts/curie".into(),
                    listen_host: None,
                    listen_port: message::DEFAULT_LISTEN_PORT,
                    valkey_local_port: message::DEFAULT_VALKEY_LOCAL_PORT,
                    valkey_password,
                    api_local_port: message::DEFAULT_API_LOCAL_PORT,
                    api_key: resolved.api_key,
                    user,
                    stream,
                    timeout_secs: resolved.timeout_secs,
                    dry_run,
                    local: true,
                    api_url: resolved.api_url,
                })
                .await
            }
            LocalAction::Eval {
                cases,
                case_id,
                channel,
                valkey_password,
                api_url,
                api_key,
                user,
                stream,
                timeout_secs,
                model,
                concurrency,
                sampling,
                dry_run,
            } => {
                message::eval(message::EvalOpts {
                    cases,
                    case_ids: case_id,
                    channel,
                    namespace: "curie".into(),
                    release: "curie".into(),
                    listen_host: None,
                    listen_port: message::DEFAULT_LISTEN_PORT,
                    valkey_local_port: message::DEFAULT_VALKEY_LOCAL_PORT,
                    valkey_password,
                    api_local_port: message::DEFAULT_API_LOCAL_PORT,
                    api_key,
                    user,
                    stream,
                    timeout_secs,
                    dry_run,
                    local: true,
                    api_url,
                    models: model,
                    concurrency,
                    sampling: sampling.config()?,
                })
                .await
            }
            LocalAction::Deploy {
                plugin_dir,
                agent,
                target,
                api_url,
                api_key,
                slack_channel,
                repo,
                workspace,
                no_workspace,
                env,
                label,
                secret,
            } => {
                let local_api_url = api_url.clone();
                let result = commands::deploy(DeployOpts {
                    plugin_dir,
                    agent,
                    target,
                    api_url,
                    api_key,
                    slack_channel,
                    repo,
                    workspace: commands::WorkspaceIntent::from_flags(workspace, no_workspace),
                    tier: commands::DeployTier::Local,
                    env,
                    label,
                    secret,
                    // `local deploy` offers `--secret`, so enforce the
                    // declared-secrets policy gate (#464).
                    secret_binding_supported: true,
                    connect_hint: "the platform API is unreachable.".to_string(),
                })
                .await;
                emit(local::with_deploy_unreachable_hint(result, &local_api_url).await?)
            }
            LocalAction::Console { action } => match action {
                ConsoleAction::Login {
                    api_url,
                    api_key,
                    subject,
                    console_url,
                    dry_run,
                    ..
                } => emit(
                    commands::console_login(
                        api_url,
                        api_key,
                        subject,
                        // The console is served beside the API in the dev stack;
                        // an explicit --console-url wins for anything else.
                        console_url.unwrap_or_else(|| "http://localhost:28080".to_string()),
                        dry_run,
                    )
                    .await?,
                ),
            },
            LocalAction::Versions { target } => emit(commands::versions(target.into()).await?),
            LocalAction::Memory { target, add } => match add {
                None => emit(commands::memory(target.into()).await?),
                Some(content) => emit(commands::memory_add(target.into(), content).await?),
            },
            LocalAction::Approvals {
                target,
                gate,
                clear,
                list,
                resolve,
                reject,
                note,
                mint_operator_principal,
                mint_console_login_code,
                route_resolution,
                route_approvers,
                routes_from,
                list_routes,
                clear_routes,
            } => emit(
                commands::approvals(
                    target.into(),
                    gate,
                    clear,
                    commands::ApprovalCmd {
                        list,
                        resolve,
                        reject,
                        note,
                        mint_operator_principal,
                        mint_console_login_code,
                        route_resolution,
                        route_approvers,
                        routes_from,
                        list_routes,
                        clear_routes,
                    },
                )
                .await?,
            ),
            LocalAction::Observability { query, open } => match query {
                None => emit(commands::observability(open).await?),
                Some(_) if open => Err(curie::exit::usage(
                    "--open cannot be combined with an observability query",
                )),
                Some(query) => emit_boxed(run_local_observability_query(query).await?),
            },
            LocalAction::Overrides {
                agent,
                model,
                clear_model,
                thinking,
                clear_thinking,
                api_url,
                api_key,
                dry_run,
            } => emit(
                commands::overrides(
                    AgentActionOpts {
                        api_url,
                        api_key,
                        agent,
                        dry_run,
                    },
                    commands::OverrideChange::resolve("model", model, clear_model)?,
                    commands::OverrideChange::resolve("thinking", thinking, clear_thinking)?,
                )
                .await?,
            ),
            LocalAction::Surfaces {
                target,
                add,
                endpoint,
                adapter,
                remove,
            } => emit(
                commands::channel_bindings(
                    target.into(),
                    commands::ChannelChange::resolve(add, remove, endpoint, adapter)?,
                )
                .await?,
            ),
            LocalAction::Budget {
                agent,
                limit,
                api_url,
                api_key,
                dry_run,
            } => emit(
                commands::budget(
                    AgentActionOpts {
                        api_url,
                        api_key,
                        agent,
                        dry_run,
                    },
                    limit,
                )
                .await?,
            ),
            LocalAction::Kill {
                agent,
                api_url,
                api_key,
                yes,
                dry_run,
            } => emit(
                commands::kill(
                    AgentActionOpts {
                        api_url,
                        api_key,
                        agent,
                        dry_run,
                    },
                    yes,
                )
                .await?,
            ),
            LocalAction::Resume {
                agent,
                api_url,
                api_key,
                dry_run,
            } => emit(
                commands::resume(AgentActionOpts {
                    api_url,
                    api_key,
                    agent,
                    dry_run,
                })
                .await?,
            ),
            LocalAction::ResetThread {
                agent,
                thread_key,
                api_url,
                api_key,
                yes,
                dry_run,
            } => emit(
                commands::reset_thread(
                    AgentActionOpts {
                        api_url,
                        api_key,
                        agent,
                        dry_run,
                    },
                    thread_key,
                    yes,
                )
                .await?,
            ),
            LocalAction::Delete {
                agent,
                api_url,
                api_key,
                yes,
                dry_run,
            } => emit(
                commands::delete(
                    AgentActionOpts {
                        api_url,
                        api_key,
                        agent,
                        dry_run,
                    },
                    yes,
                )
                .await?,
            ),
        },
        Some(Command::Cluster { action }) => match action {
            ClusterAction::Up {
                namespace,
                release,
                chart,
                no_expose,
                fake_model,
                model,
                local_model,
                allow_egress_host,
                allow_web_egress,
                github_token,
                clear_github_token,
                set,
                dev,
                dry_run,
                forward_only,
            } => {
                let mut set = set;
                if forward_only {
                    set.push("api.migrate.forwardOnly=true".to_string());
                }
                let resolved = artifacts::resolve_chart(
                    chart.as_deref(),
                    artifacts::Channel::current(),
                    artifacts::version(),
                    artifacts::cache_root,
                    std::path::Path::new("charts/curie").is_dir(),
                )?;
                let chart = materialize_artifact(resolved, dry_run, "chart").await?;
                let credentials = if fake_model || local_model.is_some() {
                    None
                } else {
                    ops::resolve_up_credentials(fake_model, ops::model_credential_env()?)
                };
                emit(
                    ops::up(
                        UpOpts {
                            common: CommonOpts {
                                namespace,
                                release,
                                dry_run,
                            },
                            chart,
                            no_expose,
                            set,
                            set_string: vec![],
                            allow_egress_host,
                            // Populated by ops::up (resolve named providers to host
                            // routes on a live run); empty here so the pure builder and
                            // --dry-run start clean.
                            resolved_egress_cidrs: vec![],
                            allow_web_egress,
                            fake_model,
                            credentials,
                            local_model,
                            // Default `agentSandbox.runner.model` from the shell
                            // `CURIE_MODEL` (None when unset/empty) for cross-tier
                            // parity with `local up` (#361).
                            model: model.or_else(|| {
                                std::env::var("CURIE_MODEL").ok().filter(|s| !s.is_empty())
                            }),
                            // Populated by ops::up (generate on fresh install / reuse on
                            // upgrade); empty here so the pure builder starts clean.
                            secrets: vec![],
                            // Resolved by ops::up from the flags below plus the
                            // value the release already recorded; Untouched here so
                            // the pure builder starts clean.
                            github_token: ops::GithubTokenPlan::Untouched,
                            dev,
                        },
                        github_token,
                        clear_github_token,
                    )
                    .await?,
                )
            }
            ClusterAction::Down {
                namespace,
                release,
                yes,
                dry_run,
            } => emit(
                ops::down(DownOpts {
                    common: CommonOpts {
                        namespace,
                        release,
                        dry_run,
                    },
                    yes,
                })
                .await?,
            ),
            ClusterAction::Rollback {
                revision,
                allow_failed_revision,
                namespace,
                release,
                yes,
                dry_run,
            } => emit(
                ops::rollback(RollbackOpts {
                    common: CommonOpts {
                        namespace,
                        release,
                        dry_run,
                    },
                    revision,
                    allow_failed_revision,
                    yes,
                })
                .await?,
            ),
            ClusterAction::Upgrade {
                to,
                namespace,
                release,
                chart,
                yes,
                dry_run,
            } => emit(
                ops::upgrade(UpgradeOpts {
                    common: CommonOpts {
                        namespace,
                        release,
                        dry_run,
                    },
                    to,
                    chart,
                    yes,
                })
                .await?,
            ),
            ClusterAction::Status {
                namespace,
                release,
                dry_run,
            } => emit(
                ops::status(CommonOpts {
                    namespace,
                    release,
                    dry_run,
                })
                .await?,
            ),
            ClusterAction::Observability {
                query,
                namespace,
                release,
                dry_run,
                open,
            } => match query {
                None => emit(
                    ops::observability(
                        CommonOpts {
                            namespace,
                            release,
                            dry_run,
                        },
                        open,
                    )
                    .await?,
                ),
                Some(_) if open => Err(curie::exit::usage(
                    "--open cannot be combined with an observability query",
                )),
                Some(_) if dry_run => Err(curie::exit::usage(
                    "--dry-run applies to bare cluster observability discovery, not API queries",
                )),
                Some(query) => {
                    emit_boxed(run_cluster_observability_query(query, namespace, release).await?)
                }
            },
            ClusterAction::MigrateStore {
                phase,
                namespace,
                release,
                chart,
                bucket,
                keep_staging,
                dry_run,
            } => {
                use curie::migrate_store as ms;
                let common = curie::ops::CommonOpts {
                    namespace,
                    release,
                    dry_run,
                };
                let out = match phase.as_deref() {
                    Some("import") => ms::run_import(&common, &bucket, keep_staging).await?,
                    other => {
                        let resolved = artifacts::resolve_chart(
                            chart.as_deref(),
                            artifacts::Channel::current(),
                            artifacts::version(),
                            artifacts::cache_root,
                            std::path::Path::new("charts/curie").is_dir(),
                        )?;
                        let chart = materialize_artifact(resolved, dry_run, "chart").await?;
                        if other == Some("export") {
                            ms::run_export(&common, &chart, &bucket).await?
                        } else {
                            ms::run_auto(&common, &chart, &bucket).await?
                        }
                    }
                };
                emit(out)
            }
            ClusterAction::Comms {
                slack,
                disconnect,
                app_token,
                bot_token,
                namespace,
                release,
                chart,
                dry_run,
            } => {
                comms::require_provider(slack)?;
                let resolved = artifacts::resolve_chart(
                    chart.as_deref(),
                    artifacts::Channel::current(),
                    artifacts::version(),
                    artifacts::cache_root,
                    std::path::Path::new("charts/curie").is_dir(),
                )?;
                let chart = materialize_artifact(resolved, dry_run, "chart").await?;
                emit(
                    comms::comms(CommsOpts {
                        common: CommonOpts {
                            namespace,
                            release,
                            dry_run,
                        },
                        chart,
                        app_token,
                        bot_token,
                        disconnect,
                    })
                    .await?,
                )
            }
            ClusterAction::GithubApp {
                app_id,
                private_key,
                existing_secret,
                existing_secret_key,
                clone_base,
                disconnect,
                namespace,
                release,
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
                    crate_github_app::github_app(
                        crate_github_app::GithubAppOpts {
                            common: CommonOpts {
                                namespace,
                                release,
                                dry_run,
                            },
                            chart,
                            app_id,
                            private_key_path: private_key,
                            existing_secret,
                            existing_secret_key,
                            disconnect,
                        },
                        &clone_base,
                    )
                    .await?,
                )
            }
            ClusterAction::Message {
                text,
                channel,
                thread,
                r#continue,
                namespace,
                release,
                chart,
                listen_host,
                listen_port,
                valkey_local_port,
                valkey_password,
                api_local_port,
                api_key,
                user,
                stream,
                timeout_secs,
                dry_run,
            } => {
                let state = if r#continue {
                    match load_turn(&std::env::current_dir()?)? {
                        Some(state) => Some(state),
                        None => anyhow::bail!(
                            "no previous turn recorded in .curie/last-turn.json; run a message without --continue first"
                        ),
                    }
                } else {
                    None
                };
                let resolved = apply_continue(
                    TurnVerb::Cluster,
                    CliTurnArgs {
                        channel,
                        thread,
                        namespace,
                        release,
                        chart,
                        listen_host,
                        timeout_secs,
                        api_url: None,
                        // The cluster tier has no dev default to bind (#786), so
                        // hand `apply_continue` the sentinel it compares against
                        // when nothing was supplied; the real value is resolved
                        // below, once the release is known.
                        api_key: api_key
                            .clone()
                            .unwrap_or_else(|| message::DEFAULT_API_KEY.to_string()),
                    },
                    state,
                    // Empty is unset (#540), so the recorded-env bail below still
                    // fires when $CURIE_API_KEY is exported blank.
                    std::env::var("CURIE_API_KEY")
                        .ok()
                        .filter(|v| !v.is_empty()),
                )?;
                let resolved_chart = artifacts::resolve_chart(
                    resolved.chart.as_deref(),
                    artifacts::Channel::current(),
                    artifacts::version(),
                    artifacts::cache_root,
                    std::path::Path::new("charts/curie").is_dir(),
                )?;
                let chart = materialize_artifact(resolved_chart, dry_run, "chart").await?;
                // `cluster up` randomizes both credentials per release, so an
                // omitted flag reads the release's own Secret rather than the
                // dev sentinel that 401s / fails Valkey auth on a real install
                // (#786). Explicit flag or env still wins.
                let api_key = message::resolve_cluster_credential(
                    api_key,
                    dry_run,
                    message::DEFAULT_API_KEY,
                    || ops::discover_api_key(&resolved.namespace, &resolved.release),
                )
                .await?;
                let valkey_password = message::resolve_cluster_credential(
                    valkey_password,
                    dry_run,
                    message::DEFAULT_VALKEY_PASSWORD,
                    || ops::discover_valkey_password(&resolved.namespace, &resolved.release),
                )
                .await?;
                message::message(MessageOpts {
                    text,
                    channel: resolved.channel,
                    thread: resolved.thread,
                    namespace: resolved.namespace,
                    release: resolved.release,
                    chart,
                    listen_host: resolved.listen_host,
                    listen_port,
                    valkey_local_port,
                    valkey_password,
                    api_local_port,
                    api_key,
                    user,
                    stream,
                    timeout_secs: resolved.timeout_secs,
                    dry_run,
                    local: false,
                    api_url: None,
                })
                .await
            }
            ClusterAction::Eval {
                cases,
                case_id,
                channel,
                namespace,
                release,
                listen_host,
                listen_port,
                valkey_local_port,
                valkey_password,
                api_local_port,
                api_key,
                user,
                stream,
                timeout_secs,
                model,
                concurrency,
                sampling,
                dry_run,
            } => {
                message::validate_eval_models(&model)?;
                // `cluster up` randomizes both credentials per release, so an
                // omitted flag reads the release's own Secret rather than the
                // dev sentinel that 401s / fails Valkey auth on a real install
                // (#790, mirroring the #786 fix for `cluster message`). Explicit
                // flag or env still wins.
                let api_key = message::resolve_cluster_credential(
                    api_key,
                    dry_run,
                    message::DEFAULT_API_KEY,
                    || ops::discover_api_key(&namespace, &release),
                )
                .await?;
                let valkey_password = message::resolve_cluster_credential(
                    valkey_password,
                    dry_run,
                    message::DEFAULT_VALKEY_PASSWORD,
                    || ops::discover_valkey_password(&namespace, &release),
                )
                .await?;
                message::eval(message::EvalOpts {
                    cases,
                    case_ids: case_id,
                    channel,
                    namespace,
                    release,
                    listen_host,
                    listen_port,
                    valkey_local_port,
                    valkey_password,
                    api_local_port,
                    api_key,
                    user,
                    stream,
                    timeout_secs,
                    dry_run,
                    local: false,
                    api_url: None,
                    models: model,
                    concurrency,
                    sampling: sampling.config()?,
                })
                .await
            }
            ClusterAction::Deploy {
                plugin_dir,
                agent,
                target,
                all_targets,
                api_url,
                namespace,
                release,
                chart,
                api_key,
                slack_channel,
                repo,
                workspace,
                no_workspace,
                env,
                label,
                secret,
                api_local_port,
            } => {
                if workspace {
                    commands::warn_if_empty_github_repo_allowlist(&namespace, &release).await;
                }
                let api_key = commands::normalize_deploy_api_key(api_key);
                // ADR-0057 (supersedes ADR-0024's deploy transport): with no
                // explicit --api-key, discover the release's strong Secret key;
                // an explicit key wins.
                let key_auto_discovered = commands::deploy_needs_key_discovery(api_key.as_deref());
                let api_key = if key_auto_discovered {
                    ops::discover_api_key(&namespace, &release).await?
                } else {
                    api_key.expect("explicit key present when discovery not needed")
                };
                // An explicit --api-url / CURIE_API_URL direct-dials the given
                // URL. Otherwise self-plumb a kubectl port-forward to the
                // release's api service so the discovered strong key travels only
                // over the loopback tunnel, never over the cleartext UI /api
                // NodePort proxy. Hold the child until after deploy returns;
                // kill_on_drop tears it down on every exit path.
                // 0 (the clap default) requests a kernel-assigned port, so a
                // squatted 8123 and two concurrent deploys are both structurally
                // impossible; an explicit --api-local-port stays an exact
                // override and still gets #1739's occupied-port refusal (#1533
                // symptom 2, the verb #1740 did not reach).
                let local_port = api_local_port;
                let _deploy_pf;
                // `Some` iff this deploy self-plumbed, carrying the fullname the
                // tunnel actually forwards to. That one value decides the
                // transport, names the Service in the diagnostics below, and
                // tells the unreachable hint which recovery to describe: the
                // auto path really opened a tunnel; the explicit path
                // direct-dialed and did not.
                let tunnel = commands::deploy_api_tunnel(
                    api_url.as_deref(),
                    &namespace,
                    &release,
                    local_port,
                    message::API_REMOTE_PORT,
                )
                .await;
                let api_url = match &tunnel {
                    Some((fullname, pf_cmd)) => {
                        let (deploy_pf, effective_port) =
                            message::start_port_forward(pf_cmd, local_port, "deploy api").await?;
                        _deploy_pf = Some(deploy_pf);
                        // svc/<release>-api serves the platform API at ROOT, so the
                        // base URL has NO /api suffix (the /api in ADR-0024 was only
                        // because the request went through the UI pod).
                        let base_url = format!("http://127.0.0.1:{effective_port}");
                        // The tunnel is TCP-alive, but a squatted local port and a
                        // Service name that resolved to the wrong workload are both
                        // TCP-alive too -- `start_port_forward`'s bind and readiness
                        // checks cannot tell them from the real API. Reading the
                        // UNAUTHENTICATED /health is the only check that can, and it
                        // is the compensating control for name resolution falling
                        // back to the chart rule.
                        //
                        // Placed here, at tunnel setup, deliberately: `--all-targets`
                        // posts several bundles over this one tunnel dev-before-prod
                        // (#1279), so verifying once BEFORE the target loop is what
                        // keeps a refusal a clean refusal instead of a half-deployed
                        // repository.
                        //
                        // #705: the auto-discovered strong release key is already in
                        // hand and this endpoint is not yet proven to be Curie, so the
                        // probe carries no key and a 401/403 is never retried with one.
                        //
                        // #1908: this returns `Err` rather than exiting, so the scope
                        // unwinds, `_deploy_pf` drops, and `kill_on_drop` reaps the
                        // kubectl child instead of orphaning it with its port held.
                        if let Err(observed) = api::verify_is_curie_api(&base_url).await {
                            let api_svc = fullname.resource("api");
                            return Err(anyhow::Error::from(
                                curie::exit::CliError::failure(format!(
                                    "refusing to deploy: the endpoint behind the self-plumbed \
                                     tunnel is not the Curie API. {observed}. `cluster deploy` \
                                     forwarded svc/{api_svc} in namespace {namespace} to \
                                     127.0.0.1:{effective_port} and posted nothing. Re-run with \
                                     --api-local-port <port> to bind a different local port, or \
                                     pass --api-url <url> to dial the API directly without a \
                                     tunnel."
                                ))
                                .with_fix(
                                    "re-run with --api-local-port <port>, or pass --api-url <url> \
                                     to dial the platform API directly",
                                ),
                            ));
                        }
                        base_url
                    }
                    None => {
                        _deploy_pf = None;
                        let url = api_url.expect("explicit url when no port-forward");
                        // #705: the strong auto-discovered key must never egress
                        // cleartext. An explicit non-loopback `http://` --api-url
                        // would leak it on the wire, and no loopback tunnel is in
                        // play here to protect it. Refuse rather than warn, unless
                        // the operator opted in with an explicit --api-key.
                        if key_auto_discovered && api::is_insecure_endpoint(&url) {
                            bail!(
                                "refusing to send the auto-discovered release key over cleartext \
                                 HTTP to {url}: the strong key would leak on the wire. Pass \
                                 --api-key explicitly to acknowledge, use an https:// URL, or omit \
                                 --api-url to reach the release over the loopback port-forward."
                            );
                        }
                        url
                    }
                };
                let connect_hint = match tunnel.as_ref() {
                    // Self-plumbed: name the Service the tunnel ACTUALLY used,
                    // as resolved from the cluster, so the operator can look up
                    // the same object the CLI did.
                    Some((fullname, _)) => {
                        let api_svc = fullname.resource("api");
                        format!(
                            "the platform API at {api_url} is unreachable. `cluster deploy` self-plumbs a kubectl port-forward to svc/{api_svc}; confirm the release is healthy with `curie cluster status`, or pass --api-url to dial the API directly."
                        )
                    }
                    // Explicit --api-url: no Service was contacted and none was
                    // resolved, so this hint names the chart's no-override rule
                    // rather than triggering a discovery round-trip on a path
                    // that is pinned cluster-offline. Under an override install
                    // the printed name may differ from the rendered one; the
                    // recovery it suggests (omit --api-url) resolves it live.
                    None => {
                        let api_svc = ops::chart_fullname(&release).resource("api");
                        format!(
                            "the platform API at {api_url} (from --api-url/CURIE_API_URL) is unreachable. `cluster deploy` dialed it directly with no port-forward; confirm that URL is reachable and the release is healthy with `curie cluster status`, or omit --api-url to self-plumb a loopback port-forward to svc/{api_svc}."
                        )
                    }
                };
                // --all-targets onboards a repository in one invocation.
                // The list comes from the API, not a Rust YAML parse: ADR-0089
                // keeps exactly one parser for this file, and a second could
                // disagree with it about where a deploy lands.
                let targets: Vec<Option<String>> = if all_targets {
                    let path = plugin_dir.join("deploy.yaml");
                    let content = std::fs::read_to_string(&path).map_err(|err| {
                        curie::exit::usage(format!(
                            "--all-targets needs a deploy.yaml in the bundle, but {} could \
                             not be read: {err}",
                            path.display()
                        ))
                    })?;
                    let listed = api::ApiClient::new(&api_url, &api_key)?
                        .list_deploy_targets(&content)
                        .await?;
                    if listed.targets.is_empty() {
                        bail!(
                            "deploy.yaml declares no targets, so --all-targets has nothing to \
                             deploy. Declare at least one (ADR-0089), or drop the flag and pass \
                             --agent/--env/--slack-channel."
                        );
                    }
                    ui::ui().note(&format!(
                        "onboarding {} target(s): {}",
                        listed.targets.len(),
                        listed
                            .targets
                            .iter()
                            .map(|t| t.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                    listed.targets.into_iter().map(|t| Some(t.name)).collect()
                } else {
                    vec![target]
                };

                if all_targets {
                    let first_target = targets
                        .first()
                        .cloned()
                        .flatten()
                        .expect("all target entries always have a target name");
                    let connector_target =
                        match curie::connectors::bind_current_cluster(&namespace, &release).await {
                            Ok(target) => target,
                            Err(err) => {
                                let payload = commands::all_targets_deploy_failure_json(
                                    &first_target,
                                    &[],
                                    None,
                                    &err,
                                );
                                return Err(curie::exit::with_json_payload(err, payload));
                            }
                        };
                    let app_name =
                        match curie::connectors::discover_app_name(&connector_target).await {
                            Ok(app_name) => app_name,
                            Err(err) => {
                                let payload = commands::all_targets_deploy_failure_json(
                                    &first_target,
                                    &[],
                                    None,
                                    &err,
                                );
                                return Err(curie::exit::with_json_payload(err, payload));
                            }
                        };

                    // Resolve every target and every connector credential before
                    // activating the first deployment. Preparation may create
                    // agents and versions, but it does not POST a deployment.
                    let mut prepared_targets = Vec::new();
                    for target in targets {
                        let target = target.expect("all target entries always have a target name");
                        let prepared_deploy = match commands::prepare_deploy(DeployOpts {
                            plugin_dir: plugin_dir.clone(),
                            agent: agent.clone(),
                            target: Some(target.clone()),
                            api_url: api_url.clone(),
                            api_key: api_key.clone(),
                            slack_channel: slack_channel.clone(),
                            repo: repo.clone(),
                            workspace: commands::WorkspaceIntent::from_flags(
                                workspace,
                                no_workspace,
                            ),
                            tier: commands::DeployTier::Cluster,
                            env,
                            label: label.clone(),
                            secret: Vec::new(),
                            secret_binding_supported: false,
                            connect_hint: connect_hint.clone(),
                        })
                        .await
                        {
                            Ok(prepared) => prepared,
                            Err(err) => {
                                let payload = commands::all_targets_deploy_failure_json(
                                    &target,
                                    &[],
                                    None,
                                    &err,
                                );
                                return Err(curie::exit::with_json_payload(err, payload));
                            }
                        };
                        let connector_version = ConnectorVersion {
                            agent_id: prepared_deploy.agent_id(),
                            agent_name: prepared_deploy.agent_name(),
                            version_id: prepared_deploy.version_id(),
                        };
                        let prepared_connectors = match prepare_connectors(
                            &api_url,
                            &api_key,
                            &namespace,
                            &release,
                            &app_name,
                            connector_version,
                            connector_target.clone(),
                        )
                        .await
                        {
                            Ok(prepared) => prepared,
                            Err(err) => {
                                let payload = commands::all_targets_deploy_failure_json(
                                    &target,
                                    &[],
                                    None,
                                    &err,
                                );
                                return Err(curie::exit::with_json_payload(err, payload));
                            }
                        };
                        prepared_targets.push((target, prepared_deploy, prepared_connectors));
                    }

                    // Activate and reconcile in the API's declared target order.
                    let mut completed = Vec::new();
                    for (target, prepared_deploy, prepared_connectors) in prepared_targets {
                        let deployed = match commands::deploy_prepared(prepared_deploy).await {
                            Ok(deployed) => deployed,
                            Err(err) => {
                                let payload = commands::all_targets_deploy_failure_json(
                                    &target, &completed, None, &err,
                                );
                                return Err(curie::exit::with_json_payload(err, payload));
                            }
                        };

                        if !secret.is_empty() {
                            if let Err(err) = bind_cluster_connector_secrets(
                                &namespace,
                                &release,
                                chart.as_deref(),
                                &deployed.agent_name,
                                &secret,
                            )
                            .await
                            {
                                let payload = commands::all_targets_deploy_failure_json(
                                    &target,
                                    &completed,
                                    Some(&deployed),
                                    &err,
                                );
                                return Err(curie::exit::with_json_payload(err, payload));
                            }
                        }

                        if let Err(err) = apply_connectors(prepared_connectors).await {
                            let payload = commands::all_targets_deploy_failure_json(
                                &target,
                                &completed,
                                Some(&deployed),
                                &err,
                            );
                            return Err(curie::exit::with_json_payload(err, payload));
                        }
                        completed.push(commands::AllTargetsDeployResult {
                            target,
                            result: deployed,
                        });
                    }
                    emit(commands::AllTargetsDeployOutput { results: completed })
                } else {
                    let target = targets
                        .into_iter()
                        .next()
                        .expect("the target list is never empty");
                    let deployed = commands::deploy(DeployOpts {
                        plugin_dir: plugin_dir.clone(),
                        agent: agent.clone(),
                        target,
                        api_url: api_url.clone(),
                        api_key: api_key.clone(),
                        slack_channel: slack_channel.clone(),
                        repo: repo.clone(),
                        workspace: commands::WorkspaceIntent::from_flags(workspace, no_workspace),
                        env,
                        label: label.clone(),
                        secret: secret.clone(),
                        secret_binding_supported: true,
                        connect_hint: connect_hint.clone(),
                        tier: commands::DeployTier::Cluster,
                    })
                    .await?;

                    if !secret.is_empty() {
                        bind_cluster_connector_secrets(
                            &namespace,
                            &release,
                            chart.as_deref(),
                            &deployed.agent_name,
                            &secret,
                        )
                        .await?;
                    }

                    // Stand up whatever the bundle's connectors.yaml declares
                    // (ADR-0086, #1063). After the deploy, so the objects exist
                    // before the next turn reaches for them; the credentials here
                    // are the CONNECTOR's, resolved locally and written straight to
                    // a K8s Secret, which is a different path from the sandbox
                    // secret delivery #440 tracks.
                    sync_connectors(
                        &api_url,
                        &api_key,
                        &namespace,
                        &release,
                        &deployed.agent_id,
                        &deployed.agent_name,
                        &deployed.version_id,
                    )
                    .await?;
                    emit(deployed)
                }
            }
            ClusterAction::Kill {
                agent,
                conn,
                yes,
                dry_run,
            } => {
                let (api_url, api_key, _cluster_api_pf) =
                    resolve_cluster_conn(conn, dry_run).await?;
                emit(
                    commands::kill(
                        AgentActionOpts {
                            api_url,
                            api_key,
                            agent,
                            dry_run,
                        },
                        yes,
                    )
                    .await?,
                )
            }
            ClusterAction::Resume {
                agent,
                conn,
                dry_run,
            } => {
                let (api_url, api_key, _cluster_api_pf) =
                    resolve_cluster_conn(conn, dry_run).await?;
                emit(
                    commands::resume(AgentActionOpts {
                        api_url,
                        api_key,
                        agent,
                        dry_run,
                    })
                    .await?,
                )
            }
            ClusterAction::Overrides {
                agent,
                model,
                clear_model,
                thinking,
                clear_thinking,
                conn,
                dry_run,
            } => {
                // Resolve the flag pairs BEFORE discovering the connection: a
                // contradictory invocation is a usage error, and making the
                // operator wait on a cluster lookup to be told so is worse than
                // telling them immediately.
                let model = commands::OverrideChange::resolve("model", model, clear_model)?;
                let thinking =
                    commands::OverrideChange::resolve("thinking", thinking, clear_thinking)?;
                let (api_url, api_key, _cluster_api_pf) =
                    resolve_cluster_conn(conn, dry_run).await?;
                emit(
                    commands::overrides(
                        AgentActionOpts {
                            api_url,
                            api_key,
                            agent,
                            dry_run,
                        },
                        model,
                        thinking,
                    )
                    .await?,
                )
            }
            ClusterAction::Surfaces {
                agent,
                add,
                endpoint,
                adapter,
                remove,
                conn,
                dry_run,
            } => {
                // Resolve the flag pair BEFORE discovering the connection, for
                // the same reason `Overrides` does: a mistyped pair must not
                // cost a cluster lookup, nor be reported as a connection
                // failure instead of the usage error it is.
                let change = commands::ChannelChange::resolve(add, remove, endpoint, adapter)?;
                let (api_url, api_key, _port_forward) = resolve_cluster_conn(conn, dry_run).await?;
                emit(
                    commands::channel_bindings(
                        AgentActionOpts {
                            api_url,
                            api_key,
                            agent,
                            dry_run,
                        },
                        change,
                    )
                    .await?,
                )
            }
            ClusterAction::Budget {
                agent,
                limit,
                conn,
                dry_run,
            } => {
                let (api_url, api_key, _cluster_api_pf) =
                    resolve_cluster_conn(conn, dry_run).await?;
                emit(
                    commands::budget(
                        AgentActionOpts {
                            api_url,
                            api_key,
                            agent,
                            dry_run,
                        },
                        limit,
                    )
                    .await?,
                )
            }
            ClusterAction::ResetThread {
                agent,
                thread_key,
                conn,
                yes,
                dry_run,
            } => {
                let (api_url, api_key, _cluster_api_pf) =
                    resolve_cluster_conn(conn, dry_run).await?;
                emit(
                    commands::reset_thread(
                        AgentActionOpts {
                            api_url,
                            api_key,
                            agent,
                            dry_run,
                        },
                        thread_key,
                        yes,
                    )
                    .await?,
                )
            }
            ClusterAction::Delete {
                agent,
                conn,
                yes,
                dry_run,
            } => {
                let (api_url, api_key, _cluster_api_pf) =
                    resolve_cluster_conn(conn, dry_run).await?;
                emit(
                    commands::delete(
                        AgentActionOpts {
                            api_url,
                            api_key,
                            agent,
                            dry_run,
                        },
                        yes,
                    )
                    .await?,
                )
            }
            ClusterAction::Console { action } => match action {
                ClusterConsoleAction::Login {
                    conn,
                    subject,
                    console_url,
                    dry_run,
                } => {
                    let (api_url, api_key, _pf) = resolve_cluster_conn(conn, dry_run).await?;
                    let where_console = console_url.unwrap_or_else(|| api_url.clone());
                    emit(
                        commands::console_login(api_url, api_key, subject, where_console, dry_run)
                            .await?,
                    )
                }
            },
            ClusterAction::Versions { target } => {
                let ClusterAgentTarget {
                    agent,
                    conn,
                    dry_run,
                } = target;
                let (api_url, api_key, _cluster_api_pf) =
                    resolve_cluster_conn(conn, dry_run).await?;
                emit(
                    commands::versions(AgentActionOpts {
                        api_url,
                        api_key,
                        agent,
                        dry_run,
                    })
                    .await?,
                )
            }
            ClusterAction::Memory { target, add } => {
                let ClusterAgentTarget {
                    agent,
                    conn,
                    dry_run,
                } = target;
                let (api_url, api_key, _cluster_api_pf) =
                    resolve_cluster_conn(conn, dry_run).await?;
                let opts = AgentActionOpts {
                    api_url,
                    api_key,
                    agent,
                    dry_run,
                };
                match add {
                    None => emit(commands::memory(opts).await?),
                    Some(content) => emit(commands::memory_add(opts, content).await?),
                }
            }
            ClusterAction::Approvals {
                target,
                gate,
                clear,
                list,
                resolve,
                reject,
                note,
                mint_operator_principal,
                mint_console_login_code,
                route_resolution,
                route_approvers,
                routes_from,
                list_routes,
                clear_routes,
            } => {
                let ClusterAgentTarget {
                    agent,
                    conn,
                    dry_run,
                } = target;
                let (api_url, api_key, _cluster_api_pf) =
                    resolve_cluster_conn(conn, dry_run).await?;
                emit(
                    commands::approvals(
                        AgentActionOpts {
                            api_url,
                            api_key,
                            agent,
                            dry_run,
                        },
                        gate,
                        clear,
                        commands::ApprovalCmd {
                            list,
                            resolve,
                            reject,
                            note,
                            mint_operator_principal,
                            mint_console_login_code,
                            route_resolution,
                            route_approvers,
                            routes_from,
                            list_routes,
                            clear_routes,
                        },
                    )
                    .await?,
                )
            }
        },
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
            dry_run,
            chart,
            migrate_store,
            allow_stateful_removal,
        }) => {
            let cfg = curie::installation::Installation::load(&file)?;
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
            emit(
                curie::doctor::doctor(
                    &target.namespace,
                    &target.release,
                    api_url.as_deref(),
                    api_key.as_deref(),
                )
                .await,
            )
        }
        Some(Command::Diff { file, chart }) => {
            let cfg = curie::installation::Installation::load(&file)?;
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
                    chart,
                    chart_target,
                })
                .await?,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn clap_surface_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn skill_approvals_accepts_list_and_resolve_to_decline_them() {
        // The flags exist so the skill tier DECLINES them with a reason
        // (ADR-0077), not clap-erroring like an unknown-flag typo.
        Cli::try_parse_from(["curie", "skill", "approvals", "--list"])
            .expect("skill approvals --list should parse");
        Cli::try_parse_from(["curie", "skill", "approvals", "--resolve", "abc"])
            .expect("skill approvals --resolve should parse");
    }

    /// Serializes the two `cluster up` GitHub-credential cases below.
    ///
    /// `cluster_up_clap_accepts_the_token_from_the_environment_only` has to arm
    /// the input through `CURIE_GITHUB_TOKEN` alone, and clap reads an `env =`
    /// binding from the PROCESS environment at parse time, with no injection
    /// seam, so the variable is set on this process and restored afterwards.
    ///
    /// What this lock DOES guarantee, precisely: the two tests that take it
    /// never run concurrently, so neither observes the other's mutation and
    /// neither leaves `CURIE_GITHUB_TOKEN` behind.
    ///
    /// What it does NOT guarantee: `setenv` is not thread-safe against a
    /// concurrent `getenv` from a thread that does not take this lock, and cargo
    /// runs this binary's tests in parallel. Other tests call `std::env::var`
    /// and `std::env::temp_dir()` (which reads `TMPDIR`) while these two mutate
    /// the environment. That is a latent data race, not a race this `Mutex` can
    /// close -- it is exactly why Rust 2024 made `std::env::set_var` `unsafe`;
    /// this crate is `edition = "2021"`, so it compiles. The precedent already
    /// in the tree is `cli/src/slack.rs`. The real fix is a clap-level
    /// injection seam (parsing from an explicit env source rather than the
    /// process environment), which is a production change.
    static GITHUB_TOKEN_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// A failing assertion panics while holding the lock, which poisons it. The
    /// data is `()`, so there is nothing to corrupt: recover the guard rather
    /// than let the first red case cascade into bogus `PoisonError` failures.
    fn github_token_env_lock() -> std::sync::MutexGuard<'static, ()> {
        GITHUB_TOKEN_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn cluster_up_clap_accepts_the_token_from_the_environment_only() {
        // #1124 AC1, armed through the SECONDARY path: the environment variable
        // alone, never the command line. The env var is the input that keeps the
        // credential out of shell history, so it has to work with no flag
        // present. `Option<String>` with no `default_value` is what makes
        // "absent" distinguishable from "empty"; an empty variable is covered by
        // the resolver's `empty_value_preserves_and_never_clears`.
        //
        // The name is CURIE_GITHUB_TOKEN, never GITHUB_TOKEN: the latter is
        // exported in the shells of most people who use `gh` and in most CI
        // runners, so binding to it would silently capture a personal PAT into a
        // persistent cluster Secret (#496 canonicalized the CURIE_ namespace).
        let _guard = github_token_env_lock();
        let previous = std::env::var("CURIE_GITHUB_TOKEN").ok();

        std::env::set_var("CURIE_GITHUB_TOKEN", "ghp-SENTINEL-1124-leak-canary"); // gitleaks:allow -- test leak canary, not a real token
        let from_env = Cli::try_parse_from(["curie", "cluster", "up"])
            .expect("cluster up should parse with only the env var set");
        std::env::remove_var("CURIE_GITHUB_TOKEN");
        let without = Cli::try_parse_from(["curie", "cluster", "up"])
            .expect("cluster up should parse with nothing set");
        match previous {
            Some(value) => std::env::set_var("CURIE_GITHUB_TOKEN", value),
            None => std::env::remove_var("CURIE_GITHUB_TOKEN"),
        }

        match from_env.command {
            Some(Command::Cluster {
                action: ClusterAction::Up { github_token, .. },
            }) => assert_eq!(
                github_token.as_deref(),
                Some("ghp-SENTINEL-1124-leak-canary")
            ),
            _ => panic!("expected cluster up"),
        }
        match without.command {
            Some(Command::Cluster {
                action: ClusterAction::Up { github_token, .. },
            }) => assert_eq!(
                github_token, None,
                "an unset variable must be absence, not an empty credential"
            ),
            _ => panic!("expected cluster up"),
        }
    }

    #[test]
    fn cluster_upgrade_requires_to_and_reads_namespace_env() {
        let parsed =
            Cli::try_parse_from(["curie", "cluster", "upgrade", "--to", "0.9.0", "--dry-run"])
                .expect("cluster upgrade --to should parse");
        match parsed.command {
            Some(Command::Cluster {
                action:
                    ClusterAction::Upgrade {
                        to,
                        namespace,
                        dry_run,
                        ..
                    },
            }) => {
                assert_eq!(to, "0.9.0");
                assert_eq!(namespace, "curie");
                assert!(dry_run);
            }
            _ => panic!("expected cluster upgrade"),
        }
        let missing = Cli::try_parse_from(["curie", "cluster", "upgrade"]);
        assert!(missing.is_err(), "--to is required");
    }

    #[test]
    fn clap_rejects_flag_and_clear_together() {
        // #1124 AC4, armed through the PARSER rather than the resolver: the
        // fourth, invalid state (set and clear at once) never reaches
        // `resolve_github_token` at all. This also catches `conflicts_with`
        // naming clap's arg ID wrongly -- a spelling like "clear-github-token"
        // compiles and then panics at runtime on the first parse.
        let _guard = github_token_env_lock();
        let previous = std::env::var("CURIE_GITHUB_TOKEN").ok();
        std::env::remove_var("CURIE_GITHUB_TOKEN");

        let both = Cli::try_parse_from([
            "curie",
            "cluster",
            "up",
            "--github-token",
            "ghp-SENTINEL-1124-leak-canary",
            "--clear-github-token",
        ]);
        // Each alone still parses, so the rejection is the conflict and not a
        // broken flag.
        let set_only = Cli::try_parse_from([
            "curie",
            "cluster",
            "up",
            "--github-token",
            "ghp-SENTINEL-1124-leak-canary",
        ]);
        let clear_only = Cli::try_parse_from(["curie", "cluster", "up", "--clear-github-token"]);

        if let Some(value) = previous {
            std::env::set_var("CURIE_GITHUB_TOKEN", value);
        }

        assert!(
            both.is_err(),
            "--github-token with --clear-github-token must be a clap conflict"
        );
        assert!(set_only.is_ok(), "--github-token alone must parse");
        assert!(clear_only.is_ok(), "--clear-github-token alone must parse");
    }

    #[test]
    fn clap_rejects_migrate_store_with_allow_stateful_removal() {
        // #1351: the pair states contradictory intent (carry the object store's
        // data across the upgrade, versus proceed WITHOUT it). Refused at the
        // parser, so the contradiction never reaches the code that silently
        // picked one and took the data destroying path with exit 0.
        //
        // Both orderings are asserted rather than trusting that a
        // `conflicts_with` declared on one arg is mutual. Each flag alone must
        // still parse: a conflict naming an arg id that does not exist panics
        // at parse time, and the "alone" arms are what catch that.
        let both = Cli::try_parse_from([
            "curie",
            "apply",
            "--migrate-store",
            "--allow-stateful-removal",
        ]);
        let reversed = Cli::try_parse_from([
            "curie",
            "apply",
            "--allow-stateful-removal",
            "--migrate-store",
        ]);
        let migrate_only = Cli::try_parse_from(["curie", "apply", "--migrate-store"]);
        let allow_only = Cli::try_parse_from(["curie", "apply", "--allow-stateful-removal"]);

        assert!(
            both.is_err(),
            "--migrate-store with --allow-stateful-removal must be a clap conflict"
        );
        assert!(
            reversed.is_err(),
            "the conflict must hold in either argument order"
        );
        assert!(migrate_only.is_ok(), "--migrate-store alone must parse");
        assert!(
            allow_only.is_ok(),
            "--allow-stateful-removal alone must parse"
        );
    }

    #[test]
    fn build_defaults_tag_and_accepts_override() {
        let cli = Cli::try_parse_from(["curie", "build"]).expect("build should parse");
        match cli.command {
            Some(Command::Build { tag, .. }) => assert_eq!(tag, "curie-runner"),
            _ => panic!("expected build command"),
        }
        let cli = Cli::try_parse_from(["curie", "build", "--tag", "my-runner:dev"])
            .expect("build --tag should parse");
        match cli.command {
            Some(Command::Build { tag, .. }) => assert_eq!(tag, "my-runner:dev"),
            _ => panic!("expected build command"),
        }
    }

    #[test]
    fn list_agents_parses() {
        let cli = Cli::try_parse_from(["curie", "list-agents"]).expect("list-agents should parse");
        assert!(matches!(cli.command, Some(Command::ListAgents)));
    }

    #[test]
    fn deploy_local_parses_the_folder_positional_and_defaults() {
        let cli = Cli::try_parse_from(["curie", "deploy-local", "revenue-leak"])
            .expect("deploy-local should parse");
        match cli.command {
            Some(Command::DeployLocal {
                folder,
                slack_channel,
                secret,
                ..
            }) => {
                assert_eq!(folder, "revenue-leak");
                assert_eq!(slack_channel, None);
                assert!(secret.is_empty());
            }
            _ => panic!("expected deploy-local command"),
        }
    }

    #[test]
    fn no_subcommand_defaults_to_interactive() {
        let cli = Cli::try_parse_from(["curie"]).expect("bare curie should parse");
        assert!(cli.command.is_none());
    }

    #[test]
    fn install_parses() {
        let cli = Cli::try_parse_from(["curie", "install"]).expect("install should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Install { update: false })
        ));
    }

    #[test]
    fn install_update_parses() {
        let cli =
            Cli::try_parse_from(["curie", "install", "--update"]).expect("install should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Install { update: true })
        ));
    }

    #[test]
    fn skill_eval_model_is_repeatable_for_a_sweep() {
        // No --model -> drive the running runner (empty models vec).
        match Cli::try_parse_from(["curie", "skill", "eval"])
            .expect("skill eval should parse")
            .command
        {
            Some(Command::Skill {
                action: SkillAction::Eval { model, .. },
            }) => assert!(model.is_empty()),
            _ => panic!("expected skill eval"),
        }
        // Repeated --model collects into the sweep list.
        match Cli::try_parse_from([
            "curie",
            "skill",
            "eval",
            "--model",
            "claude-haiku-4-5",
            "--model",
            "claude-sonnet-5",
        ])
        .expect("skill eval sweep should parse")
        .command
        {
            Some(Command::Skill {
                action: SkillAction::Eval { model, .. },
            }) => assert_eq!(model, vec!["claude-haiku-4-5", "claude-sonnet-5"]),
            _ => panic!("expected skill eval sweep"),
        }
    }

    #[test]
    fn local_and_cluster_eval_model_are_repeatable_for_a_sweep() {
        // local eval --model repeats into the sweep list (#526).
        match Cli::try_parse_from([
            "curie", "local", "eval", "--model", "opus", "--model", "sonnet",
        ])
        .expect("local eval sweep should parse")
        .command
        {
            Some(Command::Local {
                action: LocalAction::Eval { model, .. },
            }) => assert_eq!(model, vec!["opus", "sonnet"]),
            _ => panic!("expected local eval sweep"),
        }
        // Bare local eval -> no models (the in-CLI parity gate).
        match Cli::try_parse_from(["curie", "local", "eval"])
            .expect("local eval should parse")
            .command
        {
            Some(Command::Local {
                action: LocalAction::Eval { model, .. },
            }) => assert!(model.is_empty()),
            _ => panic!("expected local eval"),
        }
        // cluster eval --model likewise.
        match Cli::try_parse_from(["curie", "cluster", "eval", "--model", "opus"])
            .expect("cluster eval sweep should parse")
            .command
        {
            Some(Command::Cluster {
                action: ClusterAction::Eval { model, .. },
            }) => assert_eq!(model, vec!["opus"]),
            _ => panic!("expected cluster eval sweep"),
        }
    }

    #[test]
    fn eval_case_id_is_repeatable_at_every_tier_and_empty_when_absent() {
        // #2007: `--case-id` is the eval case SELECTOR (distinct from `--cases`,
        // the suite FILE). Absent means the whole suite; repeated means a subset,
        // and a value matching nothing exits 2 rather than greening an empty run.
        match Cli::try_parse_from([
            "curie",
            "skill",
            "eval",
            "--case-id",
            "greets-the-user",
            "--case-id",
            "escalates",
        ])
        .expect("skill eval --case-id should parse")
        .command
        {
            Some(Command::Skill {
                action: SkillAction::Eval { case_id, .. },
            }) => assert_eq!(case_id, vec!["greets-the-user", "escalates"]),
            _ => panic!("expected skill eval with a selector"),
        }
        match Cli::try_parse_from(["curie", "skill", "eval"])
            .expect("skill eval should parse")
            .command
        {
            Some(Command::Skill {
                action: SkillAction::Eval { case_id, .. },
            }) => assert!(case_id.is_empty(), "no selector -> the whole suite"),
            _ => panic!("expected skill eval"),
        }
        match Cli::try_parse_from([
            "curie",
            "local",
            "eval",
            "--case-id",
            "greets-the-user",
            "--case-id",
            "escalates",
        ])
        .expect("local eval --case-id should parse")
        .command
        {
            Some(Command::Local {
                action: LocalAction::Eval { case_id, .. },
            }) => assert_eq!(case_id, vec!["greets-the-user", "escalates"]),
            _ => panic!("expected local eval with a selector"),
        }
        match Cli::try_parse_from(["curie", "local", "eval"])
            .expect("local eval should parse")
            .command
        {
            Some(Command::Local {
                action: LocalAction::Eval { case_id, .. },
            }) => assert!(case_id.is_empty()),
            _ => panic!("expected local eval"),
        }
        match Cli::try_parse_from([
            "curie",
            "cluster",
            "eval",
            "--case-id",
            "greets-the-user",
            "--case-id",
            "escalates",
        ])
        .expect("cluster eval --case-id should parse")
        .command
        {
            Some(Command::Cluster {
                action: ClusterAction::Eval { case_id, .. },
            }) => assert_eq!(case_id, vec!["greets-the-user", "escalates"]),
            _ => panic!("expected cluster eval with a selector"),
        }
        match Cli::try_parse_from(["curie", "cluster", "eval"])
            .expect("cluster eval should parse")
            .command
        {
            Some(Command::Cluster {
                action: ClusterAction::Eval { case_id, .. },
            }) => assert!(case_id.is_empty()),
            _ => panic!("expected cluster eval"),
        }
    }

    #[test]
    fn update_parses_with_and_without_image() {
        let bare = Cli::try_parse_from(["curie", "update"]).expect("update should parse");
        assert!(matches!(
            bare.command,
            Some(Command::Update { image: false })
        ));
        let with_image =
            Cli::try_parse_from(["curie", "update", "--image"]).expect("update should parse");
        assert!(matches!(
            with_image.command,
            Some(Command::Update { image: true })
        ));
    }

    #[test]
    fn interactive_parses_with_aliases() {
        let cli = Cli::try_parse_from(["curie", "interactive"]).expect("interactive should parse");
        assert!(matches!(cli.command, Some(Command::Interactive)));
        let cli = Cli::try_parse_from(["curie", "ui"]).expect("ui alias should parse");
        assert!(matches!(cli.command, Some(Command::Interactive)));
        let cli = Cli::try_parse_from(["curie", "tui"]).expect("tui alias should parse");
        assert!(matches!(cli.command, Some(Command::Interactive)));
    }

    #[test]
    fn secrets_subcommands_parse() {
        let cli = Cli::try_parse_from(["curie", "secrets", "set", "GITHUB_TOKEN"])
            .expect("secrets set should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Secrets {
                action: SecretsAction::Set { .. }
            })
        ));
        let cli = Cli::try_parse_from([
            "curie",
            "secrets",
            "set",
            "GITHUB_TOKEN",
            "--from-env",
            "TMP_TOKEN",
        ])
        .expect("secrets set --from-env should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Secrets {
                action: SecretsAction::Set {
                    from_env: Some(_),
                    ..
                }
            })
        ));
        let cli =
            Cli::try_parse_from(["curie", "secrets", "list"]).expect("secrets list should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Secrets {
                action: SecretsAction::List
            })
        ));
        let cli = Cli::try_parse_from(["curie", "secrets", "unset", "GITHUB_TOKEN"])
            .expect("secrets unset should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Secrets {
                action: SecretsAction::Unset { .. }
            })
        ));
        let cli = Cli::try_parse_from([
            "curie",
            "secrets",
            "set",
            "K8S_WRITE_KUBECONFIG",
            "--from-env",
            "K8S_WRITE_KUBECONFIG",
            "--cluster-identity",
            "ca:a",
            "--release",
            "curie",
            "--namespace",
            "curie-test",
            "--expected-version",
            "1",
        ])
        .expect("scoped secrets set should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Secrets {
                action: SecretsAction::Set {
                    cluster_identity: Some(_),
                    expected_version: Some(1),
                    ..
                }
            })
        ));
    }

    #[test]
    fn dev_subcommands_parse() {
        let cli =
            Cli::try_parse_from(["curie", "dev", "contracts"]).expect("dev contracts should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Dev {
                action: DevAction::Contracts
            })
        ));
        let cli = Cli::try_parse_from(["curie", "dev", "chart-check"])
            .expect("dev chart-check should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Dev {
                action: DevAction::ChartCheck
            })
        ));
        let cli = Cli::try_parse_from(["curie", "dev", "e2e"]).expect("dev e2e should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Dev {
                action: DevAction::E2e
            })
        ));
        let cli =
            Cli::try_parse_from(["curie", "dev", "docs-lint"]).expect("dev docs-lint should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Dev {
                action: DevAction::DocsLint
            })
        ));
        let cli = Cli::try_parse_from(["curie", "dev", "agent-skills"])
            .expect("dev agent-skills should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Dev {
                action: DevAction::AgentSkills
            })
        ));
        let cli = Cli::try_parse_from(["curie", "dev", "eval-falsifiability"])
            .expect("dev eval-falsifiability should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Dev {
                action: DevAction::EvalFalsifiability
            })
        ));
        let cli = Cli::try_parse_from(["curie", "dev", "e2e-ladder"])
            .expect("dev e2e-ladder should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Dev {
                action: DevAction::E2eLadder
            })
        ));
        let cli = Cli::try_parse_from(["curie", "dev", "sre-demo-e2e"])
            .expect("dev sre-demo-e2e should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Dev {
                action: DevAction::SreDemoE2e
            })
        ));
        let cli = Cli::try_parse_from(["curie", "dev", "chart-runtime-e2e"])
            .expect("dev chart-runtime-e2e should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Dev {
                action: DevAction::ChartRuntimeE2e { force: false }
            })
        ));
        // The script's context guard names `--force` as its only override, so the
        // flag has to survive the `curie dev` hop or the guard is unoverridable
        // through the documented entry point.
        let cli = Cli::try_parse_from(["curie", "dev", "chart-runtime-e2e", "--force"])
            .expect("dev chart-runtime-e2e --force should parse");
        assert!(matches!(
            cli.command,
            Some(Command::Dev {
                action: DevAction::ChartRuntimeE2e { force: true }
            })
        ));
    }

    // Proves the `dev` verb set is closed: an unrecognized verb must fail to
    // parse rather than silently falling through. Without this negative case,
    // a typo'd verb name (e.g. a future rename that missed a call site) could
    // ship without ever being caught by the positive-path tests above.
    #[test]
    fn dev_unknown_subcommand_rejected() {
        let result = Cli::try_parse_from(["curie", "dev", "e2e-ladder-typo"]);
        assert!(result.is_err());
    }

    #[test]
    fn local_message_accepts_api_key() {
        let cli = Cli::try_parse_from(["curie", "local", "message", "--api-key", "K", "hi"])
            .expect("local message --api-key should parse");
        match cli.command {
            Some(Command::Local {
                action: LocalAction::Message { api_key, .. },
            }) => assert_eq!(api_key, "K"),
            _ => panic!("expected local message command"),
        }
    }

    /// `cluster message` carries no dev-default sentinel for either credential
    /// (#786): an omitted flag must parse to `None`, not a bound default, so the
    /// handler discovers the release's own Secret instead.
    #[test]
    fn cluster_message_credentials_default_to_discovery() {
        let cli = Cli::try_parse_from(["curie", "cluster", "message", "hi"])
            .expect("cluster message should parse");
        match cli.command {
            Some(Command::Cluster {
                action:
                    ClusterAction::Message {
                        api_key,
                        valkey_password,
                        ..
                    },
            }) => {
                assert_eq!(api_key, None, "an omitted --api-key must not default");
                assert_eq!(
                    valkey_password, None,
                    "an omitted --valkey-password must not default"
                );
            }
            _ => panic!("expected cluster message command"),
        }
    }

    #[test]
    fn cluster_message_accepts_explicit_credentials() {
        let cli = Cli::try_parse_from([
            "curie",
            "cluster",
            "message",
            "--api-key",
            "K",
            "--valkey-password",
            "P",
            "hi",
        ])
        .expect("cluster message with explicit credentials should parse");
        match cli.command {
            Some(Command::Cluster {
                action:
                    ClusterAction::Message {
                        api_key,
                        valkey_password,
                        ..
                    },
            }) => {
                assert_eq!(api_key, Some("K".to_string()));
                assert_eq!(valkey_password, Some("P".to_string()));
            }
            _ => panic!("expected cluster message command"),
        }
    }

    /// `cluster eval` had the identical defect (#790): it still bound the
    /// dev-default sentinel for both credentials after #786 fixed `message`, so
    /// mirror the same "no default, resolves to None" contract here.
    #[test]
    fn cluster_eval_credentials_default_to_discovery() {
        let cli =
            Cli::try_parse_from(["curie", "cluster", "eval"]).expect("cluster eval should parse");
        match cli.command {
            Some(Command::Cluster {
                action:
                    ClusterAction::Eval {
                        api_key,
                        valkey_password,
                        ..
                    },
            }) => {
                assert_eq!(api_key, None, "an omitted --api-key must not default");
                assert_eq!(
                    valkey_password, None,
                    "an omitted --valkey-password must not default"
                );
            }
            _ => panic!("expected cluster eval command"),
        }
    }

    #[test]
    fn cluster_eval_accepts_explicit_credentials() {
        let cli = Cli::try_parse_from([
            "curie",
            "cluster",
            "eval",
            "--api-key",
            "K",
            "--valkey-password",
            "P",
        ])
        .expect("cluster eval with explicit credentials should parse");
        match cli.command {
            Some(Command::Cluster {
                action:
                    ClusterAction::Eval {
                        api_key,
                        valkey_password,
                        ..
                    },
            }) => {
                assert_eq!(api_key, Some("K".to_string()));
                assert_eq!(valkey_password, Some("P".to_string()));
            }
            _ => panic!("expected cluster eval command"),
        }
    }

    /// #1908: a red cluster eval used to leak a kubectl child bound to the
    /// fixed 56381 default, so the next eval selected that same occupied port.
    /// Omitted eval ports must request kernel-assigned 0, matching `cluster
    /// message` (#1652 / #1740).
    #[test]
    fn cluster_eval_omitted_ports_default_to_ephemeral() {
        let cli =
            Cli::try_parse_from(["curie", "cluster", "eval"]).expect("cluster eval should parse");
        match cli.command {
            Some(Command::Cluster {
                action:
                    ClusterAction::Eval {
                        listen_port,
                        valkey_local_port,
                        api_local_port,
                        ..
                    },
            }) => {
                assert_eq!(listen_port, 0, "an omitted --listen-port must request 0");
                assert_eq!(
                    valkey_local_port, 0,
                    "an omitted --valkey-local-port must request 0"
                );
                assert_eq!(
                    api_local_port, 0,
                    "an omitted --api-local-port must request 0"
                );
            }
            _ => panic!("expected cluster eval command"),
        }
    }

    /// #1533 symptom 2: `cluster deploy` hardcoded `DEFAULT_API_LOCAL_PORT`
    /// (8123) for its self-plumbed tunnel, so two concurrent deploys collided
    /// and anything already holding 8123 broke the deploy. `cluster message`
    /// and `cluster eval` were fixed by #1740 / #1652; deploy was the verb that
    /// PR did not reach. An omitted flag must request a kernel-assigned port.
    #[test]
    fn cluster_deploy_defaults_api_local_port_to_zero() {
        let cli = Cli::try_parse_from(["curie", "cluster", "deploy"])
            .expect("cluster deploy should parse");
        match cli.command {
            Some(Command::Cluster {
                action: ClusterAction::Deploy { api_local_port, .. },
            }) => assert_eq!(
                api_local_port, 0,
                "an omitted --api-local-port must request a kernel-assigned port"
            ),
            _ => panic!("expected cluster deploy command"),
        }
    }

    /// The escape hatch stays exact: an explicit port is an override, not a
    /// hint, so an operator can still pin a tunnel port (and get the #1739
    /// occupied-port refusal when it is squatted).
    #[test]
    fn an_explicit_api_local_port_is_honoured() {
        let cli = Cli::try_parse_from(["curie", "cluster", "deploy", "--api-local-port", "18123"])
            .expect("cluster deploy with an explicit port should parse");
        match cli.command {
            Some(Command::Cluster {
                action: ClusterAction::Deploy { api_local_port, .. },
            }) => assert_eq!(api_local_port, 18123),
            _ => panic!("expected cluster deploy command"),
        }
    }

    #[test]
    fn cluster_eval_preserves_explicit_port_overrides() {
        let cli = Cli::try_parse_from([
            "curie",
            "cluster",
            "eval",
            "--listen-port",
            "18155",
            "--valkey-local-port",
            "18156",
            "--api-local-port",
            "18157",
        ])
        .expect("cluster eval with explicit ports should parse");
        match cli.command {
            Some(Command::Cluster {
                action:
                    ClusterAction::Eval {
                        listen_port,
                        valkey_local_port,
                        api_local_port,
                        ..
                    },
            }) => {
                assert_eq!(listen_port, 18155);
                assert_eq!(valkey_local_port, 18156);
                assert_eq!(api_local_port, 18157);
            }
            _ => panic!("expected cluster eval command"),
        }
    }

    #[test]
    fn cluster_deploy_defaults_to_proxy_discovery() {
        let cli = Cli::try_parse_from(["curie", "cluster", "deploy"])
            .expect("cluster deploy should parse");
        match cli.command {
            Some(Command::Cluster {
                action:
                    ClusterAction::Deploy {
                        api_url,
                        namespace,
                        release,
                        ..
                    },
            }) => {
                assert_eq!(api_url, None);
                assert_eq!(namespace, "curie");
                assert_eq!(release, "curie");
            }
            _ => panic!("expected cluster deploy command"),
        }
    }

    #[test]
    fn cluster_deploy_accepts_explicit_api_url() {
        let cli = Cli::try_parse_from([
            "curie",
            "cluster",
            "deploy",
            "--api-url",
            "http://h:30080/api",
        ])
        .expect("cluster deploy --api-url should parse");
        match cli.command {
            Some(Command::Cluster {
                action: ClusterAction::Deploy { api_url, .. },
            }) => assert_eq!(api_url.as_deref(), Some("http://h:30080/api")),
            _ => panic!("expected cluster deploy command"),
        }
    }

    #[test]
    fn cluster_deploy_captures_namespace_and_release() {
        let cli = Cli::try_parse_from([
            "curie",
            "cluster",
            "deploy",
            "--namespace",
            "ns1",
            "--release",
            "rel1",
        ])
        .expect("cluster deploy --namespace --release should parse");
        match cli.command {
            Some(Command::Cluster {
                action:
                    ClusterAction::Deploy {
                        namespace, release, ..
                    },
            }) => {
                assert_eq!(namespace, "ns1");
                assert_eq!(release, "rel1");
            }
            _ => panic!("expected cluster deploy command"),
        }
    }

    #[test]
    fn local_short_file_flag_parses_for_all_verbs() {
        let cases = [
            (["curie", "local", "up", "-f", "custom.yaml"], "up"),
            (["curie", "local", "down", "-f", "custom.yaml"], "down"),
            (["curie", "local", "status", "-f", "custom.yaml"], "status"),
        ];

        for (argv, verb) in cases {
            let cli = Cli::try_parse_from(argv).expect("local verb accepts -f");
            match cli.command {
                Some(Command::Local {
                    action: LocalAction::Up { file, .. },
                }) => {
                    assert_eq!(verb, "up");
                    assert_eq!(file.as_deref(), Some("custom.yaml"));
                }
                Some(Command::Local {
                    action: LocalAction::Down { file, .. },
                }) => {
                    assert_eq!(verb, "down");
                    assert_eq!(file.as_deref(), Some("custom.yaml"));
                }
                Some(Command::Local {
                    action: LocalAction::Status { file, .. },
                }) => {
                    assert_eq!(verb, "status");
                    assert_eq!(file.as_deref(), Some("custom.yaml"));
                }
                _ => panic!("expected the local subcommand"),
            }
        }
    }

    #[test]
    fn local_up_parses_minimal_flag() {
        let cli = Cli::try_parse_from(["curie", "local", "up", "--minimal"])
            .expect("local up --minimal should parse");
        match cli.command {
            Some(Command::Local {
                action: LocalAction::Up { minimal, .. },
            }) => assert!(minimal),
            _ => panic!("expected local up command"),
        }
    }

    #[test]
    fn local_up_parses_slack_flag() {
        let cli = Cli::try_parse_from(["curie", "local", "up", "--slack"])
            .expect("local up --slack should parse");
        match cli.command {
            Some(Command::Local {
                action: LocalAction::Up { slack, .. },
            }) => assert!(slack),
            _ => panic!("expected local up command"),
        }
    }

    #[test]
    fn local_comms_parses_slack_disconnect_and_app_token() {
        let cli = Cli::try_parse_from([
            "curie",
            "local",
            "comms",
            "--slack",
            "--disconnect",
            "--app-token",
            "X",
        ])
        .expect("local comms flags should parse");
        match cli.command {
            Some(Command::Local {
                action:
                    LocalAction::Comms {
                        slack,
                        disconnect,
                        app_token,
                        ..
                    },
            }) => {
                assert!(slack);
                assert!(disconnect);
                assert_eq!(app_token, "X");
            }
            _ => panic!("expected local comms command"),
        }
    }

    #[tokio::test]
    async fn resolve_cluster_conn_prefers_explicit_over_discovery() {
        // #524: an explicit --api-url/--api-key (or env) wins and short-circuits
        // discovery entirely -- no kubectl is shelled, so this resolves with no
        // cluster. (The discovery branch is covered by ops::ui_api_url_from_parts
        // unit tests + the actionable release-named errors.)
        let conn = ClusterConn {
            api_url: Some("https://api.example.test".into()),
            api_key: Some("real-release-key".into()),
            namespace: "curie".into(),
            release: "curie".into(),
        };
        let (url, key, port_forward) = resolve_cluster_conn(conn, false)
            .await
            .expect("explicit conn resolves");
        assert_eq!(url, "https://api.example.test");
        assert_eq!(key, "real-release-key");
        assert!(port_forward.is_none());
    }

    #[test]
    fn cluster_governance_verbs_take_namespace_and_release() {
        // The discovery flags exist on a cluster governance verb so an omitted
        // --api-url/--api-key can be resolved from the named release (#524).
        let cli = Cli::try_parse_from([
            "curie",
            "cluster",
            "versions",
            "demo",
            "--namespace",
            "prod",
            "--release",
            "acme",
        ])
        .expect("cluster versions with --namespace/--release should parse");
        match cli.command {
            Some(Command::Cluster {
                action: ClusterAction::Versions { target },
            }) => {
                assert_eq!(target.agent, "demo");
                assert_eq!(target.conn.namespace, "prod");
                assert_eq!(target.conn.release, "acme");
                assert!(
                    target.conn.api_url.is_none(),
                    "omitted --api-url stays None for discovery"
                );
            }
            _ => panic!("expected cluster versions"),
        }
    }

    #[test]
    fn cluster_kill_parses_agent_and_yes() {
        let cli = Cli::try_parse_from(["curie", "cluster", "kill", "deal-desk", "--yes"])
            .expect("cluster kill should parse");
        match cli.command {
            Some(Command::Cluster {
                action: ClusterAction::Kill { agent, yes, .. },
            }) => {
                assert_eq!(agent, "deal-desk");
                assert!(yes);
            }
            _ => panic!("expected cluster kill command"),
        }
    }

    #[test]
    fn cluster_kill_defaults_yes_and_dry_run_off() {
        let cli = Cli::try_parse_from(["curie", "cluster", "kill", "a"])
            .expect("cluster kill without flags should parse");
        match cli.command {
            Some(Command::Cluster {
                action:
                    ClusterAction::Kill {
                        agent,
                        yes,
                        dry_run,
                        ..
                    },
            }) => {
                assert_eq!(agent, "a");
                assert!(!yes);
                assert!(!dry_run);
            }
            _ => panic!("expected cluster kill command"),
        }
    }

    #[test]
    fn cluster_resume_parses_agent_and_dry_run() {
        let cli = Cli::try_parse_from(["curie", "cluster", "resume", "a", "--dry-run"])
            .expect("cluster resume should parse");
        match cli.command {
            Some(Command::Cluster {
                action: ClusterAction::Resume { agent, dry_run, .. },
            }) => {
                assert_eq!(agent, "a");
                assert!(dry_run);
            }
            _ => panic!("expected cluster resume command"),
        }
    }

    #[test]
    fn cluster_budget_parses_agent_and_limit() {
        let cli = Cli::try_parse_from(["curie", "cluster", "budget", "a", "--limit", "12.5"])
            .expect("cluster budget should parse");
        match cli.command {
            Some(Command::Cluster {
                action: ClusterAction::Budget { agent, limit, .. },
            }) => {
                assert_eq!(agent, "a");
                assert_eq!(limit, 12.5);
            }
            _ => panic!("expected cluster budget command"),
        }
    }

    #[test]
    fn cluster_reset_thread_parses_agent_thread_key_and_yes() {
        let cli = Cli::try_parse_from([
            "curie",
            "cluster",
            "reset-thread",
            "deal-desk",
            "--thread-key",
            "1234.5678",
            "--yes",
        ])
        .expect("cluster reset-thread should parse");
        match cli.command {
            Some(Command::Cluster {
                action:
                    ClusterAction::ResetThread {
                        agent,
                        thread_key,
                        yes,
                        ..
                    },
            }) => {
                assert_eq!(agent, "deal-desk");
                assert_eq!(thread_key, "1234.5678");
                assert!(yes);
            }
            _ => panic!("expected cluster reset-thread command"),
        }
    }

    #[test]
    fn cluster_reset_thread_defaults_yes_and_dry_run_off() {
        let cli = Cli::try_parse_from([
            "curie",
            "cluster",
            "reset-thread",
            "a",
            "--thread-key",
            "t1",
        ])
        .expect("cluster reset-thread without flags should parse");
        match cli.command {
            Some(Command::Cluster {
                action:
                    ClusterAction::ResetThread {
                        agent,
                        thread_key,
                        yes,
                        dry_run,
                        ..
                    },
            }) => {
                assert_eq!(agent, "a");
                assert_eq!(thread_key, "t1");
                assert!(!yes);
                assert!(!dry_run);
            }
            _ => panic!("expected cluster reset-thread command"),
        }
    }

    #[test]
    fn cluster_reset_thread_requires_thread_key() {
        // --thread-key has no default; omitting it must be a parse error.
        assert!(Cli::try_parse_from(["curie", "cluster", "reset-thread", "a", "--yes"]).is_err());
    }

    #[test]
    fn local_platform_verbs_parse() {
        // The inspection/governance verbs mirrored onto the local tier.
        assert!(matches!(
            Cli::try_parse_from(["curie", "local", "versions", "gh"])
                .expect("local versions")
                .command,
            Some(Command::Local {
                action: LocalAction::Versions { .. }
            })
        ));
        assert!(matches!(
            Cli::try_parse_from(["curie", "local", "memory", "gh"])
                .expect("local memory")
                .command,
            Some(Command::Local {
                action: LocalAction::Memory { .. }
            })
        ));
        assert!(matches!(
            Cli::try_parse_from(["curie", "local", "observability"])
                .expect("local observability")
                .command,
            Some(Command::Local {
                action: LocalAction::Observability { .. }
            })
        ));
        // local budget/kill/resume are the mirrored lifecycle verbs.
        assert!(Cli::try_parse_from(["curie", "local", "budget", "gh", "--limit", "1"]).is_ok());
        assert!(Cli::try_parse_from(["curie", "local", "kill", "gh", "--yes"]).is_ok());
    }

    #[test]
    fn local_memory_add_parses_agent_and_content() {
        let cli = Cli::try_parse_from([
            "curie",
            "local",
            "memory",
            "translation-bot",
            "--add",
            "ask before translating to French",
        ])
        .expect("local memory --add should parse");
        match cli.command {
            Some(Command::Local {
                action: LocalAction::Memory { target, add },
            }) => {
                assert_eq!(target.agent, "translation-bot");
                assert_eq!(add.as_deref(), Some("ask before translating to French"));
            }
            _ => panic!("expected local memory"),
        }
    }

    #[test]
    fn cluster_memory_add_parses_agent_and_content() {
        let cli = Cli::try_parse_from([
            "curie",
            "cluster",
            "memory",
            "translation-bot",
            "--add",
            "ask before translating to French",
        ])
        .expect("cluster memory --add should parse");
        match cli.command {
            Some(Command::Cluster {
                action: ClusterAction::Memory { target, add },
            }) => {
                assert_eq!(target.agent, "translation-bot");
                assert_eq!(add.as_deref(), Some("ask before translating to French"));
            }
            _ => panic!("expected cluster memory"),
        }
    }

    #[test]
    fn local_memory_list_still_parses_without_add() {
        assert!(matches!(
            Cli::try_parse_from(["curie", "local", "memory", "translation-bot"])
                .expect("local memory list")
                .command,
            Some(Command::Local {
                action: LocalAction::Memory { add: None, .. }
            })
        ));
    }

    #[test]
    fn local_reset_thread_parses_agent_thread_key_and_yes() {
        let cli = Cli::try_parse_from([
            "curie",
            "local",
            "reset-thread",
            "gh",
            "--thread-key",
            "1234.5678",
            "--yes",
        ])
        .expect("local reset-thread should parse");
        match cli.command {
            Some(Command::Local {
                action:
                    LocalAction::ResetThread {
                        agent,
                        thread_key,
                        yes,
                        dry_run,
                        ..
                    },
            }) => {
                assert_eq!(agent, "gh");
                assert_eq!(thread_key, "1234.5678");
                assert!(yes);
                assert!(!dry_run);
            }
            _ => panic!("expected local reset-thread command"),
        }
    }

    #[test]
    fn local_reset_thread_dry_run_skips_yes_requirement_at_parse_time() {
        // --dry-run parses fine without --yes; the refusal-without-yes check
        // happens in commands::reset_thread, not at the clap layer.
        let cli = Cli::try_parse_from([
            "curie",
            "local",
            "reset-thread",
            "gh",
            "--thread-key",
            "t1",
            "--dry-run",
        ])
        .expect("local reset-thread --dry-run should parse");
        match cli.command {
            Some(Command::Local {
                action: LocalAction::ResetThread { dry_run, yes, .. },
            }) => {
                assert!(dry_run);
                assert!(!yes);
            }
            _ => panic!("expected local reset-thread command"),
        }
    }

    // -----------------------------------------------------------------------
    // Observability twin (issue #460): the `--open` gate is agent-first, so it
    // must default OFF on both tiers, and `--json` is the global flag from #456.
    // -----------------------------------------------------------------------

    #[test]
    fn local_observability_open_flag_defaults_off_and_parses() {
        // Bare `local observability` must NOT open a browser (agent-first default).
        match Cli::try_parse_from(["curie", "local", "observability"])
            .expect("local observability")
            .command
        {
            Some(Command::Local {
                action: LocalAction::Observability { open, .. },
            }) => assert!(!open, "--open must default to false"),
            _ => panic!("expected local observability command"),
        }
        // `--open` is the explicit human opt-in.
        match Cli::try_parse_from(["curie", "local", "observability", "--open"])
            .expect("local observability --open")
            .command
        {
            Some(Command::Local {
                action: LocalAction::Observability { open, .. },
            }) => assert!(open, "--open must parse to true"),
            _ => panic!("expected local observability command"),
        }
    }

    #[test]
    fn cluster_observability_parses_with_namespace_release_defaults() {
        match Cli::try_parse_from(["curie", "cluster", "observability"])
            .expect("cluster observability")
            .command
        {
            Some(Command::Cluster {
                action:
                    ClusterAction::Observability {
                        namespace,
                        release,
                        dry_run,
                        open,
                        ..
                    },
            }) => {
                assert_eq!(namespace, "curie");
                assert_eq!(release, "curie");
                assert!(!dry_run);
                assert!(!open, "--open must default to false");
            }
            _ => panic!("expected cluster observability command"),
        }
    }

    #[test]
    fn cluster_observability_accepts_the_global_json_flag() {
        // `--json` is a GLOBAL flag on `Cli` (issue #456), not a subcommand flag,
        // so it parses onto the top-level struct while the subcommand still binds.
        let cli = Cli::try_parse_from(["curie", "cluster", "observability", "--json"])
            .expect("cluster observability --json");
        assert!(cli.json, "--json must set the global json flag");
        assert!(matches!(
            cli.command,
            Some(Command::Cluster {
                action: ClusterAction::Observability { .. }
            })
        ));
    }

    #[test]
    fn cluster_observability_parses_open_and_dry_run_together() {
        match Cli::try_parse_from(["curie", "cluster", "observability", "--open", "--dry-run"])
            .expect("cluster observability --open --dry-run")
            .command
        {
            Some(Command::Cluster {
                action: ClusterAction::Observability { dry_run, open, .. },
            }) => {
                assert!(dry_run, "--dry-run must parse to true");
                assert!(open, "--open must parse to true");
            }
            _ => panic!("expected cluster observability command"),
        }
    }

    #[test]
    fn approvals_parses_repeatable_gate_and_clear() {
        let cli = Cli::try_parse_from([
            "curie",
            "local",
            "approvals",
            "gh",
            "--gate",
            "Bash",
            "--gate",
            "mcp__x__y",
        ])
        .expect("local approvals should parse");
        match cli.command {
            Some(Command::Local {
                action:
                    LocalAction::Approvals {
                        target,
                        gate,
                        clear,
                        ..
                    },
            }) => {
                assert_eq!(target.agent, "gh");
                assert_eq!(gate, vec!["Bash".to_string(), "mcp__x__y".to_string()]);
                assert!(!clear);
            }
            _ => panic!("expected local approvals command"),
        }
        // --clear parses on both tiers.
        assert!(Cli::try_parse_from(["curie", "cluster", "approvals", "gh", "--clear"]).is_ok());
    }

    #[test]
    fn cluster_budget_requires_limit() {
        // `--limit` has no default, so omitting it is a parse error (not a silent
        // zero-budget request).
        assert!(Cli::try_parse_from(["curie", "cluster", "budget", "a"]).is_err());
    }

    #[test]
    fn cluster_delete_parses_agent_and_yes() {
        let cli = Cli::try_parse_from(["curie", "cluster", "delete", "a", "--yes"])
            .expect("cluster delete should parse");
        match cli.command {
            Some(Command::Cluster {
                action: ClusterAction::Delete { agent, yes, .. },
            }) => {
                assert_eq!(agent, "a");
                assert!(yes);
            }
            _ => panic!("expected cluster delete command"),
        }
    }

    #[test]
    fn skill_approvals_parses_plugin_dir_and_repeatable_gate() {
        let cli = Cli::try_parse_from([
            "curie",
            "skill",
            "approvals",
            "--plugin-dir",
            "/tmp/bundle",
            "--gate",
            "A",
            "--gate",
            "B",
        ])
        .expect("skill approvals should parse");
        match cli.command {
            Some(Command::Skill {
                action:
                    SkillAction::Approvals {
                        plugin_dir,
                        gate,
                        clear,
                        ..
                    },
            }) => {
                assert_eq!(plugin_dir, std::path::PathBuf::from("/tmp/bundle"));
                assert_eq!(gate, vec!["A".to_string(), "B".to_string()]);
                assert!(!clear);
            }
            _ => panic!("expected skill approvals command"),
        }
    }

    #[test]
    fn skill_approvals_parses_clear() {
        let cli = Cli::try_parse_from(["curie", "skill", "approvals", "--clear"])
            .expect("skill approvals --clear should parse");
        match cli.command {
            Some(Command::Skill {
                action: SkillAction::Approvals { gate, clear, .. },
            }) => {
                assert!(clear);
                assert!(gate.is_empty());
            }
            _ => panic!("expected skill approvals command"),
        }
    }

    #[test]
    fn skill_approvals_clear_and_gate_parse_ok_at_clap_layer() {
        // The --clear + --gate conflict is a RUNTIME usage error (asserted in the
        // commands.rs handler tests), not a clap parse error.
        assert!(
            Cli::try_parse_from(["curie", "skill", "approvals", "--clear", "--gate", "X"]).is_ok()
        );
    }

    #[test]
    fn skill_versions_parses_as_a_known_verb() {
        // The verb EXISTS at the skill tier (answered, not a clap unknown
        // subcommand): parsing succeeds and the runtime reports unavailability.
        assert!(matches!(
            Cli::try_parse_from(["curie", "skill", "versions"])
                .expect("skill versions should parse")
                .command,
            Some(Command::Skill {
                action: SkillAction::Versions
            })
        ));
    }

    #[test]
    fn cluster_deploy_accepts_secret_flag() {
        // `--secret` must PARSE (not error like a typo). Cluster delivery is
        // implemented (#1488); this lock is the clap surface, not the helm path.
        match Cli::try_parse_from([
            "curie",
            "cluster",
            "deploy",
            "--secret",
            "GITHUB_PERSONAL_ACCESS_TOKEN",
        ])
        .expect("cluster deploy --secret should parse")
        .command
        {
            Some(Command::Cluster {
                action: ClusterAction::Deploy { secret, .. },
            }) => assert_eq!(secret, vec!["GITHUB_PERSONAL_ACCESS_TOKEN"]),
            _ => panic!("expected cluster deploy"),
        }
        // Bare cluster deploy still parses with no secrets.
        match Cli::try_parse_from(["curie", "cluster", "deploy"])
            .expect("bare cluster deploy should parse")
            .command
        {
            Some(Command::Cluster {
                action: ClusterAction::Deploy { secret, .. },
            }) => assert!(secret.is_empty()),
            _ => panic!("expected cluster deploy"),
        }
    }

    #[test]
    fn skill_memory_parses_as_a_known_verb() {
        assert!(matches!(
            Cli::try_parse_from(["curie", "skill", "memory"])
                .expect("skill memory should parse")
                .command,
            Some(Command::Skill {
                action: SkillAction::Memory
            })
        ));
    }

    #[test]
    fn cluster_comms_parses_slack_disconnect_and_app_token() {
        let cli = Cli::try_parse_from([
            "curie",
            "cluster",
            "comms",
            "--slack",
            "--disconnect",
            "--app-token",
            "X",
        ])
        .expect("cluster comms flags should parse");
        match cli.command {
            Some(Command::Cluster {
                action:
                    ClusterAction::Comms {
                        slack,
                        disconnect,
                        app_token,
                        ..
                    },
            }) => {
                assert!(slack);
                assert!(disconnect);
                assert_eq!(app_token, "X");
            }
            _ => panic!("expected cluster comms command"),
        }
    }
}
