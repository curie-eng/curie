//! The `curie` clap surface: the top-level [`Cli`] and [`Command`], the
//! arguments shared across tiers, and one submodule per tier's subcommands.

use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand, ValueEnum};
use curie::api;
use curie::channel_token as crate_channel_token;
use curie::commands::{self, AgentActionOpts, DeployEnv, SendType, DEFAULT_PORT};
use curie::docker;
use curie::github_app as crate_github_app;
use curie::message;
use curie::ui::ColorFlag;

mod cluster;
mod dev;
mod local;
mod skill;

pub(crate) use cluster::*;
pub(crate) use dev::*;
pub(crate) use local::*;
pub(crate) use skill::*;

/// Per-tier defaults for the flags shared by the agent-target verbs. The only
/// thing that differs between `local` and `cluster` is where the platform API
/// listens, so that is the single const each tier supplies.
pub(crate) trait TierDefaults: Clone + Send + Sync + std::fmt::Debug + 'static {
    const API_URL: &'static str;
}

#[derive(Clone, Debug)]
pub(crate) struct LocalTier;

impl TierDefaults for LocalTier {
    const API_URL: &'static str = "http://localhost:28000";
}

/// The flags a LOCAL agent-target verb (`versions`, `memory`, `approvals`) takes.
/// The local tier correctly defaults to the compose stack on localhost; the
/// cluster tier discovers its connection from the release instead (see
/// [`ClusterAgentTarget`] / [`ClusterConn`], #524), so it no longer shares this.
/// The administrative approval recovery verbs (#2753), flattened into each
/// tier's `approvals` so their parse runs in its own frame.
#[derive(Args, Debug, Clone, Default)]
pub(crate) struct ApprovalRecoveryArgs {
    /// Report installation-wide approval identity FACTS, plus the
    /// declaration skeleton to fill in and feed back to the upgrade. A pure
    /// read: no agent lookup, no principal, nothing mutated.
    #[arg(long)]
    pub(crate) report_identity: bool,
    /// Administratively reject this approval under the installation-wide
    /// recovery grant (`api.approvalRecovery.enabled`). Requires --reason
    /// and --recovery-key; every use is audited.
    #[arg(long, value_name = "APPROVAL_ID")]
    pub(crate) recover: Option<String>,
    /// Why this administrative recovery is being performed. Written
    /// verbatim to the durable audit row. Required by --recover.
    #[arg(long, value_name = "TEXT")]
    pub(crate) reason: Option<String>,
    /// The caller-chosen idempotency key for --recover.
    /// Retrying the identical command with the same key is absorbed by the
    /// server as one act; the CLI never generates or decorates it.
    #[arg(long = "recovery-key", value_name = "KEY")]
    pub(crate) recovery_key: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub(crate) struct AgentTarget<T: TierDefaults> {
    /// Agent name or id.
    pub(crate) agent: String,
    #[arg(long, default_value = T::API_URL, env = "CURIE_API_URL")]
    pub(crate) api_url: String,
    #[arg(long, default_value = "curie-dev-key", env = "CURIE_API_KEY", hide_env_values = true, value_parser = message::api_key_or_default)]
    pub(crate) api_key: String,
    #[arg(long)]
    pub(crate) dry_run: bool,
    #[arg(skip)]
    pub(crate) _tier: std::marker::PhantomData<T>,
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
pub(crate) struct ClusterConn {
    /// Platform API base URL. Omit to self-plumb a loopback tunnel to the release API.
    #[arg(long, env = "CURIE_API_URL")]
    pub(crate) api_url: Option<String>,
    /// Platform API key. Omit to read the release's `api.apiKey` from its Secret.
    #[arg(long, env = "CURIE_API_KEY", hide_env_values = true)]
    pub(crate) api_key: Option<String>,
    /// Kubernetes namespace of the release. Default: curie.
    #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
    pub(crate) namespace: String,
    /// Helm release name. Default: curie.
    #[arg(long, default_value = "curie")]
    pub(crate) release: String,
}

/// Same connection as [`ClusterConn`], but the flags are global so they parse
/// after `cluster hook fire` as well as on `cluster hook`.
#[derive(Args, Debug, Clone)]
pub(crate) struct ClusterHookConn {
    /// Platform API base URL. Omit to self-plumb a loopback tunnel to the release API.
    #[arg(long, env = "CURIE_API_URL", global = true)]
    pub(crate) api_url: Option<String>,
    /// Platform API key. Omit to read the release's `api.apiKey` from its Secret.
    #[arg(long, env = "CURIE_API_KEY", hide_env_values = true, global = true)]
    pub(crate) api_key: Option<String>,
    /// Kubernetes namespace of the release. Default: curie.
    #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE", global = true)]
    pub(crate) namespace: String,
    /// Helm release name. Default: curie.
    #[arg(long, default_value = "curie", global = true)]
    pub(crate) release: String,
}

/// Local API connection flags shared by every observability query leaf. They
/// intentionally live on the leaves: bare `local observability` remains the
/// existing URL printer and does not grow a transport contract.
#[derive(Args, Debug, Clone)]
pub(crate) struct LocalObservabilityConn {
    /// Platform API base URL.
    #[arg(
        long,
        default_value = message::DEFAULT_LOCAL_API_URL,
        env = "CURIE_API_URL"
    )]
    pub(crate) api_url: String,
    /// Platform API key.
    #[arg(long, default_value = message::DEFAULT_API_KEY, env = "CURIE_API_KEY", hide_env_values = true, value_parser = message::api_key_or_default)]
    pub(crate) api_key: String,
}

/// Explicit cluster API overrides shared by every observability query leaf.
/// Namespace and release stay on the parent `cluster observability` command so
/// self-plumbing and the bare surface report always target the same release.
#[derive(Args, Debug, Clone)]
pub(crate) struct ClusterObservabilityConn {
    /// Platform API base URL. Omit to self-plumb the release API over loopback.
    #[arg(long, env = "CURIE_API_URL")]
    pub(crate) api_url: Option<String>,
    /// Platform API key. Required with --api-url; omit both for release discovery.
    #[arg(long, env = "CURIE_API_KEY", hide_env_values = true)]
    pub(crate) api_key: Option<String>,
}

pub(crate) fn parse_observability_limit(raw: &str) -> std::result::Result<usize, String> {
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
pub(crate) struct ObservabilityRunsArgs {
    /// Maximum newest-first trace rows to return (1-100).
    #[arg(long, default_value = "20", value_parser = parse_observability_limit)]
    pub(crate) limit: usize,
    /// Restrict traces to one agent id.
    #[arg(long)]
    pub(crate) agent_id: Option<String>,
}

/// Shared detail selector for the local and cluster sibling leaves.
#[derive(Args, Debug, Clone)]
pub(crate) struct ObservabilityRunArgs {
    /// Trace id previously returned by `observability runs` or a completed turn.
    #[arg(value_parser = api::parse_trace_id)]
    pub(crate) trace_id: String,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub(crate) enum ObservabilityMetric {
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
pub(crate) enum ObservabilityGranularity {
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
pub(crate) struct ObservabilityMetricsArgs {
    /// Return a time series for this metric; omit for the scalar summary.
    #[arg(long, value_enum)]
    pub(crate) metric: Option<ObservabilityMetric>,
    /// Series bucket size. Defaults to day when --metric is present.
    #[arg(long, value_enum)]
    pub(crate) granularity: Option<ObservabilityGranularity>,
    /// Optional metrics-window start accepted by the platform API.
    #[arg(long)]
    pub(crate) start: Option<String>,
    /// Optional metrics-window end accepted by the platform API.
    #[arg(long)]
    pub(crate) end: Option<String>,
    /// Restrict metrics to one deployment environment.
    #[arg(long)]
    pub(crate) environment: Option<String>,
    /// Restrict metrics to one agent name.
    #[arg(long)]
    pub(crate) agent: Option<String>,
}

impl ObservabilityMetricsArgs {
    pub(crate) fn into_query(self) -> Result<curie::observability::ObservabilityQuery> {
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
pub(crate) enum LocalObservabilityQuery {
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
pub(crate) enum ClusterObservabilityQuery {
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
pub(crate) enum SkillObservabilityQuery {
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
pub(crate) struct ClusterAgentTarget {
    /// Agent name or id.
    pub(crate) agent: String,
    #[command(flatten)]
    pub(crate) conn: ClusterConn,
    #[arg(long)]
    pub(crate) dry_run: bool,
}

// @spec ACTION-EXECUTOR-23
/// The `actions` verbs, shared by `local` and `cluster` so the two tiers cannot
/// drift. `C` is the tier's connection flags, carried on each leaf so they
/// parse after the verb's own arguments.
#[derive(Subcommand, Debug, Clone)]
pub(crate) enum ActionsCommand<C: clap::Args> {
    /// List recorded actions (`GET /actions`), each with whether it can be undone.
    List {
        /// Scope to one agent (name or id).
        #[arg(long, value_name = "NAME_OR_ID")]
        agent: Option<String>,
        /// Scope to one conversation (thread) id.
        #[arg(long, value_name = "ID")]
        conversation: Option<String>,
        #[command(flatten)]
        conn: C,
    },
    /// Read one recorded action (`GET /actions/{id}`). Snapshot material is never shown.
    Show {
        /// Action id.
        #[arg(value_name = "ID")]
        id: String,
        #[command(flatten)]
        conn: C,
    },
    /// Ask for an undo of one action (`POST /actions/{id}/undo`) and print the
    /// execution it created. Authentication comes from
    /// CURIE_APPROVAL_PRINCIPAL_TOKEN.
    Undo {
        /// Action id.
        #[arg(value_name = "ID")]
        id: String,
        #[command(flatten)]
        conn: C,
    },
    /// Read an execution's receipt (`GET /action-executions/{id}`): its state
    /// and its refusal or failure code.
    Execution {
        /// Execution id.
        #[arg(value_name = "ID")]
        id: String,
        #[command(flatten)]
        conn: C,
    },
}

impl<C: clap::Args> ActionsCommand<C> {
    /// Split the verb from its connection flags.
    pub(crate) fn into_parts(self) -> (commands::ActionsVerb, C) {
        match self {
            ActionsCommand::List {
                agent,
                conversation,
                conn,
            } => (
                commands::ActionsVerb::List {
                    agent,
                    conversation,
                },
                conn,
            ),
            ActionsCommand::Show { id, conn } => (commands::ActionsVerb::Show { id }, conn),
            ActionsCommand::Undo { id, conn } => (commands::ActionsVerb::Undo { id }, conn),
            ActionsCommand::Execution { id, conn } => {
                (commands::ActionsVerb::Execution { id }, conn)
            }
        }
    }

    /// The leaf's connection flags.
    pub(crate) fn conn(&self) -> &C {
        match self {
            ActionsCommand::List { conn, .. }
            | ActionsCommand::Show { conn, .. }
            | ActionsCommand::Undo { conn, .. }
            | ActionsCommand::Execution { conn, .. } => conn,
        }
    }

    /// The leaf's connection flags, mutably.
    pub(crate) fn conn_mut(&mut self) -> &mut C {
        match self {
            ActionsCommand::List { conn, .. }
            | ActionsCommand::Show { conn, .. }
            | ActionsCommand::Undo { conn, .. }
            | ActionsCommand::Execution { conn, .. } => conn,
        }
    }
}

#[derive(Subcommand, Debug)]
pub(crate) enum LocalHooksAction {
    /// Show an agent's hook partition and source binding maps.
    Show {
        #[command(flatten)]
        target: AgentTarget<LocalTier>,
    },
    /// Replace one or both hook maps from a JSON file. An empty map clears it.
    Configure {
        #[command(flatten)]
        target: AgentTarget<LocalTier>,
        #[arg(long, value_name = "PATH")]
        file: PathBuf,
    },
    /// Read the derived hook signing secret for an agent.
    Secret {
        #[command(flatten)]
        target: AgentTarget<LocalTier>,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum ClusterHooksAction {
    /// Show an agent's hook partition and source binding maps.
    Show {
        #[command(flatten)]
        target: ClusterAgentTarget,
    },
    /// Replace one or both hook maps from a JSON file. An empty map clears it.
    Configure {
        #[command(flatten)]
        target: ClusterAgentTarget,
        #[arg(long, value_name = "PATH")]
        file: PathBuf,
    },
    /// Read the derived hook signing secret for an agent.
    Secret {
        #[command(flatten)]
        target: ClusterAgentTarget,
    },
}

/// clap `value_parser` for every `--local-model` (#1254). All four sites carry the
/// same value and hand it to the same downstream consumers, so validating one and
/// not the others is the sibling-path drift this repo keeps getting bitten by.
pub(crate) fn parse_model_ref(raw: &str) -> Result<String, String> {
    curie::docker::validate_model_ref(raw).map_err(|e| e.to_string())?;
    Ok(raw.to_string())
}

fn parse_console_subject(raw: &str) -> Result<String, String> {
    if raw.trim().is_empty() {
        return Err("the principal subject must not be blank".to_string());
    }
    Ok(raw.to_string())
}

#[derive(Parser)]
#[command(
    name = "curie",
    version,
    about = "Curie CLI: run `curie` for the interactive terminal, or pass a subcommand for scripts"
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Option<Command>,
    /// Show verbose plumbing (helm/kubectl/rollout/port-forward).
    #[arg(
        long,
        global = true,
        help = "Show verbose plumbing (helm/kubectl/rollout/port-forward)"
    )]
    pub(crate) debug: bool,
    /// Payload only; suppress progress and diagnostics.
    #[arg(
        short = 'q',
        long,
        global = true,
        help = "Payload only; suppress progress and diagnostics"
    )]
    pub(crate) quiet: bool,
    /// Colorize output.
    #[arg(
        long,
        global = true,
        value_enum,
        default_value_t = ColorFlag::Auto,
        help = "Colorize output"
    )]
    pub(crate) color: ColorFlag,
    /// Machine-readable JSON to stdout; human/log text to stderr.
    #[arg(
        long,
        global = true,
        help = "Machine-readable JSON to stdout; human/log text to stderr"
    )]
    pub(crate) json: bool,
}

#[derive(Subcommand)]
pub(crate) enum Command {
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
        /// Kubernetes context for every helm and kubectl call. Defaults to the
        /// kubeconfig current-context, which is resolved once and pinned.
        #[arg(long, global = true, value_name = "NAME")]
        context: Option<String>,
    },
    /// Bring up Curie and the dark factory without a webhook or a tunnel.
    Factory {
        #[command(subcommand)]
        action: FactoryAction,
    },
    /// Install a complete first party example workflow.
    Example {
        #[command(subcommand)]
        action: ExampleAction,
        /// Kubernetes context for every helm and kubectl call. Defaults to the
        /// kubeconfig current-context, which is resolved once and pinned.
        #[arg(long, global = true, value_name = "NAME")]
        context: Option<String>,
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
        #[arg(long, default_value = "curie-dev-key", env = "CURIE_API_KEY", hide_env_values = true, value_parser = message::api_key_or_default)]
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
        /// Target environment. Defaults to dev.
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
        /// Push every declared platform (or the `--platform` subset) to this
        /// registry (e.g. ghcr.io/acme-corp).
        #[arg(long, value_name = "REF", requires = "plugin_dir")]
        registry: Option<String>,
        /// The platform runner a declared runner layer builds on (default: the
        /// runner `curie skill up` uses). Resolved to a digest before building.
        #[arg(long, value_name = "REF", requires = "plugin_dir")]
        runner_image: Option<String>,
        /// Replace a registry lock with a local-daemon one deliberately.
        #[arg(long, requires = "plugin_dir")]
        force: bool,
        /// Push only these declared platforms (repeatable; requires `--registry`), e.g. the one architecture a laptop cluster runs.
        /// The default Docker driver can push a single platform; a multi-platform push needs a docker-container builder.
        /// Without `--registry` the build is the host platform only, so `--platform` is refused there.
        #[arg(long = "platform", value_name = "OS/ARCH", requires = "registry")]
        platform: Vec<String>,
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
    /// Print the committed, versioned JSON Schemas for `--json` results and the
    /// `curie.yaml` installation input (`curie-yaml`).
    ///
    /// With no NAME, emits the schema inventory index (`cli/schema/index.json`):
    /// every agent-facing result family, the schema file it maps to, and its
    /// version. With a NAME (e.g. `kill`, or `kill.schema.json`, or
    /// `curie-yaml`), emits that schema. The schemas are embedded in the binary,
    /// so this works from a released `curie` with no source checkout (issue #634).
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
    /// the Curie repository. A released binary writes the same starter with
    /// `curie apply --init`.
    Apply {
        /// Path to the installation file.
        #[arg(short = 'f', long = "file", default_value = "curie.yaml")]
        file: std::path::PathBuf,
        /// Write a starter `curie.yaml` from this binary and exit. Refuses to
        /// overwrite an existing file.
        #[arg(
            long,
            conflicts_with_all = ["dry_run", "chart", "migrate_store", "allow_stateful_removal", "context"]
        )]
        init: bool,
        /// Kubernetes context for every helm and kubectl call. Wins over
        /// `install.context` in the file. Defaults to the kubeconfig
        /// current-context, which is resolved once and pinned.
        #[arg(long, value_name = "NAME")]
        context: Option<String>,
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
        /// Kubernetes context for every helm and kubectl call. Wins over
        /// `install.context` in `curie.yaml`. Defaults to the kubeconfig
        /// current-context, which is resolved once and pinned.
        #[arg(long, value_name = "NAME")]
        context: Option<String>,
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
        #[arg(long, env = "CURIE_API_KEY", hide_env_values = true)]
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
        /// Kubernetes context for every helm and kubectl call. Wins over
        /// `install.context` in the file. Defaults to the kubeconfig
        /// current-context, which is resolved once and pinned. Diff prints the
        /// cluster this context names.
        #[arg(long, value_name = "NAME")]
        context: Option<String>,
        /// Chart reference override, as `cluster up` takes. Diff RENDERS this
        /// chart to detect stateful components the apply would delete, so point
        /// it at the same chart `curie apply --chart` would use.
        #[arg(long)]
        chart: Option<String>,
    },
}

#[derive(Subcommand)]
pub(crate) enum FactoryAction {
    /// Create a cluster when none is targeted, install Curie, register the
    /// GitHub App, and on the second run deploy the published dark factory.
    /// Never opens a browser and never runs gh.
    Quickstart {
        /// GitHub repository the factory may work in, as owner/name.
        #[arg(long = "repo", value_name = "OWNER/NAME")]
        repo: String,
        /// Numeric GitHub App ID. Pair with --private-key-file. Without both,
        /// the command prints the registration link and exits after cluster up.
        #[arg(long, value_name = "ID", requires = "private_key_file")]
        app_id: Option<String>,
        /// PEM private key downloaded from the App. The path is passed through;
        /// the key contents never enter argv.
        #[arg(long, value_name = "PATH", requires = "app_id")]
        private_key_file: Option<PathBuf>,
        /// Kubernetes context. An explicit name is used with no prompt. When
        /// omitted, a current kind context proceeds with no prompt, and no
        /// current context creates a kind cluster. Any other current context
        /// requires confirmation in a terminal and is refused without one.
        /// Pass this flag to proceed without a prompt.
        #[arg(long, value_name = "NAME")]
        context: Option<String>,
        /// Kubernetes namespace. Default: curie.
        #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        /// Helm release name. Default: curie.
        #[arg(long, default_value = "curie")]
        release: String,
        /// Print the registration link for this GitHub organization instead of
        /// a personal account.
        #[arg(long, value_name = "ORG")]
        org: Option<String>,
        /// Kind cluster name used only when no kube context is targeted.
        #[arg(long, default_value = curie::factory_quickstart::DEFAULT_KIND_NAME)]
        kind_name: String,
        /// Model id installed by cluster up.
        #[arg(long, default_value = curie::factory_quickstart::DEFAULT_MODEL)]
        model: String,
        /// Per run execution deadline in seconds for the deployed agent.
        #[arg(long, default_value_t = curie::factory_quickstart::DEFAULT_DEADLINE_SECONDS)]
        execution_deadline: u32,
        /// Daily USD budget for the deployed agent.
        #[arg(long, default_value_t = curie::factory_quickstart::DEFAULT_BUDGET_USD)]
        budget: f64,
        /// Helm chart. Default: the version pinned chart on release builds;
        /// local charts/curie on dev builds.
        #[arg(long)]
        chart: Option<String>,
        /// Print the steps and exit without creating a cluster or applying anything.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
pub(crate) enum ExampleAction {
    /// Install the self referential SRE bot example.
    SreBot {
        #[command(subcommand)]
        action: SreBotAction,
    },
    /// Work with the dark-factory example bundle.
    DarkFactory {
        #[command(subcommand)]
        action: DarkFactoryAction,
    },
}

#[derive(Subcommand)]
pub(crate) enum DarkFactoryAction {
    /// Write the dark-factory bundle into a new or empty directory. Touches no cluster.
    Render {
        /// Directory to write the bundle into. Must not exist or be empty.
        #[arg(long, value_name = "DIR")]
        out: PathBuf,
    },
}

#[derive(Subcommand)]
pub(crate) enum SreBotAction {
    /// Install Curie, its observability stack, and the SRE bot bundle.
    Install {
        /// Install the fixed self referential Grafana, Loki, Alloy, Tempo, and Prometheus stack.
        #[arg(
            long,
            required_unless_present = "observability_only",
            conflicts_with = "observability_only"
        )]
        observability: bool,
        /// Install only the observability stack; do not change the Curie release or deploy the bot.
        #[arg(long, conflicts_with = "platform_upgrade")]
        observability_only: bool,
        /// Print the ordered plan without mutating the cluster.
        #[arg(long)]
        dry_run: bool,
        /// Bind the installed bot to this Slack channel.
        #[arg(long, value_name = "CHANNEL", conflicts_with = "observability_only")]
        slack_channel: Option<String>,
        /// Slack user IDs allowed to resolve the bot's Kubernetes mutation
        /// gates (route sre-approvals). Comma separated and repeatable.
        /// At least one explicit user is required.
        #[arg(
            long,
            value_name = "USER_IDS",
            required_unless_present = "observability_only",
            conflicts_with = "observability_only"
        )]
        approvers: Vec<String>,
        /// Install the upgrade path: the self-upgrade connector, the platform
        /// upgrade Job, and the two identities behind them. Applies
        /// upgrade-role.yaml, platform-upgrade-role.yaml, and the rendered
        /// platform-upgrade ConfigMap and suspended CronJob. Arms only
        /// upgrade_platform: no self-upgrade CronJob is applied, so
        /// upgrade_self stays unarmed.
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
        #[arg(
            long = "workspace-repo",
            value_name = "OWNER/REPO",
            conflicts_with = "observability_only"
        )]
        workspace_repo: Vec<String>,
    },
    /// Render the deployable SRE bot bundle without changing a cluster.
    Render {
        /// New directory where the runtime bundle will be written; existing paths are refused.
        #[arg(long, value_name = "DIR")]
        out: std::path::PathBuf,
        /// Include the gated platform-upgrade connector and rendered manifests.
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
    },
    /// Provision the observability stack on an existing Curie release and
    /// require the Grafana connector token. Does not install the platform
    /// and does not deploy the SRE bot.
    ProvisionObservability {
        /// Kubernetes namespace of the Curie release. Default: curie.
        #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        /// Helm release name of the Curie install. Default: curie.
        #[arg(long, default_value = "curie")]
        release: String,
        /// Kubernetes namespace of the retained observability stack. Default: observability.
        #[arg(long, default_value = "observability")]
        observability_namespace: String,
        /// Chart directory. When omitted, use the same chart resolution as install.
        #[arg(long)]
        chart: Option<String>,
        /// Print the ordered plan without calling kubectl or helm.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
pub(crate) enum HooksAction {
    /// Install the tracked Git hooks in this checkout.
    Install,
}

#[derive(Subcommand)]
pub(crate) enum SecretsAction {
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
pub(crate) struct EvalSamplingArgs {
    /// Independent samples per case for live-model grading. Default: 1. A
    /// single sample is not proof of tier drift; raise this to distinguish
    /// variance from a real miss. Same policy on skill, local, and cluster.
    #[arg(long, default_value_t = 1, env = "CURIE_EVAL_SAMPLES", value_parser = clap::value_parser!(u32).range(1..))]
    pub(crate) samples: u32,
    /// How to reduce N sample verdicts. Default: majority.
    #[arg(long, default_value_t = curie::eval_sampling::AggregationPolicy::Majority, env = "CURIE_EVAL_AGGREGATION")]
    pub(crate) aggregation: curie::eval_sampling::AggregationPolicy,
    /// Pass@k threshold when --aggregation pass_at_k. Default: 1.
    #[arg(long = "pass-at-k", default_value_t = 1, env = "CURIE_EVAL_PASS_AT_K", value_parser = clap::value_parser!(u32).range(1..))]
    pub(crate) pass_at_k: u32,
}

impl EvalSamplingArgs {
    pub(crate) fn config(self) -> anyhow::Result<curie::eval_sampling::SampleConfig> {
        curie::eval_sampling::SampleConfig::new(self.samples, self.aggregation, self.pass_at_k)
    }
}

/// The memory-guidance flags shared by `local memory` and `cluster memory`
/// (#1461). `--guidance-from` and `--reset-guidance` are two different writes,
/// and none of them combines with add, delete, or channel selection.
#[derive(clap::Args, Debug, Default, Clone)]
pub(crate) struct MemoryGuidanceArgs {
    /// Show the guidance the agent gets beside its memory tools, and whether it
    /// is the platform default or operator-set
    /// (`GET /agents/{id}/memory/guidance`).
    #[arg(long, conflicts_with_all = ["add", "delete", "channel"])]
    pub(crate) guidance: bool,
    /// Replace the agent's memory guidance with this file's text
    /// (`PUT /agents/{id}/memory/guidance`). An empty file is refused.
    #[arg(
        long,
        value_name = "FILE",
        conflicts_with_all = ["reset_guidance", "add", "delete", "channel"]
    )]
    pub(crate) guidance_from: Option<std::path::PathBuf>,
    /// Remove operator guidance so the platform default applies again
    /// (`DELETE /agents/{id}/memory/guidance`).
    #[arg(long, conflicts_with_all = ["add", "delete", "channel"])]
    pub(crate) reset_guidance: bool,
}

impl MemoryGuidanceArgs {
    /// The one guidance action asked for, or `None` for the plain memory verb.
    /// A write wins over `--guidance`, whose output it already is.
    pub(crate) fn action(&self) -> Option<commands::MemoryGuidanceAction> {
        if let Some(path) = &self.guidance_from {
            Some(commands::MemoryGuidanceAction::SetFrom(path.clone()))
        } else if self.reset_guidance {
            Some(commands::MemoryGuidanceAction::Reset)
        } else if self.guidance {
            Some(commands::MemoryGuidanceAction::Show)
        } else {
            None
        }
    }
}
