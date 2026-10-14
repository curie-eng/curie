//! Arguments for `curie skill`: the local runner tier.

use super::*;

#[derive(Subcommand)]
pub(crate) enum SkillAction {
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
        #[command(flatten)]
        recovery: ApprovalRecoveryArgs,
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
        commands::WORK_ITEMS_REASON, commands::WORK_ITEMS_ALT,
    ))]
    WorkItems {
        /// Accepts any arguments so every form reaches the exit-4 capability
        /// refusal instead of a clap usage error (#2577, like #1955).
        #[arg(trailing_var_arg = true, allow_hyphen_values = true, hide = true)]
        _rest: Vec<String>,
    },
    #[command(about = format!(
        "Not available at this tier: {}; {}",
        commands::SCHEDULES_REASON, commands::SCHEDULES_ALT,
    ))]
    Schedules {
        /// Accepts any arguments so every form reaches the exit-4 capability
        /// refusal instead of a clap usage error.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true, hide = true)]
        _rest: Vec<String>,
    },
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
    /// Run a declared cron hook against the local runner, or report that a
    /// durable schedule or record is unavailable at this tier (ADR-0099).
    Hook {
        #[command(subcommand)]
        action: SkillHookAction,
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

#[derive(Subcommand)]
pub(crate) enum SkillHookAction {
    /// Run the named cron hook now against the local runner. No durable record.
    Fire {
        /// Trigger name from `.claude-plugin/plugin.json`.
        name: String,
        /// Plugin bundle directory.
        #[arg(long, default_value = ".")]
        plugin_dir: PathBuf,
        /// Runner base URL. Default: the URL recorded by `skill up`.
        #[arg(long)]
        url: Option<String>,
    },
    /// Not available at this tier: there is no scheduler.
    Schedule,
    /// Not available at this tier: there is no hook run record.
    Record,
}
