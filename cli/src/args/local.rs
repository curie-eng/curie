//! Arguments for `curie local`: the compose stack tier.

use super::*;

/// Subcommands of `curie local hook`.
#[derive(Subcommand)]
pub(crate) enum LocalHookAction {
    /// Run one cron hook now, bypassing its schedule, and print the run record.
    Fire {
        /// Agent name or id.
        agent: String,
        /// Trigger name on the in-force bundle.
        name: String,
        /// How long to wait for the turn to settle, in seconds.
        #[arg(long, default_value_t = 120)]
        wait_secs: u64,
        #[arg(
            long,
            default_value = message::DEFAULT_LOCAL_API_URL,
            env = "CURIE_API_URL"
        )]
        api_url: String,
        #[arg(long, default_value = message::DEFAULT_API_KEY, env = "CURIE_API_KEY", hide_env_values = true, value_parser = message::api_key_or_default)]
        api_key: String,
        /// Print what would be requested and exit without making a request.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
pub(crate) enum LocalConsoleAction {
    /// Mint a Console login code using the stored local installation credential.
    Login {
        /// Subject bound to the Console session created from this code.
        #[arg(long, value_name = "SUBJECT", value_parser = parse_console_subject)]
        subject: String,
        /// Platform API base URL.
        #[arg(long, default_value = message::DEFAULT_LOCAL_API_URL, env = "CURIE_API_URL")]
        api_url: String,
        /// Print the request plan without minting a code.
        #[arg(long)]
        dry_run: bool,
    },
}

/// Subcommands of `curie local`.
#[derive(Subcommand)]
pub(crate) enum LocalAction {
    /// Bootstrap access to the Curie Console.
    Console {
        #[command(subcommand)]
        action: LocalConsoleAction,
    },
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
        /// Compose project. Default: `curie`. Isolation requires this with ordered `-f` files and matching host endpoints.
        #[arg(long, env = "COMPOSE_PROJECT_NAME")]
        project: Option<String>,
        /// Ordered compose files. Repeat `-f` for a base then override. Default: version-pinned `compose.release.yaml` from the remote on release builds; local `compose.dev.yaml` on dev builds.
        #[arg(short = 'f', long = "file", action = clap::ArgAction::Append)]
        files: Vec<String>,
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
        /// Compose project. Default: `curie`. Isolation requires this with ordered `-f` files and matching host endpoints.
        #[arg(long, env = "COMPOSE_PROJECT_NAME")]
        project: Option<String>,
        /// Ordered compose files. Repeat `-f` for a base then override. Default: version-pinned `compose.release.yaml` from the remote on release builds; local `compose.dev.yaml` on dev builds.
        #[arg(short = 'f', long = "file", action = clap::ArgAction::Append)]
        files: Vec<String>,
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
        /// Compose project. Default: `curie`. Isolation requires this with ordered `-f` files and matching host endpoints.
        #[arg(long, env = "COMPOSE_PROJECT_NAME")]
        project: Option<String>,
        /// Ordered compose files. Repeat `-f` for a base then override. Default: version-pinned `compose.release.yaml` from the remote on release builds; local `compose.dev.yaml` on dev builds.
        #[arg(short = 'f', long = "file", action = clap::ArgAction::Append)]
        files: Vec<String>,
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
        /// Compose project. Default: `curie`. Isolation requires this with ordered `-f` files and matching host endpoints.
        #[arg(long, env = "COMPOSE_PROJECT_NAME")]
        project: Option<String>,
        /// Ordered compose files. Repeat `-f` for a base then override. Default: version-pinned `compose.release.yaml` from the remote on release builds; local `compose.dev.yaml` on dev builds.
        #[arg(short = 'f', long = "file", action = clap::ArgAction::Append)]
        files: Vec<String>,
        /// Print the docker compose command and exit without executing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Connect or disconnect the local compose stack from a real Slack workspace. Exactly one Curie release may connect to a given Slack app.
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
        /// Compose project. Default: `curie`. Isolation requires this with ordered `-f` files and matching host endpoints.
        #[arg(long, env = "COMPOSE_PROJECT_NAME")]
        project: Option<String>,
        /// Ordered compose files. Repeat `-f` for a base then override. Default: version-pinned `compose.release.yaml` from the remote on release builds; local `compose.dev.yaml` on dev builds.
        #[arg(short = 'f', long = "file", action = clap::ArgAction::Append)]
        files: Vec<String>,
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
        /// Send as this agent's Slack binding (ADR-0168 decision 8): the channel
        /// and the identity come from the binding. Pair with --channel when the
        /// agent answers on several.
        #[arg(long, value_name = "NAME")]
        agent: Option<String>,
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
        #[arg(long, env = "CURIE_API_KEY", hide_env_values = true, default_value = message::DEFAULT_API_KEY, value_parser = message::api_key_or_default)]
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
        /// Send as this agent's Slack binding (ADR-0168 decision 8): the channel
        /// and the identity come from the binding. Pair with --channel when the
        /// agent answers on several.
        #[arg(long, value_name = "NAME")]
        agent: Option<String>,
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
        #[arg(long, env = "CURIE_API_KEY", hide_env_values = true, default_value = message::DEFAULT_API_KEY, value_parser = message::api_key_or_default)]
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
        #[arg(long, default_value = "curie-dev-key", env = "CURIE_API_KEY", hide_env_values = true, value_parser = message::api_key_or_default)]
        api_key: String,
        /// Slack channel to bind the agent to. On first create it defaults to
        /// C0LOCALDEV; on redeploy the channel is ADDED when the agent is not
        /// already bound to it, never moved and never removed, so omitting the
        /// flag leaves the deployed agent's binding set untouched.
        #[arg(long)]
        slack_channel: Option<String>,
        /// Identity (bot) the Slack binding this deploy writes speaks through
        /// (ADR-0168 decision 8). Overrides the target's `identity`; omitted,
        /// the target's is used, else the installation's own. Needs a channel:
        /// --slack-channel, or the target's slack_channel.
        #[arg(long, value_name = "NAME")]
        identity: Option<String>,
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
        /// MCP server. The value never appears in argv. Repeatable. A hosted
        /// connector's Bearer secret (`bearer_secret`, or its single `secrets:`
        /// name) is bound automatically (from the value this deploy already
        /// resolved for the connector) so the runner can expand the derived
        /// header and drop the name from the sandbox env; this flag is for names
        /// beyond that (#2503, #2559).
        #[arg(long = "secret", value_name = "NAME")]
        secret: Vec<String>,
    },
    /// List an agent's immutable versions (`GET /agents/{id}/versions`).
    Versions {
        #[command(flatten)]
        target: AgentTarget<LocalTier>,
    },
    /// Manage an agent's hook configuration and signing secret.
    Hooks {
        #[command(subcommand)]
        action: LocalHooksAction,
    },
    /// List an agent's individual facts and memory log. `--channel KIND=ADDRESS`
    /// selects one bound channel; `--delete FACT_ID` removes one fact.
    /// `--add <content>` seeds an operator-authored record; a fresh session is
    /// required before it is injected at boot. `--guidance` shows the guidance
    /// the agent gets beside its memory tools, `--guidance-from <file>` replaces
    /// it and `--reset-guidance` restores the platform default.
    Memory {
        #[command(flatten)]
        target: AgentTarget<LocalTier>,
        /// Append this content as an operator-authored memory record.
        #[arg(long, value_name = "CONTENT", conflicts_with_all = ["delete", "channel"])]
        add: Option<String>,
        /// Delete this fact from agent memory, or the selected channel.
        #[arg(long, value_name = "FACT_ID", conflicts_with_all = ["add", "guidance", "guidance_from", "reset_guidance"])]
        delete: Option<String>,
        /// Select the facts of this exact bound channel pair.
        #[arg(long, value_name = "KIND=ADDRESS", conflicts_with_all = ["add", "guidance", "guidance_from", "reset_guidance"])]
        channel: Option<String>,
        #[command(flatten)]
        guidance: MemoryGuidanceArgs,
    },
    /// The action ledger and its undo: list and read an agent's recorded
    /// actions, ask for an undo, and read an execution's receipt.
    Actions {
        #[command(subcommand)]
        verb: ActionsCommand<LocalObservabilityConn>,
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
        #[command(flatten)]
        recovery: ApprovalRecoveryArgs,
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
        /// Pin this agent's run deadline in seconds (60-10800; platform
        /// default is 1800s).
        #[arg(long)]
        execution_deadline: Option<String>,
        /// Clear the execution-deadline override back to the platform default.
        #[arg(long)]
        clear_execution_deadline: bool,
        /// Pin runner cpu, memory, and ephemeral-storage. JSON object with
        /// requests and limits. Null on the API means the chart block.
        #[arg(long)]
        runner_resources: Option<String>,
        /// Clear the runner resource override back to the chart block.
        #[arg(long)]
        clear_runner_resources: bool,
        /// Turn the agent's remember/update/forget memory tools on or off
        /// (`memory_writes`, #1461). Off stops saving only: stored agent and
        /// channel memory stays readable. Takes effect at the next sandbox boot.
        #[arg(long, value_name = "on|off", value_parser = ["on", "off"])]
        memory_writes: Option<String>,
        #[arg(long, default_value = "http://localhost:28000", env = "CURIE_API_URL")]
        api_url: String,
        #[arg(long, default_value = "curie-dev-key", env = "CURIE_API_KEY", hide_env_values = true, value_parser = message::api_key_or_default)]
        api_key: String,
        #[arg(long)]
        dry_run: bool,
    },
    /// Read or set one agent's publication policy (`PATCH /agents/{id}`).
    ///
    /// With no change flags this inspects. `approve` is the default and keeps
    /// the human gate. `auto` lets the platform resolve that same approval.
    PublicationPolicy {
        /// Agent name or id.
        agent: String,
        /// `approve` or `auto`.
        #[arg(long, value_parser = ["approve", "auto"])]
        policy: Option<String>,
        /// Open the pull request as a draft. Only applied while policy is auto.
        #[arg(long, conflicts_with = "no_draft")]
        draft: bool,
        /// Open the pull request ready for review.
        #[arg(long)]
        no_draft: bool,
        /// Required branch prefix, ending in `/`.
        #[arg(long, conflicts_with = "clear_branch_prefix")]
        branch_prefix: Option<String>,
        /// Remove the branch prefix.
        #[arg(long)]
        clear_branch_prefix: bool,
        #[arg(long, default_value = "http://localhost:28000", env = "CURIE_API_URL")]
        api_url: String,
        #[arg(long, default_value = "curie-dev-key", env = "CURIE_API_KEY", hide_env_values = true, value_parser = message::api_key_or_default)]
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
        /// Identity for a Slack surface (default: default), or the worker
        /// credential selector for a non-Slack adapter.
        #[arg(long)]
        adapter: Option<String>,
        /// Remove this surface, as KIND=ADDRESS. The API refuses to remove an
        /// agent's final surface.
        #[arg(long, value_name = "KIND=ADDRESS", conflicts_with = "add")]
        remove: Option<String>,
    },
    /// Show, set, or clear who may talk to the bot through one surface
    /// (`PUT /agents/{id}/channels/callers`, ADR 0175).
    ///
    /// With neither `--set` nor `--clear` this shows the list. `--set`
    /// replaces it with exactly the ids given; `--clear` removes it so
    /// everyone may talk to the bot again. Anyone not on a list gets no
    /// reply at all. Editing the list does not revoke the surface's adapter
    /// token.
    Callers {
        #[command(flatten)]
        target: AgentTarget<LocalTier>,
        /// The surface, as KIND=ADDRESS (e.g. slack=C0EXAMPLE1).
        #[arg(long, value_name = "KIND=ADDRESS")]
        surface: String,
        /// The Slack identity whose route to select when several share the
        /// surface (default: the one route on it).
        #[arg(long)]
        adapter: Option<String>,
        /// Allow exactly these caller ids, comma separated: Slack user or bot
        /// ids for a Slack surface, bare email addresses for an email one.
        /// Replaces the whole list.
        #[arg(
            long,
            value_name = "ID[,ID...]",
            value_delimiter = ',',
            conflicts_with = "clear"
        )]
        set: Vec<String>,
        /// Remove the list, so everyone may talk to the bot again.
        #[arg(long)]
        clear: bool,
    },
    /// Update an agent's budget, preserving unspecified limits.
    Budget {
        /// Agent name or id.
        agent: String,
        /// Daily spend cap in USD. Must be > 0.
        #[arg(long, required_unless_present = "output_tokens")]
        limit: Option<f64>,
        /// Output token cap for each run. Must be > 0.
        #[arg(long, required_unless_present = "limit")]
        output_tokens: Option<u64>,
        #[arg(long, default_value = "http://localhost:28000", env = "CURIE_API_URL")]
        api_url: String,
        #[arg(long, default_value = "curie-dev-key", env = "CURIE_API_KEY", hide_env_values = true, value_parser = message::api_key_or_default)]
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
        #[arg(long, default_value = "curie-dev-key", env = "CURIE_API_KEY", hide_env_values = true, value_parser = message::api_key_or_default)]
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
        #[arg(long, default_value = "curie-dev-key", env = "CURIE_API_KEY", hide_env_values = true, value_parser = message::api_key_or_default)]
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
        /// The worker's composed key: kind[:identity]:channel:thread-ts, each
        /// part percent-encoded; identity only when the route names one other
        /// than `default` (e.g. slack:C0EXAMPLE1:1700000000.000100).
        #[arg(long, value_name = "THREAD_KEY")]
        thread_key: String,
        #[arg(long, default_value = "http://localhost:28000", env = "CURIE_API_URL")]
        api_url: String,
        #[arg(long, default_value = "curie-dev-key", env = "CURIE_API_KEY", hide_env_values = true, value_parser = message::api_key_or_default)]
        api_key: String,
        /// Confirm the action; it interrupts any live turn on the thread.
        #[arg(long)]
        yes: bool,
        #[arg(long)]
        dry_run: bool,
    },
    /// List factory work item outcomes (`GET /work-items`), or read one with
    /// its live CI (`GET /work-items/{id}`).
    WorkItems {
        /// Work item id to read. Omit to list.
        #[arg(value_name = "ID")]
        id: Option<String>,
        /// Scope to one agent (name or id).
        #[arg(long, value_name = "NAME_OR_ID")]
        agent: Option<String>,
        #[arg(
            long,
            default_value = message::DEFAULT_LOCAL_API_URL,
            env = "CURIE_API_URL"
        )]
        api_url: String,
        #[arg(long, default_value = message::DEFAULT_API_KEY, env = "CURIE_API_KEY", hide_env_values = true, value_parser = message::api_key_or_default)]
        api_key: String,
        /// Print what would be requested and exit without making a request.
        #[arg(long)]
        dry_run: bool,
    },
    /// List each cron hook on the in-force deployment (`GET /schedules`).
    Schedules {
        /// Scope to one agent (name or id). Omit to list every deployed agent.
        #[arg(long, value_name = "NAME_OR_ID")]
        agent: Option<String>,
        /// Pause one named cron hook on the selected agent.
        #[arg(
            long,
            value_name = "HOOK",
            conflicts_with = "resume",
            requires = "agent"
        )]
        pause: Option<String>,
        /// Resume one named cron hook on the selected agent.
        #[arg(
            long,
            value_name = "HOOK",
            conflicts_with = "pause",
            requires = "agent"
        )]
        resume: Option<String>,
        #[arg(
            long,
            default_value = message::DEFAULT_LOCAL_API_URL,
            env = "CURIE_API_URL"
        )]
        api_url: String,
        #[arg(long, default_value = message::DEFAULT_API_KEY, env = "CURIE_API_KEY", hide_env_values = true, value_parser = message::api_key_or_default)]
        api_key: String,
        /// Print what would be requested and exit without making a request.
        #[arg(long)]
        dry_run: bool,
    },
    /// Fire a declared cron hook now (`POST /agents/{agent}/hooks/{name}/fire`).
    Hook {
        #[command(subcommand)]
        action: LocalHookAction,
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
        #[arg(long, default_value = message::DEFAULT_API_KEY, env = "CURIE_API_KEY", hide_env_values = true, value_parser = message::api_key_or_default)]
        api_key: String,
        /// Confirm this destructive action.
        #[arg(long)]
        yes: bool,
        /// Print what would be done and exit without making a request.
        #[arg(long)]
        dry_run: bool,
    },
}
