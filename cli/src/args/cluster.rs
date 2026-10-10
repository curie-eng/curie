//! Arguments for `curie cluster`: the Kubernetes tier.

use super::*;

/// Subcommands of `curie cluster hook`.
#[derive(Subcommand)]
pub(crate) enum ClusterHookAction {
    /// Run one cron hook now, bypassing its schedule, and print the run record.
    Fire {
        /// Agent name or id.
        agent: String,
        /// Trigger name on the in-force bundle.
        name: String,
        /// How long to wait for the turn to settle, in seconds.
        #[arg(long, default_value_t = 120)]
        wait_secs: u64,
        /// Print what would be requested and exit without making a request.
        #[arg(long)]
        dry_run: bool,
    },
    /// Read one durable hook run, including a run that is still in flight.
    Record {
        /// Agent name or id.
        agent: String,
        /// Trigger name on the in-force bundle.
        name: String,
        /// Hook run id.
        id: String,
        /// Print what would be requested and exit without making a request.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
pub(crate) enum ClusterConsoleAction {
    /// Mint a Console login code using the selected release credential.
    Login {
        /// Subject bound to the Console session created from this code.
        #[arg(long, value_name = "SUBJECT", value_parser = parse_console_subject)]
        subject: String,
        /// Platform API base URL. Omit to reach the release API over loopback.
        #[arg(long, env = "CURIE_API_URL")]
        api_url: Option<String>,
        /// Kubernetes namespace of the release. Default: curie.
        #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        /// Helm release name. Default: curie.
        #[arg(long, default_value = "curie")]
        release: String,
        /// Print the request plan without minting a code.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
pub(crate) enum ClusterAction {
    /// Bootstrap access to the Curie Console.
    Console {
        #[command(subcommand)]
        action: ClusterConsoleAction,
    },
    /// Report value paths that differ between a release and pending Helm files.
    /// A nonempty report is advisory and exits successfully.
    LintValues {
        /// Pending Helm values files in application order.
        #[arg(short = 'f', long = "values", required = true, value_name = "FILE")]
        files: Vec<PathBuf>,
        /// Kubernetes namespace.
        #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        /// Helm release name.
        #[arg(long, default_value = "curie")]
        release: String,
    },
    /// Install or upgrade the Curie release via Helm (helm upgrade --install).
    /// By default it puts the UI and Langfuse on node ports for tailnet/LAN
    /// access; pass --no-expose to keep them ClusterIP-only. Set
    /// CURIE_CREDENTIALS to a supported model provider credential
    /// (CURIE_MODEL_CREDENTIALS is a deprecated alias) to install with the real
    /// model. A fresh install without it uses fake mode. A rerun preserves the
    /// recorded model configuration. Use --fake-model to explicitly downgrade
    /// to fake mode. An sk-ant- or sk-or- credential infers its provider egress
    /// when --allow-egress-host is absent. Other credential shapes remain sealed
    /// until their provider or a raw range is explicit. A controller owned by
    /// another Helm release is reused. A healthy unowned controller whose image
    /// matches the chart is reused. An unhealthy or different unowned controller
    /// stops the install and names the kubectl repair. A direct GET that
    /// returns NotFound applies security.gvisor.mode=off before the first
    /// install and prints the inference. A forbidden lookup still applies that
    /// override from the exact admission result and retries once. Every
    /// inferred value is printed.
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
        /// Adopt a pre-existing namespace that already has its own labels or
        /// objects. Without this, such a namespace is refused. The adoption is
        /// recorded on the namespace (curietech.ai/adopted-by, adopted-in, and
        /// the adopted-at/adopted-labels/adopted-contents annotations), and an
        /// adopted namespace is RETAINED by cluster down rather than deleted,
        /// so your pre-existing objects are never swept. It never adopts the
        /// shared agent-sandbox-system namespace or a terminating one, and it
        /// never admits a namespace whose contents cannot be read.
        #[arg(long)]
        adopt: bool,
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
        /// Install the end to end connector's identity on this release's
        /// cluster (ADR 0176 decision 4). Pass it only on the owner release of
        /// a separate TEST cluster. It renders a service account that may
        /// create and delete only namespaces carrying the connector's prefix
        /// and ownership label, may act only inside them, and holds no cluster
        /// scoped write; a ValidatingAdmissionPolicy enforces the prefix and
        /// label at the API server (Kubernetes 1.30 or newer). A later
        /// `cluster up` without this flag removes the identity.
        #[arg(long = "e2e-connector-identity")]
        e2e_connector_identity: bool,
    },
    /// Uninstall the release and sweep its runtime namespaces, running helm
    /// uninstall followed by kubectl delete namespace. The namespace delete
    /// is scoped to namespaces this release created, matched by both its
    /// release name and install namespace, so another release's namespaces
    /// on the same cluster are never touched. Pre-existing namespaces and
    /// the agents.x-k8s.io CRDs are left in place. The sweep waits at most
    /// 300s. If owned namespaces remain, the command exits 3 and does not
    /// remove finalizers.
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
        /// Assert the live Alembic revision instead of reading it from the API
        /// pod. The schema-window check still runs against this value. Use when
        /// every API replica is unexecutable (CrashLoopBackOff, Init,
        /// ImagePullBackOff).
        #[arg(long, value_name = "REV")]
        live_schema_revision: Option<String>,
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
    /// Plans, validates, checks the worker workload is reachable, checkpoints,
    /// migrates, applies, proves exact convergence, runs a target-version
    /// canary, and records the new known-good revision. The worker drain gate
    /// itself is the chart's own pre-upgrade Helm hook, which runs during
    /// apply and is observed at the convergence step. The operator does not
    /// pass Helm merge flags. A failed attempt either leaves the previous
    /// known-good version serving or returns one fail-forward command. See
    /// issue #2301.
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
        /// Helm chart. An explicit path or ref overrides the default. Default:
        /// the version-pinned release asset for `--to` on release builds; local
        /// `charts/curie` on dev builds.
        #[arg(long)]
        chart: Option<String>,
        /// Skip the interactive confirmation prompt.
        #[arg(long)]
        yes: bool,
        /// Print the redacted upgrade plan and exit without mutating.
        #[arg(long)]
        dry_run: bool,
        /// Apply contract or irreversible schema migrations. Without this flag
        /// the upgrade Job refuses those migrations before mutation so a patch
        /// rollback window stays intact (#2300).
        #[arg(long = "forward-only")]
        forward_only: bool,
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
    /// Connect or disconnect the cluster release from a real Slack workspace. Exactly one Curie release may connect to a given Slack app.
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
    /// Turn on the GitHub factory intake (label or mention triggers, repo
    /// allowlist, GitHub API egress) on an existing release.
    Factory {
        /// Select polling or webhook intake. Omit to keep the recorded mode.
        #[arg(long, value_parser = ["poll", "webhook"])]
        intake: Option<String>,
        /// Allow this GitHub repository (`owner/repo` or `owner/*`). Repeatable.
        /// Sets `api.githubRepoAllowlist`.
        #[arg(long = "repo", value_name = "OWNER/REPO")]
        repos: Vec<String>,
        /// Issue label that hands an issue to the factory. Required (here or
        /// already recorded on the release); no whitespace, 50 chars max.
        #[arg(long)]
        label: Option<String>,
        /// GitHub App login (the app slug, no '@', e.g. `my-app-slug`) whose
        /// comment mention hands an issue to the factory. Required (here or
        /// already recorded on the release).
        #[arg(long)]
        mention: Option<String>,
        /// Public base URL the factory links its progress cards to.
        #[arg(long, value_name = "URL")]
        card_base_url: Option<String>,
        /// File holding the GitHub webhook secret. Alternatively set
        /// CURIE_GITHUB_WEBHOOK_SECRET; never both. The secret never enters argv.
        #[arg(long, value_name = "PATH")]
        webhook_secret_file: Option<PathBuf>,
        /// Give this agent egress to the GitHub API ranges published at
        /// <api>/meta (port 443). Repeatable.
        #[arg(long = "github-api-egress", value_name = "AGENT")]
        github_api_egress: Vec<String>,
        /// Turn the factory intake off; changes nothing else.
        #[arg(long)]
        disable: bool,
        /// The factory GitHub App's numeric App ID. Pair with
        /// --private-key-file. Without both, and with no App recorded on the
        /// release, the command prints the App registration link and applies
        /// nothing. Mention, allowlist, and label default from the App.
        #[arg(long, value_name = "ID", requires = "private_key_file")]
        app_id: Option<String>,
        /// File holding the App's PEM private key. Stored in a Kubernetes
        /// Secret through kubectl stdin; the key never enters argv.
        #[arg(long, value_name = "PATH", requires = "app_id")]
        private_key_file: Option<PathBuf>,
        /// Print the registration link for this GitHub organization instead
        /// of your personal account.
        #[arg(long, value_name = "ORG")]
        org: Option<String>,
        /// Helm `--timeout` in seconds for the upgrade. Default: the release's
        /// own drain contract, the `curie.ai/minimum-helm-timeout-seconds`
        /// annotation on its pre-upgrade worker drain hook (worker
        /// deliveryBudgetSeconds + reserve + Job and grace slack), never below
        /// 900. A factory install with a 10800s budget needs about 21900s.
        #[arg(long, value_name = "SECONDS", value_parser = clap::value_parser!(u64).range(1..))]
        timeout: Option<u64>,
        /// Bind this digest-pinned runner image in the same helm upgrade as
        /// the intake settings (`AGENT=ghcr.io/example/runner@sha256:<digest>`).
        /// The chart is still `--chart`. The values mode stays `--reuse-values`.
        #[arg(long, value_name = "AGENT=IMAGE")]
        runner_image: Option<String>,
        /// Kubernetes namespace.
        #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        /// Helm release name.
        #[arg(long, default_value = "curie")]
        release: String,
        /// Helm chart. Default: the version-pinned chart release asset on release builds; local `charts/curie` on dev builds. Pass a path or ref to override.
        #[arg(long)]
        chart: Option<String>,
        /// Print the commands that would run and exit without executing.
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
        #[arg(long, env = "CURIE_API_KEY", hide_env_values = true, value_parser = message::cluster_api_key)]
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
        /// Send as this agent's Slack binding (ADR-0168 decision 8): the channel
        /// and the identity come from the binding. Pair with --channel when the
        /// agent answers on several.
        #[arg(long, value_name = "NAME")]
        agent: Option<String>,
        /// Kubernetes namespace of the release. Default: curie.
        #[arg(long, default_value = "curie", env = "CURIE_NAMESPACE")]
        namespace: String,
        /// Helm release name. Default: curie.
        #[arg(long, default_value = "curie")]
        release: String,
        /// Accepted so older command lines still parse. Text-graded cluster eval
        /// does not use it: replies go through the cluster message relay.
        #[arg(long)]
        listen_host: Option<String>,
        /// Accepted so older command lines still parse. Text-graded cluster eval
        /// does not use it: replies go through the cluster message relay.
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
        /// Local port the API port-forward binds. The relay poll and a missing
        /// channel lookup both use it. Default 0 is kernel-assigned, matching
        /// `cluster message`.
        #[arg(long, default_value_t = 0)]
        api_local_port: u16,
        /// Platform API key. It authenticates the relay poll and a missing-channel
        /// lookup. Omit to read the release's own key from its chart Secret.
        #[arg(long, env = "CURIE_API_KEY", hide_env_values = true, value_parser = message::cluster_api_key)]
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
        /// Print the kubectl port-forwards and relay poll a real run would use,
        /// and exit without executing anything.
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
        #[arg(long, conflicts_with_all = ["target", "agent", "env", "slack_channel", "identity"])]
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
        /// Target environment. Infers the sole active deployment on redeploy,
        /// or defaults to dev on a first deploy. A `--target` supplies it
        /// instead, and an explicit value wins over the target.
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
        /// Pin the reviewer model used at boot.
        #[arg(long)]
        reviewer_model: Option<String>,
        /// Clear the reviewer model override.
        #[arg(long)]
        clear_reviewer_model: bool,
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
        #[command(flatten)]
        conn: ClusterConn,
        /// Print what would be done and exit without making a request.
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
        /// Identity for a Slack surface (default: default), or the worker
        /// credential selector for a non-Slack adapter.
        #[arg(long)]
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
    /// Show, set, or clear who may talk to the bot through one surface
    /// (`PUT /agents/{id}/channels/callers`, ADR 0175).
    ///
    /// With neither `--set` nor `--clear` this shows the list. `--set`
    /// replaces it with exactly the ids given; `--clear` removes it so
    /// everyone may talk to the bot again. Anyone not on a list gets no
    /// reply at all. Editing the list does not revoke the surface's adapter
    /// token.
    Callers {
        /// Agent name or id.
        agent: String,
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
        #[command(flatten)]
        conn: ClusterConn,
        /// Print what would be done and exit without making a request.
        #[arg(long)]
        dry_run: bool,
    },
    /// Mint or inspect the mail adapter's channel token
    /// (`POST /channels/token`).
    ///
    /// Writes the Secret the adapter actually reads (chart Secret or
    /// `mailAdapter.channelTokenExistingSecret`), rolls the adapter, prints
    /// `exp`, and never prints the token. `--show-exp` is read-only.
    ChannelToken {
        /// Agent name or id that owns the binding.
        agent: String,
        /// Channel kind to mint for (e.g. email). Required unless --show-exp.
        #[arg(long)]
        kind: Option<String>,
        /// Channel address to mint for (e.g. the inbox). Required unless --show-exp.
        #[arg(long)]
        address: Option<String>,
        /// Token lifetime: 7d, 24h, 60m, or seconds. Default 7d; at most 7 days.
        #[arg(long, default_value = crate_channel_token::DEFAULT_TTL)]
        ttl: String,
        /// Print the installed token's exp and whether the platform still
        /// accepts it. Read-only: no mint, no write.
        #[arg(long)]
        show_exp: bool,
        #[command(flatten)]
        conn: ClusterConn,
        /// Print what would be done and exit without making a request.
        #[arg(long)]
        dry_run: bool,
    },
    /// Update an agent's budget, preserving unspecified limits.
    Budget {
        /// Agent name or id.
        agent: String,
        /// Daily spend cap in USD (BudgetConfig.max_usd_per_day). Must be > 0.
        #[arg(long, required_unless_present = "output_tokens")]
        limit: Option<f64>,
        /// Output token cap for each run (BudgetConfig.max_output_tokens_per_run).
        /// Must be > 0.
        #[arg(long, required_unless_present = "limit")]
        output_tokens: Option<u64>,
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
        /// The worker's composed key: kind[:identity]:channel:thread-ts, each
        /// part percent-encoded; identity only when the route names one other
        /// than `default` (e.g. slack:C0EXAMPLE1:1700000000.000100).
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
    /// List factory work item outcomes (`GET /work-items`), or read one with
    /// its live CI (`GET /work-items/{id}`).
    WorkItems {
        /// Work item id to read. Omit to list.
        #[arg(value_name = "ID")]
        id: Option<String>,
        /// Scope to one agent (name or id).
        #[arg(long, value_name = "NAME_OR_ID")]
        agent: Option<String>,
        #[command(flatten)]
        conn: ClusterConn,
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
        #[command(flatten)]
        conn: ClusterConn,
        /// Print what would be requested and exit without making a request.
        #[arg(long)]
        dry_run: bool,
    },
    /// Fire a declared cron hook now or read a durable run record.
    Hook {
        #[command(subcommand)]
        action: ClusterHookAction,
        #[command(flatten)]
        conn: ClusterHookConn,
    },
    /// List an agent's immutable versions (`GET /agents/{id}/versions`).
    Versions {
        #[command(flatten)]
        target: ClusterAgentTarget,
    },
    /// Manage an agent's webhook partitions and source bindings. Not cron triggers: see
    /// `schedules` and `hook fire`.
    Hooks {
        #[command(subcommand)]
        action: ClusterHooksAction,
    },
    /// List an agent's individual facts and memory log. `--channel KIND=ADDRESS`
    /// selects one bound channel; `--delete FACT_ID` removes one fact.
    /// `--add <content>` seeds an operator-authored record; a fresh session is
    /// required before it is injected at boot. `--guidance` shows the guidance
    /// the agent gets beside its memory tools, `--guidance-from <file>` replaces
    /// it and `--reset-guidance` restores the platform default.
    Memory {
        #[command(flatten)]
        target: ClusterAgentTarget,
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
        verb: ActionsCommand<ClusterConn>,
    },
    /// A protected hook's remediation policy: show it, apply a document, arm,
    /// disarm or remove it, or close an open circuit breaker. Writes run as the
    /// operator principal in CURIE_APPROVAL_PRINCIPAL_TOKEN.
    RemediationPolicy {
        #[command(subcommand)]
        verb: RemediationPolicyCommand<ClusterConn>,
    },
    /// The operator receipt of each remediation nomination: list them, or show
    /// one with its stage, authority and code. Read only.
    Remediation {
        #[command(subcommand)]
        verb: RemediationCommand<ClusterConn>,
    },
    /// Record a remediation qualification and run its verifier against an
    /// allowed target. Writes run as the operator principal in
    /// CURIE_APPROVAL_PRINCIPAL_TOKEN.
    RemediationQualification {
        #[command(subcommand)]
        verb: RemediationQualificationCommand<ClusterConn>,
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
        #[command(flatten)]
        recovery: ApprovalRecoveryArgs,
    },
}
