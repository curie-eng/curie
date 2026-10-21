//! Arguments for `curie dev` contributor commands.

use super::*;

/// Forge kinds `curie dev forge-fixture` can drive (#4398).
#[derive(Copy, Clone, Debug, ValueEnum)]
pub(crate) enum ForgeFixtureKind {
    Gitlab,
    #[value(name = "bitbucket-dc")]
    BitbucketDc,
}

impl ForgeFixtureKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Gitlab => "gitlab",
            Self::BitbucketDc => "bitbucket-dc",
        }
    }
}

/// Lifecycle actions for a forge fixture (#4398).
#[derive(Copy, Clone, Debug, ValueEnum)]
pub(crate) enum ForgeFixtureAction {
    Up,
    #[value(name = "self-test")]
    SelfTest,
    Down,
}

impl ForgeFixtureAction {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::SelfTest => "self-test",
            Self::Down => "down",
        }
    }
}

#[derive(Subcommand)]
pub(crate) enum DevAction {
    /// Check the committed change before opening or updating a pull request.
    Preflight {
        /// Run only the fast tier; the full tier is the default.
        #[arg(long)]
        fast: bool,
        /// Origin branch to compare against (for example, main or next).
        #[arg(long, default_value = "main")]
        base: String,
        /// Print the selected checks without running them.
        #[arg(long)]
        dry_run: bool,
        /// File containing the proposed pull request body.
        #[arg(long, conflicts_with = "fast")]
        pr_body: Option<PathBuf>,
        /// Proposed pull request title used by the body guard.
        #[arg(long, requires = "pr_body")]
        title: Option<String>,
    },
    /// Manage hooks for this source checkout.
    Hooks {
        #[command(subcommand)]
        action: HooksAction,
    },
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
    /// Two Helm releases on one kind cluster, one Slack app, owner-only approval without retry-until-acked (#2307, `bash cli/scripts/two-release-approval-e2e.sh`).
    TwoReleaseApprovalE2e,
    /// Drive the dark factory against a disposable install on a named kube
    /// context, a real GitHub App and a fixture repository (#2966,
    /// `python3 tools/factory-e2e/factory_e2e.py`). `preflight` installs the
    /// candidate's published images with factory intake on, tunnels the api
    /// webhook, labels one issue, asserts the delivery is accepted and a
    /// WorkItem is admitted, then undoes every change and writes JSON evidence.
    /// `run --scenario <name>` adds one scenario driver after the preflight.
    /// Every identity comes from CURIE_FACTORY_* variables or files.
    FactoryE2e {
        /// `preflight` or `run --scenario <name>`, then driver flags; see
        /// `curie dev factory-e2e -- --help`.
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Start, self-test, or stop a local forge fixture (#4398,
    /// `bash cli/scripts/forge-fixture.sh`). Each fixture is
    /// `tools/forge-fixtures/<kind>/fixture.py` and keeps its state under
    /// `--state-dir`. A kind whose fixture is not installed fails with a
    /// "not installed" message and creates no state.
    ForgeFixture {
        /// Forge to run: `gitlab` or `bitbucket-dc`.
        #[arg(value_enum)]
        kind: ForgeFixtureKind,
        /// `up` starts the fixture, `self-test` checks a running one, `down`
        /// stops it and removes its state.
        #[arg(value_enum)]
        action: ForgeFixtureAction,
        /// Directory that holds the fixture's state and connection details.
        #[arg(long)]
        state_dir: PathBuf,
        /// Host name or address the fixture advertises to clients, such as
        /// cluster pods that reach it over the network.
        #[arg(long)]
        advertise_host: Option<String>,
        /// Port the fixture listens on; the fixture picks one when omitted.
        #[arg(long)]
        port: Option<u16>,
    },
    /// Serve or record a scripted Anthropic Messages endpoint (#3814).
    /// `serve` replays a transcript and fails on an unexpected request.
    /// `record` proxies to a provider and writes the transcript.
    ModelScript {
        /// `serve` or `record`, followed by endpoint flags.
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Check the OpenRouter credit on CURIE_CREDENTIALS before a graded ladder spends a build.
    ModelCredit,
    /// Evaluate conversational learning on a private local stack (#4780).
    ///
    /// Owns the whole lifecycle: a private compose project, the pinned actor
    /// and judge models, one campaign budget, report.json and report.md in
    /// the output directory, and teardown of everything it created.
    LearningEval {
        /// Number of fresh repetitions of the 24-scenario corpus.
        #[arg(long, default_value_t = 1)]
        repetitions: u32,
        /// Campaign budget in USD for every paid call, at most 5.00.
        #[arg(long, default_value = "5.00")]
        budget_usd: String,
        /// Minimum per-class precision with complete six-class coverage.
        #[arg(long, default_value_t = 0.9)]
        precision_threshold: f64,
        /// Minimum per-class recall with complete six-class coverage.
        #[arg(long, default_value_t = 0.8)]
        recall_threshold: f64,
        /// Maximum distractor false-write rate.
        #[arg(long, default_value_t = 0.05)]
        falsewrite_threshold: f64,
        /// Maximum correction duplicates.
        #[arg(long, default_value_t = 0)]
        duplicate_threshold: u32,
        /// Learned-minus-removed held-out success difference must exceed this.
        #[arg(long, default_value_t = 0.0)]
        heldout_control_improvement_threshold: f64,
        /// Exact final deliveries required per due positive schedule.
        #[arg(long, default_value_t = 1)]
        scheduled_delivery_threshold: u32,
        /// Directory for report.json, report.md and turn context.
        #[arg(long)]
        output_dir: PathBuf,
        /// Validate inputs and print the plan without services or paid calls.
        #[arg(long)]
        dry_run: bool,
    },
    /// Validate recorded learning-eval evidence without paid calls.
    LearningEvalVerify {
        /// Which validator to run.
        #[arg(value_parser = ["report", "calibration", "negative", "cleanup"])]
        validator: String,
        /// Path to the recorded report.json.
        report: PathBuf,
    },
    /// Drive generic OIDC console login end to end against a real Dex IdP
    /// (#2908, #3800, `python3 tools/oidc-e2e/oidc_e2e.py`). Boots this
    /// checkout's API and console UI as an isolated compose project beside Dex,
    /// logs in through the console's /api proxy, and asserts PKCE and cookie
    /// flags, callback binding and replay, logout's Origin check, required
    /// claims, issuer binding, the rate limit, and that the login-code console
    /// login still resolves an approval with OIDC off. Removes everything on
    /// exit and writes JSON evidence. Needs Docker; no credentials.
    OidcE2e {
        /// Driver flags (`--build`, `--keep`, `--evidence <path>`); see
        /// `curie dev oidc-e2e -- --help`.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Serve a TLS GitHub fixture or capture public check lifecycle recordings.
    GithubStub {
        /// `serve` or `capture`, followed by fixture flags.
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
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
    /// Isolated worker/runner recovery drills (#2425,
    /// `bash cli/scripts/recovery-drill.sh`): worker death mid-turn, runner
    /// death, a configured deadline plus follow-up, Valkey outage, and API
    /// restart on a task-owned local or cluster install. Refuses the permanent
    /// soak namespace/release. Checkout-only.
    RecoveryDrill {
        /// `local` compose stack or a task-owned `cluster` Helm install.
        #[arg(long, default_value = "local")]
        surface: String,
        /// Scenario to run, or `all`.
        #[arg(long, default_value = "all")]
        scenario: String,
        /// Recovery bound in seconds after a healthy worker replacement is available.
        #[arg(long, default_value_t = 120)]
        bound_seconds: u32,
        /// Allow a kube context other than `k8scratch` (cluster surface only).
        #[arg(long)]
        force: bool,
    },
    /// Disposable two-worker cluster proof for lease-expiry reclaim (#2453,
    /// `bash cli/scripts/lease-expiry-cluster-proof.sh`): default
    /// `reclaim_min_idle_ms` 900000, three concurrent test Slack mentions, in-place
    /// placeholder edits, XPENDING delivery increments, the no-lease 900 s
    /// backstop control, and SIGKILL takeover. Refuses the permanent soak.
    /// Checkout-only. Never shortens the backstop.
    LeaseExpiryClusterProof {
        /// Allow recreating a leftover task-owned kind cluster of the same name.
        #[arg(long)]
        force: bool,
        /// Leave the kind cluster and Helm release running after the proof.
        #[arg(long)]
        keep: bool,
        /// Guard checks only: soak refusal, default backstop, missing Slack.
        #[arg(long)]
        self_test: bool,
    },
    /// Isolated retained-upgrade and interrupted-upgrade recovery drill (#2426,
    /// `bash cli/scripts/upgrade-drill.sh`): published v0.8.6 CLI/chart/images
    /// on a task-owned kind install, candidate CLI upgrade, drain/apply
    /// interrupt recovery, leftover-hook non-quiesce, compatible rollback that
    /// serves a new turn, and incompatible 0.8.4 schema rollback refused before
    /// Helm mutates. Refuses the permanent soak. Checkout-only. Live
    /// provider/channel rows fail closed when credential references are absent.
    UpgradeDrill {
        /// Scenario to run, or `all` (0.8.6 baseline matrix).
        #[arg(long, default_value = "all")]
        scenario: String,
        /// Also run the published v0.8.7 predecessor happy-path (latest stable
        /// when it differs from the required 0.8.6 baseline).
        #[arg(long)]
        also_predecessor: bool,
        /// Recreate a leftover task-owned kind cluster of the same name.
        #[arg(long)]
        force: bool,
        /// Leave the kind cluster and Helm release running after the drill.
        #[arg(long)]
        keep: bool,
        /// Guard checks only: soak refusal, checksum pins, missing live creds.
        #[arg(long)]
        self_test: bool,
    },
    /// Isolated next-train `cluster upgrade` matrix (#2590,
    /// `bash cli/scripts/cluster-upgrade-matrix.sh`): published v0.8.8 on a
    /// task-owned kind install, packaged candidate charts, fail-at and
    /// interrupt-after hooks, migration crash retry, image/object convergence,
    /// and compatible plus published-window rollback. Refuses the permanent
    /// soak. Checkout-only.
    ClusterUpgradeMatrix {
        /// Scenario to run, or `all`.
        #[arg(long, default_value = "all")]
        scenario: String,
        /// Recreate a leftover task-owned kind cluster of the same name.
        #[arg(long)]
        force: bool,
        /// Leave the kind cluster and Helm release running after the matrix.
        #[arg(long)]
        keep: bool,
        /// Guard checks only: soak refusal, checksum pins, mutator verb.
        #[arg(long)]
        self_test: bool,
    },
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
    /// Bounded synthetic restore drill for #2427: back up postgres, bundles,
    /// mail SQLite, and Valkey from a disposable compose install, restore onto
    /// a distinct target, and refuse an omitted or corrupt component
    /// (`bash cli/scripts/restore-drill.sh`). Not a production backup product
    /// and not an RPO/RTO claim.
    RestoreDrill {
        /// Validate an existing backup directory without starting a stack.
        #[arg(long)]
        check_backup: Option<PathBuf>,
        /// JSON object of separately supplied key names. Required with --check-backup.
        #[arg(long)]
        supplied_config: Option<PathBuf>,
        /// Omit this required component and expect the completeness guard to refuse.
        #[arg(long)]
        negative: Option<String>,
    },
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
    /// Read-only evaluator for the seven-day / 200-canary release gate (#2430).
    ///
    /// Scores a private evidence ledger against pinned window, candidate,
    /// workload, and percentile-method fields. A synthetic qualifying fixture
    /// passes `--self-test`; independently missing each campaign criterion
    /// fails closed with that criterion id. Fixture or source proof cannot
    /// qualify as a live release result. Does not start a seven-day campaign,
    /// rotate credentials, mutate the permanent soak, merge, or close #2430.
    /// Checkout-only.
    ReleaseAccept {
        /// JSON evidence ledger. Evaluated as a live release result.
        #[arg(long, value_name = "PATH", conflicts_with = "self_test")]
        ledger: Option<PathBuf>,
        /// Run committed fixture controls: qualifying pass plus each
        /// independent miss. Does not evaluate a live window.
        #[arg(long)]
        self_test: bool,
    },
}
