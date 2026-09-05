//! `curie doctor`: what is set up, what is missing, and the command that fixes it.
//!
//! A first-time user learns the required inputs one failure at a time --
//! `skill up` succeeds and `skill message` fails on a credential; a deploy works
//! and the next `git push` does nothing because no ingress exists. The list is
//! about ten items long, and nothing states it up front.
//!
//! Documentation is the obvious answer and the wrong one: a checklist in a
//! README goes stale the moment a flag changes, and it cannot tell an operator
//! which items THEY are missing. This can, and it reports what is actually
//! observable rather than what a doc claims.
//!
//! Two rules the checks follow:
//!
//! - **Names, never values.** A credential is reported by the variable that
//!   holds it. This output is pasted into issues and chat.
//! - **Absent is not broken.** Someone on the laptop rung has no cluster and is
//!   not misconfigured. Cluster checks report `NotApplicable` rather than
//!   failing, so the output stays readable at every rung.

use serde::Serialize;

use crate::modelpin::{classify, PinStatus};

/// What one check found.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// Configured and usable.
    Ok,
    /// Genuinely missing, and something the user is trying to do needs it.
    Missing,
    /// Not needed at the rung this install is on.
    NotApplicable,
}

impl State {
    pub fn glyph(self) -> &'static str {
        match self {
            State::Ok => "ok  ",
            State::Missing => "MISS",
            State::NotApplicable => "--  ",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Check {
    /// Stable identifier, for a consumer gating on a specific check.
    pub id: &'static str,
    pub title: &'static str,
    pub state: State,
    /// What was observed. Never a credential value.
    pub detail: String,
    /// The exact command that fixes it, when it is fixable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fix: Option<String>,
}

/// What the `helm list` probe established about the release.
///
/// `Option<(String, String)>` could not express "we do not know", so a laptop
/// with no helm, an expired cluster credential and a genuinely empty namespace
/// all printed the same two lines (#1358). Every variant is diagnostic-free by
/// construction: helm's stderr is an arbitrary external line that can carry
/// an `Authorization` header, an exec-plugin's argv, or a token-bearing URL, and
/// this report is pasted into issues. No prefix denylist can enumerate that, so
/// no subprocess stderr is carried here at all -- only the bounded, structured
/// `chart` field that this report exists to display (#1348).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ReleaseProbe {
    /// `helm` is not on PATH, so the release was never inspected at all.
    HelmMissing,
    /// helm ran and did not answer: a nonzero exit, or a zero exit whose stdout
    /// is not the release list this code understands. The `fix` hands the
    /// operator the diagnostic to run themselves.
    ProbeFailed,
    /// helm answered, and reports no deployed release by this name here.
    #[default]
    NotInstalled,
    /// helm answered, and this release is deployed.
    Installed { chart: String },
}

/// Everything the checks reason about, gathered once.
///
/// Separating observation from judgement is what makes the judgement testable:
/// every check below is a pure function of this struct, so the interesting
/// cases -- half-configured, laptop-only, fully wired -- are unit tests rather
/// than cluster fixtures.
#[derive(Debug, Clone, Default)]
pub struct Facts {
    /// NAME of the model credential found, never its value.
    pub model_credential: Option<String>,
    /// Where it came from, for the detail line.
    pub model_credential_source: Option<String>,
    /// Verbatim `CURIE_MODEL` from the invoking shell. Lowest precedence: the
    /// shell is not a declared producer of this variable (#1950). A value here
    /// is not a claim that the id is valid, only that something set it.
    pub model_shell: Option<String>,
    /// The model the release's sandboxes boot, read from its COMPUTED helm
    /// values, so a chart default the operator never supplied is still
    /// observed. See [`runner_model_from_values`] for which key that is.
    pub model_release_default: Option<String>,
    /// WHICH chart key [`Facts::model_release_default`] was read from. The
    /// chart branches between two of them, and a report that names the wrong
    /// one hands the operator a `--set` the chart ignores. `None` means the key
    /// was not observed -- `gather` always sets both together -- and falls back
    /// to the key a default install boots from.
    pub model_release_key: Option<ReleaseModelKey>,
    /// Whether the release currently boots the runner's SCRIPTED FAKE model,
    /// which makes [`Facts::model_release_default`] configured-but-unused. See
    /// [`release_fake_model`]: the chart's fake-model flag and the model id are
    /// independent template arms, so a default install renders BOTH
    /// `CURIE_FAKE_MODEL=1` and `CURIE_MODEL=claude-sonnet-5` and the id alone
    /// is not proof the pod boots it -- the same "names a model the pod does
    /// not boot" defect class as #1950 itself.
    pub model_release_fake: bool,
    /// `(agent name, model)` for every agent carrying a per-agent override,
    /// forwarded as `CURIE_MODEL` at sandbox boot. Highest precedence.
    pub model_agent_overrides: Vec<(String, String)>,
    /// The `(namespace, release)` this run was invoked with. An observed fact
    /// about the run, kept on `Facts` so `evaluate` stays pure and the fix
    /// string can name the release actually diagnosed rather than `curie/curie`
    /// (#1358 item 1).
    ///
    /// ONE field for one fact: #1950 and #1358 each arrived with their own
    /// spelling of it, and two fields carrying the same fact drift a writer at
    /// a time until the model-pin fix and the cluster fixes name different
    /// releases. `None` is a run or fixture that never exercised targeting;
    /// [`target`] renders that as [`DEFAULT_TARGET`] so a cluster fix stays
    /// runnable, while `model_pin_fix` keeps its own `<ns>`/`<release>`
    /// placeholders (#1950).
    pub target: Option<(String, String)>,
    /// Non-secret provider inferred from the bound `CURIE_CREDENTIALS` value.
    /// The credential itself is deliberately discarded during observation.
    pub model_credential_provider: Option<&'static str>,
    pub docker_ok: bool,
    /// Plugin name from `.claude-plugin/plugin.json` in the working directory.
    pub bundle_name: Option<String>,
    pub kube_context: Option<String>,
    /// What the `helm list` probe established. `NotInstalled` is the `Default`
    /// only because it is the state a fixture that says nothing means.
    pub release: ReleaseProbe,
    /// The CIDRs `security.networkPolicy.allowedEgress` records, in recorded
    /// order. Empty means either nothing recorded or nothing this reader can
    /// reproduce -- the two are never distinguished, because a partial list is
    /// exactly what must not be emitted.
    pub sandbox_egress_cidrs: Vec<String>,
    /// Whether `cluster up` can re-supply that allowlist EXACTLY. One fact
    /// rather than two conditions: the consumer's only question is "can the fix
    /// reproduce this, yes or no", and splitting it invites a partial-emission
    /// branch. `false` by default, so a fixture silent about egress can never
    /// produce egress flags.
    pub sandbox_egress_is_reproducible: bool,
    /// Whether the release records a non-empty Slack app token. Presence only --
    /// the value is never read.
    pub slack_app_token: bool,
    /// Whether the release records a non-empty Slack bot token.
    pub slack_bot_token: bool,
    /// Which clone credential the release carries, if any.
    pub clone_credential: Option<String>,
    /// Every agent and its repository binding. `None` means the platform API
    /// was not reached, which is a fact to report rather than a failure -- the
    /// other checks need only kubectl and helm.
    pub agents: Option<Vec<(String, Option<String>)>>,
    /// How the API is reachable from outside, if it is. `None` means neither
    /// mechanism the chart knows about is in place -- which is NOT proof it is
    /// unreachable, since a load balancer or tunnel in front is invisible here.
    pub api_exposure: Option<String>,
}

fn ok(id: &'static str, title: &'static str, detail: impl Into<String>) -> Check {
    Check {
        id,
        title,
        state: State::Ok,
        detail: detail.into(),
        fix: None,
    }
}

fn missing(
    id: &'static str,
    title: &'static str,
    detail: impl Into<String>,
    fix: impl Into<String>,
) -> Check {
    Check {
        id,
        title,
        state: State::Missing,
        detail: detail.into(),
        fix: Some(fix.into()),
    }
}

fn skipped(id: &'static str, title: &'static str, detail: impl Into<String>) -> Check {
    Check {
        id,
        title,
        state: State::NotApplicable,
        detail: detail.into(),
        fix: None,
    }
}

/// The target a fix names when `Facts` carries none, and what `resolve_target`
/// falls back to when neither a flag nor a `curie.yaml` supplies one.
const DEFAULT_TARGET: &str = "curie";

/// The `release` detail when helm itself is absent.
///
/// `HelmMissing` and `NotInstalled` both render as (cluster `Ok`, release
/// `Missing`), so `summary` cannot tell them apart from the check states alone
/// and matches this sentinel instead. Shared rather than written out twice, so a
/// later wording tweak cannot silently break the verdict with every test green.
const HELM_ABSENT_DETAIL: &str = "helm is not installed, so nothing about a cluster release \
                                  could be read";

/// Classify a `helm list` run. Pure, so every outcome is a unit test rather than
/// a cluster fixture -- and it takes the exit status and stdout ONLY. helm's
/// stderr is never passed in, because nothing it says may reach a payload.
fn classify_release_probe(
    helm_present: bool,
    ok: bool,
    stdout: &str,
    release: &str,
) -> ReleaseProbe {
    if !helm_present {
        return ReleaseProbe::HelmMissing;
    }
    if !ok {
        return ReleaseProbe::ProbeFailed;
    }
    let stdout = stdout.trim();
    // helm prints `null`, or nothing at all, for a namespace holding no
    // releases. That is a known output shape, not a failure to read one.
    if stdout.is_empty() || stdout == "null" {
        return ReleaseProbe::NotInstalled;
    }
    let Ok(serde_json::Value::Array(listed)) = serde_json::from_str::<serde_json::Value>(stdout)
    else {
        // Exit zero this reader cannot parse is NOT an absence claim. Turning
        // "I could not read the answer" into "your release is gone" is the
        // #1354 shape, and it is what `ops::fetch_existing_values` already fails
        // closed on: "the release state is unknown".
        return ReleaseProbe::ProbeFailed;
    };
    // By name, never by position: the namespace can hold other releases, and
    // taking the first element reports someone else's chart as this one's.
    let Some(entry) = listed
        .iter()
        .find(|e| e.get("name").and_then(serde_json::Value::as_str) == Some(release))
    else {
        return ReleaseProbe::NotInstalled;
    };
    match entry.get("chart").and_then(serde_json::Value::as_str) {
        Some(chart) if !chart.is_empty() => ReleaseProbe::Installed {
            chart: chart.to_string(),
        },
        // Listed, but without the one field this reader needs: a helm whose
        // `list -o json` shape moved must make doctor say it could not tell.
        _ => ReleaseProbe::ProbeFailed,
    }
}

/// The namespace and release to name in a fix string.
///
/// A `Facts` built by a test that never exercised targeting carries no target,
/// and `--namespace  --release ` is not a runnable command. This is a RENDERING
/// fallback only; resolution itself lives in `resolve_target`.
fn target(f: &Facts) -> (&str, &str) {
    match &f.target {
        Some((namespace, release)) => (namespace.as_str(), release.as_str()),
        None => (DEFAULT_TARGET, DEFAULT_TARGET),
    }
}

/// The `curie cluster <subcommand>` prefix every targeted fix opens with. Five
/// fixes name the same release, and a prefix spelled five times is a prefix that
/// drifts a flag at a time; each site appends only what is its own.
fn targeted(subcommand: &str, namespace: &str, release: &str) -> String {
    format!("curie cluster {subcommand} --namespace {namespace} --release {release}")
}

/// The recovery command for a missing cluster release. Provider egress follows
/// the same credential-prefix map as `cluster up`; an absent or unrecognized
/// credential leaves egress sealed rather than guessing Anthropic.
///
/// It deliberately does NOT re-supply a recorded egress allowlist the way the
/// webhook fix does: it fires only when there is no release, so there is nothing
/// recorded to preserve, and `--allow-egress-host` is the right flag for a fresh
/// install (#1813).
fn missing_release_recovery(namespace: &str, release: &str, provider: Option<&str>) -> String {
    let mut command = targeted("up", namespace, release);
    if let Some(provider) = provider {
        command.push_str(&format!(" --allow-egress-host {provider}"));
    }
    command
}

/// Which chart key the release's model came from.
///
/// `charts/curie/templates/agent-sandbox.yaml` renders exactly one
/// `CURIE_MODEL` entry and picks its value on a branch, so the release default
/// has two possible homes and only one of them is live on any given install.
/// Carrying the key is what keeps the report and the fix honest: while the
/// in-cluster inference service is deployed the chart IGNORES
/// `agentSandbox.runner.model` entirely, so naming that key on a
/// `curie cluster up --local-model` install both mislabels the source and
/// prints a `--set` that cannot change the model in force.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReleaseModelKey {
    /// `agentSandbox.runner.model` -- what a default install boots.
    #[default]
    Runner,
    /// `inference.model` -- live only while `inference.deploy` is truthy.
    Inference,
}

impl ReleaseModelKey {
    /// The dotted key exactly as the chart and `--set` spell it.
    fn chart_key(self) -> &'static str {
        match self {
            ReleaseModelKey::Runner => "agentSandbox.runner.model",
            ReleaseModelKey::Inference => "inference.model",
        }
    }
}

/// Where a model id was observed. Three sources, and they can disagree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelSource {
    /// A per-agent override, carrying the agent's real name. The worker
    /// forwards it as `CURIE_MODEL` at sandbox boot, so it wins outright.
    Agent(String),
    /// The release's own default, from its computed helm values, carrying the
    /// chart key it was read from -- the two keys are not interchangeable, see
    /// [`ReleaseModelKey`].
    ReleaseDefault(ReleaseModelKey),
    /// The invoking shell's `CURIE_MODEL`. Lowest, because the shell is not a
    /// declared producer of that variable.
    Shell,
}

impl ModelSource {
    /// How the source reads in the detail line. An id with no label is not
    /// something an operator can go and change.
    fn label(&self) -> String {
        match self {
            ModelSource::Agent(name) => format!("agent \"{name}\""),
            ModelSource::ReleaseDefault(key) => {
                format!("release default {}", key.chart_key())
            }
            ModelSource::Shell => "CURIE_MODEL".to_string(),
        }
    }
}

/// The model this install will actually boot, and what else claimed otherwise.
#[derive(Debug, Clone)]
pub struct InForceModel {
    /// Which source won precedence.
    pub source: ModelSource,
    /// The id that source carries, trimmed and non-empty.
    pub id: String,
    /// Every other source whose id DIFFERS from the one in force. An identical
    /// id is not a disagreement.
    pub disagreeing: Vec<(ModelSource, String)>,
}

/// Which model the install boots, out of the three places one can be set. Pure.
///
/// Precedence is the boot path, not a preference: a per-agent override beats
/// the release default, which beats the invoking shell. `None` only when no
/// source yields a model at all.
///
/// When several agents set DIFFERENT models there is no single answer, so the
/// tie-break is chosen to make the check safe rather than pretty: rank the
/// WEAKEST pin first (`Floating`, then `Unrecognized`, then `Pinned`), so the
/// check can never report clean while one agent floats, then break the
/// remaining ties on agent name ascending so the report is stable across runs.
fn resolve_model(f: &Facts) -> Option<InForceModel> {
    /// #229's footgun at three sources instead of one: an exported-but-empty
    /// value is not a configured model, and letting one win precedence would
    /// resolve to `Unset` and report not-applicable on an install that boots
    /// something.
    fn cleaned(id: &str) -> Option<String> {
        let id = id.trim();
        (!id.is_empty()).then(|| id.to_string())
    }

    fn weakness(id: &str) -> u8 {
        match classify(Some(id)) {
            PinStatus::Floating { .. } => 0,
            PinStatus::Unrecognized { .. } => 1,
            _ => 2,
        }
    }

    let mut agents: Vec<(String, String)> = f
        .model_agent_overrides
        .iter()
        .filter_map(|(name, id)| cleaned(id).map(|id| (name.clone(), id)))
        .collect();
    agents.sort_by(|a, b| a.0.cmp(&b.0));
    let release = f.model_release_default.as_deref().and_then(cleaned);
    let shell = f.model_shell.as_deref().and_then(cleaned);

    // `min_by_key` keeps the first of several equal minima, and the list is
    // already name-ascending, so this IS the documented tie-break.
    let (source, id) = match agents.iter().min_by_key(|(_, id)| weakness(id)) {
        Some((name, id)) => (ModelSource::Agent(name.clone()), id.clone()),
        None => match &release {
            Some(id) => (
                ModelSource::ReleaseDefault(f.model_release_key.unwrap_or_default()),
                id.clone(),
            ),
            None => (ModelSource::Shell, shell.clone()?),
        },
    };

    let mut disagreeing: Vec<(ModelSource, String)> = agents
        .iter()
        .filter(|(_, other)| *other != id)
        .map(|(name, other)| (ModelSource::Agent(name.clone()), other.clone()))
        .collect();
    for (candidate, other) in [
        (
            ModelSource::ReleaseDefault(f.model_release_key.unwrap_or_default()),
            &release,
        ),
        (ModelSource::Shell, &shell),
    ] {
        if let Some(other) = other {
            if *other != id {
                disagreeing.push((candidate, other.clone()));
            }
        }
    }

    Some(InForceModel {
        source,
        id,
        disagreeing,
    })
}

/// The model the release's sandboxes boot, out of its computed helm values.
///
/// It is NOT a single path. `charts/curie/templates/agent-sandbox.yaml` renders
/// exactly one `CURIE_MODEL` env entry and picks its value on a branch:
/// `.Values.inference.model` when `.Values.inference.deploy` is TRUTHY by Go's
/// rule (the `curie cluster up --local-model` shape; see [`helm_truthy`], which
/// is not the same test as "is the boolean true"), otherwise
/// `.Values.agentSandbox.runner.model`. Reproducing that branch is what keeps
/// this from naming a model the pod never boots.
///
/// It returns the key alongside the id: the label and the fix both have to
/// name the key that is actually live, or the operator is told to `--set` one
/// the chart ignores.
///
/// An empty or whitespace-only value at either key is not a configured model
/// (#229) and falls through. The read ends in `as_str`, so a non-string at the
/// path reads as absent -- safe here because the key is a string in the chart
/// and in every `--set` form, and deliberately not widened (#1358 item 4).
fn runner_model_from_values(values: &serde_json::Value) -> Option<(String, ReleaseModelKey)> {
    fn at(values: &serde_json::Value, path: &[&str]) -> Option<String> {
        let mut node = values;
        for key in path {
            node = node.get(key)?;
        }
        let id = node.as_str()?.trim();
        (!id.is_empty()).then(|| id.to_string())
    }

    if helm_truthy(values.get("inference").and_then(|i| i.get("deploy"))) {
        if let Some(model) = at(values, &["inference", "model"]) {
            return Some((model, ReleaseModelKey::Inference));
        }
    }
    at(values, &["agentSandbox", "runner", "model"]).map(|id| (id, ReleaseModelKey::Runner))
}

/// Whether the release's sandboxes boot the runner's SCRIPTED FAKE model.
///
/// `charts/curie/templates/agent-sandbox.yaml` renders `CURIE_FAKE_MODEL=1` on
/// `and $runner.fakeModel (not .Values.inference.deploy)`, and picks
/// `CURIE_MODEL` on a SEPARATE arm. The two are independent, so on the chart's
/// shipped defaults (`agentSandbox.runner.fakeModel: true`) a release renders
/// BOTH `CURIE_FAKE_MODEL=1` and `CURIE_MODEL=claude-sonnet-5` -- the id is
/// configured and the pod boots the scripted fake instead. Reporting that id
/// as the model in force with no caveat is the very defect #1950 exists to
/// kill, one level down.
///
/// Both legs go through [`helm_truthy`] rather than `as_bool`, for the reason
/// spelled out there: a value that reaches Helm as a string still takes the
/// template branch, and disagreeing with Helm is how doctor names a model the
/// pod does not boot.
fn release_fake_model(values: &serde_json::Value) -> bool {
    let fake = values
        .get("agentSandbox")
        .and_then(|a| a.get("runner"))
        .and_then(|r| r.get("fakeModel"));
    let inference = values.get("inference").and_then(|i| i.get("deploy"));
    helm_truthy(fake) && !helm_truthy(inference)
}

/// Go template truthiness, mirrored for the one value the chart branches on.
///
/// `agent-sandbox.yaml` gates on `if .Values.inference.deploy`, and Go's
/// `empty` is false only for `false`, a zero number, an empty string and nil.
/// Reading it with `as_bool` instead made a value that arrives as a STRING --
/// a generic `--set-string inference.deploy=true`, say -- send Helm down the
/// inference branch while doctor went down the runner one, and doctor then
/// reported a model the pod does not boot.
///
/// Yes, this means the string `"false"` is truthy. That is Helm's rule, not a
/// bug here: a non-empty string is non-empty whatever it spells.
///
/// This ladder exists TWICE in the crate: see
/// `classify_existing_secret_field` in `cli/src/github_app.rs`, which mirrors
/// the same Go rule for `api.githubAppExistingSecret`. They must agree, so a
/// change to one belongs in both.
fn helm_truthy(value: Option<&serde_json::Value>) -> bool {
    match value {
        None | Some(serde_json::Value::Null) => false,
        Some(serde_json::Value::Bool(b)) => *b,
        Some(serde_json::Value::Number(n)) => n.as_f64() != Some(0.0),
        Some(serde_json::Value::String(s)) => !s.is_empty(),
        // Go calls an EMPTY list or map empty exactly as it does `""`, so both
        // are falsy here and only a populated one is true. A scalar key like
        // this one should never carry either, but agreeing with Go costs
        // nothing and disagreeing with the copy in `github_app.rs` does.
        Some(serde_json::Value::Array(a)) => !a.is_empty(),
        Some(serde_json::Value::Object(o)) => !o.is_empty(),
    }
}

/// The command that pins the model at the source it is actually in force from.
///
/// Bare and runnable, with angle-bracket placeholders only: a fix string that
/// names a flag which does not exist fails for whoever pastes it (#1813). Note
/// `curie cluster up` has NO `--model` -- that flag belongs to `skill up` -- so
/// the release default is set through `--set <key>=`, where the key is the one
/// the release actually reads ([`ReleaseModelKey`]) rather than always
/// `agentSandbox.runner.model`, which a local-inference install ignores. The
/// namespace and release come from the run itself rather than defaulting to
/// `curie/curie` (#1358 item 1).
fn model_pin_fix(f: &Facts, source: &ModelSource) -> String {
    match source {
        ModelSource::Agent(name) => {
            format!("curie cluster overrides {name} --model <dated-snapshot-id>")
        }
        ModelSource::ReleaseDefault(key) => {
            let (namespace, release) = match &f.target {
                Some((namespace, release)) => (namespace.as_str(), release.as_str()),
                None => ("<ns>", "<release>"),
            };
            let key = key.chart_key();
            format!(
                "curie cluster up --namespace {namespace} --release {release} \
                 --set {key}=<dated-snapshot-id>"
            )
        }
        ModelSource::Shell => "export CURIE_MODEL=<dated-snapshot-id>".to_string(),
    }
}

/// The one not-applicable detail: no source yields a model at all.
///
/// It has to say WHICH sources were looked at, or it is indistinguishable from
/// the check being blind again -- and it must not claim there is no per-agent
/// override when the platform API was never reached to look.
fn no_model_determined(f: &Facts) -> String {
    let overrides = if f.agents.is_some() {
        "no per-agent override"
    } else {
        "per-agent overrides could not be read because the platform API was not reached"
    };
    format!(
        "no model determined: {overrides}, no release default \
         agentSandbox.runner.model or inference.model, and no CURIE_MODEL"
    )
}

/// The caveat for a report that could not see the highest-precedence source.
///
/// `Facts::agents` is `None` only when the platform API was not reached at all,
/// which is NOT the same fact as "reached, and no agent sets a model" -- and a
/// per-agent override outranks every other source. Reporting a clean pinned
/// release default while an agent quietly carries a floating one is the exact
/// failure #1950 exists to kill, so the check says what it could not see. The
/// state stays as it is (absent is not broken); the honesty is in the detail.
fn unread_agent_overrides(f: &Facts, in_force: &ModelSource) -> &'static str {
    if f.agents.is_none() && !matches!(in_force, ModelSource::Agent(_)) {
        "; per-agent model overrides could not be read because the platform \
         API was not reached, so an agent may boot a different model"
    } else {
        ""
    }
}

/// The caveat for an id the release has configured but does NOT currently boot.
///
/// See [`release_fake_model`] for why a real id and the fake model coexist. The
/// id is NOT suppressed and the state does NOT change: it is exactly what
/// applies the moment fake model is turned off, and the credential story has
/// its own `model-credential` check. Only the release source can be in this
/// position -- an agent override or a shell value is forwarded as `CURIE_MODEL`
/// regardless of what the chart's fake-model arm renders.
fn release_fake_model_clause(f: &Facts, in_force: &ModelSource) -> &'static str {
    if f.model_release_fake && matches!(in_force, ModelSource::ReleaseDefault(_)) {
        "; the sandbox currently boots the runner's scripted fake model, so \
         this id is configured but not in use"
    } else {
        ""
    }
}

/// The exposure fix, which is a full `cluster up` and therefore drops every
/// value nothing re-supplies -- including the sandbox's egress allowlist, which
/// is how following doctor's advice sealed a working release's model path.
///
/// The allowlist is reproduced in full or not at all. `ops::up_value_plan`
/// rewrites every `--allow-web-egress` CIDR as exactly one TCP/443 rule, so a
/// rule shaped any other way would be COERCED, and a capped list would drop
/// entries outright; both NARROW a live NetworkPolicy, and a caveat is read
/// after the policy is already broken. There is deliberately no middle branch.
fn webhook_recovery(f: &Facts, namespace: &str, release: &str) -> String {
    let mut command = targeted("up", namespace, release);
    command.push_str(" --set api.ingress.enabled=true --set api.ingress.host=<host>");
    if f.sandbox_egress_is_reproducible {
        // `--allow-web-egress` rather than `--allow-egress-host`: a CIDR read
        // back off a release cannot be reversed to a provider name, and
        // ADR-0114 errors when an explicit provider list omits the detected one.
        for cidr in &f.sandbox_egress_cidrs {
            command.push_str(&format!(" --allow-web-egress {cidr}"));
        }
    } else {
        command.push_str(&format!(
            "   (WARNING: this release records a sandbox egress allowlist this command \
             cannot reproduce; running it as-is would narrow the policy. Read it first \
             with `helm get values {release} -n {namespace}` and re-supply those rules \
             deliberately.)"
        ));
    }
    command
}

/// Judge the gathered facts. Pure.
pub fn evaluate(f: &Facts) -> Vec<Check> {
    let mut out = Vec::new();

    out.push(match (&f.model_credential, &f.model_credential_source) {
        (Some(name), Some(src)) => ok(
            "model-credential",
            "Model credential",
            format!("{name} ({src})"),
        ),
        (Some(name), None) => ok("model-credential", "Model credential", name.clone()),
        _ => missing(
            "model-credential",
            "Model credential",
            "none found",
            "export CURIE_CREDENTIALS=sk-ant-...   (or `curie secrets set CURIE_CREDENTIALS`; \
             `curie skill up --fake-model` needs none)",
        ),
    });

    // The model the install actually BOOTS, not the one the invoking shell
    // happens to name (#1950). `not_applicable` is reserved for the single case
    // where no source yields a model at all, and says which three were looked
    // at -- otherwise it is indistinguishable from the check being blind again.
    out.push(match resolve_model(f) {
        None => skipped("model-pin", "Model pin", no_model_determined(f)),
        Some(m) => {
            let source = m.source.label();
            // Nothing appended when nothing disagrees: a detail ending on a
            // dangling "other sources disagree:" reads as a truncated report.
            let disagreement = if m.disagreeing.is_empty() {
                String::new()
            } else {
                let named: Vec<String> = m
                    .disagreeing
                    .iter()
                    .map(|(source, id)| format!("{} = {id}", source.label()))
                    .collect();
                format!("; other sources disagree: {}", named.join(", "))
            };
            // What the report could NOT see, appended to every state: a
            // silently-unread override outranks whatever is named above.
            let unread = unread_agent_overrides(f, &m.source);
            // The id is configured but not what the pod boots. Placed after
            // the source label and before the disagreement/unread clauses, so
            // it qualifies the id it is about and nothing else.
            let fake = release_fake_model_clause(f, &m.source);
            match classify(Some(m.id.as_str())) {
                PinStatus::Pinned { id, date } => ok(
                    "model-pin",
                    "Model pin",
                    format!(
                        "{id} (snapshot {date}), in force from \
                         {source}{fake}{disagreement}{unread}"
                    ),
                ),
                // Ok rather than Missing, deliberately: a floating name works,
                // and a working install must not report as unready. What is at
                // risk is reproducibility, so this carries a fix without
                // failing the check.
                PinStatus::Floating { id } => Check {
                    id: "model-pin",
                    title: "Model pin",
                    state: State::Ok,
                    detail: format!(
                        "{id} is a floating name, in force from {source}; the \
                         provider can repoint it at new weights with no change \
                         here, and no gate would see it{fake}{disagreement}{unread}"
                    ),
                    fix: Some(model_pin_fix(f, &m.source)),
                },
                // No fix, and no claim about whether the id moves: the shape
                // rule cannot read this one, and a wrong fix string is worse
                // than none (#1813).
                PinStatus::Unrecognized { id } => ok(
                    "model-pin",
                    "Model pin",
                    format!(
                        "{id}, in force from {source}; this check reads a model \
                         id by shape alone and does not recognise this one, so \
                         it cannot say whether the id moves{fake}{disagreement}{unread}"
                    ),
                ),
                // Unreachable: `resolve_model` yields only trimmed, non-empty
                // ids. Report it as no model rather than assert it away.
                PinStatus::Unset => skipped("model-pin", "Model pin", no_model_determined(f)),
            }
        }
    });

    out.push(if f.docker_ok {
        ok("docker", "Docker", "running")
    } else {
        missing(
            "docker",
            "Docker",
            "not reachable",
            "start Docker Desktop, or install it: https://docs.docker.com/get-docker/",
        )
    });

    out.push(match &f.bundle_name {
        Some(name) => ok("bundle", "Bundle in this directory", name.clone()),
        None => missing(
            "bundle",
            "Bundle in this directory",
            "no .claude-plugin/plugin.json",
            "curie init my-agent && cd my-agent",
        ),
    });

    // Everything below needs a cluster. Without one this install is on the
    // laptop rung, which is a complete way to use Curie -- so these report as
    // not-applicable rather than as failures.
    let Some(context) = &f.kube_context else {
        out.push(skipped(
            "cluster",
            "Cluster",
            "no kube context — laptop rung only, which is fine",
        ));
        for (id, title) in [
            ("release", "Curie release"),
            ("slack", "Slack"),
            ("clone-credential", "Clone credential"),
            ("webhook", "Webhook exposure"),
            ("repo-binding", "Repo binding"),
        ] {
            out.push(skipped(id, title, "needs a cluster"));
        }
        return out;
    };
    let (namespace, release) = target(f);

    // helm is what answers "is there a release here", so these two checks may
    // only assert what the probe proved. `Ok` on Cluster means a kube context is
    // configured; the detail says whether anything actually contacted the
    // cluster, and `Missing` is reserved for the one outcome where doctor has
    // positive evidence that something did not work. Before this, a missing
    // helm, an expired credential and an empty namespace printed the same two
    // lines (#1358).
    let downstream_blocked = match &f.release {
        ReleaseProbe::HelmMissing => {
            out.push(ok(
                "cluster",
                "Cluster",
                format!(
                    "{context} (from the kubeconfig; helm is not installed, so the \
                     cluster was not contacted)"
                ),
            ));
            out.push(missing(
                "release",
                "Curie release",
                HELM_ABSENT_DETAIL,
                "install helm, then re-run: https://helm.sh/docs/intro/install/",
            ));
            Some("needs helm to read the release")
        }
        ReleaseProbe::ProbeFailed => {
            out.push(missing(
                "cluster",
                "Cluster",
                format!(
                    "{context} — from the kubeconfig; `helm list` did not answer, so the \
                     cluster was not confirmed reachable"
                ),
                // The diagnostic the operator runs themselves, which is what
                // replaces echoing helm's message: it enumerates the causes
                // categorically without claiming which one occurred, and
                // without carrying a byte doctor did not author.
                format!(
                    "run `helm list -n {namespace}` and `kubectl config current-context` to \
                     see why — an expired credential, an unreachable API server and an RBAC \
                     denial all land here — then re-run `curie doctor`"
                ),
            ));
            out.push(skipped(
                "release",
                "Curie release",
                "`helm list` did not answer, so nothing about the release is known",
            ));
            Some("the release could not be inspected")
        }
        ReleaseProbe::NotInstalled => {
            out.push(ok("cluster", "Cluster", format!("{context} (reached)")));
            // "no DEPLOYED release" is what plain `helm list` actually shows:
            // it filters out pending and failed ones. `helm list -a` was the
            // alternative and is worse -- it also lists releases kept with
            // --keep-history, so a deleted release would read as present, and
            // over-claiming presence is the wrong direction for a check whose
            // fix is "install it".
            out.push(missing(
                "release",
                "Curie release",
                format!(
                    "helm reports no deployed release named {release} in this namespace \
                     (a pending or failed release is not listed)"
                ),
                missing_release_recovery(namespace, release, f.model_credential_provider),
            ));
            Some("needs a release")
        }
        ReleaseProbe::Installed { chart } => {
            out.push(ok("cluster", "Cluster", format!("{context} (reached)")));
            out.push(ok(
                "release",
                "Curie release",
                format!("{release} ({chart})"),
            ));
            None
        }
    };

    // Everything below reads the release's own values, so an outcome that did
    // not reach one skips them with the reason it did not -- not with a verdict
    // about a release nobody could see.
    if let Some(reason) = downstream_blocked {
        for (id, title) in [
            ("slack", "Slack"),
            ("clone-credential", "Clone credential"),
            ("webhook", "Webhook exposure"),
            ("repo-binding", "Repo binding"),
        ] {
            out.push(skipped(id, title, reason));
        }
        return out;
    }

    // Socket mode needs BOTH tokens, and this check used to read only the bot
    // token while claiming both were recorded -- so a half-configured release
    // read as wired and the bot silently never connected.
    let slack_fix = || {
        targeted("comms --slack", namespace, release) + " --app-token xapp-... --bot-token xoxb-..."
    };
    out.push(match (f.slack_app_token, f.slack_bot_token) {
        (true, true) => ok("slack", "Slack", "app and bot tokens recorded"),
        (true, false) => missing(
            "slack",
            "Slack",
            "only the app token is recorded — socket mode needs both",
            slack_fix(),
        ),
        (false, true) => missing(
            "slack",
            "Slack",
            "only the bot token is recorded — socket mode needs both",
            slack_fix(),
        ),
        (false, false) => missing("slack", "Slack", "no tokens recorded", slack_fix()),
    });

    out.push(match &f.clone_credential {
        Some(which) => ok("clone-credential", "Clone credential", which.clone()),
        None => missing(
            "clone-credential",
            "Clone credential",
            "none — a private repo cannot be cloned, so git-push deploys will fail",
            targeted("github-app", namespace, release) + " --app-id <id> --private-key ./key.pem",
        ),
    });

    // Exposure has more than one shape, and asserting otherwise cries wolf on a
    // working install: sre-bot serves its webhook on a NodePort with no ingress
    // at all, and an early version of this check called that broken. A doctor
    // that is wrong about a working setup is worse than no doctor, because it
    // teaches people to ignore it.
    out.push(match &f.api_exposure {
        Some(how) => ok("webhook", "Webhook exposure", how.clone()),
        None => missing(
            "webhook",
            "Webhook exposure",
            "no ingress and no NodePort — if a load balancer or tunnel fronts the API, \
             this check cannot see it and you can ignore this",
            webhook_recovery(f, namespace, release),
        ),
    });

    // The binding decides whether a push reaches this agent at all. A push for
    // an agent with none matches nothing and is answered `ignored` -- nothing is
    // logged, so the only symptom is a green delivery in GitHub and no deploy.
    out.push(match &f.agents {
        None => skipped(
            "repo-binding",
            "Repo binding",
            "platform API not reached — could not discover it from the release; \
             pass --api-url/--api-key to include this",
        ),
        Some(agents) if agents.is_empty() => missing(
            "repo-binding",
            "Repo binding",
            "no agents deployed yet — a push matches nothing and is silently ignored",
            targeted("deploy", namespace, release) + " --plugin-dir . --repo <owner>/<name>",
        ),
        Some(agents) => {
            let unbound: Vec<&str> = agents
                .iter()
                .filter(|(_, repo)| repo.is_none())
                .map(|(name, _)| name.as_str())
                .collect();
            if unbound.is_empty() {
                ok(
                    "repo-binding",
                    "Repo binding",
                    format!("{} agent(s), all bound", agents.len()),
                )
            } else {
                missing(
                    "repo-binding",
                    "Repo binding",
                    format!(
                        "unbound: {} — a push for these matches no agent and is \
                         silently ignored",
                        unbound.join(", ")
                    ),
                    targeted("deploy", namespace, release)
                        + " --plugin-dir . --repo <owner>/<name>   (binds an agent that has \
                           none; it will NOT rebind one already pointing at a different \
                           repository)",
                )
            }
        }
    });

    out
}

/// Point at the guided path when there is more than one thing to fix.
///
/// A single missing item needs no signpost: the `→ fix` line beside it is the
/// whole answer. Several is different -- the fixes have an order, some depend
/// on values the operator has not collected yet, and reading eight lines and
/// sequencing them is exactly the work the guided workflow already does.
///
/// `curie` with no arguments opens that workflow. It is discoverable in
/// principle and invisible in practice, because a first-time user reaching for
/// help types `curie --help` and gets an alphabetical list of eighteen verbs
/// with `interactive` eleventh.
pub fn guidance(checks: &[Check]) -> Option<String> {
    let missing = checks.iter().filter(|c| c.state == State::Missing).count();
    if missing < 2 {
        return None;
    }
    Some(format!(
        "{missing} things to set up. Run `curie` with no arguments for a guided \
         walkthrough, or fix them one at a time with the commands above."
    ))
}

/// The one-line verdict: what this install can do right now.
///
/// Deliberately capability-shaped rather than a count. "3 of 8 checks passed"
/// tells an operator nothing; "you can run locally but not deploy" tells them
/// where they are.
pub fn summary(checks: &[Check]) -> String {
    let state = |id: &str| checks.iter().find(|c| c.id == id).map(|c| c.state);
    let has = |id: &str| state(id) == Some(State::Ok);
    let release_detail = checks
        .iter()
        .find(|c| c.id == "release")
        .map(|c| c.detail.as_str());

    if !has("bundle") {
        return "No bundle here. Start with `curie init my-agent`.".to_string();
    }
    if !has("docker") {
        return "Docker is not reachable, so nothing can run locally yet.".to_string();
    }
    if !has("model-credential") {
        return "Ready to run offline (`curie skill up --fake-model`). A model \
                credential is needed for real replies."
            .to_string();
    }
    // Two ways the release is UNKNOWN rather than absent, and neither may reach
    // the verdict below it -- reporting "no cluster release yet" off a probe
    // that never answered is a positive absence claim built on no evidence
    // (#1354, in the states #1358 introduced).
    if state("cluster") == Some(State::Missing) {
        return "A cluster context is configured but `helm list` did not answer, so \
                nothing about the release is known -- see the Cluster line above."
            .to_string();
    }
    // `HelmMissing` and `NotInstalled` are both (cluster Ok, release Missing),
    // so only the detail separates them; hence the shared sentinel.
    if release_detail == Some(HELM_ABSENT_DETAIL) {
        return "Ready to run locally. helm is not installed, so nothing about a \
                cluster release could be read."
            .to_string();
    }
    if !has("release") {
        return "Ready to run locally. No cluster release yet, so no Slack and no \
                deploys."
            .to_string();
    }
    if !has("slack") {
        return "Deployable to the cluster. Slack is not wired, so the agent has no \
                way to be reached."
            .to_string();
    }
    if !has("clone-credential") || !has("webhook") || state("repo-binding") == Some(State::Missing)
    {
        return "Answering in Slack. Git-push deploys are not wired yet -- see the \
                missing items above."
            .to_string();
    }
    // repo-binding reads NotApplicable only when the platform API was never
    // consulted (discovery failed, an unreachable API, or a rejected key --
    // indistinguishable from here). Zero agents is Missing, not this hedge
    // (#1367). Claiming "Fully wired" here asserted the one capability the
    // run did not check (#1354).
    if !has("repo-binding") {
        return "Answering in Slack. Git-push deploys are unverified -- see the \
                Repo binding line above."
            .to_string();
    }
    "Fully wired: local runs, Slack, and git-push deploys.".to_string()
}

// -- targeting ----------------------------------------------------------------

/// Which release doctor will inspect, and whether that was inferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub namespace: String,
    pub release: String,
    /// The line to print when a field came from `curie.yaml`. `None` when
    /// nothing was inferred: a built-in default is not an inference from a file,
    /// and announcing one would be noise on every run.
    pub announcement: Option<String>,
}

/// Resolve the target: the flag wins, else `curie.yaml`'s `install:` block, else
/// the built-in default. Pure, so the precedence table is a unit test with no
/// filesystem access; reading the file is the caller's job, and doctor must not
/// fail when it cannot.
///
/// Precedence is per FIELD. An all-or-nothing implementation throws away the
/// file's release the moment `--namespace` alone is passed, which is how an
/// operator ends up reported on a release they did not name. `diff` already
/// takes its target from `install:` (ADR-0097); this makes `doctor` follow.
pub fn resolve_target(
    flag_namespace: Option<&str>,
    flag_release: Option<&str>,
    declared: Option<(&str, &str)>,
) -> Target {
    let declared_namespace = declared.map(|(namespace, _)| namespace);
    let declared_release = declared.map(|(_, release)| release);
    let namespace = flag_namespace
        .or(declared_namespace)
        .unwrap_or(DEFAULT_TARGET);
    let release = flag_release.or(declared_release).unwrap_or(DEFAULT_TARGET);
    // Announced, never silent: doctor run in someone else's installation
    // directory would otherwise report on a release the operator never named,
    // and the announcement is what makes that survivable.
    let inferred = (flag_namespace.is_none() && declared_namespace.is_some())
        || (flag_release.is_none() && declared_release.is_some());
    Target {
        namespace: namespace.to_string(),
        release: release.to_string(),
        announcement: inferred.then(|| {
            format!("inferred from curie.yaml: --namespace {namespace} --release {release}")
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ui::CliOutput;
    use serde_json::json;
    use std::collections::BTreeSet;

    fn laptop() -> Facts {
        Facts {
            docker_ok: true,
            bundle_name: Some("my-agent".into()),
            ..Default::default()
        }
    }

    fn checks_with(missing: usize) -> Vec<Check> {
        (0..missing)
            .map(|i| Check {
                id: "x",
                title: "t",
                state: State::Missing,
                detail: format!("{i}"),
                fix: None,
            })
            .collect()
    }

    /// One gap needs no signpost -- the `-> fix` beside it is the whole answer.
    /// Sending someone to a TUI to set one environment variable is worse than
    /// telling them the variable.
    #[test]
    fn a_single_gap_does_not_advertise_the_walkthrough() {
        assert_eq!(guidance(&checks_with(0)), None);
        assert_eq!(guidance(&checks_with(1)), None);
    }

    /// #1533 (S17). `api_nodeport` reads the API Service to report how the API
    /// is exposed. It spawns kubectl, so the NAME it asks for is extracted into
    /// a pure command builder and asserted here -- no cluster, no child
    /// process.
    ///
    /// A wrong name makes the read return nothing, which `cluster_facts`
    /// renders as "API not exposed": a FALSE readiness verdict, the precise
    /// failure mode PR #1348 built `curie doctor` to prevent.
    ///
    /// Required signature (`cli/src/doctor.rs`):
    ///   fn api_nodeport_command(
    ///       namespace: &str,
    ///       fullname: &crate::ops::ReleaseFullname,
    ///   ) -> crate::ops::OpsCommand
    /// with `api_nodeport` calling it and running it through
    /// `crate::ops::run_capture`.
    #[test]
    fn api_nodeport_reads_the_chart_rendered_service() {
        let argv =
            api_nodeport_command("acme-system", &crate::ops::chart_fullname("platform")).argv();
        assert!(
            argv.iter().any(|a| a == "platform-curie-api"),
            "doctor must read the chart-rendered API service: {argv:?}"
        );
        assert!(
            !argv.iter().any(|a| a == "platform-api"),
            "doctor must not compute `{{release}}-api`: {argv:?}"
        );
        assert!(
            argv.iter().any(|a| a == "acme-system"),
            "the namespace must still be passed through: {argv:?}"
        );
        assert!(
            argv.iter()
                .any(|a| a.contains("jsonpath={.spec.ports[?(@.nodePort)].nodePort}")),
            "the nodePort jsonpath must be preserved: {argv:?}"
        );

        // Negative control: byte-identical for the default release.
        let control = api_nodeport_command("curie", &crate::ops::chart_fullname("curie")).argv();
        assert!(
            control.iter().any(|a| a == "curie-api"),
            "the default release must be unchanged: {control:?}"
        );
    }

    /// Several gaps have an ORDER and depend on values not yet collected, which
    /// is the work the guided workflow exists to do.
    #[test]
    fn several_gaps_name_the_guided_path() {
        let hint = guidance(&checks_with(3)).expect("should advertise");
        assert!(hint.contains("3 things"), "{hint}");
        assert!(hint.contains("`curie`"), "must name the command: {hint}");
        assert!(
            hint.contains("one at a time"),
            "must leave the manual path open: {hint}"
        );
    }

    /// A check that is NotApplicable is not a gap. Counting it would advertise
    /// a walkthrough for a cluster the operator does not have.
    #[test]
    fn checks_that_do_not_apply_are_not_counted_as_gaps() {
        let mut checks = checks_with(1);
        for i in 0..4 {
            checks.push(Check {
                id: "n",
                title: "t",
                state: State::NotApplicable,
                detail: format!("{i}"),
                fix: None,
            });
        }
        assert_eq!(
            guidance(&checks),
            None,
            "one real gap plus four n/a is one gap"
        );
    }

    /// Cluster, release, Slack and clone credential all in place. The target is
    /// `acme/acme` rather than the default, so a fix string that hardcodes
    /// `curie` is visible to every test built on this fixture.
    fn wired() -> Facts {
        Facts {
            model_credential: Some("CURIE_CREDENTIALS".into()),
            kube_context: Some("minikube".into()),
            target: Some(("acme".into(), "acme".into())),
            release: ReleaseProbe::Installed {
                chart: "curie-0.6.0".into(),
            },
            slack_app_token: true,
            slack_bot_token: true,
            clone_credential: Some("github app".into()),
            api_exposure: Some("NodePort 30799".into()),
            ..laptop()
        }
    }

    /// `wired()` with one release probe outcome swapped in, for the D5 matrix.
    fn probed(release: ReleaseProbe) -> Facts {
        Facts { release, ..wired() }
    }

    fn find<'a>(checks: &'a [Check], id: &str) -> &'a Check {
        checks.iter().find(|c| c.id == id).expect(id)
    }

    /// The one check this issue is about, pulled out of a full `evaluate`.
    fn model_pin(f: &Facts) -> Check {
        find(&evaluate(f), "model-pin").clone()
    }

    /// The set of check ids whose fix is a `curie cluster ...` command.
    fn cluster_fix_ids(checks: &[Check]) -> BTreeSet<&'static str> {
        checks
            .iter()
            .filter(|c| {
                c.fix
                    .as_deref()
                    .is_some_and(|f| f.starts_with("curie cluster "))
            })
            .map(|c| c.id)
            .collect()
    }

    /// The laptop rung is a complete way to use Curie. Reporting five failures
    /// at someone who has not asked for a cluster is how a doctor command
    /// becomes noise people stop reading.
    #[test]
    fn no_cluster_is_not_a_failure() {
        let checks = evaluate(&laptop());
        for id in ["cluster", "release", "slack", "clone-credential", "webhook"] {
            assert_eq!(
                find(&checks, id).state,
                State::NotApplicable,
                "{id} must not read as broken on the laptop rung"
            );
        }
        assert!(
            find(&checks, "cluster").detail.contains("which is fine"),
            "the detail should reassure, not accuse"
        );
    }

    /// The failure a first-time user actually hits: boot succeeds, the next
    /// command fails. The fix has to name the variable AND the offline escape.
    #[test]
    fn a_missing_credential_names_both_ways_forward() {
        let checks = evaluate(&laptop());
        let c = find(&checks, "model-credential");
        assert_eq!(c.state, State::Missing);
        let fix = c.fix.as_deref().expect("must offer a fix");
        assert!(fix.contains("CURIE_CREDENTIALS"), "{fix}");
        assert!(
            fix.contains("--fake-model"),
            "the offline path matters: {fix}"
        );
    }

    /// This output gets pasted into issues and chat.
    #[test]
    fn no_check_can_carry_a_credential_value() {
        let credential = "sk-or-PLACEHOLDER";
        let f = Facts {
            model_credential: Some("CURIE_CREDENTIALS".into()),
            model_credential_source: Some("environment".into()),
            model_credential_provider: crate::ops::provider_from_credential_prefix(credential),
            clone_credential: Some("github app (app_id=4475970)".into()),
            slack_app_token: true,
            slack_bot_token: true,
            ..laptop()
        };
        let rendered = format!("{:?}", evaluate(&f));
        for leaked in [
            credential,
            "sk-or-",
            "sk-ant-",
            "xoxb-",
            "xapp-",
            "ghp_",
            "-----BEGIN",
        ] {
            assert!(!rendered.contains(leaked), "{leaked} leaked: {rendered}");
        }
    }

    /// Each rung's summary has to say what you can DO, not how many checks
    /// passed -- a count tells an operator nothing about where they are.
    ///
    /// The rungs below name `curie/acme`: these fixtures only ever set the
    /// release, so the namespace half was the rendering default, and the pair
    /// is carried over verbatim rather than tidied to `acme/acme` -- nothing
    /// here reads a target, and a fixture edit is not the place to change what
    /// a fix string would print.
    #[test]
    fn the_summary_reports_capability_at_each_rung() {
        let cases = [
            (Facts::default(), "curie init"),
            (laptop(), "fake-model"),
            (
                Facts {
                    model_credential: Some("CURIE_CREDENTIALS".into()),
                    ..laptop()
                },
                "No cluster release",
            ),
            (
                Facts {
                    model_credential: Some("CURIE_CREDENTIALS".into()),
                    kube_context: Some("minikube".into()),
                    target: Some(("curie".into(), "acme".into())),
                    release: ReleaseProbe::Installed {
                        chart: "curie-0.6.0".into(),
                    },
                    ..laptop()
                },
                "Slack is not wired",
            ),
            (
                Facts {
                    model_credential: Some("CURIE_CREDENTIALS".into()),
                    kube_context: Some("minikube".into()),
                    target: Some(("curie".into(), "acme".into())),
                    release: ReleaseProbe::Installed {
                        chart: "curie-0.6.0".into(),
                    },
                    slack_app_token: true,
                    slack_bot_token: true,
                    ..laptop()
                },
                "Git-push deploys are not wired",
            ),
            (
                Facts {
                    model_credential: Some("CURIE_CREDENTIALS".into()),
                    kube_context: Some("minikube".into()),
                    target: Some(("curie".into(), "acme".into())),
                    release: ReleaseProbe::Installed {
                        chart: "curie-0.6.0".into(),
                    },
                    slack_app_token: true,
                    slack_bot_token: true,
                    clone_credential: Some("github app".into()),
                    api_exposure: Some("NodePort 30799".into()),
                    agents: Some(vec![("bot".into(), Some("acme/bot".into()))]),
                    ..laptop()
                },
                "Fully wired",
            ),
        ];
        for (facts, expected) in cases {
            let s = summary(&evaluate(&facts));
            assert!(s.contains(expected), "expected {expected:?} in {s:?}");
        }
    }

    /// A floating model name is reported without failing the install: it works
    /// today, so `Missing` would make a usable setup look broken. The fix is
    /// what carries the advice.
    #[test]
    fn a_floating_model_reports_ok_with_a_fix() {
        let f = Facts {
            model_shell: Some("gpt-4o".into()),
            ..Default::default()
        };
        let c = evaluate(&f)
            .into_iter()
            .find(|c| c.id == "model-pin")
            .expect("model-pin check");
        assert_eq!(c.state, State::Ok);
        assert!(c.detail.contains("gpt-4o"), "{}", c.detail);
        assert!(
            c.fix.as_deref().unwrap_or("").contains("CURIE_MODEL"),
            "the fix must name the variable to set"
        );
    }

    /// A dated snapshot is clean: no advice, nothing to do.
    #[test]
    fn a_pinned_snapshot_carries_no_fix() {
        let f = Facts {
            model_shell: Some("claude-haiku-4-5-20251001".into()),
            ..Default::default()
        };
        let c = evaluate(&f)
            .into_iter()
            .find(|c| c.id == "model-pin")
            .expect("model-pin check");
        assert_eq!(c.state, State::Ok);
        assert!(c.fix.is_none(), "a pinned snapshot needs no fix");
        assert!(c.detail.contains("20251001"), "{}", c.detail);
    }

    /// No model configured anywhere is not a gap: the platform default is a
    /// valid way to run, so this must not count against readiness. What #1950
    /// narrowed is what "unset" MEANS -- it is no longer "the invoking shell
    /// did not export CURIE_MODEL", it is "no source yields a model at all",
    /// and the detail has to say which three sources were looked at or the
    /// operator cannot tell this apart from the check being blind again.
    #[test]
    fn an_unset_model_is_not_applicable() {
        let c = evaluate(&Facts::default())
            .into_iter()
            .find(|c| c.id == "model-pin")
            .expect("model-pin check");
        assert_eq!(c.state, State::NotApplicable);
        let detail = &c.detail;
        assert!(
            detail.contains("override"),
            "must name the per-agent override as one of the sources looked at: {detail}"
        );
        assert!(
            detail.contains("agentSandbox.runner.model"),
            "must name the release default by its real key: {detail}"
        );
        assert!(
            detail.contains("CURIE_MODEL"),
            "must name the shell variable: {detail}"
        );
    }

    /// The exact scenario #1950 reports, and the single most important
    /// assertion in this change. An operator runs `curie cluster up`, never
    /// exports CURIE_MODEL, and the chart's own default `claude-sonnet-5` --
    /// which this repo's own `an_undated_name_floats` calls a floating alias --
    /// is what every sandbox boots. Before this, the shell was the only source
    /// read, so the check reported `not_applicable` with no fix: the
    /// diagnostic built to catch a floating alias reported clean on the
    /// shipped default.
    #[test]
    fn a_floating_release_default_on_a_live_release_reports_floating() {
        let f = Facts {
            model_release_default: Some("claude-sonnet-5".into()),
            target: Some(("curie".into(), "curie".into())),
            ..wired()
        };
        let c = model_pin(&f);
        assert_ne!(
            c.state,
            State::NotApplicable,
            "a floating model on a live release is not 'not applicable': {}",
            c.detail
        );
        assert_eq!(c.state, State::Ok, "{}", c.detail);
        assert!(
            c.detail.contains("claude-sonnet-5"),
            "must name the id actually in force: {}",
            c.detail
        );
        assert!(
            c.detail.contains("release default"),
            "must say WHERE it is in force from, or the operator cannot act on \
             it: {}",
            c.detail
        );
        assert!(
            c.fix.is_some(),
            "a floating name in force is exactly the case that carries advice"
        );
    }

    /// AC1's precedence, exercised as a ladder: the per-agent override is what
    /// the worker forwards as CURIE_MODEL at sandbox boot, so it beats the
    /// release default, which in turn beats the invoking shell -- a value the
    /// boot-env contract does not even declare the CLI as a producer of. Each
    /// rung is asserted by removing the one above it, so a precedence order
    /// written backwards fails here rather than in production.
    #[test]
    fn an_agent_override_beats_the_release_default_beats_the_shell() {
        let all = Facts {
            model_shell: Some("shell-model-20250101".into()),
            model_release_default: Some("claude-sonnet-5".into()),
            model_agent_overrides: vec![("bot".into(), "gpt-4o-mini".into())],
            target: Some(("curie".into(), "curie".into())),
            ..wired()
        };

        let r = resolve_model(&all).expect("a model is in force");
        assert_eq!(r.id, "gpt-4o-mini");
        assert!(
            matches!(&r.source, ModelSource::Agent(name) if name == "bot"),
            "the agent override must win, and must carry the agent's real name"
        );
        let c = model_pin(&all);
        assert!(c.detail.contains("gpt-4o-mini"), "{}", c.detail);
        assert!(
            c.detail.contains("bot"),
            "the operator has to know WHICH agent is in force: {}",
            c.detail
        );

        let no_agent = Facts {
            model_agent_overrides: vec![],
            ..all.clone()
        };
        let r = resolve_model(&no_agent).expect("a model is in force");
        assert_eq!(r.id, "claude-sonnet-5");
        assert!(matches!(
            r.source,
            ModelSource::ReleaseDefault(ReleaseModelKey::Runner)
        ));

        let shell_only = Facts {
            model_agent_overrides: vec![],
            model_release_default: None,
            ..all
        };
        let r = resolve_model(&shell_only).expect("a model is in force");
        assert_eq!(r.id, "shell-model-20250101");
        assert!(matches!(r.source, ModelSource::Shell));
    }

    /// The inverse failure the issue names: a shell that happens to export a
    /// dated snapshot while the release still floats. Reporting only the
    /// winner would hide the disagreement entirely, so the detail has to carry
    /// both ids AND both source labels -- an id with no label is not something
    /// an operator can go and change.
    #[test]
    fn disagreement_is_named() {
        let f = Facts {
            model_shell: Some("claude-haiku-4-5-20251001".into()),
            model_release_default: Some("claude-sonnet-5".into()),
            target: Some(("curie".into(), "curie".into())),
            ..wired()
        };
        let c = model_pin(&f);
        assert_eq!(c.state, State::Ok, "{}", c.detail);
        assert!(
            c.detail.contains("claude-sonnet-5"),
            "the in-force id: {}",
            c.detail
        );
        assert!(
            c.detail.contains("release default"),
            "the in-force source: {}",
            c.detail
        );
        assert!(
            c.detail.contains("claude-haiku-4-5-20251001"),
            "the disagreeing id: {}",
            c.detail
        );
        assert!(
            c.detail.contains("CURIE_MODEL"),
            "the disagreeing source: {}",
            c.detail
        );
    }

    /// Several agents, several models. The check must never read clean because
    /// it happened to pick the pinned one: the weakest pin is the risk the
    /// install actually carries, so `Floating` outranks `Unrecognized`
    /// outranks `Pinned`, and ties break on agent name so the report is
    /// deterministic across runs.
    #[test]
    fn the_weakest_agent_pin_is_the_one_reported() {
        let f = Facts {
            model_agent_overrides: vec![
                ("alpha".into(), "claude-haiku-4-5-20251001".into()),
                ("beta".into(), "claude-sonnet-5".into()),
            ],
            target: Some(("curie".into(), "curie".into())),
            ..wired()
        };
        let r = resolve_model(&f).expect("a model is in force");
        assert_eq!(r.id, "claude-sonnet-5");
        assert!(
            matches!(&r.source, ModelSource::Agent(name) if name == "beta"),
            "the floating agent must be the one reported in force"
        );
        let c = model_pin(&f);
        assert_eq!(c.state, State::Ok, "{}", c.detail);
        assert!(c.fix.is_some(), "a floating name in force carries advice");
        assert!(c.detail.contains("beta"), "must name it: {}", c.detail);
        assert!(
            c.detail.contains("alpha") && c.detail.contains("claude-haiku-4-5-20251001"),
            "the other agent still disagrees and must be listed: {}",
            c.detail
        );
    }

    /// AC2's other half. `not_applicable` used to mean "the invoking shell did
    /// not export CURIE_MODEL", which is why the shipped default reported
    /// clean. It now means exactly one thing -- no source yields a model at
    /// all -- so any single source present must move the check off it.
    #[test]
    fn only_a_total_absence_is_not_applicable() {
        let one_source_each = [
            Facts {
                model_shell: Some("claude-sonnet-5".into()),
                ..Default::default()
            },
            Facts {
                model_release_default: Some("claude-sonnet-5".into()),
                ..Default::default()
            },
            Facts {
                model_agent_overrides: vec![("bot".into(), "claude-sonnet-5".into())],
                ..Default::default()
            },
        ];
        for f in one_source_each {
            let c = model_pin(&f);
            assert_ne!(
                c.state,
                State::NotApplicable,
                "a model IS determinable here, so this is not 'not applicable': {}",
                c.detail
            );
        }

        let c = model_pin(&Facts::default());
        assert_eq!(c.state, State::NotApplicable);
        for source in ["override", "agentSandbox.runner.model", "CURIE_MODEL"] {
            assert!(
                c.detail.contains(source),
                "the one not-applicable branch must say which sources were \
                 looked at, and name {source}: {}",
                c.detail
            );
        }
    }

    /// AC3. `glm-4-0520` is a fully pinned zhipu snapshot; the old catch-all
    /// asserted it floats and offered `export CURIE_MODEL=<id>-YYYYMMDD`, which
    /// produces an id that provider will reject. The honest report makes no
    /// claim about whether the id moves and carries no fix at all -- a wrong
    /// fix string is worse than none (#1813).
    #[test]
    fn an_unrecognized_id_carries_no_fix() {
        let f = Facts {
            model_release_default: Some("glm-4-0520".into()),
            target: Some(("curie".into(), "curie".into())),
            ..wired()
        };
        let c = model_pin(&f);
        assert_eq!(c.state, State::Ok, "{}", c.detail);
        assert!(
            c.fix.is_none(),
            "an unrecognised shape must not be told to pin a date it may not \
             accept: {:?}",
            c.fix
        );
        assert!(c.detail.contains("glm-4-0520"), "{}", c.detail);
        assert!(
            c.detail.contains("does not recognise"),
            "must say plainly that the rule cannot read this shape: {}",
            c.detail
        );
        assert!(
            !c.detail.contains("is a floating name"),
            "must not assert the claim it is declining to make: {}",
            c.detail
        );
    }

    /// AC4, and the guard against re-inventing a flag. `curie cluster up` has
    /// no `--model` (that flag belongs to `skill up`); the release default is
    /// set with `--set agentSandbox.runner.model=`. A fix string naming a flag
    /// that does not exist fails for whoever pastes it, which is the exact
    /// defect #1813 was filed for. Every shape that can emit a fix is swept,
    /// so a new branch cannot slip past this.
    #[test]
    fn every_model_pin_fix_is_a_bare_runnable_command() {
        // Flags `ClusterAction` really declares for each verb. Anything else in
        // an emitted fix is invented.
        let declared = |verb: &str| -> &'static [&'static str] {
            match verb {
                "up" => &["--namespace", "--release", "--set"],
                "overrides" => &["--model"],
                other => panic!("fix names an unknown `curie cluster` verb: {other}"),
            }
        };

        let mut shapes: Vec<Facts> = Vec::new();
        for id in [
            "claude-sonnet-5",           // floating -- emits a fix
            "claude-haiku-4-5-20251001", // pinned -- must not
            "glm-4-0520",                // unrecognised -- must not
        ] {
            for target in [None, Some(("acme".to_string(), "acme-bot".to_string()))] {
                shapes.push(Facts {
                    model_agent_overrides: vec![("bot".into(), id.into())],
                    target: target.clone(),
                    ..wired()
                });
                shapes.push(Facts {
                    model_release_default: Some(id.into()),
                    target: target.clone(),
                    ..wired()
                });
                // The same source on the branch a --local-model install takes.
                // Its fix names a different key and must still be runnable.
                shapes.push(Facts {
                    model_release_default: Some(id.into()),
                    model_release_key: Some(ReleaseModelKey::Inference),
                    target: target.clone(),
                    ..wired()
                });
                shapes.push(Facts {
                    model_shell: Some(id.into()),
                    target,
                    ..wired()
                });
            }
        }

        let mut saw_a_fix = false;
        for f in shapes {
            let c = model_pin(&f);
            let Some(fix) = c.fix.as_deref() else {
                continue;
            };
            saw_a_fix = true;
            assert!(
                fix.starts_with("curie ") || fix.starts_with("export "),
                "a fix must be a command someone can paste, got {fix:?}"
            );
            assert!(
                !fix.contains('('),
                "prose in a fix string makes it unrunnable -- move it to the \
                 detail: {fix:?}"
            );
            if let Some(rest) = fix.strip_prefix("curie cluster ") {
                let verb = rest.split_whitespace().next().unwrap_or_default();
                let allowed = declared(verb);
                for flag in fix.split_whitespace().filter(|t| t.starts_with("--")) {
                    assert!(
                        allowed.contains(&flag),
                        "`curie cluster {verb}` does not declare {flag}: {fix:?}"
                    );
                }
            }
        }
        assert!(
            saw_a_fix,
            "the sweep exercised no fix-emitting shape at all"
        );
    }

    /// #1358 item 1: every other doctor fix omits the --namespace/--release the
    /// run was invoked with, so pasting one operates on curie/curie instead of
    /// the release just diagnosed. This check's fix must not repeat that.
    #[test]
    fn the_release_fix_names_the_real_namespace_and_release() {
        let f = Facts {
            model_release_default: Some("claude-sonnet-5".into()),
            target: Some(("acme".into(), "acme".into())),
            ..wired()
        };
        let fix = model_pin(&f).fix.expect("a floating default carries a fix");
        assert!(
            fix.contains("--namespace acme --release acme"),
            "the fix must target the release doctor actually looked at: {fix}"
        );
        assert!(
            fix.contains("agentSandbox.runner.model="),
            "must name the key that actually sets the release default: {fix}"
        );
    }

    /// A `curie cluster up --local-model` install. The chart IGNORES
    /// `agentSandbox.runner.model` while the in-cluster inference service is
    /// deployed -- `helm template --set inference.deploy=true --set
    /// inference.model=qwen3:4b` renders exactly one `CURIE_MODEL`, and its
    /// value is `qwen3:4b`. So the label naming the runner key would name a key
    /// that is not in force, and the fix would print a `--set` that CANNOT
    /// change the model the sandboxes boot: a command that does nothing is not
    /// a fix (AC1, AC4).
    #[test]
    fn a_local_inference_release_names_the_key_that_is_in_force() {
        let f = Facts {
            model_release_default: Some("qwen3:4b".into()),
            model_release_key: Some(ReleaseModelKey::Inference),
            target: Some(("curie".into(), "curie".into())),
            ..wired()
        };
        let c = model_pin(&f);
        assert_eq!(c.state, State::Ok, "{}", c.detail);
        assert!(c.detail.contains("qwen3:4b"), "{}", c.detail);
        assert!(
            c.detail.contains("release default inference.model"),
            "the label must name the key the chart actually reads: {}",
            c.detail
        );
        assert!(
            !c.detail.contains("agentSandbox.runner.model"),
            "the runner key is not in force on this install and naming it \
             sends the operator to a value the chart ignores: {}",
            c.detail
        );
        let fix = c
            .fix
            .expect("qwen3:4b is a floating name and carries a fix");
        assert!(
            fix.contains("--set inference.model=<dated-snapshot-id>"),
            "the fix has to set the key in force: {fix}"
        );
        assert!(
            !fix.contains("agentSandbox.runner.model"),
            "this --set changes nothing while inference is deployed: {fix}"
        );
    }

    /// The other side of the same branch, so the fix for the ordinary install
    /// cannot regress into naming the inference key.
    #[test]
    fn a_default_release_still_names_the_runner_key() {
        let f = Facts {
            model_release_default: Some("claude-sonnet-5".into()),
            model_release_key: Some(ReleaseModelKey::Runner),
            target: Some(("curie".into(), "curie".into())),
            ..wired()
        };
        let c = model_pin(&f);
        assert!(
            c.detail
                .contains("release default agentSandbox.runner.model"),
            "{}",
            c.detail
        );
        assert!(!c.detail.contains("inference.model"), "{}", c.detail);
        let fix = c.fix.expect("a floating default carries a fix");
        assert!(
            fix.contains("--set agentSandbox.runner.model=<dated-snapshot-id>"),
            "{fix}"
        );
        assert!(!fix.contains("--set inference.model"), "{fix}");
    }

    /// Helm decides which key is live by Go template truthiness, not by
    /// `as_bool`. A value that arrives as a string -- a generic
    /// `--set-string inference.deploy=true`, or anything that round-trips
    /// through a values file as one -- sends Helm down the inference branch,
    /// and a doctor that demanded a real boolean went down the other one and
    /// reported a model the pod does not boot.
    ///
    /// The string `"false"` being truthy looks like a bug and is not: Go calls
    /// a non-empty string non-empty whatever it spells, so the chart takes the
    /// inference branch there too. Mirroring the wrong-looking half is the
    /// whole point -- doctor has to agree with Helm, not with intuition.
    #[test]
    fn inference_deploy_follows_helm_truthiness() {
        let with_deploy = |deploy: serde_json::Value| {
            runner_model_from_values(&serde_json::json!({
                "inference": { "deploy": deploy, "model": "qwen3:4b" },
                "agentSandbox": { "runner": { "model": "claude-sonnet-5" } }
            }))
        };

        for truthy in [
            serde_json::json!(true),
            serde_json::json!("true"),
            serde_json::json!("false"),
            serde_json::json!("0"),
            serde_json::json!(1),
        ] {
            assert_eq!(
                with_deploy(truthy.clone()),
                Some(("qwen3:4b".to_string(), ReleaseModelKey::Inference)),
                "helm renders the inference branch for {truthy}, so this must too"
            );
        }

        for falsey in [
            serde_json::json!(false),
            serde_json::json!(0),
            serde_json::json!(""),
            serde_json::json!(null),
            // Go's `empty` calls an empty list and an empty map empty too, and
            // `classify_existing_secret_field` in `github_app.rs` already
            // reads them that way. The two copies of this ladder must agree.
            serde_json::json!([]),
            serde_json::json!({}),
        ] {
            assert_eq!(
                with_deploy(falsey.clone()),
                Some(("claude-sonnet-5".to_string(), ReleaseModelKey::Runner)),
                "helm skips the inference branch for {falsey}, so this must too"
            );
        }

        // Absent entirely -- the shape of every install that never asked for
        // local inference.
        assert_eq!(
            runner_model_from_values(&serde_json::json!({
                "agentSandbox": { "runner": { "model": "claude-sonnet-5" } }
            })),
            Some(("claude-sonnet-5".to_string(), ReleaseModelKey::Runner))
        );
    }

    /// The chart's SHIPPED DEFAULTS render `CURIE_FAKE_MODEL=1` and
    /// `CURIE_MODEL=claude-sonnet-5` at once, off two independent template
    /// arms (`charts/curie/templates/agent-sandbox.yaml`, verified with `helm
    /// template`). Before this, doctor read the id and announced it as the
    /// model in force from the release default, on an install whose sandboxes
    /// never send it anywhere -- the same "names a model the pod does not
    /// boot" defect #1950 exists to kill.
    ///
    /// The id stays in the detail and the state stays `Ok`: the id IS what
    /// applies the moment fake model is turned off, and the credential story
    /// belongs to the separate `model-credential` check.
    #[test]
    fn a_fake_model_release_says_the_configured_id_is_not_in_use() {
        let f = Facts {
            model_release_default: Some("claude-sonnet-5".into()),
            model_release_key: Some(ReleaseModelKey::Runner),
            model_release_fake: true,
            target: Some(("curie".into(), "curie".into())),
            ..wired()
        };
        let c = model_pin(&f);
        assert_eq!(
            c.state,
            State::Ok,
            "a fake-model install is not broken: {}",
            c.detail
        );
        assert!(
            c.detail.contains("scripted fake model") && c.detail.contains("not in use"),
            "the detail must say the pod boots the fake model instead: {}",
            c.detail
        );
        assert!(
            c.detail.contains("claude-sonnet-5"),
            "the id is not suppressed -- it applies the moment fake model is \
             turned off: {}",
            c.detail
        );
        assert_eq!(
            c.fix.as_deref(),
            Some(
                "curie cluster up --namespace curie --release curie \
                 --set agentSandbox.runner.model=<dated-snapshot-id>"
            ),
            "the fix is unchanged by the fake-model caveat"
        );
    }

    /// The negative control for the test above: the ordinary release, boots
    /// what it names. A caveat that shows up here would tell every real
    /// install its model is not in use.
    #[test]
    fn a_real_model_release_carries_no_fake_model_clause() {
        let f = Facts {
            model_release_default: Some("claude-sonnet-5".into()),
            model_release_key: Some(ReleaseModelKey::Runner),
            model_release_fake: false,
            target: Some(("curie".into(), "curie".into())),
            ..wired()
        };
        assert!(
            !model_pin(&f).detail.contains("scripted fake model"),
            "{}",
            model_pin(&f).detail
        );
    }

    /// `agent-sandbox.yaml:482` gates `CURIE_FAKE_MODEL` on
    /// `and $runner.fakeModel (not .Values.inference.deploy)`, so a release
    /// deploying in-cluster inference boots the real local model even with
    /// `fakeModel: true` still sitting in its values. Reading the flag alone
    /// would caveat an install that has nothing to caveat.
    ///
    /// Asserted at the values level, because that is where the two-legged
    /// branch lives; `gather` does nothing but hand this its computed values.
    #[test]
    fn local_inference_beats_the_fake_model_flag() {
        let with = |fake: serde_json::Value, deploy: serde_json::Value| {
            release_fake_model(&serde_json::json!({
                "inference": { "deploy": deploy, "model": "qwen3:4b" },
                "agentSandbox": { "runner": { "fakeModel": fake } }
            }))
        };
        assert!(
            with(serde_json::json!(true), serde_json::json!(false)),
            "fakeModel with no inference deployed IS the fake-model shape"
        );
        assert!(
            !with(serde_json::json!(true), serde_json::json!(true)),
            "the chart omits CURIE_FAKE_MODEL when inference is deployed"
        );
        assert!(
            !with(serde_json::json!(false), serde_json::json!(false)),
            "no fakeModel, no caveat"
        );
        // Both legs are Go-truthy, not `as_bool`: a `--set-string` round trip
        // stores either as a string and Helm still takes the branch.
        assert!(
            with(serde_json::json!("true"), serde_json::json!("")),
            "a string fakeModel is truthy to Helm, so it must be here too"
        );
        // Neither key present at all -- an older release, or a values file
        // that never mentioned the runner.
        assert!(!release_fake_model(&serde_json::json!({})));
    }

    /// The caveat is about the RELEASE source only. A shell `CURIE_MODEL` is
    /// forwarded at sandbox boot whatever the chart's fake-model arm renders,
    /// so attaching the caveat to it would be a claim about a value the chart
    /// never produced.
    #[test]
    fn a_shell_model_carries_no_fake_model_clause() {
        let f = Facts {
            model_shell: Some("claude-sonnet-5".into()),
            model_release_default: None,
            model_release_key: None,
            model_release_fake: true,
            target: Some(("curie".into(), "curie".into())),
            ..wired()
        };
        let c = model_pin(&f);
        assert!(
            !c.detail.contains("scripted fake model"),
            "the release's fake-model flag says nothing about a shell value: {}",
            c.detail
        );
    }

    /// The per-agent override outranks every other source, and `doctor` run
    /// without --api-url/--api-key cannot see it. Before this, an install with
    /// a pinned release default and an agent quietly carrying a floating
    /// override reported clean with no fix -- the exact failure this check
    /// exists to catch. `Facts::agents` already distinguishes "not reached"
    /// from "reached, none set", so the report says which one it is instead of
    /// claiming an absence it never looked for.
    #[test]
    fn an_unreadable_platform_api_is_said_out_loud() {
        let unreachable = Facts {
            model_release_default: Some("claude-haiku-4-5-20251001".into()),
            model_release_key: Some(ReleaseModelKey::Runner),
            target: Some(("curie".into(), "curie".into())),
            agents: None,
            ..wired()
        };
        let c = model_pin(&unreachable);
        assert_eq!(
            c.state,
            State::Ok,
            "absent is not broken -- the honesty belongs in the detail: {}",
            c.detail
        );
        assert!(
            c.detail.contains("could not be read"),
            "a pinned report that never looked at the highest-precedence \
             source has to say so: {}",
            c.detail
        );
        assert!(
            c.detail.contains("platform API"),
            "and say WHY it could not look: {}",
            c.detail
        );

        // Reached, and genuinely nothing set. Repeating the caveat here would
        // train the operator to ignore it on the runs where it is true.
        let reached = Facts {
            agents: Some(vec![]),
            ..unreachable.clone()
        };
        assert!(
            !model_pin(&reached).detail.contains("could not be read"),
            "the API WAS reached: {}",
            model_pin(&reached).detail
        );

        // An override in force IS the highest-precedence source, so there is
        // nothing unread to warn about.
        let from_an_agent = Facts {
            model_agent_overrides: vec![("bot".into(), "claude-sonnet-5".into())],
            ..unreachable
        };
        assert!(
            !model_pin(&from_an_agent)
                .detail
                .contains("could not be read"),
            "{}",
            model_pin(&from_an_agent).detail
        );
    }

    /// The same blindness on the not-applicable branch. "no per-agent
    /// override" is a claim, and it is false whenever the API was never
    /// reached to look.
    #[test]
    fn a_total_absence_does_not_claim_there_are_no_overrides() {
        let c = model_pin(&Facts::default());
        assert_eq!(c.state, State::NotApplicable);
        assert!(
            c.detail.contains("could not be read"),
            "must not assert an absence it never looked for: {}",
            c.detail
        );

        let reached = Facts {
            agents: Some(vec![]),
            ..Default::default()
        };
        let c = model_pin(&reached);
        assert_eq!(c.state, State::NotApplicable);
        assert!(
            c.detail.contains("no per-agent override"),
            "the API was reached, so the absence is a real observation: {}",
            c.detail
        );
    }

    /// #229's footgun, now at three sources instead of one. An agent row with
    /// `model: ""` would otherwise win precedence and resolve to Unset,
    /// producing exactly the `not_applicable` AC2 forbids -- on an install
    /// whose release default is a floating alias.
    #[test]
    fn an_empty_value_at_any_source_never_wins_precedence() {
        let f = Facts {
            model_agent_overrides: vec![("bot".into(), "   ".into())],
            model_release_default: Some(String::new()),
            model_shell: Some("claude-sonnet-5".into()),
            target: Some(("curie".into(), "curie".into())),
            ..wired()
        };
        let r = resolve_model(&f).expect("the shell value is a real model");
        assert_eq!(r.id, "claude-sonnet-5");
        assert!(matches!(r.source, ModelSource::Shell));
        let c = model_pin(&f);
        assert_eq!(c.state, State::Ok, "{}", c.detail);
        assert!(
            !c.detail.contains("bot"),
            "an empty override is not an override and must not be reported as \
             one: {}",
            c.detail
        );

        let all_blank = Facts {
            model_agent_overrides: vec![("bot".into(), String::new())],
            model_release_default: Some("  ".into()),
            model_shell: Some("".into()),
            ..Default::default()
        };
        assert_eq!(model_pin(&all_blank).state, State::NotApplicable);
    }

    /// Two agents on the same model is the ordinary shape of a multi-agent
    /// install, not a disagreement. Listing the second one would train the
    /// operator to ignore the disagreement clause on the runs where it matters.
    #[test]
    fn several_agents_on_the_same_model_are_not_a_disagreement() {
        let f = Facts {
            model_agent_overrides: vec![
                ("beta".into(), "claude-sonnet-5".into()),
                ("alpha".into(), "claude-sonnet-5".into()),
            ],
            target: Some(("curie".into(), "curie".into())),
            ..wired()
        };
        let r = resolve_model(&f).expect("a model is in force");
        assert_eq!(r.id, "claude-sonnet-5");
        assert!(
            matches!(&r.source, ModelSource::Agent(name) if name == "alpha"),
            "ties break on name ascending so the report is stable across runs"
        );
        assert!(
            r.disagreeing.is_empty(),
            "an identical id is not a disagreement: {:?}",
            r.disagreeing
        );
        assert!(
            !model_pin(&f).detail.contains("disagree"),
            "{}",
            model_pin(&f).detail
        );
    }

    /// When the override, the release default and the shell all agree there is
    /// nothing to append -- and a detail ending in a dangling
    /// "other sources disagree:" with nothing after it reads as a truncated
    /// report and sends someone looking for a problem that is not there.
    #[test]
    fn agreement_across_sources_leaves_no_dangling_disagreement_clause() {
        let f = Facts {
            model_agent_overrides: vec![("bot".into(), "claude-sonnet-5".into())],
            model_release_default: Some("claude-sonnet-5".into()),
            model_shell: Some("claude-sonnet-5".into()),
            target: Some(("curie".into(), "curie".into())),
            ..wired()
        };
        let r = resolve_model(&f).expect("a model is in force");
        assert!(
            r.disagreeing.is_empty(),
            "nothing disagrees: {:?}",
            r.disagreeing
        );
        let detail = model_pin(&f).detail;
        assert!(
            !detail.contains("disagree"),
            "no disagreement clause at all when nothing disagrees: {detail}"
        );
        assert!(
            !detail.trim_end().ends_with(':'),
            "a detail must never end on a dangling clause: {detail}"
        );
    }

    /// The direction this check must NOT fail in. A correctly pinned snapshot
    /// in force is not a problem, even while a floating alias sits at a source
    /// that loses precedence: the alias is worth surfacing in the disagreement
    /// list, and emitting a fix for an install that is already pinned is how a
    /// doctor teaches people to ignore it.
    #[test]
    fn a_pinned_id_in_force_emits_no_fix_even_beside_a_floating_alias() {
        let f = Facts {
            model_agent_overrides: vec![("bot".into(), "claude-haiku-4-5-20251001".into())],
            model_release_default: Some("claude-sonnet-5".into()),
            target: Some(("curie".into(), "curie".into())),
            ..wired()
        };
        let c = model_pin(&f);
        assert_eq!(c.state, State::Ok, "{}", c.detail);
        assert!(
            c.fix.is_none(),
            "the model in force is already a dated snapshot: {:?}",
            c.fix
        );
        assert!(
            c.detail.contains("snapshot 20251001"),
            "the pinned spelling is load-bearing: {}",
            c.detail
        );
        assert!(
            c.detail.contains("claude-sonnet-5"),
            "the floating alias still has to be visible: {}",
            c.detail
        );
    }

    /// The pure half of the release-default probe: `gather()` cannot reach it
    /// without a live cluster, so the path walk is unit-tested against a
    /// hand-built computed-values document instead. Empty and absent both read
    /// as "no default observed" -- see the blank-value case above for why.
    ///
    /// It is NOT a single path. `charts/curie/templates/agent-sandbox.yaml`
    /// renders exactly one `CURIE_MODEL` env entry and picks its value on a
    /// branch: `inference.model` when `inference.deploy` is true, otherwise
    /// `agentSandbox.runner.model`. This function has to reproduce that branch
    /// or it names a model the pod never boots.
    #[test]
    fn runner_model_from_values_reads_the_chart_path() {
        let values = serde_json::json!({
            "agentSandbox": { "runner": { "model": "claude-sonnet-5" } }
        });
        assert_eq!(
            runner_model_from_values(&values),
            Some(("claude-sonnet-5".to_string(), ReleaseModelKey::Runner))
        );

        // `curie cluster up --local-model` produces exactly this shape, and the
        // sandbox then boots `qwen3:4b` against the in-cluster inference
        // service -- `agentSandbox.runner.model` is not used for the boot env
        // on this branch at all. Reporting `claude-sonnet-5` here would name a
        // model the install never runs, which is the AC1 failure.
        assert_eq!(
            runner_model_from_values(&serde_json::json!({
                "inference": { "deploy": true, "model": "qwen3:4b" },
                "agentSandbox": { "runner": { "model": "claude-sonnet-5" } }
            })),
            Some(("qwen3:4b".to_string(), ReleaseModelKey::Inference)),
            "the in-cluster inference model wins when inference.deploy is true"
        );

        // The chart's own default. `inference.model` is populated in
        // values.yaml whether or not inference is deployed, so a read that
        // ignored `deploy` would report `qwen3:4b` on every ordinary install.
        assert_eq!(
            runner_model_from_values(&serde_json::json!({
                "inference": { "deploy": false, "model": "qwen3:4b" },
                "agentSandbox": { "runner": { "model": "claude-sonnet-5" } }
            })),
            Some(("claude-sonnet-5".to_string(), ReleaseModelKey::Runner)),
            "deploy:false must not let inference.model win"
        );

        // #229's empty-value footgun on the inference branch: an empty string
        // is not a configured model anywhere else in this code, so it falls
        // through rather than resolving to Unset and reporting not-applicable
        // on an install that boots something.
        assert_eq!(
            runner_model_from_values(&serde_json::json!({
                "inference": { "deploy": true, "model": "" },
                "agentSandbox": { "runner": { "model": "claude-sonnet-5" } }
            })),
            Some(("claude-sonnet-5".to_string(), ReleaseModelKey::Runner)),
            "an empty inference.model is not a configured model"
        );

        // A local-model install need not carry an agentSandbox block at all in
        // the computed values; the inference branch stands on its own.
        assert_eq!(
            runner_model_from_values(&serde_json::json!({
                "inference": { "deploy": true, "model": "qwen3:4b" }
            })),
            Some(("qwen3:4b".to_string(), ReleaseModelKey::Inference)),
            "the inference branch must not require agentSandbox to be present"
        );

        assert_eq!(runner_model_from_values(&serde_json::json!({})), None);
        assert_eq!(
            runner_model_from_values(&serde_json::json!({
                "agentSandbox": { "runner": { "model": "" } }
            })),
            None
        );
        assert_eq!(
            runner_model_from_values(&serde_json::json!({
                "agentSandbox": { "runner": {} }
            })),
            None
        );
    }

    /// The coupling this check depends on and cannot see: doctor reads the
    /// model out of the release's computed values, and if the chart ever
    /// renames that path the check goes quietly blind again -- reporting "no
    /// model determined" on an install that boots one. Reading the shipped
    /// values.yaml means a rename breaks this test instead.
    ///
    /// The `inference.deploy` assertion is the other half: it is what makes
    /// `agentSandbox.runner.model` the live source on a default install today.
    /// If the chart ever flips that default, this branch of
    /// `runner_model_from_values` changes which key is authoritative, and the
    /// test should say so rather than the check silently reporting the wrong
    /// source.
    #[test]
    fn the_chart_still_ships_a_model_at_the_path_doctor_reads() {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../charts/curie/values.yaml");
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("the shipped chart values must be readable: {e}"));
        let values: serde_json::Value =
            serde_norway::from_str(&raw).expect("the shipped chart values must parse");

        assert_eq!(
            values
                .get("inference")
                .and_then(|i| i.get("deploy"))
                .and_then(serde_json::Value::as_bool),
            Some(false),
            "the chart must still default inference.deploy to false -- that is \
             what makes agentSandbox.runner.model the model a default install \
             actually boots"
        );

        let (model, key) = runner_model_from_values(&values).unwrap_or_else(|| {
            panic!(
                "the chart no longer ships a model at either path curie doctor \
                 reads: inference.model when inference.deploy, else \
                 agentSandbox.runner.model"
            )
        });
        assert!(!model.trim().is_empty(), "{model:?}");
        assert_eq!(
            key,
            ReleaseModelKey::Runner,
            "with inference.deploy false the shipped default must come from \
             agentSandbox.runner.model, which is the key the fix names"
        );
    }

    /// Every failing check must be actionable. A report that says "missing"
    /// without saying what to run is the checklist problem restated. The four
    /// `ReleaseProbe` outcomes are included because #1261's rule -- no `missing`
    /// without a `fix` -- covers the states D5 introduces too.
    #[test]
    fn every_missing_check_carries_a_command() {
        let facts = vec![
            Facts::default(),
            laptop(),
            probed(ReleaseProbe::HelmMissing),
            probed(ReleaseProbe::ProbeFailed),
            probed(ReleaseProbe::NotInstalled),
            probed(ReleaseProbe::Installed {
                chart: "curie-0.7.0".into(),
            }),
        ];
        for f in facts {
            for c in evaluate(&f).iter().filter(|c| c.state == State::Missing) {
                let fix = c.fix.as_deref().unwrap_or("");
                assert!(
                    fix.contains("curie ") || fix.contains("export ") || fix.contains("http"),
                    "{} must name a command, got {fix:?}",
                    c.id
                );
            }
        }
    }

    /// The binding is what makes a push reach an agent. An unbound one fails
    /// SILENTLY: the webhook returns 200, GitHub shows a green delivery, and
    /// nothing is logged -- so the check has to say that out loud. This also
    /// pins the Missing versus NotApplicable distinction the verdict turns
    /// on: a KNOWN unbound agent must reach the actionable "not wired yet"
    /// summary, not the "unverified" hedge reserved for never having checked.
    #[test]
    fn an_unbound_agent_is_reported_with_what_it_costs() {
        let f = Facts {
            agents: Some(vec![
                ("bot".into(), Some("acme/bot".into())),
                ("bot-dev".into(), None),
            ]),
            ..wired()
        };
        let checks = evaluate(&f);
        let c = find(&checks, "repo-binding").clone();
        assert_eq!(c.state, State::Missing);
        assert!(c.detail.contains("bot-dev"), "must name it: {}", c.detail);
        assert!(
            c.detail.contains("silently ignored"),
            "must say the failure is silent: {}",
            c.detail
        );
        assert!(
            summary(&checks).contains("Git-push deploys are not wired yet"),
            "a KNOWN unbound agent must route to the actionable verdict, not \
             the unverified hedge: {}",
            summary(&checks)
        );
    }

    /// The advice has to be right about what CAN be fixed. An agent with no
    /// binding is bindable by a later deploy (#1194); only one already pointing
    /// at a DIFFERENT repository is left alone. Telling someone to delete and
    /// recreate an unbound agent would destroy its version history for nothing.
    #[test]
    fn the_fix_distinguishes_unbound_from_misbound() {
        let f = Facts {
            agents: Some(vec![("bot".into(), None)]),
            ..wired()
        };
        let fix = find(&evaluate(&f), "repo-binding")
            .fix
            .clone()
            .expect("must offer a fix");
        assert!(fix.contains("--repo"), "{fix}");
        assert!(
            fix.contains("NOT rebind"),
            "must be explicit that a wrong binding is not fixed this way: {fix}"
        );
    }

    /// Not reaching the API is a fact, not a failure -- doctor needs only
    /// kubectl and helm for everything else.
    #[test]
    fn an_unreachable_api_is_not_a_failure() {
        let c = find(&evaluate(&wired()), "repo-binding").clone();
        assert_eq!(c.state, State::NotApplicable);
        assert!(c.detail.contains("--api-url"), "{}", c.detail);
    }

    #[test]
    fn all_bound_agents_pass() {
        let f = Facts {
            agents: Some(vec![("bot".into(), Some("acme/bot".into()))]),
            ..wired()
        };
        assert_eq!(find(&evaluate(&f), "repo-binding").state, State::Ok);
    }

    /// Found on a real install (#1354): every other check passed, the platform
    /// API was never reached (no --api-url/--api-key), and the summary still
    /// said "Fully wired" -- asserting the one capability the run did not
    /// check. `wired()` has no `agents` set, so repo-binding is NotApplicable
    /// here, not Ok.
    #[test]
    fn unreached_api_does_not_claim_fully_wired() {
        let checks = evaluate(&wired());
        let s = summary(&checks);
        assert!(!s.contains("Fully wired"), "{s}");
        assert!(s.contains("Git-push deploys are unverified"), "{s}");
    }

    /// #1367 item 2: zero agents is a proven negative, not an unknown. A push
    /// matching no agent is answered `ignored` with nothing logged. Reporting
    /// NotApplicable shared the unverified verdict with "the API was never
    /// reached", which is the wrong epistemic state.
    #[test]
    fn no_agents_deployed_is_a_missing_binding() {
        let f = Facts {
            agents: Some(vec![]),
            ..wired()
        };
        let checks = evaluate(&f);
        let c = find(&checks, "repo-binding").clone();
        assert_eq!(c.state, State::Missing, "{}", c.detail);
        assert!(
            c.detail.contains("no agents"),
            "must say the API was reached and found none: {}",
            c.detail
        );
        let fix = c.fix.expect("a proven negative must carry a fix");
        assert!(
            fix.contains("cluster deploy"),
            "the fix is to deploy an agent: {fix}"
        );
        assert!(fix.contains("--repo"), "{fix}");
        let s = summary(&checks);
        assert!(!s.contains("Fully wired"), "{s}");
        assert!(
            s.contains("Git-push deploys are not wired yet"),
            "zero agents must route to the actionable verdict: {s}"
        );
        assert!(
            !s.contains("unverified"),
            "unverified is reserved for never having checked: {s}"
        );
    }

    /// The sibling NotApplicable path is only "the platform API was never
    /// reached". Reaching it and finding no agents is item 2 above.
    #[test]
    fn no_agents_deployed_does_not_claim_fully_wired() {
        let f = Facts {
            agents: Some(vec![]),
            ..wired()
        };
        let checks = evaluate(&f);
        let s = summary(&checks);
        assert!(!s.contains("Fully wired"), "{s}");
        assert!(s.contains("Git-push deploys are not wired yet"), "{s}");
    }

    /// #1367 item 3: `ready` stays the "no check is missing" carve-out, so an
    /// unverified deploy path still reports ready=true. A machine consumer
    /// needs the other half of what the summary already says.
    #[test]
    fn deploys_verified_tracks_repo_binding_ok() {
        let bound = Facts {
            agents: Some(vec![("bot".into(), Some("acme/bot".into()))]),
            ..wired()
        };
        let bound_out = DoctorOutput {
            checks: evaluate(&bound),
            summary: summary(&evaluate(&bound)),
        };
        assert_eq!(bound_out.to_json()["deploys_verified"], json!(true));

        let unread = DoctorOutput {
            checks: evaluate(&wired()),
            summary: summary(&evaluate(&wired())),
        };
        assert_eq!(unread.to_json()["deploys_verified"], json!(false));
        assert_eq!(
            unread.to_json()["ready"],
            json!(true),
            "ready must keep the not_applicable carve-out: {}",
            unread.to_json()
        );

        let empty = Facts {
            agents: Some(vec![]),
            ..wired()
        };
        let empty_out = DoctorOutput {
            checks: evaluate(&empty),
            summary: summary(&evaluate(&empty)),
        };
        assert_eq!(empty_out.to_json()["deploys_verified"], json!(false));
        assert_eq!(
            empty_out.to_json()["ready"],
            json!(false),
            "zero agents is missing, so ready flips: {}",
            empty_out.to_json()
        );
    }

    /// Found by running this against a real install. sre-bot serves its webhook
    /// on a NodePort with no ingress, and the first version of this check called
    /// that broken -- on a cluster where git-push deploys demonstrably work.
    #[test]
    fn a_nodeport_counts_as_exposure() {
        let f = Facts {
            model_credential: Some("CURIE_CREDENTIALS".into()),
            kube_context: Some("default".into()),
            target: Some(("sre-bot".into(), "sre-bot".into())),
            release: ReleaseProbe::Installed {
                chart: "curie-0.6.0".into(),
            },
            slack_app_token: true,
            slack_bot_token: true,
            clone_credential: Some("github app".into()),
            api_exposure: Some("NodePort 30799".into()),
            agents: Some(vec![("sre-bot".into(), Some("acme/sre-bot".into()))]),
            ..laptop()
        };
        let checks = evaluate(&f);
        assert_eq!(find(&checks, "webhook").state, State::Ok);
        assert!(
            summary(&checks).contains("Fully wired"),
            "{}",
            summary(&checks)
        );
    }

    /// And when nothing is found, it must not claim the API is unreachable --
    /// a load balancer or tunnel in front is invisible to this check.
    #[test]
    fn no_known_exposure_is_hedged_not_asserted() {
        let f = Facts {
            api_exposure: None,
            clone_credential: Some("pat".into()),
            ..wired()
        };
        let c = find(&evaluate(&f), "webhook").clone();
        assert_eq!(c.state, State::Missing);
        assert!(c.detail.contains("you can ignore this"), "{}", c.detail);
    }

    /// A half-wired release is the state that looks fine and is not: the bot
    /// answers, and every push silently does nothing.
    #[test]
    fn slack_without_deploy_wiring_is_reported_precisely() {
        let f = Facts {
            model_credential: Some("CURIE_CREDENTIALS".into()),
            kube_context: Some("minikube".into()),
            target: Some(("acme".into(), "acme".into())),
            release: ReleaseProbe::Installed {
                chart: "curie-0.6.0".into(),
            },
            slack_app_token: true,
            slack_bot_token: true,
            ..laptop()
        };
        let checks = evaluate(&f);
        assert_eq!(find(&checks, "slack").state, State::Ok);
        assert_eq!(find(&checks, "webhook").state, State::Missing);
        assert!(
            find(&checks, "webhook")
                .detail
                .contains("no ingress and no NodePort"),
            "must name what it looked for: {}",
            find(&checks, "webhook").detail
        );
    }

    // -- gather(), driven ------------------------------------------------
    //
    // Env mutation is process-global, so this reuses the save/clear/restore
    // idiom already in this crate (`cli/src/installation.rs`'s `diff_tests`)
    // rather than inventing a second one. `CURIE_MODEL` is read by
    // `installation.rs`'s planner as well as by `gather()`, so a test that
    // sets it must serialise against every other env-mutating test in the
    // crate, not just its own file's -- hence the crate-wide
    // `crate::PROCESS_ENV_LOCK` rather than a lock private to this file. The
    // lock is held for the whole test so a parallel test cannot observe the
    // mutated variable.

    struct ModelEnvRestore(Vec<(&'static str, Option<std::ffi::OsString>)>);

    impl ModelEnvRestore {
        fn clear(names: &[&'static str]) -> Self {
            let saved = names
                .iter()
                .map(|name| (*name, std::env::var_os(*name)))
                .collect();
            for name in names {
                std::env::remove_var(*name);
            }
            Self(saved)
        }

        fn set(&self, name: &str, value: &str) {
            std::env::set_var(name, value);
        }
    }

    impl Drop for ModelEnvRestore {
        fn drop(&mut self) {
            for (name, value) in &self.0 {
                match value {
                    Some(value) => std::env::set_var(*name, value),
                    None => std::env::remove_var(*name),
                }
            }
        }
    }

    /// AC5. Before this, deleting the entire model wiring from `gather()` left
    /// 20 doctor tests, 7 modelpin tests and both contract tests green --
    /// `gather()` had no coverage anywhere in the repo, so the check was
    /// judged only on facts a test handed it. This drives the real function.
    ///
    /// No cluster is needed: `gather()` returns early when
    /// `kubectl config current-context` is unavailable or empty, and both the
    /// shell read and the target record happen before that point. Setting
    /// `f.model_shell = None` or dropping the `f.target` assignment at the
    /// probe site must fail this test.
    #[tokio::test]
    async fn gather_reads_the_shell_model_and_records_the_target() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        let names = [curie_aci_protocol::env_keys::CURIE_MODEL];
        let env = ModelEnvRestore::clear(&names);
        env.set(
            curie_aci_protocol::env_keys::CURIE_MODEL,
            "some-model-20250101",
        );

        let f = gather("ns", "rel", None).await;

        assert_eq!(
            f.model_shell.as_deref(),
            Some("some-model-20250101"),
            "gather() must actually read CURIE_MODEL from the environment"
        );
        assert_eq!(
            f.target,
            Some(("ns".to_string(), "rel".to_string())),
            "the namespace and release doctor was invoked with are facts about \
             the run, and the model-pin fix string is built from them"
        );
    }

    // -- gather(), driven against a stubbed cluster ----------------------
    //
    // The test above returns early at `kubectl config current-context`, so it
    // cannot see a single helm read. The release default is read from the
    // COMPUTED values (`helm get values --all`), which is the source #1950 was
    // blind to, so it needs a cluster that answers. Rather than a real one,
    // fake `kubectl`/`helm`/`docker` executables go on `PATH` -- the same
    // harness shape `cli/src/ops.rs`'s cluster-diagnosis tests already use.

    /// `kubectl` for a reachable cluster: a context name, and nothing else.
    /// The NodePort probe is deliberately left failing -- `gather()` reads an
    /// unavailable Service as "not exposed that way", which keeps this harness
    /// about the model reads.
    const KUBECTL_STUB: &str = r#"#!/bin/sh
case "$*" in
  "config current-context") printf '%s\n' 'doctor-stub-context' ;;
  *) printf 'unexpected kubectl invocation: %s\n' "$*" >&2; exit 64 ;;
esac
"#;

    /// `helm` for an installed release. The `--all` arm is the load-bearing
    /// one: those are the COMPUTED values, the only read in which a chart
    /// default the operator never supplied is visible at all.
    const HELM_STUB: &str = r#"#!/bin/sh
case "$*" in
  # `gather()` asks whether helm exists at all before it asks what is
  # installed, so that "no helm" and "helm could not answer" stay distinct
  # states rather than one absence claim (#1358). A real helm answers this.
  version*) printf 'v3.14.0+gstub\n' ;;
  list*) printf '%s\n' "$CURIE_TEST_DOCTOR_HELM_LIST" ;;
  *"--all"*) printf '%s\n' "$CURIE_TEST_DOCTOR_HELM_COMPUTED" ;;
  "get values"*) printf '%s\n' "$CURIE_TEST_DOCTOR_HELM_VALUES" ;;
  *) printf 'unexpected helm invocation: %s\n' "$*" >&2; exit 64 ;;
esac
"#;

    fn write_executable(path: &std::path::Path, body: &str) {
        std::fs::write(path, body).expect("write fake cluster executable");
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(path)
            .expect("read fake cluster executable metadata")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions).expect("make fake cluster executable runnable");
    }

    /// Fake `kubectl`, `helm` and `docker` on `PATH`, plus the variables their
    /// bodies read, so `gather()` can be driven all the way through its
    /// cluster reads without a cluster.
    ///
    /// `PATH` and those variables are process-global, so a caller must hold
    /// `crate::PROCESS_ENV_LOCK` for as long as this guard lives. Every
    /// variable is saved and restored in `Drop`, which runs on the unwind of a
    /// failed assertion exactly as it does on a clean return -- a panicking
    /// test therefore cannot leak a fake `helm` into the rest of the suite.
    /// Each variable is overwritten in place rather than cleared first, so
    /// `PATH` is never momentarily absent.
    struct StubbedCluster {
        restore: Vec<(&'static str, Option<std::ffi::OsString>)>,
        // Declared last so the stub directory is removed only after `Drop`
        // above has already taken the stubs back off `PATH`.
        _tools: tempfile::TempDir,
    }

    impl StubbedCluster {
        /// `computed` is what `helm get values --all` returns: the chart's own
        /// defaults merged with the operator's overrides, which is the read
        /// `Facts::model_release_default` is fed from.
        fn install(computed: &str) -> Self {
            let tools = tempfile::tempdir().expect("create fake cluster tool directory");
            write_executable(&tools.path().join("docker"), "#!/bin/sh\nexit 0\n");
            write_executable(&tools.path().join("kubectl"), KUBECTL_STUB);
            write_executable(&tools.path().join("helm"), HELM_STUB);

            let mut entries = vec![tools.path().to_path_buf()];
            entries.extend(std::env::split_paths(
                &std::env::var_os("PATH").unwrap_or_default(),
            ));
            let path = std::env::join_paths(entries).expect("join the stub directory onto PATH");

            let assignments: [(&'static str, std::ffi::OsString); 4] = [
                ("PATH", path),
                (
                    "CURIE_TEST_DOCTOR_HELM_LIST",
                    r#"[{"name":"rel","chart":"curie-0.6.0"}]"#.into(),
                ),
                ("CURIE_TEST_DOCTOR_HELM_COMPUTED", computed.into()),
                // What the OPERATOR supplied. Empty on purpose: the whole
                // point of the computed read is that a release whose operator
                // supplied nothing still boots a model.
                ("CURIE_TEST_DOCTOR_HELM_VALUES", "{}".into()),
            ];
            let restore = assignments
                .iter()
                .map(|(name, _)| (*name, std::env::var_os(*name)))
                .collect();
            for (name, value) in &assignments {
                std::env::set_var(name, value);
            }
            Self {
                restore,
                _tools: tools,
            }
        }
    }

    impl Drop for StubbedCluster {
        fn drop(&mut self) {
            for (name, value) in &self.restore {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }

    /// AC5, the release leg. Pins the paired
    /// `f.model_release_default` / `f.model_release_key` assignment in
    /// `gather()` that `crate::ops::fetch_release_computed_values` feeds.
    /// Deleting either half of that assignment, or switching the read back to
    /// `fetch_release_values` (the operator-supplied values, where a chart
    /// default nobody set is invisible -- the #1950 defect), must fail this
    /// test. The shell-model test above cannot catch any of that: the helm
    /// version and helm list probes are issued concurrently with the kubectl
    /// context probe and their answers are discarded on that test's early
    /// return, so only the computed and operator-supplied values reads are
    /// skipped.
    #[tokio::test]
    async fn gather_reads_the_release_default_model_from_computed_helm_values() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        let _cluster = StubbedCluster::install(
            r#"{"agentSandbox":{"runner":{"model":"chart-default-model-20250101"}}}"#,
        );

        let f = gather("ns", "rel", None).await;

        // `Installed` alone proves the stub's `rel` entry was matched:
        // `classify_release_probe` finds the release BY NAME, so the name half
        // of the pair this assertion used to carry is structural here rather
        // than dropped (#1358).
        assert_eq!(
            f.release,
            ReleaseProbe::Installed {
                chart: "curie-0.6.0".to_string()
            },
            "the stubbed release was never read, so gather() bailed out before \
             the model reads and the assertions below are judging nothing"
        );
        assert_eq!(
            f.model_release_default.as_deref(),
            Some("chart-default-model-20250101"),
            "gather() must record the model from the release's COMPUTED helm \
             values -- an operator who ran `curie cluster up` and never set a \
             model supplied nothing, and that default is what the sandboxes boot"
        );
        assert_eq!(
            f.model_release_key,
            Some(ReleaseModelKey::Runner),
            "the key must travel with the id: the chart reads one of two, and \
             a fix naming the other one is a command that changes nothing"
        );
    }

    /// AC5, the branch a `curie cluster up --local-model` install actually
    /// renders. `agent-sandbox.yaml` ignores `agentSandbox.runner.model` while
    /// `inference.deploy` is truthy, so both values are present in these
    /// computed values and only the inference one is in force. Pins the same
    /// `gather()` assignment as the test above, on the branch where a
    /// hard-coded `ReleaseModelKey::Runner`, a dropped key assignment, or a
    /// read that ignored `runner_model_from_values`'s precedence would all
    /// report a model the pods do not boot.
    #[tokio::test]
    async fn gather_records_the_inference_model_and_key_on_a_local_model_install() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        let _cluster = StubbedCluster::install(
            r#"{"inference":{"deploy":true,"model":"local-inference-model"},"agentSandbox":{"runner":{"model":"chart-default-model-20250101"}}}"#,
        );

        let f = gather("ns", "rel", None).await;

        // `Installed` alone proves the stub's `rel` entry was matched:
        // `classify_release_probe` finds the release BY NAME, so the name half
        // of the pair this assertion used to carry is structural here rather
        // than dropped (#1358).
        assert_eq!(
            f.release,
            ReleaseProbe::Installed {
                chart: "curie-0.6.0".to_string()
            },
            "the stubbed release was never read, so gather() bailed out before \
             the model reads and the assertions below are judging nothing"
        );
        assert_eq!(
            f.model_release_default.as_deref(),
            Some("local-inference-model"),
            "while inference.deploy is truthy the chart renders inference.model \
             and ignores the runner key, so reporting the runner value would \
             name a model no sandbox boots"
        );
        assert_eq!(
            f.model_release_key,
            Some(ReleaseModelKey::Inference),
            "the fix string is built from this key, and `--set \
             agentSandbox.runner.model=` on a --local-model install changes nothing"
        );
    }

    // -- D1: every cluster fix names the invoked target ----------------------

    /// The fix strings were pasted verbatim with `<ns>`/`<name>` placeholders or
    /// no target at all, so an operator running doctor against `acme/acme` was
    /// handed commands that would act on `curie/curie` -- a different release,
    /// silently. Two fixtures because the two fix populations are DISJOINT:
    /// `NotInstalled` short-circuits the four downstream checks, and `Installed`
    /// never produces the missing-release recovery. Asserting the exact id set
    /// (not "whatever fixes exist are targeted") is what keeps this honest: a
    /// fix that stops being produced fails here rather than passing vacuously.
    #[test]
    fn every_cluster_fix_names_the_invoked_target() {
        assert_targeted_fix_sets("acme", "acme");
    }

    /// The same two fixtures on the DEFAULT target. Emission is unconditional by
    /// decision: making it conditional would couple doctor's defaults to
    /// `cluster up`'s defaults, which is the exact assumption class that
    /// produced this bug. Without this test the obvious "only print it when it
    /// differs" shortcut passes.
    #[test]
    fn the_target_is_emitted_even_when_it_equals_the_default() {
        assert_targeted_fix_sets("curie", "curie");
    }

    fn assert_targeted_fix_sets(namespace: &str, release: &str) {
        // Fixture A: no release yet. Only the missing-release recovery fires.
        let absent = Facts {
            target: Some((namespace.into(), release.into())),
            release: ReleaseProbe::NotInstalled,
            ..wired()
        };
        let checks = evaluate(&absent);
        assert_eq!(
            cluster_fix_ids(&checks),
            BTreeSet::from(["release"]),
            "an absent release produces exactly the recovery fix: {checks:?}"
        );
        assert_each_fix_targets(&checks, namespace, release);

        // Fixture B: a release IS installed, and every downstream check is in
        // its Missing state, so all four of the other cluster fixes fire.
        let installed = Facts {
            target: Some((namespace.into(), release.into())),
            release: ReleaseProbe::Installed {
                chart: "curie-0.7.0".into(),
            },
            slack_app_token: false,
            slack_bot_token: false,
            clone_credential: None,
            api_exposure: None,
            agents: Some(vec![("bot".into(), None)]),
            ..wired()
        };
        let checks = evaluate(&installed);
        assert_eq!(
            cluster_fix_ids(&checks),
            BTreeSet::from(["clone-credential", "repo-binding", "slack", "webhook"]),
            "an installed release produces exactly the four downstream fixes: {checks:?}"
        );
        assert_each_fix_targets(&checks, namespace, release);
    }

    fn assert_each_fix_targets(checks: &[Check], namespace: &str, release: &str) {
        for c in checks.iter().filter(|c| {
            c.fix
                .as_deref()
                .is_some_and(|f| f.starts_with("curie cluster "))
        }) {
            let fix = c.fix.as_deref().expect("filtered on having a fix");
            assert!(
                fix.contains(&format!("--namespace {namespace}")),
                "{} must name the namespace doctor was invoked with: {fix}",
                c.id
            );
            assert!(
                fix.contains(&format!("--release {release}")),
                "{} must name the release doctor was invoked with: {fix}",
                c.id
            );
        }
    }

    /// A fixture that never exercised targeting carries no target, and a fix
    /// rendered as `--namespace  --release ` is not runnable. The rendering
    /// fallback is `curie`; resolution itself lives in `resolve_target`.
    #[test]
    fn an_untargeted_fixture_still_renders_a_runnable_fix() {
        let f = Facts {
            target: None,
            release: ReleaseProbe::NotInstalled,
            ..wired()
        };
        let fix = find(&evaluate(&f), "release")
            .fix
            .clone()
            .expect("a missing release must offer a recovery command");
        assert!(
            fix.contains("--namespace curie --release curie"),
            "an empty target must render as the default, not as blanks: {fix}"
        );
    }

    // -- D2: the webhook fix reproduces the allowlist, or refuses -------------

    fn webhook_facts(cidrs: &[&str], reproducible: bool) -> Facts {
        Facts {
            api_exposure: None,
            sandbox_egress_cidrs: cidrs.iter().map(|c| (*c).to_string()).collect(),
            sandbox_egress_is_reproducible: reproducible,
            ..wired()
        }
    }

    fn webhook_fix(f: &Facts) -> String {
        find(&evaluate(f), "webhook")
            .fix
            .clone()
            .expect("an unexposed API must offer a fix")
    }

    /// `cluster up` is a full upgrade: a value nothing re-supplies is dropped.
    /// The webhook fix used to supply only the two ingress `--set`s, so an
    /// operator who followed doctor's advice sealed the sandbox's egress
    /// allowlist and broke the model path on a release that had been working.
    #[test]
    fn the_webhook_fix_resupplies_a_reproducible_egress_allowlist() {
        let f = webhook_facts(&["160.79.104.0/23", "192.0.2.0/24"], true);
        let fix = webhook_fix(&f);
        let first = fix
            .find("--allow-web-egress 160.79.104.0/23")
            .unwrap_or_else(|| panic!("the first recorded CIDR must be re-supplied: {fix}"));
        let second = fix
            .find("--allow-web-egress 192.0.2.0/24")
            .unwrap_or_else(|| panic!("the second recorded CIDR must be re-supplied: {fix}"));
        assert!(
            first < second,
            "the allowlist must be re-supplied in recorded order: {fix}"
        );
        assert!(
            !fix.contains("--allow-egress-host"),
            "a CIDR read back off a release cannot be reversed to a provider \
             name, and ADR-0114 errors when an explicit provider list omits the \
             detected one: {fix}"
        );
    }

    /// Nothing recorded means nothing to preserve. An implementation that
    /// always appends the flag would emit `--allow-web-egress` with no value.
    #[test]
    fn an_empty_allowlist_adds_no_egress_flags() {
        let fix = webhook_fix(&webhook_facts(&[], true));
        assert!(
            !fix.contains("--allow-web-egress"),
            "an empty allowlist has nothing to re-supply: {fix}"
        );
        assert!(
            !fix.contains("WARNING"),
            "an empty allowlist is trivially reproducible, so there is no hazard \
             to warn about: {fix}"
        );
    }

    /// The key negative. `up_value_plan` rewrites every supplied entry as
    /// TCP/443, so a UDP or port-53 rule cannot be reproduced by either flag,
    /// and a capped list drops entries outright -- both NARROW a live
    /// NetworkPolicy. A caveat is read after the policy is already broken, so
    /// the only safe direction is to emit nothing. A caveat-based design would
    /// have PASSED a test that only checked for a warning.
    #[test]
    fn a_non_reproducible_allowlist_emits_no_egress_flags() {
        let owned: Vec<String> = (0..11).map(|i| format!("192.0.2.{i}/32")).collect();
        let eleven: Vec<&str> = owned.iter().map(String::as_str).collect();
        let cases = [
            // (a) a rule that is not TCP/443 -- reproducing it would coerce it.
            (vec!["192.0.2.0/24"], "a non-TCP/443 rule"),
            // (b) more entries than the readability bound -- which must never
            // silently truncate.
            (eleven, "an eleven-entry allowlist"),
        ];
        for (cidrs, what) in cases {
            let fix = webhook_fix(&webhook_facts(&cidrs, false));
            assert_eq!(
                fix.matches("--allow-web-egress").count(),
                0,
                "{what} must emit ZERO egress flags rather than a lossy subset: {fix}"
            );
            assert!(
                fix.contains("WARNING"),
                "{what} must name the hazard: {fix}"
            );
            assert!(
                fix.contains("helm get values"),
                "{what} must point at the recorded policy: {fix}"
            );
        }
    }

    /// Every shape `security.networkPolicy.allowedEgress` can take, and the one
    /// bit that decides whether the webhook fix may re-supply it.
    ///
    /// `up_value_plan` coerces every supplied entry to exactly one TCP/443 port
    /// rule, so anything else on the release cannot be reproduced by
    /// `--allow-web-egress`; the count bound is readability only and flips the
    /// gate rather than truncating. An unreadable ENTRY is not skipped either --
    /// skipping one would hand the operator a `cluster up` carrying fewer CIDRs
    /// than the release records, which is D2 wearing a different hat.
    #[test]
    fn sandbox_egress_faithfulness_rule() {
        let tcp443 = || json!([{"protocol": "TCP", "port": 443}]);
        let policy = |entries: serde_json::Value| json!({"security": {"networkPolicy": {"allowedEgress": entries}}});
        let cidrs_of =
            |n: u64| -> Vec<String> { (0..n).map(|i| format!("192.0.2.{i}/32")).collect() };
        let entries_of = |n: u64| -> serde_json::Value {
            serde_json::Value::Array(
                cidrs_of(n)
                    .into_iter()
                    .map(|cidr| json!({"cidr": cidr, "ports": tcp443()}))
                    .collect(),
            )
        };

        let reproducible: Vec<(&str, serde_json::Value, Vec<String>)> = vec![
            (
                "one plain HTTPS entry",
                policy(json!([{"cidr": "160.79.104.0/23", "ports": tcp443()}])),
                vec!["160.79.104.0/23".to_string()],
            ),
            (
                "a string-typed port, which a values file produces",
                policy(json!([{"cidr": "160.79.104.0/23",
                               "ports": [{"protocol": "TCP", "port": "443"}]}])),
                vec!["160.79.104.0/23".to_string()],
            ),
            (
                "a lower-case protocol, which a values file also produces",
                policy(json!([{"cidr": "160.79.104.0/23",
                               "ports": [{"protocol": "tcp", "port": 443}]}])),
                vec!["160.79.104.0/23".to_string()],
            ),
            ("an empty allowlist", policy(json!([])), vec![]),
            (
                "ten entries -- the readability bound is inclusive",
                policy(entries_of(10)),
                cidrs_of(10),
            ),
            // No policy recorded at all is not a lossy read: emitting no flags
            // reproduces "nothing" exactly, so the gate stays open. Otherwise
            // every release without an allowlist would carry the hazard clause.
            (
                "no allowedEgress key at all",
                json!({"security": {"networkPolicy": {}}}),
                vec![],
            ),
            (
                "an explicitly null allowedEgress",
                policy(serde_json::Value::Null),
                vec![],
            ),
            ("no security block at all", json!({}), vec![]),
        ];
        for (what, values, expected) in reproducible {
            let (cidrs, ok) = sandbox_egress_from_values(&values);
            assert_eq!(cidrs, expected, "{what}: recorded order and content");
            assert!(ok, "{what} must be reproducible");
        }

        let not_reproducible: Vec<(&str, serde_json::Value)> = vec![
            (
                "a UDP rule",
                policy(json!([{"cidr": "192.0.2.0/24",
                               "ports": [{"protocol": "UDP", "port": 443}]}])),
            ),
            (
                "a port-53 rule",
                policy(json!([{"cidr": "192.0.2.0/24",
                               "ports": [{"protocol": "TCP", "port": 53}]}])),
            ),
            (
                "two port rules on one entry",
                policy(json!([{"cidr": "192.0.2.0/24",
                               "ports": [{"protocol": "TCP", "port": 443},
                                         {"protocol": "TCP", "port": 80}]}])),
            ),
            (
                "an entry with no ports at all",
                policy(json!([{"cidr": "192.0.2.0/24"}])),
            ),
            (
                "an entry with an empty ports list",
                policy(json!([{"cidr": "192.0.2.0/24", "ports": []}])),
            ),
            (
                "a port rule with no protocol",
                policy(json!([{"cidr": "192.0.2.0/24", "ports": [{"port": 443}]}])),
            ),
            ("eleven entries", policy(entries_of(11))),
            // A recorded rule this reader cannot name is still a recorded rule.
            // Dropping it and calling the rest faithful is the narrowing this
            // whole gate exists to prevent.
            (
                "an entry with no cidr",
                policy(json!([{"ports": tcp443()}])),
            ),
            (
                "an entry whose cidr is empty after trimming",
                policy(json!([{"cidr": "   ", "ports": tcp443()}])),
            ),
            (
                "an entry whose cidr is not a scalar",
                policy(json!([{"cidr": ["192.0.2.0/24"], "ports": tcp443()}])),
            ),
            (
                "one good entry beside one with no cidr",
                policy(json!([{"cidr": "160.79.104.0/23", "ports": tcp443()},
                              {"ports": tcp443()}])),
            ),
            (
                "an allowedEgress that is not a list at all",
                policy(json!("not-a-list")),
            ),
            (
                "an allowedEgress recorded as an object",
                policy(json!({"cidr": "192.0.2.0/24"})),
            ),
        ];
        for (what, values) in not_reproducible {
            let (cidrs, ok) = sandbox_egress_from_values(&values);
            assert!(!ok, "{what} must NOT be reproducible");
            assert!(
                cidrs.is_empty(),
                "{what} must return nothing to re-supply, got {cidrs:?}"
            );
        }
    }

    /// The invariant the whole faithfulness gate rests on, asserted on its own
    /// so it cannot be lost in a table: **a false gate always returns an empty
    /// vec.** Any non-empty list beside a false gate is a partial allowlist, and
    /// a partial allowlist pasted into `cluster up` NARROWS a live NetworkPolicy
    /// -- the exact failure D2 exists to prevent, reintroduced one entry at a
    /// time instead of all at once.
    #[test]
    fn a_false_egress_gate_never_returns_a_partial_list() {
        let tcp443 = || json!([{"protocol": "TCP", "port": 443}]);
        let good = || json!({"cidr": "160.79.104.0/23", "ports": tcp443()});
        let policy = |entries: serde_json::Value| json!({"security": {"networkPolicy": {"allowedEgress": entries}}});
        let many: Vec<serde_json::Value> = (0..11)
            .map(|i| json!({"cidr": format!("192.0.2.{i}/32"), "ports": tcp443()}))
            .collect();

        // Each case pairs at least one perfectly readable entry with one this
        // reader cannot reproduce, so an implementation that filters instead of
        // refusing returns a non-empty -- and lossy -- list.
        let mixed = [
            policy(
                json!([good(), {"cidr": "192.0.2.0/24", "ports": [{"protocol": "UDP", "port": 443}]}]),
            ),
            policy(
                json!([good(), {"cidr": "192.0.2.0/24", "ports": [{"protocol": "TCP", "port": 53}]}]),
            ),
            policy(json!([good(), {"cidr": "192.0.2.0/24"}])),
            policy(json!([good(), {"cidr": "", "ports": tcp443()}])),
            policy(json!([good(), {"ports": tcp443()}])),
            policy(serde_json::Value::Array(many)),
            policy(json!("not-a-list")),
        ];
        for values in mixed {
            let (cidrs, ok) = sandbox_egress_from_values(&values);
            assert!(!ok, "{values} must not read as reproducible");
            assert!(
                cidrs.is_empty(),
                "a false gate must yield NO cidrs, or the fix emits a narrowed \
                 allowlist: {cidrs:?} from {values}"
            );
        }
    }

    /// The sibling that must NOT be "fixed" to match. `missing_release_recovery`
    /// fires only when there is no release, so there is nothing recorded to
    /// preserve, and its credential-derived `--allow-egress-host` is the right
    /// flag for a FRESH install (#1813). Rewriting it to use `--allow-web-egress`
    /// would regress the provider inference that ticket landed.
    #[test]
    fn the_missing_release_recovery_keeps_allow_egress_host() {
        let f = Facts {
            release: ReleaseProbe::NotInstalled,
            model_credential_provider: crate::ops::provider_from_credential_prefix(
                "sk-ant-PLACEHOLDER",
            ),
            sandbox_egress_cidrs: vec!["160.79.104.0/23".into()],
            sandbox_egress_is_reproducible: true,
            ..wired()
        };
        let fix = find(&evaluate(&f), "release")
            .fix
            .clone()
            .expect("a missing release must offer a recovery command");
        assert!(
            fix.contains("--allow-egress-host anthropic"),
            "the fresh-install recovery keeps its provider inference: {fix}"
        );
        assert!(
            !fix.contains("--allow-web-egress"),
            "there is no recorded policy to preserve on a release that does not \
             exist: {fix}"
        );
    }

    // -- D3: the BYO-Secret clone credential ---------------------------------

    /// The chart calls `githubAppExistingSecret` the RECOMMENDED path, and
    /// doctor reported an install using it as having no clone credential at all
    /// -- telling an operator to run `cluster github-app` over a working setup.
    #[test]
    fn a_secret_backed_github_app_is_a_clone_credential() {
        let f = Facts {
            clone_credential: Some("github app (secret=gh-app)".into()),
            ..wired()
        };
        let c = find(&evaluate(&f), "clone-credential").clone();
        assert_eq!(c.state, State::Ok);
        assert!(
            c.detail.contains("gh-app"),
            "the Secret must be reported by name: {}",
            c.detail
        );
        assert!(c.fix.is_none(), "a working credential needs no fix");
    }

    /// The chart template consumes the existing Secret when one is set, so that
    /// is what the report must name. Reporting both would invite an operator to
    /// "fix" an install that is already working.
    #[test]
    fn clone_credential_prefers_the_existing_secret_over_inline_key_material() {
        assert_eq!(
            clone_credential_from_values(&json!({"api": {"githubAppExistingSecret": "gh-app"}})),
            Some("github app (secret=gh-app)".to_string())
        );
        assert_eq!(
            clone_credential_from_values(&json!({"api": {"githubAppId": "4475970"}})),
            Some("github app (app_id=4475970)".to_string())
        );
        assert_eq!(
            clone_credential_from_values(&json!({"api": {"githubToken": "ghp_PLACEHOLDER"}})),
            Some("personal access token".to_string())
        );
        assert_eq!(
            clone_credential_from_values(&json!({"api": {
                "githubAppExistingSecret": "gh-app",
                "githubAppId": "4475970"
            }})),
            Some("github app (secret=gh-app)".to_string()),
            "the Secret path is what the chart consumes, so it wins the report"
        );
        assert_eq!(
            clone_credential_from_values(&json!({"api": {}})),
            None,
            "nothing recorded is still nothing"
        );
    }

    /// Names, never values (#1348). The Secret's KEY name is adjacent in the
    /// same values block, and reading the wrong field is how key material
    /// reaches a payload that gets pasted into issues.
    #[test]
    fn a_secret_backed_credential_never_reports_key_material() {
        let values = json!({"api": {
            "githubAppExistingSecret": "gh-app",
            "githubAppExistingSecretKey": "private-key.pem"
        }});
        let f = Facts {
            clone_credential: clone_credential_from_values(&values),
            ..wired()
        };
        let detail = find(&evaluate(&f), "clone-credential").detail.clone();
        assert!(detail.contains("gh-app"), "{detail}");
        assert!(
            !detail.contains("private-key.pem"),
            "the key name is not the Secret name: {detail}"
        );
        assert!(!detail.contains("-----BEGIN"), "{detail}");
    }

    // -- D4: type-tolerant reads ---------------------------------------------

    /// #1253: an unquoted `githubAppId: 4475970` in a values file is a live,
    /// GitOps-common shape, and a `as_str()`-only read called that install
    /// credential-less. The scientific-notation form is the same value.
    #[test]
    fn a_numeric_app_id_in_a_values_file_is_still_a_clone_credential() {
        assert_eq!(
            clone_credential_from_values(&json!({"api": {"githubAppId": 4475970}})),
            Some("github app (app_id=4475970)".to_string())
        );
        assert_eq!(
            clone_credential_from_values(&json!({"api": {"githubAppId": 4.47597e6}})),
            Some("github app (app_id=4475970)".to_string()),
            "an integral float is the same app id, not a new one"
        );
    }

    /// The issue's §4 symptom, asserted as the operator sees it: an install
    /// whose ingress is already on printed `MISS  Webhook exposure`, because
    /// `--set-string api.ingress.enabled=true` and a quoted values-file entry
    /// both record a STRING and the read was `as_bool()`-only. Asserted through
    /// the exposure string and the check state, not through the predicate --
    /// a truthiness helper can be right while the report is still wrong.
    #[test]
    fn a_string_typed_ingress_flag_counts_as_enabled() {
        let values = json!({"api": {"ingress": {"enabled": "true", "host": "bot.example.com"}}});
        assert_eq!(
            api_exposure_from_values(&values, None),
            Some("ingress (bot.example.com)".to_string()),
            "a quoted true is how helm --set-string records it"
        );

        // And the user-visible half: that exposure must land the check on Ok.
        let f = Facts {
            api_exposure: api_exposure_from_values(&values, None),
            ..wired()
        };
        let c = find(&evaluate(&f), "webhook").clone();
        assert_eq!(
            c.state,
            State::Ok,
            "an install with ingress on must not read as MISS: {}",
            c.detail
        );
        assert!(c.detail.contains("bot.example.com"), "{}", c.detail);
        assert!(c.fix.is_none(), "there is nothing to fix: {:?}", c.fix);
    }

    /// The rest of the exposure decision, in the one place it is now pure: a
    /// real bool still works, a host-less ingress keeps its existing wording,
    /// the NodePort fallback is only consulted when ingress is off, and a value
    /// that is not truthy must fall through rather than claim an ingress.
    #[test]
    fn exposure_falls_back_to_the_nodeport_only_when_ingress_is_off() {
        let on = json!({"api": {"ingress": {"enabled": true, "host": "bot.example.com"}}});
        assert_eq!(
            api_exposure_from_values(&on, Some("30799".to_string())),
            Some("ingress (bot.example.com)".to_string()),
            "an enabled ingress wins over a NodePort that also exists"
        );

        let hostless = json!({"api": {"ingress": {"enabled": true}}});
        assert_eq!(
            api_exposure_from_values(&hostless, None),
            Some("ingress".to_string()),
            "an ingress with no host keeps its existing wording"
        );

        let off = json!({"api": {"ingress": {"enabled": false}}});
        assert_eq!(
            api_exposure_from_values(&off, Some("30799".to_string())),
            Some("NodePort 30799".to_string())
        );
        assert_eq!(
            api_exposure_from_values(&off, None),
            None,
            "neither mechanism in place is reported as unknown, not as broken"
        );

        // The negative that matters here: "1" is not the literal true, so it
        // must NOT be reported as an ingress the operator does not have.
        let typo = json!({"api": {"ingress": {"enabled": "1", "host": "bot.example.com"}}});
        assert_eq!(
            api_exposure_from_values(&typo, None),
            None,
            "a value that is not the literal true must not claim an ingress"
        );
        assert_eq!(
            api_exposure_from_values(&typo, Some("30799".to_string())),
            Some("NodePort 30799".to_string()),
            "and it falls through to the NodePort read like any other off state"
        );
    }

    /// The negative that keeps the widening honest. Treating any non-empty
    /// string as true would report an install as exposed on a typo -- a
    /// security-relevant over-claim, in a check whose whole job is to say
    /// whether the API is reachable from outside.
    #[test]
    fn only_a_true_bool_or_the_literal_string_true_enables_ingress() {
        for enabled in [json!(true), json!("true"), json!("TRUE"), json!(" True ")] {
            assert!(truthy(Some(&enabled)), "{enabled} must enable ingress");
        }
        for disabled in [
            json!(false),
            json!("false"),
            json!("1"),
            json!("yes"),
            json!("on"),
            json!(""),
            json!(0),
            json!(1),
            json!(null),
            json!([true]),
            json!({"enabled": true}),
        ] {
            assert!(
                !truthy(Some(&disabled)),
                "{disabled} must NOT enable ingress"
            );
        }
        assert!(!truthy(None), "an absent flag must not enable ingress");
    }

    /// The empty-string filter predates the coercion widening and must survive
    /// it: a chart default of `""` is an unset field, not a credential.
    #[test]
    fn an_empty_string_value_is_still_absent() {
        assert_eq!(
            scalar_at(
                &json!({"api": {"githubAppId": ""}}),
                &["api", "githubAppId"]
            ),
            None
        );
        assert_eq!(
            clone_credential_from_values(&json!({"api": {"githubAppId": ""}})),
            None,
            "an empty app id is not a clone credential"
        );
    }

    /// Widening string|number|bool must not become "stringify anything": an
    /// array or an object at a scalar path is a shape this reader does not
    /// understand, and rendering its debug form into a detail is how structure
    /// leaks into a report.
    #[test]
    fn a_non_scalar_value_is_absent() {
        assert_eq!(
            scalar_at(
                &json!({"api": {"githubAppId": ["4475970"]}}),
                &["api", "githubAppId"]
            ),
            None
        );
        assert_eq!(
            scalar_at(
                &json!({"api": {"githubAppId": {"value": "4475970"}}}),
                &["api", "githubAppId"]
            ),
            None
        );
        assert_eq!(
            scalar_at(
                &json!({"api": {"githubAppId": null}}),
                &["api", "githubAppId"]
            ),
            None
        );
        assert_eq!(
            scalar_at(&json!({"api": {}}), &["api", "githubAppId"]),
            None,
            "an absent path is absent"
        );
    }

    // -- D5: helm-missing, could-not-answer and absent are distinguishable ----

    /// The three states were byte-identical: `fetch_release_chart` collapsed
    /// every nonzero `helm list` to `Ok(None)`, so a laptop with no helm, an
    /// expired cluster credential, and a genuinely empty namespace all printed
    /// "not installed in this namespace".
    #[test]
    fn classify_release_probe_separates_every_outcome() {
        assert_eq!(
            classify_release_probe(false, true, "[]", "acme"),
            ReleaseProbe::HelmMissing,
            "no helm means the release cannot be inspected at all"
        );
        assert_eq!(
            classify_release_probe(
                true,
                true,
                r#"[{"name":"acme","chart":"curie-0.7.0"}]"#,
                "acme"
            ),
            ReleaseProbe::Installed {
                chart: "curie-0.7.0".to_string()
            }
        );
        assert_eq!(
            classify_release_probe(true, true, "[]", "acme"),
            ReleaseProbe::NotInstalled
        );
        assert_eq!(
            classify_release_probe(true, true, "null", "acme"),
            ReleaseProbe::NotInstalled,
            "helm prints null for an empty namespace in some versions"
        );
        assert_eq!(
            classify_release_probe(true, true, "   ", "acme"),
            ReleaseProbe::NotInstalled,
            "empty stdout on success is the empty-namespace shape"
        );
        for stdout in ["", "[]", r#"[{"name":"acme","chart":"curie-0.7.0"}]"#] {
            assert_eq!(
                classify_release_probe(true, false, stdout, "acme"),
                ReleaseProbe::ProbeFailed,
                "a nonzero exit is not an answer, whatever landed on stdout"
            );
        }
    }

    /// The namespace can hold other releases. Taking the first array element
    /// would report someone else's chart as this release's.
    #[test]
    fn exit_zero_for_a_different_release_is_not_installed() {
        assert_eq!(
            classify_release_probe(
                true,
                true,
                r#"[{"name":"other","chart":"curie-0.7.0"}]"#,
                "acme"
            ),
            ReleaseProbe::NotInstalled
        );
    }

    /// The key negative. Turning "I could not read the answer" into "there is no
    /// release" is a positive absence claim built on no evidence -- the #1354
    /// shape -- and it is what `fetch_existing_values` already fails closed on
    /// ("the release state is unknown"). A helm whose `list -o json` shape
    /// changes must make doctor say so, not report a live release as gone.
    #[test]
    fn exit_zero_with_unreadable_stdout_is_probe_failed_not_not_installed() {
        for stdout in [
            "not json at all",
            r#"{"releases":[]}"#,
            r#"[{"name":"acme"}]"#,
        ] {
            assert_eq!(
                classify_release_probe(true, true, stdout, "acme"),
                ReleaseProbe::ProbeFailed,
                "unreadable stdout {stdout:?} must not become an absence claim"
            );
        }
    }

    /// The user-visible half of the same defect: four different causes must not
    /// produce the same two lines. Pairwise distinctness is the assertion the
    /// issue's report ("byte-identical") maps onto directly.
    #[test]
    fn each_release_probe_renders_a_distinct_report() {
        let variants = [
            ReleaseProbe::HelmMissing,
            ReleaseProbe::ProbeFailed,
            ReleaseProbe::NotInstalled,
            ReleaseProbe::Installed {
                chart: "curie-0.7.0".into(),
            },
        ];
        let mut seen: Vec<(ReleaseProbe, (State, String, State, String))> = Vec::new();
        for probe in variants {
            let checks = evaluate(&probed(probe.clone()));
            let cluster = find(&checks, "cluster");
            let release = find(&checks, "release");
            let report = (
                cluster.state,
                cluster.detail.clone(),
                release.state,
                release.detail.clone(),
            );
            if probe != ReleaseProbe::NotInstalled {
                assert!(
                    !release.detail.contains("no deployed release"),
                    "{probe:?} must not claim the release is absent: {}",
                    release.detail
                );
            }
            for (other, previous) in &seen {
                assert_ne!(
                    &report, previous,
                    "{probe:?} and {other:?} render the same report"
                );
            }
            seen.push((probe, report));
        }
    }

    /// A JSON consumer gates on `ready`. Reporting `true` while doctor could not
    /// see the release at all would let a pipeline proceed on no evidence.
    #[test]
    fn an_unknown_release_is_never_ready() {
        for probe in [ReleaseProbe::HelmMissing, ReleaseProbe::ProbeFailed] {
            let checks = evaluate(&probed(probe.clone()));
            let out = DoctorOutput {
                summary: summary(&checks),
                checks,
            };
            assert_eq!(
                out.to_json()["ready"],
                serde_json::Value::Bool(false),
                "{probe:?} knows nothing about the release, so it is not ready"
            );
        }
    }

    /// #1354 recurring through the states D5 introduces, in both directions: a
    /// probe that failed and a missing helm must not roll up to the verdict
    /// reserved for "helm answered, and there is no release here".
    #[test]
    fn a_failed_probe_does_not_claim_the_release_is_absent() {
        let failed = summary(&evaluate(&probed(ReleaseProbe::ProbeFailed)));
        assert!(!failed.contains("No cluster release yet"), "{failed}");
        assert!(
            failed.contains("Cluster line"),
            "the verdict must send the operator to the line that explains it: {failed}"
        );

        let no_helm = summary(&evaluate(&probed(ReleaseProbe::HelmMissing)));
        assert!(!no_helm.contains("No cluster release yet"), "{no_helm}");
        assert!(
            no_helm.contains("helm"),
            "a missing helm is the reason, and the verdict must say so: {no_helm}"
        );
    }

    /// `Ok` on the cluster check means a kube context is configured -- not that
    /// anything reached the cluster. When helm never ran, nothing contacted it,
    /// and claiming otherwise is the over-claim this check exists to avoid.
    #[test]
    fn the_cluster_detail_says_whether_the_cluster_was_contacted() {
        for probe in [
            ReleaseProbe::NotInstalled,
            ReleaseProbe::Installed {
                chart: "curie-0.7.0".into(),
            },
        ] {
            let detail = find(&evaluate(&probed(probe.clone())), "cluster")
                .detail
                .clone();
            assert!(detail.contains("reached"), "{probe:?}: {detail}");
        }
        let detail = find(&evaluate(&probed(ReleaseProbe::HelmMissing)), "cluster")
            .detail
            .clone();
        assert!(
            !detail.contains("reached"),
            "helm never ran, so nothing contacted the cluster: {detail}"
        );
        assert!(
            detail.contains("kubeconfig"),
            "it must say where the context came from instead: {detail}"
        );
    }

    /// The standing structural guard. doctor's payload is pasted into issues,
    /// and helm's own output is an ARBITRARY external line: it can carry an
    /// `Authorization` header, an exec-plugin's argv, or a token-bearing URL.
    /// No prefix denylist can enumerate that, so the property is that NOTHING
    /// from a subprocess survives -- asserted over the whole planted string,
    /// token by token, not over a list of credential shapes.
    #[test]
    fn no_subprocess_output_ever_reaches_a_check() {
        // Every token is distinctive on purpose: the assertion is that NONE of
        // them survives, so a token that doctor could legitimately author (say,
        // "cluster") would make the guard fail for the wrong reason.
        //
        // The credential-shaped tokens are ASSEMBLED AT RUNTIME rather than
        // written out. A complete `xoxb-...` / `ghp_...` / `github_pat_...`
        // string sitting in this source trips the repo's secret scanners even
        // on a placeholder, and gitleaks scans full history, so one that lands
        // and is removed later still fails. The planted string is byte-for-byte
        // what it always was, so this plants the same input and proves the same
        // property. Do NOT "tidy" these back into single literals.
        let ph = "PLACEHOLDER";
        let planted_owned = [
            format!("sk-ant-{ph}"),
            format!("sk-or-{ph}"),
            format!("{}{ph}", "xoxb-"),
            format!("{}{ph}", "xapp-"),
            format!("{}{ph}", "ghp_"),
            format!("{}{ph}", "github_pat_"),
            format!("{}{} RSA PRIVATE KEY", "-----", "BEGIN"),
            format!("Authorization: Bearer {ph}"),
            format!("https://example.invalid/?access_token={ph}"),
            "curie-doctor-subprocess-marker-1358".to_string(),
        ]
        .join(" ");
        let planted = planted_owned.as_str();

        // The classifier is the one place a subprocess's bytes are handed to
        // doctor, so route the planted output through it on both exit paths.
        let mut probes = vec![
            classify_release_probe(true, false, planted, "acme"),
            classify_release_probe(true, true, planted, "acme"),
            ReleaseProbe::HelmMissing,
            ReleaseProbe::NotInstalled,
            ReleaseProbe::Installed {
                chart: "curie-0.7.0".into(),
            },
        ];
        probes.dedup();

        for probe in probes {
            let checks = evaluate(&probed(probe.clone()));
            let rendered = format!("{checks:?}");
            for token in planted.split_whitespace() {
                assert!(
                    !rendered.contains(token),
                    "{probe:?} echoed {token:?} from the subprocess: {rendered}"
                );
            }
            assert!(
                !summary(&checks).contains("curie-doctor-subprocess-marker-1358"),
                "the summary echoed the subprocess too"
            );
        }
    }

    // -- D6a: Slack partial state --------------------------------------------

    /// The check read only `botToken` and then claimed "app and bot tokens
    /// recorded". Socket mode needs BOTH, so a half-configured release read as
    /// fully wired and the bot silently never connected.
    #[test]
    fn a_bot_token_without_an_app_token_is_not_ok() {
        let f = Facts {
            slack_bot_token: true,
            slack_app_token: false,
            ..wired()
        };
        let checks = evaluate(&f);
        let c = find(&checks, "slack");
        assert_eq!(c.state, State::Missing);
        assert!(
            c.detail.contains("only the bot token"),
            "the detail must say which token was observed: {}",
            c.detail
        );
        assert!(
            c.detail.contains("socket mode needs both"),
            "and why one is not enough: {}",
            c.detail
        );
        let fix = c.fix.as_deref().expect("a partial state must be fixable");
        assert!(fix.contains("--namespace acme"), "{fix}");
        assert!(fix.contains("--release acme"), "{fix}");
        assert!(
            !summary(&checks).contains("Fully wired"),
            "{}",
            summary(&checks)
        );
    }

    /// The mirror case. An app token with no bot token is the same failure in
    /// the other direction, and an implementation that only special-cases one
    /// of the two passes the sibling test alone.
    #[test]
    fn an_app_token_without_a_bot_token_is_not_ok() {
        let f = Facts {
            slack_bot_token: false,
            slack_app_token: true,
            ..wired()
        };
        let checks = evaluate(&f);
        let c = find(&checks, "slack");
        assert_eq!(c.state, State::Missing);
        assert!(
            c.detail.contains("only the app token"),
            "the detail must say which token was observed: {}",
            c.detail
        );
        assert!(
            c.detail.contains("socket mode needs both"),
            "and why one is not enough: {}",
            c.detail
        );
        assert!(c.fix.is_some(), "a partial state must be fixable");
        assert!(
            !summary(&checks).contains("Fully wired"),
            "{}",
            summary(&checks)
        );
    }

    /// The positive path keeps its wording: both tokens recorded is Ok, with no
    /// fix and nothing to do.
    #[test]
    fn both_tokens_recorded_is_ok() {
        let c = find(&evaluate(&wired()), "slack").clone();
        assert_eq!(c.state, State::Ok);
        assert_eq!(c.detail, "app and bot tokens recorded");
        assert!(c.fix.is_none());
    }

    /// Neither token keeps the original wording and stays fixable.
    #[test]
    fn no_tokens_recorded_is_reported_as_none() {
        let f = Facts {
            slack_bot_token: false,
            slack_app_token: false,
            ..wired()
        };
        let c = find(&evaluate(&f), "slack").clone();
        assert_eq!(c.state, State::Missing);
        assert_eq!(c.detail, "no tokens recorded");
    }

    // -- D6b: curie.yaml precedence ------------------------------------------

    /// doctor hardcoded `curie/curie` and never looked at the `curie.yaml` in
    /// the directory it was run from, so in an installation directory it
    /// reported on a release that did not exist. Resolution is per FIELD: an
    /// all-or-nothing implementation drops the file's release the moment
    /// `--namespace` alone is passed. An inference is announced, never silent.
    #[test]
    fn resolve_target_precedence() {
        let declared = Some(("acme", "acme"));

        let both = resolve_target(Some("ops"), Some("ops-bot"), declared);
        assert_eq!(both.namespace, "ops");
        assert_eq!(both.release, "ops-bot");
        assert_eq!(
            both.announcement, None,
            "nothing was inferred, so there is nothing to announce"
        );

        let inferred = resolve_target(None, None, declared);
        assert_eq!(inferred.namespace, "acme");
        assert_eq!(inferred.release, "acme");
        let note = inferred
            .announcement
            .expect("an inferred target must be announced");
        assert!(note.contains("curie.yaml"), "must name the source: {note}");
        assert!(note.contains("acme"), "must name the value: {note}");

        // Secondary path: one flag set, one field taken from the file.
        let mixed = resolve_target(Some("ops"), None, declared);
        assert_eq!(mixed.namespace, "ops", "the flag wins its own field");
        assert_eq!(
            mixed.release, "acme",
            "the other field still comes from curie.yaml"
        );
        let note = mixed
            .announcement
            .expect("a partially inferred target is still an inference");
        assert!(note.contains("curie.yaml"), "{note}");

        let defaults = resolve_target(None, None, None);
        assert_eq!(defaults.namespace, "curie");
        assert_eq!(defaults.release, "curie");
        assert_eq!(
            defaults.announcement, None,
            "the built-in default is not an inference from a file"
        );
    }

    // -- D6c: the three surviving mutations ----------------------------------

    /// Hardcoding the Ok arm of the docker check survived the whole suite: no
    /// test ever set `docker_ok: false` alongside a bundle, so the Missing arm
    /// and the summary branch that depends on it were both unreached.
    #[test]
    fn docker_not_reachable_is_reported_and_gates_the_summary() {
        let f = Facts {
            docker_ok: false,
            ..laptop()
        };
        let checks = evaluate(&f);
        let c = find(&checks, "docker");
        assert_eq!(c.state, State::Missing);
        assert!(
            c.fix.as_deref().unwrap_or("").contains("Docker"),
            "the fix must name Docker: {:?}",
            c.fix
        );
        assert!(
            summary(&checks).contains("Docker is not reachable"),
            "{}",
            summary(&checks)
        );
    }

    /// The clone-credential check never reached Missing in any test, so an
    /// implementation that could not produce that arm at all stayed green. The
    /// detail has to name the CONSEQUENCE: the symptom is a push that appears
    /// to work and deploys nothing.
    #[test]
    fn a_release_with_no_clone_credential_reports_missing() {
        let f = Facts {
            clone_credential: None,
            ..wired()
        };
        let c = find(&evaluate(&f), "clone-credential").clone();
        assert_eq!(c.state, State::Missing);
        assert!(
            c.detail.contains("git-push deploys will fail"),
            "must name the consequence: {}",
            c.detail
        );
        let fix = c.fix.as_deref().expect("must offer a fix");
        assert!(fix.contains("--namespace acme"), "{fix}");
        assert!(fix.contains("--release acme"), "{fix}");
    }

    /// `"ready": true` hardcoded in `to_json` survived every test, because no
    /// test read `ready` at all. Both directions are asserted, so a constant in
    /// either position fails. Docker is the lever deliberately, NOT `model-pin`:
    /// #1663 established that a floating pin is Ok-with-a-fix and must never
    /// make a working install read as unready.
    #[test]
    fn ready_is_false_when_any_check_is_missing() {
        let clean = Facts {
            agents: Some(vec![("bot".into(), Some("acme/bot".into()))]),
            ..wired()
        };
        let checks = evaluate(&clean);
        assert!(
            checks.iter().all(|c| c.state != State::Missing),
            "the clean fixture must have no Missing check: {checks:?}"
        );
        let out = DoctorOutput {
            summary: summary(&checks),
            checks,
        };
        assert_eq!(out.to_json()["ready"], serde_json::Value::Bool(true));

        let broken = Facts {
            docker_ok: false,
            ..clean
        };
        let checks = evaluate(&broken);
        let out = DoctorOutput {
            summary: summary(&checks),
            checks,
        };
        assert_eq!(out.to_json()["ready"], serde_json::Value::Bool(false));
    }
}

// -- reading the release's values ---------------------------------------------
//
// Pure, so every shape a real values file can take is a unit test rather than a
// cluster fixture. They are extracted for that reason and no other: the reads
// they replaced were type-strict, and a bug in one could only be found by
// installing a release that had it.

/// Read a scalar at `path`, coercing the JSON kinds a Helm values file produces.
///
/// A values file is YAML, and what reaches here depends on whether the author
/// quoted the value: `githubAppId: 4475970` arrives as a number and
/// `--set-string` arrives as a string. An `as_str()`-only read called an install
/// with the unquoted form credential-less (#1253). Arrays, objects and null stay
/// `None` -- widening to "stringify anything" would let structure leak into a
/// report -- and the empty-string filter predates this and survives it, because
/// a chart default of `""` is an unset field, not a value.
fn scalar_at(values: &serde_json::Value, path: &[&str]) -> Option<String> {
    let mut node = values;
    for key in path {
        node = node.get(key)?;
    }
    let rendered = match node {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => render_number(n),
        serde_json::Value::Bool(b) => b.to_string(),
        _ => return None,
    };
    Some(rendered).filter(|s| !s.is_empty())
}

fn render_number(n: &serde_json::Number) -> String {
    if let Some(i) = n.as_i64() {
        return i.to_string();
    }
    if let Some(u) = n.as_u64() {
        return u.to_string();
    }
    match n.as_f64() {
        // The scientific-notation shape (#1253): an unquoted id can reach us as
        // `4.47597e6`, which is the same id, not a new one. Rendering it as
        // `4475970.0` would report a value nobody typed.
        Some(f) if f.is_finite() && f.fract() == 0.0 => format!("{f:.0}"),
        _ => n.to_string(),
    }
}

/// Whether a values entry means `true`.
///
/// A real bool, or the literal string, case- and space-insensitively: both
/// `--set-string api.ingress.enabled=true` and a quoted values-file entry record
/// a string, and an `as_bool()`-only read reported an install whose ingress was
/// already on as unexposed. Nothing else counts -- not `"1"`, not `"yes"`, not a
/// number. Reading any non-empty string as true would report an install as
/// exposed on a typo, in the one check whose job is to say whether the API is
/// reachable from outside.
fn truthy(node: Option<&serde_json::Value>) -> bool {
    match node {
        Some(serde_json::Value::Bool(b)) => *b,
        Some(serde_json::Value::String(s)) => s.trim().eq_ignore_ascii_case("true"),
        _ => false,
    }
}

/// Which clone credential the release carries. NAMES and shapes only -- never
/// the Secret's key name, and never key material.
///
/// The existing-Secret path is checked first because it is what the chart
/// template actually consumes when it is set, and the chart calls it the
/// recommended path while the inline flags are a quick trial (#1255). Reporting
/// both would invite an operator to "fix" an install that is already working.
fn clone_credential_from_values(values: &serde_json::Value) -> Option<String> {
    if let Some(secret) = scalar_at(values, &["api", "githubAppExistingSecret"]) {
        return Some(format!("github app (secret={secret})"));
    }
    match scalar_at(values, &["api", "githubAppId"]) {
        Some(id) => Some(format!("github app (app_id={id})")),
        None => scalar_at(values, &["api", "githubToken"]).map(|_| "personal access token".into()),
    }
}

/// How the API is reachable from outside, given the release's values and the
/// NodePort read (`None` when there is none, or when nothing looked).
fn api_exposure_from_values(
    values: &serde_json::Value,
    nodeport: Option<String>,
) -> Option<String> {
    let ingress = values.get("api").and_then(|api| api.get("ingress"));
    if truthy(ingress.and_then(|i| i.get("enabled"))) {
        return Some(match scalar_at(values, &["api", "ingress", "host"]) {
            Some(host) => format!("ingress ({host})"),
            None => "ingress".to_string(),
        });
    }
    nodeport.map(|port| format!("NodePort {port}"))
}

/// The recorded sandbox egress allowlist, and whether `cluster up` can re-supply
/// it exactly.
///
/// Returns `(cidrs, is_reproducible)`, and **a false gate always returns an empty
/// list**. `ops::up_value_plan` rewrites every `--allow-web-egress` entry as one
/// TCP/443 rule, so an entry shaped any other way cannot be reproduced by the
/// flag; an entry this reader cannot name makes the WHOLE allowlist
/// non-reproducible rather than being skipped, because handing the operator a
/// `cluster up` carrying fewer CIDRs than the release records narrows a live
/// NetworkPolicy. The count bound is readability only and flips the gate -- it
/// never truncates.
fn sandbox_egress_from_values(values: &serde_json::Value) -> (Vec<String>, bool) {
    // More than this and the fix stops being readable. Exceeding it refuses; it
    // does not drop entries.
    const READABILITY_BOUND: usize = 10;
    let recorded = values
        .get("security")
        .and_then(|s| s.get("networkPolicy"))
        .and_then(|n| n.get("allowedEgress"));
    let entries = match recorded {
        // No policy recorded is not a lossy read: emitting no flags reproduces
        // "nothing" exactly. Otherwise every release without an allowlist would
        // carry the hazard clause.
        None | Some(serde_json::Value::Null) => return (Vec::new(), true),
        Some(serde_json::Value::Array(entries)) => entries,
        // A shape this reader does not understand is still a recorded policy.
        Some(_) => return (Vec::new(), false),
    };
    if entries.len() > READABILITY_BOUND {
        return (Vec::new(), false);
    }
    let mut cidrs = Vec::with_capacity(entries.len());
    for entry in entries {
        let cidr = scalar_at(entry, &["cidr"]).unwrap_or_default();
        let cidr = cidr.trim();
        if cidr.is_empty() || !is_plain_https(entry) {
            return (Vec::new(), false);
        }
        cidrs.push(cidr.to_string());
    }
    (cidrs, true)
}

/// Whether one `allowedEgress` entry is exactly the one TCP/443 rule that
/// `--allow-web-egress` produces. Anything else -- another protocol, another
/// port, more than one rule, no rules at all -- would be COERCED to TCP/443 by
/// re-supplying it, which is a narrowing wearing the shape of a fix.
fn is_plain_https(entry: &serde_json::Value) -> bool {
    let Some(serde_json::Value::Array(ports)) = entry.get("ports") else {
        return false;
    };
    let [rule] = ports.as_slice() else {
        return false;
    };
    scalar_at(rule, &["protocol"]).is_some_and(|p| p.eq_ignore_ascii_case("TCP"))
        && scalar_at(rule, &["port"]).as_deref() == Some("443")
}

// -- observation --------------------------------------------------------------

/// Gather the facts. Every probe is read-only and failure-tolerant: a missing
/// tool or an unreachable cluster is a fact to report, never an error to raise.
pub async fn gather(namespace: &str, release: &str, api: Option<(&str, &str)>) -> Facts {
    // Four independent probes, issued as ONE stage: none reads another's
    // output, and every one of them is a subprocess plus a round trip that
    // doctor used to pay for in series. The RESULTS are consumed below in
    // exactly the order they were awaited in before -- docker here, the kube
    // context at its early return, the two helm answers at
    // `classify_release_probe` -- so a run where the context is empty simply
    // drops the helm answers on the floor rather than reordering any decision.
    let helm_list_args = ["list", "-n", namespace, "-o", "json"];
    let (docker_ok, context_probe, helm_present, release_listing) = tokio::join!(
        probe_ok("docker", &["info"]),
        capture("kubectl", &["config", "current-context"]),
        probe_ok("helm", &["version", "--short"]),
        capture("helm", &helm_list_args),
    );

    let mut f = Facts {
        docker_ok,
        bundle_name: bundle_name(),
        // What this run was pointed at, so every fix -- the model-pin `--set`
        // and the four cluster commands alike -- names the release this run
        // reported on rather than whatever `cluster up` happens to default to
        // (#1358 item 1, #1950).
        target: Some((namespace.to_string(), release.to_string())),
        ..Default::default()
    };

    // Names, never values: the id is the configuration, not a secret. Blank is
    // not a configured model (#229), and the filter is applied at every one of
    // the three sources so an empty value cannot win precedence.
    f.model_shell = std::env::var(curie_aci_protocol::env_keys::CURIE_MODEL)
        .ok()
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty());

    for name in crate::commands::MODEL_CREDENTIAL_ENV_NAMES {
        if let Ok(value) = std::env::var(name) {
            if value.is_empty() {
                continue;
            }
            f.model_credential = Some(name.to_string());
            f.model_credential_source = Some("environment".into());
            // `cluster up` binds its real-model credential from the canonical
            // CURIE_CREDENTIALS variable. Derive only its safe provider name
            // for the recovery command, then drop the credential value.
            f.model_credential_provider = (name == "CURIE_CREDENTIALS")
                .then(|| crate::ops::provider_from_credential_prefix(&value))
                .flatten();
            break;
        }
        if crate::secrets::is_saved(name).unwrap_or(false) {
            f.model_credential = Some(name.to_string());
            f.model_credential_source = Some("curie secrets".into());
            break;
        }
    }

    // Optional: everything else needs only kubectl and helm, so an absent or
    // unreachable API narrows the report rather than failing it.
    if let Some((url, key)) = api {
        if let Ok(client) = crate::api::ApiClient::new(url, key) {
            if let Ok(agents) = client.list_agents().await {
                // One pass, two facts. The `(name, repo_full_name)` collection
                // is the repo-binding check's input and stays exactly as it
                // was; the per-agent model was previously discarded here, which
                // is why the highest-precedence source was invisible (#1950).
                let mut bindings = Vec::with_capacity(agents.len());
                for a in agents {
                    if let Some(model) = a
                        .model
                        .as_deref()
                        .map(str::trim)
                        .filter(|model| !model.is_empty())
                    {
                        f.model_agent_overrides
                            .push((a.name.clone(), model.to_string()));
                    }
                    bindings.push((a.name, a.repo_full_name));
                }
                f.agents = Some(bindings);
            }
        }
    }

    let (ok, ctx, _) = context_probe;
    if !ok || ctx.trim().is_empty() {
        return f;
    }
    f.kube_context = Some(ctx.trim().to_string());

    // Both helm answers were issued alongside the context probe above and are
    // consumed here. `helm version --short` runs rather than
    // `ops::fetch_release_chart`, which collapses every nonzero exit to
    // `Ok(None)` -- the collapse that made a missing helm, a cluster that could
    // not answer and an empty namespace report identically.
    // `fetch_release_chart` is left alone; installation.rs still calls it.
    //
    // helm's stderr is bound to `_` and never read, and that is the security
    // property rather than an oversight: it is an arbitrary external line that
    // can carry an `Authorization` header, an exec-plugin's argv, or a
    // token-bearing URL, and this report is pasted into issues and chat. No
    // prefix denylist can enumerate that risk, so no subprocess stderr reaches
    // a check's `detail` or `fix` -- the chart, context and NodePort values a
    // check does render are bounded, structured fields, not diagnostic text
    // (#1348).
    let (listed, stdout, _) = release_listing;
    f.release = classify_release_probe(helm_present, listed, &stdout, release);
    if !matches!(f.release, ReleaseProbe::Installed { .. }) {
        return f;
    }

    let common = crate::ops::CommonOpts {
        namespace: namespace.to_string(),
        release: release.to_string(),
        dry_run: false,
    };
    // Two SEPARATE reads, deliberately, issued CONCURRENTLY.
    //
    // Separate: `fetch_release_values` reports only what the operator supplied,
    // so an operator who never set a model has nothing to read there and the
    // chart default the sandboxes boot is invisible -- the #1950 defect.
    // `--all` returns the computed values. The two stay apart because switching
    // slack_configured, api.ingress.* or clone_credential onto computed values
    // would silently change three unrelated checks.
    //
    // Concurrent: neither consumes the other's output and both depend only on
    // `common`, so awaiting them in turn made every cluster run pay two helm
    // spawns and two API-server round trips back to back for nothing. Each
    // result keeps its own failure-tolerant handling below.
    let (computed, values) = tokio::join!(
        crate::ops::fetch_release_computed_values(&common),
        crate::ops::fetch_release_values(&common),
    );

    if let Ok(Some(computed)) = computed {
        // The key travels with the id: the chart reads one of two, and a fix
        // naming the other one is a command that changes nothing.
        if let Some((model, key)) = runner_model_from_values(&computed) {
            f.model_release_default = Some(model);
            f.model_release_key = Some(key);
        }
        // Off the SAME read, never a third helm call: whether that id is the
        // one the pod actually boots.
        f.model_release_fake = release_fake_model(&computed);
    }

    if let Ok(Some(values)) = values {
        // Presence only, both of them: socket mode needs the pair, and reading
        // one while reporting both is what made a half-wired release read as Ok.
        f.slack_bot_token = scalar_at(&values, &["dispatcher", "slack", "botToken"]).is_some();
        f.slack_app_token = scalar_at(&values, &["dispatcher", "slack", "appToken"]).is_some();
        // Ask the single decision function first with no NodePort. A `Some`
        // means ingress already answered and the probe would only be discarded,
        // so the kubectl round-trip is skipped on the wired path -- doctor is
        // interactive and that is a subprocess plus an API call per run. The
        // decision itself still lives in exactly one place; deciding it here
        // too, to skip the read, is how a helper stays right while the report
        // goes wrong.
        f.api_exposure = match api_exposure_from_values(&values, None) {
            Some(exposure) => Some(exposure),
            None => api_exposure_from_values(&values, api_nodeport(namespace, release).await),
        };
        f.clone_credential = clone_credential_from_values(&values);
        (f.sandbox_egress_cidrs, f.sandbox_egress_is_reproducible) =
            sandbox_egress_from_values(&values);
    }
    f
}

/// The kubectl read behind `api_nodeport`, extracted pure so the Service NAME
/// it asks for is unit-testable without a cluster or a child process (#1533).
///
/// The chart renders the API Service as `{{ include "curie.fullname" . }}-api`,
/// so a `{release}-api` guess reads nothing and `cluster_facts` renders "API
/// not exposed" -- a FALSE readiness verdict from a doctor that exists to
/// prevent exactly that.
fn api_nodeport_command(
    namespace: &str,
    fullname: &crate::ops::ReleaseFullname,
) -> crate::ops::OpsCommand {
    crate::ops::OpsCommand::new(
        "kubectl",
        vec![
            crate::ops::plain("get"),
            crate::ops::plain("svc"),
            crate::ops::plain(fullname.resource("api")),
            crate::ops::plain("-n"),
            crate::ops::plain(namespace),
            crate::ops::plain("-o"),
            crate::ops::plain("jsonpath={.spec.ports[?(@.nodePort)].nodePort}"),
        ],
    )
}

/// The API Service's nodePort, when it is exposed that way.
///
/// Resolves the rendered fullname here rather than at `cluster_facts`' entry:
/// this is the only branch that needs it (the ingress branch returns before
/// reaching it), so an ingress install pays no extra kubectl round-trip.
async fn api_nodeport(namespace: &str, release: &str) -> Option<String> {
    let fullname = crate::ops::release_fullname(namespace, release).await;
    let (ok, out, _) = capture_cmd(&api_nodeport_command(namespace, &fullname)).await;
    let port = out.trim().to_string();
    (ok && !port.is_empty()).then_some(port)
}

fn bundle_name() -> Option<String> {
    let raw = std::fs::read_to_string(".claude-plugin/plugin.json").ok()?;
    let v: serde_json::Value = serde_json::from_str(&raw).ok()?;
    v.get("name")?.as_str().map(str::to_string)
}

async fn probe_ok(program: &str, args: &[&str]) -> bool {
    capture(program, args).await.0
}

async fn capture(program: &str, args: &[&str]) -> (bool, String, String) {
    let cmd = crate::ops::OpsCommand::new(
        program,
        args.iter().map(|a| crate::ops::plain(*a)).collect(),
    );
    capture_cmd(&cmd).await
}

/// Run an already-built [`crate::ops::OpsCommand`] and read it the way `doctor`
/// reads everything: a spawn failure is indistinguishable from the command
/// failing, because either way the fact being probed could not be established.
/// One spelling of that fallback, so a caller cannot accidentally treat a spawn
/// failure as a real answer.
async fn capture_cmd(cmd: &crate::ops::OpsCommand) -> (bool, String, String) {
    crate::ops::run_capture(cmd)
        .await
        .unwrap_or((false, String::new(), String::new()))
}

/// What `curie doctor` reports.
#[derive(Debug)]
pub struct DoctorOutput {
    pub checks: Vec<Check>,
    pub summary: String,
}

impl crate::ui::CliOutput for DoctorOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "summary": self.summary,
            "ready": self.checks.iter().all(|c| c.state != State::Missing),
            "deploys_verified": self
                .checks
                .iter()
                .any(|c| c.id == "repo-binding" && c.state == State::Ok),
            "checks": self.checks,
            "guidance": guidance(&self.checks),
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        for c in &self.checks {
            ui.payload_plain(&format!(
                "{}  {:<26} {}",
                c.state.glyph(),
                c.title,
                c.detail
            ));
            if let Some(fix) = &c.fix {
                ui.payload_plain(&format!("      → {fix}"));
            }
        }
        ui.payload_plain("");
        ui.payload_plain(&self.summary);
        if let Some(hint) = guidance(&self.checks) {
            ui.payload_plain(&hint);
        }
    }
}

/// Resolve the platform API connection doctor should use.
///
/// Explicit `--api-url`/`--api-key` (and their env vars, via clap) win.
/// Omitted values are discovered from the release with the same helpers
/// sibling cluster verbs use. Discovery errors are discarded: gather is
/// failure-tolerant, and an unreachable API narrows the report to
/// `agents: None` rather than failing the whole run (#1367).
///
/// The two legs are independent -- the URL comes off the release's Services and
/// the key off its chart Secret -- so they are issued concurrently rather than
/// the key waiting on the URL. One behavioral consequence, deliberate: when the
/// URL turns out not to be discoverable, the key discovery has still RUN and
/// its result is discarded. It is a read-only Secret lookup either way, and the
/// report is identical; what changes is that doctor no longer pays for the two
/// round trips back to back.
pub async fn resolve_api(
    namespace: &str,
    release: &str,
    api_url: Option<&str>,
    api_key: Option<&str>,
) -> Option<(String, String)> {
    let (url, key) = tokio::join!(
        async {
            match nonempty(api_url) {
                Some(url) => Some(url.to_string()),
                None => crate::ops::discover_api_url(namespace, release).await.ok(),
            }
        },
        async {
            match nonempty(api_key) {
                Some(key) => Some(key.to_string()),
                None => crate::ops::discover_api_key(namespace, release).await.ok(),
            }
        },
    );
    Some((url?, key?))
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

pub async fn doctor(
    namespace: &str,
    release: &str,
    api_url: Option<&str>,
    api_key: Option<&str>,
) -> DoctorOutput {
    let resolved = resolve_api(namespace, release, api_url, api_key).await;
    let api = resolved
        .as_ref()
        .map(|(url, key)| (url.as_str(), key.as_str()));
    let checks = evaluate(&gather(namespace, release, api).await);
    let summary = summary(&checks);
    DoctorOutput { checks, summary }
}
