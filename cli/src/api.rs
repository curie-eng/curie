//! Client for the platform API (apps/api, committed openapi.json contract).
//!
//! `curie cluster deploy` pushes a local bundle to the platform: find-or-create the
//! agent, create a version, upload the tar.gz bundle (validated server-side by
//! the frozen plugin-format package), and create a deployment. Auth is the
//! X-API-Key header.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub struct ApiClient {
    base_url: String,
    api_key: String,
    http: reqwest::Client,
}

/// The channel used when an agent is first created if `--slack-channel` is
/// omitted; on an existing agent an omitted channel is left untouched. Must
/// satisfy the platform API's channel-ID validation (`^[CDG][A-Z0-9]{7,}$`),
/// so this is a valid Slack channel-ID shape, not a `#name`.
pub const DEFAULT_SLACK_CHANNEL: &str = "C0LOCALDEV";

/// Kubernetes objects the API derived from a version's `connectors.yaml`.
///
/// The API renders these; the CLI applies them. Rendering is a pure function so
/// the API needs no cluster access for it, and cluster-write authority stays
/// with the operator running this command (ADR-0086, #1063).
/// What a named `deploy.yaml` target resolves to (ADR-0089).
#[derive(Debug, Clone, Deserialize)]
pub struct ResolvedTarget {
    pub agent: Option<String>,
    pub env: String,
    pub slack_channel: Option<String>,
}

/// One target plus the name it is declared under (ADR-0089).
#[derive(Debug, Clone, Deserialize)]
pub struct NamedTarget {
    pub name: String,
    pub agent: Option<String>,
    pub env: String,
    pub slack_channel: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ListedTargets {
    #[serde(default)]
    pub targets: Vec<NamedTarget>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ConnectorManifests {
    #[serde(default)]
    pub manifests: Vec<serde_json::Value>,
    /// The Secret Curie owns, and the keys whose VALUES this caller must
    /// resolve. Declared by the API rather than inferred from the manifests:
    /// since #1163 a connector may reference a Secret provisioned out of band,
    /// and resolving those keys locally would both fail and defeat the point.
    #[serde(default)]
    pub owned_secret_name: String,
    #[serde(default)]
    pub owned_secret_keys: Vec<String>,
    #[serde(default)]
    pub mcp_entries: std::collections::BTreeMap<String, serde_json::Value>,
}

/// One agent's channel binding (ADR-0096, #1459): `kind` names the ingress
/// (`"slack"` today) and `address` is the kind-specific identifier the worker
/// resolver matches turns against. Singular per ADR-0089's one-agent-one-channel
/// rule, mirroring the committed `ChannelBinding`.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ChannelBinding {
    pub kind: String,
    pub address: String,
}

/// The four-field channel route `curie adapter bind` WRITES (#1516).
///
/// A separate write-side struct rather than `Option` fields on [`ChannelBinding`]:
/// the route is write-only (an agent read returns exactly `{kind, address}`), so
/// widening the read model would make it claim a shape the API never returns.
/// All four fields travel together because the API refuses a non-`slack` binding
/// whose reply route is half set, and the `adapter` slug is always the operator's
/// `--adapter-slug` -- it selects which stored secret the worker sends, so a
/// profile-supplied value never reaches this struct on its own.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChannelBindingWrite {
    pub kind: String,
    pub address: String,
    pub endpoint: String,
    pub adapter: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Agent {
    pub id: String,
    pub name: String,
    pub channel: ChannelBinding,
    /// The repository whose pushes deploy this agent (ADR-0014). Set at
    /// creation via `--repo`, or bound later by PATCH since `AgentUpdate`
    /// carries the field (#1194). ADR-0091 dropped the unique index, so
    /// several agents may share one repository.
    #[serde(default)]
    pub repo_full_name: Option<String>,
    /// Tool names gated behind human approval (#245). Present on `AgentOut`;
    /// `#[serde(default)]` keeps older/leaner responses parsing to None.
    #[serde(default)]
    pub approval_required_tools: Option<Vec<String>>,
    /// Manifest route name -> workspace binding (#247, #420). Read back by
    /// `approvals --list-routes` and written by `--route`/`--route-approvers`.
    /// `#[serde(default)]` keeps a pre-#247 response parsing to None, which is
    /// the same fact as "no routes bound".
    #[serde(default)]
    pub approval_routes: Option<std::collections::BTreeMap<String, ApprovalRouteBinding>>,
    /// Per-agent model override, forwarded as `CURIE_MODEL` at sandbox boot
    /// (#254). `None` means no override: the platform default applies. Modeled
    /// since #1311 gave the CLI a verb that reads and writes it -- until then
    /// the only way to set it was a raw authenticated PATCH, which is exactly
    /// the one-entry-point rule this field now satisfies.
    #[serde(default)]
    pub model: Option<String>,
    /// Per-agent thinking-depth override, forwarded as `CURIE_THINKING` at
    /// sandbox boot (#1182, ADR-0098). `None` means the platform default
    /// applies. Same three-way PATCH semantics as `model`: omitted leaves it,
    /// explicit null clears it.
    #[serde(default)]
    pub thinking: Option<String>,
}

/// One route's workspace binding, mirroring the committed `ApprovalRouteBinding`.
///
/// The two fields are the axes ADR-0034 unfused and the CLI must keep visibly
/// apart: `channel` is WHERE the card posts, `approvers` is WHO may act on it.
/// Collapsing them in the output would re-fuse in presentation what the schema
/// separates.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ApprovalRouteBinding {
    pub channel: String,
    /// Absent means the card channel's members are the approvers, the zero-setup
    /// default. Skipped on serialize so a channel-only write sends no `approvers`
    /// key at all: the API models the block with `extra="forbid"`, and an explicit
    /// null is a different statement from an omitted key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approvers: Option<ApprovalApprovers>,
}

/// Who may resolve a route's approvals, mirroring the committed
/// `ApprovalApprovers`. The API settles the precedence (`users` wins over
/// `group`); the CLI never reorders or merges them, it forwards what was asked
/// for and lets the one authoritative validator answer.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ApprovalApprovers {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub users: Option<Vec<String>>,
}

// --- The input side of the same contract (#1072) -------------------------------
//
// The two structs above decode API RESPONSES and must stay tolerant: a field a
// newer server adds should not break an older CLI. The two below decode an
// OPERATOR-AUTHORED `--routes-from` file and must do the opposite.
//
// The asymmetry is the whole fix, so the pair lives here next to its lenient
// twin rather than off in the command module. A typo'd key in a route file is
// not a harmless unknown: dropping `approver` (for `approvers`) leaves a
// channel-only binding, and a binding with no approvers block falls back to
// card-channel membership, so the operator who meant to narrow authority to one
// group has instead granted it to everyone in the channel.
//
// The API already guards this with `extra="forbid"` on `ApprovalRouteBinding`,
// on the stated premise that it is the binding's only writer. #1057 made the
// CLI a second writer and re-serialized a parsed struct, so the operator's own
// bytes never reached that guard. This restores it on the writer that bypassed
// it, and follows the convention `cli/src/spec.rs` already states for
// operator-authored files: an authoring typo fails loud rather than silently
// dropping the intended field.

/// One route binding as written in a `--routes-from` file. Strict by design.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteBindingInput {
    pub channel: String,
    #[serde(default)]
    pub approvers: Option<ApproversInput>,
}

/// An `approvers` block as written in a `--routes-from` file. Strict by design.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApproversInput {
    #[serde(default)]
    pub group: Option<String>,
    #[serde(default)]
    pub users: Option<Vec<String>>,
}

impl From<ApproversInput> for ApprovalApprovers {
    fn from(input: ApproversInput) -> Self {
        ApprovalApprovers {
            group: input.group,
            users: input.users,
        }
    }
}

impl From<RouteBindingInput> for ApprovalRouteBinding {
    fn from(input: RouteBindingInput) -> Self {
        ApprovalRouteBinding {
            channel: input.channel,
            approvers: input.approvers.map(Into::into),
        }
    }
}

/// One approval record, hand-mirroring the committed `ApprovalOut` (#506). Only
/// the fields the CLI renders are modeled; serde ignores the rest of the payload.
#[derive(Debug, Clone, Deserialize)]
pub struct ApprovalRecord {
    pub id: String,
    pub author: String,
    #[serde(default)]
    pub route: Option<String>,
    #[serde(default)]
    pub gate_kind: Option<String>,
    #[serde(default)]
    pub granted_tool: Option<String>,
    pub status: String,
    pub conversation_id: String,
    pub summary: String,
    #[serde(default)]
    pub expires_at: Option<String>,
    #[serde(default)]
    pub resolved_by: Option<String>,
    /// The channel the approval card was posted to (#1078). Load-bearing, not
    /// a delivery detail: with no `approvers` block on the route this channel's
    /// MEMBERS are the approver set, and `--resolve` needs it as
    /// `--actor-channel` because that is what the server-side authorizer
    /// compares against. Without it the value is underivable from the CLI and
    /// a guess is a refusal. `#[serde(default)]` keeps a record that predates
    /// route bindings parsing to None, which is the same fact as "no route, so
    /// the requesting channel applies".
    #[serde(default)]
    pub card_channel: Option<String>,
}

/// What a deploy did with the agent's Slack channel, for the summary printout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelOutcome {
    /// A new agent was created bound to this channel.
    Created(String),
    /// An existing agent's channel was moved.
    Updated { from: String, to: String },
    /// An existing agent's channel was left as-is. `passed` records whether a
    /// `--slack-channel` was supplied (and merely matched) so the caller can hint
    /// how to move it when none was given.
    Unchanged { channel: String, passed: bool },
}

#[derive(Debug, Clone, Deserialize)]
pub struct Version {
    pub id: String,
    pub version_label: String,
    // Extra VersionOut fields for the `versions` listing verb; `#[serde(default)]`
    // keeps the deploy path (which only reads id/version_label) tolerant of a
    // leaner response.
    #[serde(default)]
    pub commit_sha: Option<String>,
    /// The bundle's content hash (`VersionOut.bundle_sha256`): the field that
    /// proves parity — "the artifact running here is the one I tested" (#548).
    /// `#[serde(default)]` keeps the deploy path (which reads only id/label)
    /// tolerant of a leaner response.
    #[serde(default)]
    pub bundle_sha256: Option<String>,
    #[serde(default)]
    pub created_by: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    /// `VersionOut.agent_id` (#691). `VersionOut` marks it required, but the
    /// deploy path tolerates lean responses, so `#[serde(default)]` is kept
    /// consistent with the other extra fields above.
    #[serde(default)]
    pub agent_id: Option<String>,
    /// `VersionOut.bundle_ref` (#691). Same lean-response tolerance as above.
    #[serde(default)]
    pub bundle_ref: Option<String>,
}

/// One learned memory entry (`MemoryEntryOut`) for the `memory` listing verb.
#[derive(Debug, Clone, Deserialize)]
pub struct MemoryEntry {
    pub index: u64,
    pub content: String,
    pub version: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Bundle {
    pub bundle_ref: String,
    pub bundle_sha256: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Deployment {
    pub id: String,
    pub environment: String,
    pub status: String,
    // Extra DeploymentOut fields used to resolve the in-force version for the
    // `approvals` gate read (#546); `#[serde(default)]` keeps the deploy path
    // (which reads only id/environment/status) tolerant of a leaner response.
    #[serde(default)]
    pub version_id: Option<String>,
    #[serde(default)]
    pub deployed_at: Option<String>,
}

/// One readable text file from a version's stored bundle (`BundleFile` in
/// openapi.json), from `GET /agents/{id}/versions/{version_id}/files`.
#[derive(Debug, Clone, Deserialize)]
pub struct BundleFile {
    pub path: String,
    pub content: String,
}

/// The agent kill-switch state (`KillState` in openapi.json): the response of
/// `POST /agents/{id}/kill` and `POST /agents/{id}/resume`.
#[derive(Debug, Clone, Deserialize)]
pub struct KillState {
    pub killed: bool,
}

/// Whether a thread has a pending forced-sandbox-release request
/// (`ThreadResetState` in openapi.json): the response of
/// `POST /agents/{id}/threads/{thread_key}/reset` (#737).
#[derive(Debug, Clone, Deserialize)]
pub struct ThreadResetState {
    pub requested: bool,
}

/// The enqueued eval job's identity (`EvalTriggerResult` in openapi.json): the
/// response of `POST /evals/trigger`. `sha` keys the run's matrix column and
/// `model` echoes the requested model (#526) so a sweep can pair each job to the
/// row it will produce.
#[derive(Debug, Clone, Deserialize)]
pub struct EvalTriggerResult {
    pub stream_id: String,
    pub sha: String,
    pub suite: String,
    #[serde(default)]
    pub model: Option<String>,
}

/// One per-`(version, model)` slice of the eval matrix rollup
/// (`EvalModelVersionSummary` in openapi.json): the graded aggregates scoped to a
/// single version column rather than blended across the shown window. `model` is
/// `None` for the matrix's unlabelled column (a run with no resolved model).
///
/// The sweep reads this rather than the window-blended `model_summaries` because
/// `completed` there sums over EVERY in-window sha for a model, so a model that
/// completed on an older in-window sha keeps `completed > 0` even when its turns
/// never complete on the triggered sha -- masking the "never completed" outcome
/// the sweep must fail on (issue #814). Scoping the row to the triggered sha (the
/// one the sweep just enqueued) is what makes `never_completed` honest.
///
/// `completed` is a subset of `total`: the graded rows whose turn actually
/// reached a verdict, as opposed to a graded fail that never completed at all
/// (a classified failure, the wrong terminal status, or a transport/runner
/// exception). `total > 0 && completed == 0` on the triggered sha is a model that
/// never produced one completed turn for this run -- distinct from a real 0%,
/// which the sweep reports and fails on (#622, #526 AC4, ADR-0068). `plumbing`
/// counts the rows that ran but were never graded (ADR-0055, #612/#606 -- the
/// fake-model tier is plumbing-only): excluded from `passed`/`total` rather than
/// fabricated into a pass or fail, so a plumbing-only model's row still lands with
/// `total == 0` (#700). `#[serde(default)]` keeps the counts tolerant of an API
/// that predates them.
#[derive(Debug, Clone, Deserialize)]
pub struct EvalModelVersionSummary {
    pub version: String,
    #[serde(default)]
    pub model: Option<String>,
    pub passed: u64,
    pub total: u64,
    #[serde(default)]
    pub completed: u64,
    #[serde(default)]
    pub plumbing: u64,
}

/// The eval matrix grid (`EvalMatrix` in openapi.json): `GET /evals/matrix`. The
/// sweep reads `model_version_summaries` (the per-`(version, model)` dimension)
/// plus `versions` (the shown version columns, newest first): a `--model` sweep
/// uses `versions` to scope readiness to the run it just triggered, so a prior
/// run's rows cannot satisfy the exit condition on the first poll (issue #608),
/// and reads the per-version rollup scoped to the triggered sha so a prior sha's
/// completions cannot mask the triggered sha's zero-completed outcome (#814). The
/// window-blended `model_summaries` and the per-case `rows`/`cases`/`models` grid
/// are carried by the endpoint but unused here.
#[derive(Debug, Clone, Deserialize)]
pub struct EvalMatrix {
    pub suite: String,
    /// The shown version columns (commit shas), most recent first. A triggered
    /// run's sha appears here only once at least one of its traces has landed.
    #[serde(default)]
    pub versions: Vec<String>,
    #[serde(default)]
    pub model_version_summaries: Vec<EvalModelVersionSummary>,
}

/// The per-agent budget (`BudgetConfig` in openapi.json): the request and
/// response body of `PUT /agents/{id}/budget`. Both fields are optional; an
/// omitted field means "platform default" server-side, so we only serialize the
/// ones the caller set.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BudgetConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens_per_run: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_usd_per_day: Option<f64>,
}

/// The artifacts a deploy produces, for the summary printout.
pub struct DeployOutcome {
    pub agent: Agent,
    pub version: Version,
    pub bundle: Bundle,
    pub deployment: Deployment,
    pub channel: ChannelOutcome,
    /// The warning about what `--repo` did, if any: either the binding was
    /// declined because the agent is already bound to a different repository,
    /// or a bind was requested and the platform's response did not carry it
    /// back. `None` on a successful bind or when `--repo` was not passed.
    /// Surfaced so the operator never believes a binding that is not there
    /// (#1064, #1212).
    pub repo_note: Option<String>,
}

/// Whether this endpoint would send the `X-API-Key` over cleartext HTTP to a
/// non-loopback host (a forgotten `https://` that leaks the key on the wire).
/// Local dev over `http://localhost` is expected and returns false.
///
/// Pure and public so `cluster deploy` can REFUSE (not merely warn) egressing an
/// auto-discovered strong release key to a cleartext non-loopback endpoint
/// (#705): the same classifier that drives [`warn_if_insecure`] gates the refusal,
/// so warn and refuse can never disagree.
pub fn is_insecure_endpoint(base_url: &str) -> bool {
    let lower = base_url.trim().to_ascii_lowercase();
    if lower.starts_with("https://") {
        return false;
    }
    let authority = lower
        .strip_prefix("http://")
        .unwrap_or(&lower)
        .split('/')
        .next()
        .unwrap_or("");
    // Strip the port, handling both `host:port` and `[::1]:port` IPv6 forms.
    let host = if let Some(rest) = authority.strip_prefix('[') {
        rest.split(']').next().unwrap_or("")
    } else {
        authority.split(':').next().unwrap_or("")
    };
    let is_loopback = host == "localhost"
        || host.ends_with(".localhost")
        || host.starts_with("127.")
        || host == "::1"
        || host == "0.0.0.0";
    !is_loopback
}

/// Warn (to stderr) when the endpoint would leak the API key over cleartext
/// HTTP. See [`is_insecure_endpoint`].
fn warn_if_insecure(base_url: &str) {
    if is_insecure_endpoint(base_url) {
        eprintln!(
            "warning: API endpoint '{base_url}' uses cleartext HTTP; the API key \
             will be sent unencrypted. Use an https:// URL for non-local endpoints."
        );
    }
}

/// The `POST /agents` body. Pure so the shape is testable without a live API.
///
/// `repo_full_name` is sent only when asked, because a value the caller did not
/// pass is not a binding the caller intended. Since ADR-0091 and migration 0018
/// the column is no longer unique, so an unsolicited value would silently bind
/// the new agent to that repository rather than 409, which is worse.
fn agent_create_body(
    name: &str,
    slack_channel: &str,
    repo_full_name: Option<&str>,
) -> serde_json::Value {
    let mut body = json!({
        "name": name,
        "channel": {"kind": "slack", "address": slack_channel},
    });
    if let Some(repo) = repo_full_name {
        body["repo_full_name"] = json!(repo);
    }
    body
}

/// The `PATCH /agents/{id}` body for the fields deploy reconciles. Pure so the
/// shape is testable without a live API.
///
/// Each key appears only when its argument is `Some`: omission is the wire
/// spelling for "leave this field unchanged". Neither key is ever emitted as an
/// explicit JSON `null`, because the router guards both behind `is not None`
/// and Pydantic decodes a `null` and an absent key to the same `None`, so a
/// `null` would read as "omitted" while looking on the wire like an intent to
/// clear (#1071, the same trap documented on [`ApiClient::set_approval_routes`]).
fn agent_update_body(
    slack_channel: Option<&str>,
    repo_full_name: Option<&str>,
) -> serde_json::Value {
    let mut body = json!({});
    if let Some(channel) = slack_channel {
        body["channel"] = json!({"kind": "slack", "address": channel});
    }
    if let Some(repo) = repo_full_name {
        body["repo_full_name"] = json!(repo);
    }
    body
}

/// Is this 404 a MISSING ENDPOINT rather than a missing resource?
///
/// FastAPI answers an unrouted path with exactly `{"detail":"Not Found"}`,
/// while a handler that ran and found nothing sets its own detail ("version
/// not found"). That difference is the only signal available for "this CLI is
/// newer than the platform it is talking to" -- and without it the operator
/// sees a bare 404 and has no way to tell a stale release from a typo.
///
/// Both of the failures this guards against happened for real: a CLI with
/// `--target` against an API that predated the resolver, and a CLI that
/// applied connectors against an API that could not render them.
fn is_unrouted(status: reqwest::StatusCode, body: &str) -> bool {
    status == reqwest::StatusCode::NOT_FOUND
        && serde_json::from_str::<serde_json::Value>(body)
            .ok()
            .and_then(|v| v.get("detail").and_then(|d| d.as_str()).map(str::to_string))
            .is_some_and(|d| d == "Not Found")
}

impl ApiClient {
    /// The server caps `/approvals` results at this many rows
    /// (`apps/api/.../routers/approvals.py`: `min(max(limit, 1), 200)`); the CLI
    /// requests exactly the cap so hitting it is how the caller detects possible
    /// truncation of the pending list (#670).
    pub const APPROVALS_LIST_LIMIT: usize = 200;

    pub fn new(base_url: &str, api_key: &str) -> Result<Self> {
        warn_if_insecure(base_url);
        let http = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .context("building HTTP client")?;
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            http,
        })
    }

    async fn expect_ok(resp: reqwest::Response, what: &str) -> Result<reqwest::Response> {
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        // An unrouted path means the platform is older than this CLI, which is
        // a different problem from a missing resource and has a different fix.
        // Saying so here covers every caller at once rather than one call site.
        if is_unrouted(status, &body) {
            bail!(
                "{what} failed: this platform release does not have that endpoint, so it is \
                 older than this CLI. Upgrade the release, or use a CLI matching it."
            );
        }
        bail!("{what} failed with {status}: {}", body.trim());
    }

    pub async fn list_agents(&self) -> Result<Vec<Agent>> {
        let resp = self
            .http
            .get(format!("{}/agents", self.base_url))
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .context("GET /agents")?;
        Self::expect_ok(resp, "listing agents")
            .await?
            .json()
            .await
            .context("decoding agent list")
    }

    pub async fn create_agent(
        &self,
        name: &str,
        slack_channel: &str,
        repo_full_name: Option<&str>,
    ) -> Result<Agent> {
        let body = agent_create_body(name, slack_channel, repo_full_name);
        let resp = self
            .http
            .post(format!("{}/agents", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(&body)
            .send()
            .await
            .context("POST /agents")?;
        Self::expect_ok(resp, "creating the agent")
            .await?
            .json()
            .await
            .context("decoding created agent")
    }

    pub async fn find_or_create_agent(&self, name: &str, slack_channel: &str) -> Result<Agent> {
        if let Some(existing) = self
            .list_agents()
            .await?
            .into_iter()
            .find(|a| a.name == name)
        {
            return Ok(existing);
        }
        self.create_agent(name, slack_channel, None).await
    }

    /// `PATCH /agents/{id}` with a body the caller already built (see
    /// [`agent_update_body`]). The returned `Agent` is the row as the API
    /// stored it, so callers report what took rather than what they intended.
    pub async fn update_agent(&self, agent_id: &str, body: &serde_json::Value) -> Result<Agent> {
        let resp = self
            .http
            .patch(format!("{}/agents/{agent_id}", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(body)
            .send()
            .await
            .context("PATCH /agents/{id}")?;
        Self::expect_ok(resp, "updating the agent")
            .await?
            .json()
            .await
            .context("decoding updated agent")
    }

    /// Write one agent's four-field channel route (#1516): `PATCH /agents/{id}`
    /// with the whole [`ChannelBindingWrite`], since the API refuses a
    /// non-`slack` binding whose reply route is half set.
    pub async fn set_agent_channel(
        &self,
        agent_id: &str,
        binding: &ChannelBindingWrite,
    ) -> Result<Agent> {
        self.update_agent(agent_id, &json!({ "channel": binding }))
            .await
    }

    /// Bind the per-agent connector secrets (ADR-0009, #429). The values travel
    /// in the JSON request body (over the API's X-API-Key channel), never in
    /// argv; the API stores them and returns the agent with names only.
    pub async fn update_agent_secrets(
        &self,
        agent_id: &str,
        secrets: &std::collections::BTreeMap<String, String>,
    ) -> Result<Agent> {
        let resp = self
            .http
            .patch(format!("{}/agents/{agent_id}", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(&json!({ "secrets": secrets }))
            .send()
            .await
            .context("PATCH /agents/{id}")?;
        Self::expect_ok(resp, "binding agent connector secrets")
            .await?
            .json()
            .await
            .context("decoding updated agent")
    }

    /// Find the agent by name (or create it), reconciling its Slack channel and
    /// its repo binding with an explicitly-passed `--slack-channel`/`--repo`.
    ///
    /// A new agent binds to the passed channel (or the default); an existing
    /// agent's channel is moved via PATCH only when a channel was passed and
    /// differs -- an omitted channel never silently overwrites what is already
    /// set. An existing agent with no repo binding is bound to `--repo` in that
    /// same PATCH; one already bound elsewhere is left alone. The third return
    /// value is the operator note, set when the repo binding did not end up
    /// where `--repo` asked and left `None` when it did.
    async fn resolve_agent(
        &self,
        name: &str,
        slack_channel: Option<&str>,
        repo_full_name: Option<&str>,
    ) -> Result<(Agent, ChannelOutcome, Option<String>)> {
        let existing = self
            .list_agents()
            .await?
            .into_iter()
            .find(|a| a.name == name);
        match existing {
            Some(agent) => {
                // `AgentUpdate` has carried `repo_full_name` since ADR-0091 and
                // #1194, so an agent with no binding is bound right here rather
                // than sent away to be built again from scratch (#1212). An
                // agent already bound elsewhere is NOT moved: a deploy must not
                // silently reroute which repository's pushes reach it.
                let channel_move = slack_channel.filter(|c| *c != agent.channel.address.as_str());
                let current_repo = agent.repo_full_name.as_deref();
                let (repo_bind, mut repo_note) = match (repo_full_name, current_repo) {
                    (Some(want), None) => (Some(want), None),
                    (Some(want), Some(have)) if want != have => (
                        None,
                        Some(format!(
                            "agent is already bound to {have}; --repo {want} was NOT \
                             applied. A deploy does not move an existing repo binding, \
                             because that reroutes which repository's pushes deploy this \
                             agent. To rebind deliberately, PATCH repo_full_name on \
                             /agents/{id} against the platform API.",
                            id = agent.id
                        )),
                    ),
                    _ => (None, None),
                };
                // One request rather than two removes the client-side window
                // where the channel moved and a second call then failed, and
                // the channel-uniqueness 409 aborts before repo_full_name is
                // reached. The server still commits per field, so the two
                // fields are not applied atomically.
                let previous_channel = channel_move.map(|_| agent.channel.address.clone());
                let agent = if channel_move.is_some() || repo_bind.is_some() {
                    let body = agent_update_body(channel_move, repo_bind);
                    self.update_agent(&agent.id, &body).await?
                } else {
                    agent
                };
                // `AgentUpdate` ignores unknown keys, so a platform older than
                // #1194 answers the bind with 200 and stores nothing. The
                // response is the only place that shows up: without this check
                // the deploy reports a binding the platform never made, which
                // is the exact failure #1064 put an operator warning here for.
                let bound = agent.repo_full_name.as_deref();
                if let Some(want) = repo_bind.filter(|&w| bound != Some(w)) {
                    repo_note = Some(format!(
                        "--repo {want} was sent but the platform did not apply it (the agent \
                         reports {stored}). That usually means the platform release predates \
                         the AgentUpdate.repo_full_name field (#1194) and ignored the key. \
                         git-flow will not route pushes to this agent until the platform is \
                         upgraded or the binding is set another way.",
                        stored = bound.unwrap_or("no binding")
                    ));
                }
                let outcome = match previous_channel {
                    Some(from) => ChannelOutcome::Updated {
                        from,
                        to: agent.channel.address.clone(),
                    },
                    None => ChannelOutcome::Unchanged {
                        channel: agent.channel.address.clone(),
                        passed: slack_channel.is_some(),
                    },
                };
                Ok((agent, outcome, repo_note))
            }
            None => {
                let channel = slack_channel.unwrap_or(DEFAULT_SLACK_CHANNEL);
                let agent = self.create_agent(name, channel, repo_full_name).await?;
                let outcome = ChannelOutcome::Created(agent.channel.address.clone());
                Ok((agent, outcome, None))
            }
        }
    }

    pub async fn create_version(
        &self,
        agent_id: &str,
        version_label: &str,
        created_by: &str,
    ) -> Result<Version> {
        let resp = self
            .http
            .post(format!("{}/agents/{agent_id}/versions", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(&json!({"version_label": version_label, "created_by": created_by}))
            .send()
            .await
            .context("POST /agents/{id}/versions")?;
        Self::expect_ok(resp, "creating the version")
            .await?
            .json()
            .await
            .context("decoding created version")
    }

    pub async fn upload_bundle(
        &self,
        agent_id: &str,
        version_id: &str,
        archive: Vec<u8>,
    ) -> Result<Bundle> {
        let part = reqwest::multipart::Part::bytes(archive)
            .file_name("bundle.tar.gz")
            .mime_str("application/gzip")
            .context("building multipart body")?;
        let form = reqwest::multipart::Form::new().part("file", part);
        let resp = self
            .http
            .put(format!(
                "{}/agents/{agent_id}/versions/{version_id}/bundle",
                self.base_url
            ))
            .header("X-API-Key", &self.api_key)
            .multipart(form)
            .send()
            .await
            .context("PUT bundle")?;
        Self::expect_ok(resp, "uploading the bundle")
            .await?
            .json()
            .await
            .context("decoding bundle result")
    }

    pub async fn create_deployment(
        &self,
        agent_id: &str,
        version_id: &str,
        environment: &str,
    ) -> Result<Deployment> {
        let resp = self
            .http
            .post(format!("{}/deployments", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(&json!({
                "agent_id": agent_id,
                "version_id": version_id,
                "environment": environment,
            }))
            .send()
            .await
            .context("POST /deployments")?;
        Self::expect_ok(resp, "creating the deployment")
            .await?
            .json()
            .await
            .context("decoding created deployment")
    }

    /// The full deploy flow: resolve agent (create or channel-reconcile),
    /// version, bundle, deployment.
    #[allow(clippy::too_many_arguments)] // one cohesive deploy call; a struct would not clarify it
    pub async fn deploy(
        &self,
        agent_name: &str,
        slack_channel: Option<&str>,
        version_label: &str,
        created_by: &str,
        environment: &str,
        archive: Vec<u8>,
        secrets: &std::collections::BTreeMap<String, String>,
        repo_full_name: Option<&str>,
    ) -> Result<DeployOutcome> {
        let (agent, channel, repo_note) = self
            .resolve_agent(agent_name, slack_channel, repo_full_name)
            .await?;
        // Bind per-agent connector secrets (ADR-0009, #429). A PATCH covers both
        // a freshly created agent and a redeploy that rotates a value; an empty
        // map leaves the agent's current secrets untouched.
        if !secrets.is_empty() {
            self.update_agent_secrets(&agent.id, secrets).await?;
        }
        let version = self
            .create_version(&agent.id, version_label, created_by)
            .await?;
        let bundle = self.upload_bundle(&agent.id, &version.id, archive).await?;
        let deployment = self
            .create_deployment(&agent.id, &version.id, environment)
            .await?;
        Ok(DeployOutcome {
            agent,
            version,
            bundle,
            deployment,
            channel,
            repo_note,
        })
    }

    /// Resolve an agent identifier (its `name`, or its `id`) to the full record
    /// by listing agents and matching -- the same name-based resolution the
    /// deploy flow uses (`resolve_agent`), so the lifecycle verbs never grow a
    /// second resolution path. Errors when nothing matches; never creates.
    pub async fn find_agent(&self, identifier: &str) -> Result<Agent> {
        self.list_agents()
            .await?
            .into_iter()
            .find(|a| a.name == identifier || a.id == identifier)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "no agent found matching {identifier:?} (by name or id); deploy it first with `curie cluster deploy`"
                )
            })
    }

    /// Flip the agent kill switch on: `POST /agents/{id}/kill` (no request body).
    pub async fn kill_agent(&self, agent_id: &str) -> Result<KillState> {
        let resp = self
            .http
            .post(format!("{}/agents/{agent_id}/kill", self.base_url))
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .context("POST /agents/{id}/kill")?;
        Self::expect_ok(resp, "killing the agent")
            .await?
            .json()
            .await
            .context("decoding kill state")
    }

    /// Flip the agent kill switch off: `POST /agents/{id}/resume` (no request body).
    pub async fn resume_agent(&self, agent_id: &str) -> Result<KillState> {
        let resp = self
            .http
            .post(format!("{}/agents/{agent_id}/resume", self.base_url))
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .context("POST /agents/{id}/resume")?;
        Self::expect_ok(resp, "resuming the agent")
            .await?
            .json()
            .await
            .context("decoding kill state")
    }

    /// Force a thread's sandbox to be released: `POST
    /// /agents/{id}/threads/{thread_key}/reset` (no request body, #737). The
    /// worker's next maintenance tick deletes the thread's claim and route, so
    /// its next message cold-creates a fresh sandbox.
    pub async fn reset_thread(&self, agent_id: &str, thread_key: &str) -> Result<ThreadResetState> {
        let resp = self
            .http
            .post(format!(
                "{}/agents/{agent_id}/threads/{thread_key}/reset",
                self.base_url
            ))
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .context("POST /agents/{id}/threads/{thread_key}/reset")?;
        Self::expect_ok(resp, "resetting the thread")
            .await?
            .json()
            .await
            .context("decoding thread reset state")
    }

    /// Poll whether a thread's forced reset is still outstanding: `GET
    /// /agents/{id}/threads/{thread_key}/reset` (#735). `requested` stays true
    /// from the POST until the worker's maintenance tick releases the sandbox,
    /// then flips to false -- so a caller can wait for the release to actually
    /// land (and the next message to be safe from adopting the pre-reset
    /// sandbox) before it acts. Mirrors the POST above.
    pub async fn thread_reset_state(
        &self,
        agent_id: &str,
        thread_key: &str,
    ) -> Result<ThreadResetState> {
        let resp = self
            .http
            .get(format!(
                "{}/agents/{agent_id}/threads/{thread_key}/reset",
                self.base_url
            ))
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .context("GET /agents/{id}/threads/{thread_key}/reset")?;
        Self::expect_ok(resp, "polling the thread reset state")
            .await?
            .json()
            .await
            .context("decoding thread reset state")
    }

    /// Set the agent budget: `PUT /agents/{id}/budget` with a `BudgetConfig` body.
    pub async fn set_budget(&self, agent_id: &str, budget: &BudgetConfig) -> Result<BudgetConfig> {
        let resp = self
            .http
            .put(format!("{}/agents/{agent_id}/budget", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(budget)
            .send()
            .await
            .context("PUT /agents/{id}/budget")?;
        Self::expect_ok(resp, "updating the budget")
            .await?
            .json()
            .await
            .context("decoding budget")
    }

    /// List an agent's immutable versions, ascending by `created_at` (oldest
    /// first): `GET /agents/{id}/versions`. `commands::versions` reverses
    /// this to newest-first before display/JSON output.
    /// Ask the API to render a version's declared connectors.
    ///
    /// `release`/`namespace`/`app_name` are install-time facts the API does not
    /// know -- they live with whoever ran `cluster up` -- so the caller supplies
    /// them and the API stays a pure function.
    /// Resolve a named target by POSTing the `deploy.yaml` TEXT.
    ///
    /// The CLI deliberately does not parse this file. One parser means the CLI
    /// and the validator cannot disagree about where a deploy lands, and it
    /// keeps a YAML crate out of this binary -- serde_yaml is deprecated and
    /// its half-dozen forks have no clear successor to depend on (ADR-0089).
    pub async fn resolve_deploy_target(
        &self,
        content: &str,
        target: &str,
    ) -> Result<ResolvedTarget> {
        let resp = self
            .http
            .post(format!("{}/deploy-targets/resolve", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(&serde_json::json!({"content": content, "target": target}))
            .send()
            .await
            .context("resolving the deploy target")?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if is_unrouted(status, &body) {
            anyhow::bail!(
                "this platform release has no deploy-target resolver, so `--target` cannot \
                 work against it. The API predates ADR-0089. Upgrade the release, or drop \
                 `--target` and pass `--agent`/`--env`/`--slack-channel` directly."
            );
        }
        if !status.is_success() {
            anyhow::bail!("resolving target `{target}` failed with {status}: {body}");
        }
        serde_json::from_str(&body).context("decoding the resolved deploy target")
    }

    /// Every target a `deploy.yaml` declares, dev before prod.
    ///
    /// The file CONTENT goes to the API rather than being parsed here, for the
    /// same reason `resolve_deploy_target` does: ADR-0089 keeps exactly one
    /// parser for this format, and a second in Rust could disagree with it
    /// about where a deploy lands.
    pub async fn list_deploy_targets(&self, content: &str) -> Result<ListedTargets> {
        let resp = self
            .http
            .post(format!("{}/deploy-targets/list", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(&serde_json::json!({"content": content, "target": ""}))
            .send()
            .await
            .context("listing the deploy targets")?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        // Same skew guard as `resolve_deploy_target`: a platform predating this
        // endpoint answers with FastAPI's bare 404 body, which is otherwise
        // indistinguishable from a real error and sends an operator hunting.
        if is_unrouted(status, &body) {
            anyhow::bail!(
                "this platform release cannot list deploy targets, so onboarding every \
                 target at once will not work against it. Upgrade the release, or deploy \
                 each target with `cluster deploy --target <name>`."
            );
        }
        if !status.is_success() {
            anyhow::bail!("listing the deploy targets failed ({status}): {body}");
        }
        serde_json::from_str(&body).context("decoding the deploy target list")
    }

    pub async fn version_connectors(
        &self,
        agent_id: &str,
        version_id: &str,
        release: &str,
        namespace: &str,
        app_name: &str,
    ) -> Result<ConnectorManifests> {
        let resp = self
            .http
            .get(format!(
                "{}/agents/{agent_id}/versions/{version_id}/connectors",
                self.base_url
            ))
            .query(&[
                ("release", release),
                ("namespace", namespace),
                ("app_name", app_name),
            ])
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .context("GET /agents/{id}/versions/{vid}/connectors")?;
        Self::expect_ok(resp, "rendering declared connectors")
            .await?
            .json()
            .await
            .context("decoding connector manifests")
    }

    pub async fn list_versions(&self, agent_id: &str) -> Result<Vec<Version>> {
        let resp = self
            .http
            .get(format!("{}/agents/{agent_id}/versions", self.base_url))
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .context("GET /agents/{id}/versions")?;
        Self::expect_ok(resp, "listing versions")
            .await?
            .json()
            .await
            .context("decoding version list")
    }

    /// List an agent's learned memory, oldest first: `GET /agents/{id}/memory`.
    pub async fn list_memory(&self, agent_id: &str) -> Result<Vec<MemoryEntry>> {
        let resp = self
            .http
            .get(format!("{}/agents/{agent_id}/memory", self.base_url))
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .context("GET /agents/{id}/memory")?;
        Self::expect_ok(resp, "listing memory")
            .await?
            .json()
            .await
            .context("decoding memory list")
    }

    /// The pending approval records for an agent: `GET /approvals?status_filter=
    /// pending&agent_id=<id>&limit=<APPROVALS_LIST_LIMIT>`. Hand-mirrors the
    /// committed `ApprovalOut` shape (only the fields the CLI renders; serde
    /// ignores the rest), the same way `Agent`/`KillState` mirror
    /// `openapi.json` (#506). The `limit` query param requests the server's max
    /// page size explicitly rather than relying on its default (#670).
    pub async fn list_pending_approvals(&self, agent_id: &str) -> Result<Vec<ApprovalRecord>> {
        let limit = Self::APPROVALS_LIST_LIMIT.to_string();
        let resp = self
            .http
            .get(format!("{}/approvals", self.base_url))
            .header("X-API-Key", &self.api_key)
            .query(&[
                ("status_filter", "pending"),
                ("agent_id", agent_id),
                ("limit", limit.as_str()),
            ])
            .send()
            .await
            .context("GET /approvals")?;
        Self::expect_ok(resp, "listing pending approvals")
            .await?
            .json()
            .await
            .context("decoding approvals")
    }

    /// Resolve one approval as a chosen actor: `POST /approvals/{id}/resolve`.
    /// The server owns the resolve-once CAS, the authorizer (self-approval block,
    /// route approvers), and the resume-turn enqueue; `resolved_by` is the acting
    /// actor (the `--as` flag), which is what makes requester != approver
    /// expressible without hand-curling the API (#506).
    pub async fn resolve_approval(
        &self,
        approval_id: &str,
        decision: &str,
        resolved_by: &str,
        note: Option<&str>,
        actor_channel: Option<&str>,
    ) -> Result<ApprovalRecord> {
        let mut body = json!({ "decision": decision, "resolved_by": resolved_by });
        if let Some(note) = note {
            body["note"] = json!(note);
        }
        if let Some(chan) = actor_channel {
            body["actor_channel"] = json!(chan);
        }
        let resp = self
            .http
            .post(format!("{}/approvals/{approval_id}/resolve", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(&body)
            .send()
            .await
            .context("POST /approvals/{id}/resolve")?;
        Self::expect_ok(resp, "resolving approval")
            .await?
            .json()
            .await
            .context("decoding resolved approval")
    }

    /// Set the agent's approval-required tool gates: `PATCH /agents/{id}` with
    /// `approval_required_tools` (an empty list clears them). Returns the updated
    /// agent so the caller can echo the effective gates.
    pub async fn set_approval_tools(&self, agent_id: &str, tools: &[String]) -> Result<Agent> {
        let resp = self
            .http
            .patch(format!("{}/agents/{agent_id}", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(&json!({ "approval_required_tools": tools }))
            .send()
            .await
            .context("PATCH /agents/{id} (approval gates)")?;
        Self::expect_ok(resp, "updating approval gates")
            .await?
            .json()
            .await
            .context("decoding updated agent")
    }

    /// Set the agent's approval route bindings: `PATCH /agents/{id}` with
    /// `approval_routes`. Like `set_approval_tools` this is a FULL REPLACEMENT of
    /// the map, not a merge, matching the field's semantics on `AgentUpdate`.
    ///
    /// An empty map is sent as `{}`, not JSON `null` (#1071). The router clears the
    /// bindings only behind `if data.approval_routes is not None` (#247), and
    /// Pydantic decodes an explicit `null` and an omitted key to the same `None`, so
    /// `null` reads as "field omitted" and the bindings survive. Only `{}` passes
    /// that guard. `crud.update_agent_approval_routes` storing `routes or None` is
    /// the STORAGE normalization applied after the guard, not the wire spelling.
    pub async fn set_approval_routes(
        &self,
        agent_id: &str,
        routes: &std::collections::BTreeMap<String, ApprovalRouteBinding>,
    ) -> Result<Agent> {
        let resp = self
            .http
            .patch(format!("{}/agents/{agent_id}", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(&json!({ "approval_routes": routes }))
            .send()
            .await
            .context("PATCH /agents/{id} (approval routes)")?;
        Self::expect_ok(resp, "updating approval routes")
            .await?
            .json()
            .await
            .context("decoding updated agent")
    }

    /// Enqueue an on-demand platform eval run: `POST /evals/trigger`. With no
    /// `version_id` the agent's active dev deployment is evaluated. `model` (#526)
    /// pins the run's model dimension so a sweep posts one trigger per model and
    /// reads the comparison back off the matrix. Returns the enqueued job identity.
    pub async fn trigger_eval(
        &self,
        agent_id: &str,
        suite: Option<&str>,
        model: Option<&str>,
    ) -> Result<EvalTriggerResult> {
        let mut body = json!({ "agent_id": agent_id });
        if let Some(suite) = suite {
            body["suite"] = json!(suite);
        }
        if let Some(model) = model {
            body["model"] = json!(model);
        }
        let resp = self
            .http
            .post(format!("{}/evals/trigger", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(&body)
            .send()
            .await
            .context("POST /evals/trigger")?;
        Self::expect_ok(resp, "triggering the eval")
            .await?
            .json()
            .await
            .context("decoding eval trigger result")
    }

    /// Read the eval matrix for a suite: `GET /evals/matrix?suite=..&versions=..`.
    /// The sweep polls this for the per-model pass-rate rollup the recorder writes.
    pub async fn eval_matrix(&self, suite: &str, versions: u32) -> Result<EvalMatrix> {
        let resp = self
            .http
            .get(format!("{}/evals/matrix", self.base_url))
            .query(&[("suite", suite), ("versions", &versions.to_string())])
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .context("GET /evals/matrix")?;
        Self::expect_ok(resp, "reading the eval matrix")
            .await?
            .json()
            .await
            .context("decoding eval matrix")
    }

    /// List an agent's deployments, oldest first: `GET /deployments?agent_id={id}`.
    /// Used to resolve the in-force version whose bundle manifest gates the
    /// `approvals` read must union in (#546).
    pub async fn list_deployments(&self, agent_id: &str) -> Result<Vec<Deployment>> {
        let resp = self
            .http
            .get(format!("{}/deployments", self.base_url))
            .query(&[("agent_id", agent_id)])
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .context("GET /deployments")?;
        Self::expect_ok(resp, "listing deployments")
            .await?
            .json()
            .await
            .context("decoding deployment list")
    }

    /// Read a version's authored text files (skills, manifest, eval cases):
    /// `GET /agents/{id}/versions/{version_id}/files`. The `approvals` read pulls
    /// the deployed bundle's manifest from here to recover its `approvalPolicy`
    /// gates (#546).
    pub async fn bundle_files(&self, agent_id: &str, version_id: &str) -> Result<Vec<BundleFile>> {
        #[derive(serde::Deserialize)]
        struct BundleFiles {
            files: Vec<BundleFile>,
        }
        let resp = self
            .http
            .get(format!(
                "{}/agents/{agent_id}/versions/{version_id}/files",
                self.base_url
            ))
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .context("GET /agents/{id}/versions/{version_id}/files")?;
        let files: BundleFiles = Self::expect_ok(resp, "reading bundle files")
            .await?
            .json()
            .await
            .context("decoding bundle files")?;
        Ok(files.files)
    }

    /// Delete the agent: `DELETE /agents/{id}` (204 No Content on success).
    pub async fn delete_agent(&self, agent_id: &str) -> Result<()> {
        let resp = self
            .http
            .delete(format!("{}/agents/{agent_id}", self.base_url))
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .context("DELETE /agents/{id}")?;
        Self::expect_ok(resp, "deleting the agent").await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{agent_create_body, agent_update_body, is_insecure_endpoint};

    #[test]
    fn create_agent_body_omits_repo_unless_asked() {
        // A value the caller did not pass is not a binding the caller intended.
        // The column is no longer unique (ADR-0091, migration 0018), so an
        // unsolicited value would silently bind rather than 409.
        let body = agent_create_body("bot", "C123", None);
        assert_eq!(body["name"], "bot");
        assert_eq!(body["channel"]["kind"], "slack");
        assert_eq!(body["channel"]["address"], "C123");
        assert!(body.get("repo_full_name").is_none());
    }

    #[test]
    fn create_agent_body_binds_the_repo_when_asked() {
        // Creation is the first chance to bind, and the only one that needs no
        // second request: AgentUpdate carries repo_full_name too (#1194).
        let body = agent_create_body("bot", "C123", Some("acme/bundle"));
        assert_eq!(body["repo_full_name"], "acme/bundle");
    }

    #[test]
    fn agent_update_body_omits_both_when_neither_is_asked() {
        // Omission is how the wire says "leave this alone", so a PATCH with
        // nothing to change carries nothing at all.
        let body = agent_update_body(None, None);
        assert!(
            body.as_object().expect("an object").is_empty(),
            "was {body}"
        );
    }

    #[test]
    fn agent_update_body_carries_only_what_was_asked() {
        // Each absent key must be ABSENT, never an explicit null: the router
        // guards both fields behind `is not None`, and Pydantic decodes a null
        // and an omitted key identically, so a null would read as "omitted"
        // while looking on the wire like an intent to clear (#1071).
        let channel = agent_update_body(Some("C123"), None);
        assert_eq!(channel["channel"]["kind"], "slack");
        assert_eq!(channel["channel"]["address"], "C123");
        assert!(channel.get("repo_full_name").is_none(), "was {channel}");

        let repo = agent_update_body(None, Some("acme/bundle"));
        assert_eq!(repo["repo_full_name"], "acme/bundle");
        assert!(repo.get("channel").is_none(), "was {repo}");

        let both = agent_update_body(Some("C123"), Some("acme/bundle"));
        assert_eq!(both["channel"]["kind"], "slack");
        assert_eq!(both["channel"]["address"], "C123");
        assert_eq!(both["repo_full_name"], "acme/bundle");
    }

    #[test]
    fn https_is_always_secure() {
        assert!(!is_insecure_endpoint("https://api.example.com"));
        assert!(!is_insecure_endpoint("HTTPS://API.EXAMPLE.COM"));
    }

    #[test]
    fn http_to_loopback_is_allowed() {
        for url in [
            "http://localhost:8000",
            "http://localhost",
            "http://127.0.0.1:8000",
            "http://[::1]:8000",
            "http://0.0.0.0:8000",
            "http://api.localhost",
        ] {
            assert!(!is_insecure_endpoint(url), "expected {url} to be allowed");
        }
    }

    #[test]
    fn http_to_remote_host_is_insecure() {
        for url in [
            "http://api.example.com",
            "http://api.example.com:8000/v1",
            "http://10.0.0.5:8000",
        ] {
            assert!(is_insecure_endpoint(url), "expected {url} to warn");
        }
    }
}

#[cfg(test)]
mod skew_tests {
    use super::is_unrouted;
    use reqwest::StatusCode;

    #[test]
    fn a_bare_fastapi_404_is_read_as_a_missing_endpoint() {
        // This is exactly what an older platform returns for a path it does not
        // route, and it is the only signal that the CLI is newer than the API.
        assert!(is_unrouted(
            StatusCode::NOT_FOUND,
            r#"{"detail":"Not Found"}"#
        ));
    }

    #[test]
    fn a_handler_404_is_not_mistaken_for_a_missing_endpoint() {
        // The endpoint exists and ran; the resource is absent. Telling the
        // operator to upgrade here would send them down entirely the wrong path.
        assert!(!is_unrouted(
            StatusCode::NOT_FOUND,
            r#"{"detail":"version not found"}"#
        ));
        assert!(!is_unrouted(
            StatusCode::NOT_FOUND,
            r#"{"detail":"no target named 'prod' in deploy.yaml. Declared: dev"}"#
        ));
    }

    #[test]
    fn other_statuses_are_never_a_skew_signal() {
        for s in [
            StatusCode::OK,
            StatusCode::UNAUTHORIZED,
            StatusCode::BAD_GATEWAY,
        ] {
            assert!(!is_unrouted(s, r#"{"detail":"Not Found"}"#));
        }
    }

    #[test]
    fn a_non_json_body_is_not_a_skew_signal() {
        // A proxy or ingress can return an HTML 404 that means something else
        // entirely; guessing "upgrade your platform" from it would be wrong.
        assert!(!is_unrouted(StatusCode::NOT_FOUND, "<html>404</html>"));
    }
}
