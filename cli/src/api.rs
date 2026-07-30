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
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ConnectorManifests {
    #[serde(default)]
    pub manifests: Vec<serde_json::Value>,
    #[serde(default)]
    pub mcp_entries: std::collections::BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Agent {
    pub id: String,
    pub name: String,
    pub slack_channel: String,
    /// The repository whose pushes deploy this agent (ADR-0014). Identity, set
    /// only at creation -- `AgentUpdate` deliberately excludes it -- so the CLI
    /// must send it up front or the agent can never reach git-flow (#1064).
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
    /// Set when `--repo` could not be applied because the agent already exists
    /// and its repo binding is immutable. Surfaced so the operator learns it
    /// silently did nothing (#1064).
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
/// `repo_full_name` is sent only when asked: it is UNIQUE per agent, so an
/// unsolicited value would 409 against whichever agent already owns that repo.
fn agent_create_body(
    name: &str,
    slack_channel: &str,
    repo_full_name: Option<&str>,
) -> serde_json::Value {
    let mut body = json!({"name": name, "slack_channel": slack_channel});
    if let Some(repo) = repo_full_name {
        body["repo_full_name"] = json!(repo);
    }
    body
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

    pub async fn update_agent_channel(&self, agent_id: &str, slack_channel: &str) -> Result<Agent> {
        let resp = self
            .http
            .patch(format!("{}/agents/{agent_id}", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(&json!({"slack_channel": slack_channel}))
            .send()
            .await
            .context("PATCH /agents/{id}")?;
        Self::expect_ok(resp, "updating the agent channel")
            .await?
            .json()
            .await
            .context("decoding updated agent")
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

    /// Find the agent by name (or create it), reconciling its Slack channel with
    /// an explicitly-passed `--slack-channel`. A new agent binds to the passed
    /// channel (or the default); an existing agent's channel is moved via PATCH
    /// only when a channel was passed and differs -- an omitted channel never
    /// silently overwrites what is already set.
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
                // The repo binding is identity: `AgentUpdate` excludes it, so a
                // PATCH would return 200 and change nothing. Say so plainly
                // rather than let the operator believe it took (#1064).
                let repo_note = match (repo_full_name, agent.repo_full_name.as_deref()) {
                    (Some(want), Some(have)) if want == have => None,
                    (Some(want), Some(have)) => Some(format!(
                        "agent is already bound to {have}; --repo {want} was NOT applied \
                         (the repo binding is set at creation and cannot be changed)"
                    )),
                    (Some(want), None) => Some(format!(
                        "agent exists with no repo binding; --repo {want} was NOT applied \
                         (the binding is set at creation only, so this agent cannot use \
                         git-flow -- recreate it to bind one)"
                    )),
                    (None, _) => None,
                };
                let outcome = match slack_channel {
                    Some(channel) if channel != agent.slack_channel => {
                        let from = agent.slack_channel.clone();
                        let updated = self.update_agent_channel(&agent.id, channel).await?;
                        let to = updated.slack_channel.clone();
                        return Ok((updated, ChannelOutcome::Updated { from, to }, repo_note));
                    }
                    other => {
                        let channel = agent.slack_channel.clone();
                        ChannelOutcome::Unchanged {
                            channel,
                            passed: other.is_some(),
                        }
                    }
                };
                Ok((agent, outcome, repo_note))
            }
            None => {
                let channel = slack_channel.unwrap_or(DEFAULT_SLACK_CHANNEL);
                let agent = self.create_agent(name, channel, repo_full_name).await?;
                let outcome = ChannelOutcome::Created(agent.slack_channel.clone());
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
    use super::{agent_create_body, is_insecure_endpoint};

    #[test]
    fn create_agent_body_omits_repo_unless_asked() {
        // repo_full_name is UNIQUE per agent, so sending an unsolicited value
        // would 409 against whichever agent already owns that repo.
        let body = agent_create_body("bot", "C123", None);
        assert_eq!(body["name"], "bot");
        assert_eq!(body["slack_channel"], "C123");
        assert!(body.get("repo_full_name").is_none());
    }

    #[test]
    fn create_agent_body_binds_the_repo_when_asked() {
        // Creation is the ONLY chance: AgentUpdate excludes repo_full_name, so
        // an agent created without it can never reach git-flow (#1064).
        let body = agent_create_body("bot", "C123", Some("acme/bundle"));
        assert_eq!(body["repo_full_name"], "acme/bundle");
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
