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

/// One-time delivery returned by the administrative operator-principal mint.
///
/// Deliberately does not derive `Debug`: the token may be printed only by the
/// explicit mint result, never by an incidental diagnostic.
pub struct OperatorPrincipalDelivery {
    pub token: String,
    pub subject: String,
    pub expires_at: String,
}

/// One-time delivery returned by the administrative console login-code mint.
///
/// Like [`OperatorPrincipalDelivery`], this is intentionally not `Debug` so a
/// future error path cannot accidentally include the credential.
pub struct ConsoleLoginCodeDelivery {
    pub code: String,
    pub subject: String,
    pub expires_at: String,
}

fn required_response_string(
    body: &serde_json::Value,
    field: &str,
    response_name: &str,
) -> Result<String> {
    body.get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .with_context(|| format!("decoding {response_name}: missing string field {field:?}"))
}

/// One cursor page from the rollout-free disconnected cluster-message reply
/// relay. The API stores channel-protocol events verbatim; the CLI projects
/// only the fields needed to render progress and recognize semantic completion.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ClusterMessageReplyPage {
    pub(crate) events: Vec<ClusterMessageReplyEvent>,
    pub(crate) next_cursor: usize,
    pub(crate) terminal: bool,
}

/// The tolerant CLI projection of one channel-protocol reply event.
///
/// Unknown fields remain server-owned, while the discriminant and the two
/// values that drive terminal behavior are retained. This is intentionally not
/// another copy of the channel protocol: the API validates and stores that
/// protocol, and the CLI is only its read-side observer.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ClusterMessageReplyEvent {
    #[serde(rename = "event")]
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) text: Option<String>,
    #[serde(default)]
    pub(crate) status: Option<String>,
    #[serde(default)]
    pub(crate) outcome: Option<String>,
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

/// One environment whose pushes a repository can no longer route (#1221).
///
/// `message` is the platform resolver's OWN text. The CLI prints it verbatim
/// and never paraphrases it: the routing rule lives in `gitflow.py`, and a
/// restatement here would be free to drift from the rule a push enforces.
#[derive(Debug, Clone, Deserialize)]
pub struct RoutingCheckProblem {
    #[serde(default)]
    pub environment: String,
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub message: String,
}

/// Whether git-flow pushes to a repository still resolve to an agent (#1221).
///
/// Every field carries `serde(default)`: this decode is advisory, and a
/// platform that answers with a narrower shape must degrade to "nothing to
/// warn about" rather than panic a deploy that otherwise succeeded.
#[derive(Debug, Clone, Deserialize)]
pub struct RoutingCheck {
    #[serde(default)]
    pub repo_full_name: String,
    #[serde(default)]
    pub agent_count: u32,
    #[serde(default)]
    pub agents: Vec<String>,
    #[serde(default = "yes")]
    pub resolvable: bool,
    #[serde(default)]
    pub unresolvable: Vec<RoutingCheckProblem>,
}

/// `resolvable` defaults to TRUE when absent: silence must mean "no problem
/// reported", never a warning invented from a field the platform never sent.
fn yes() -> bool {
    true
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

/// One of an agent's channel bindings (ADR-0118, #1525): `kind` names the
/// ingress (`"slack"` today) and `address` is the kind-specific identifier the
/// worker resolver matches turns against. An agent holds one or more, so the
/// pair -- never the address alone -- identifies a binding.
///
/// Mirrors the API's `ChannelBindingOut`, which is the READ shape and carries no
/// address-shape rule (#1914): an upgraded install can hold an address the write
/// path would now refuse, and the CLI has to be able to PRINT that value rather
/// than fail to parse the agent it belongs to.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ChannelBinding {
    pub kind: String,
    pub address: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Agent {
    pub id: String,
    pub name: String,
    /// Every channel this agent answers on, ordered by `(kind, address)`
    /// server-side. One or more (ADR-0118): the API refuses to remove the last
    /// one, because an agent bound to nothing is deployed and answers nowhere.
    pub channels: Vec<ChannelBinding>,
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
    /// Manifest route name -> workspace binding (#247, #420, #1460). Read back
    /// by `approvals --list-routes`; writes use a separate strict DTO so this
    /// display-only, transport-redacted response can never become PATCH input.
    /// `#[serde(default)]` keeps a pre-#247 response parsing to None, which is
    /// the same fact as "no routes bound".
    #[serde(default)]
    pub approval_routes: Option<std::collections::BTreeMap<String, ApprovalRouteBindingResponse>>,
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
    /// Whether this agent's bindings share one workflow-state namespace
    /// (`true`) or each get their own (`false`, the default) (#1525 follow-up,
    /// ADR-0118). Cardinality alone opts an agent into multiple surfaces; this
    /// is the separate, explicit toggle for whether those surfaces share
    /// cross-turn state.
    pub memory: bool,
}

/// One route's display-only binding, mirroring `ApprovalRouteBindingOut`.
///
/// `resolution` is the one interactive Slack card, `notification` is an
/// optional text-only ping, and `approvers` is WHO may act. The response model
/// intentionally has no transport fields and no conversion into the strict
/// PATCH graph below.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ApprovalRouteBindingResponse {
    pub resolution: ApprovalResolutionTargetResponse,
    #[serde(default)]
    pub notification: Option<ApprovalNotificationTargetResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approvers: Option<ApprovalApprovers>,
}

/// Interactive target returned by the API. The current resolver supports Slack
/// only, but response decoding stays tolerant of a future server extension.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ApprovalResolutionTargetResponse {
    pub kind: String,
    pub address: String,
}

/// Text-only notification target returned by the API. Endpoint and adapter are
/// deliberately absent: transport belongs to the write/stored model only.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ApprovalNotificationTargetResponse {
    pub kind: String,
    pub address: String,
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
// The structs above decode API RESPONSES and must stay tolerant: a field a newer
// server adds should not break an older CLI. The graph below decodes an
// OPERATOR-AUTHORED `--routes-from` file and must do the opposite.
//
// The asymmetry is the whole fix, so the pair lives here next to its lenient
// twin rather than off in the command module. A typo'd key in a route file is
// not a harmless unknown: dropping `approver` (for `approvers`) leaves a route
// whose authority falls back to resolution-card membership, so the operator who
// meant to narrow authority to one group has widened it instead.
//
// The API already guards this with `extra="forbid"` on `ApprovalRouteBinding`,
// on the stated premise that it is the binding's only writer. #1057 made the
// CLI a second writer and re-serialized a parsed struct, so the operator's own
// bytes never reached that guard. This restores it on the writer that bypassed
// it, and follows the convention `cli/src/spec.rs` already states for
// operator-authored files: an authoring typo fails loud rather than silently
// dropping the intended field.

/// The only route shape accepted by `ApiClient::set_approval_routes`.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApprovalRouteBindingWrite {
    pub resolution: ApprovalResolutionTargetWrite,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notification: Option<NotificationTargetWrite>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approvers: Option<ApprovalApprovers>,
}

/// Slack-only interactive resolution target. `kind` is kept explicit as the
/// future extension point, while command validation currently refuses anything
/// except `slack`.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApprovalResolutionTargetWrite {
    pub kind: String,
    pub address: String,
}

/// Optional notification target with write-only transport routing.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NotificationTargetWrite {
    pub kind: String,
    pub address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adapter: Option<String>,
}

/// One route binding as written in a `--routes-from` file. Strict by design and
/// intentionally separate from both the tolerant response and the PATCH DTO.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteBindingInput {
    pub resolution: ApprovalResolutionTargetWrite,
    #[serde(default)]
    pub notification: Option<NotificationTargetWrite>,
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

impl From<RouteBindingInput> for ApprovalRouteBindingWrite {
    fn from(input: RouteBindingInput) -> Self {
        ApprovalRouteBindingWrite {
            resolution: input.resolution,
            notification: input.notification,
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
    /// MEMBERS are the approver set. The terminal principal carries no channel,
    /// so the CLI reports this value as the authenticated card location instead
    /// of asserting it during resolution. `#[serde(default)]` keeps a record
    /// that predates route bindings parsing to None, which means the requesting
    /// channel applies.
    #[serde(default)]
    pub card_channel: Option<String>,
}

/// What a deploy did with the agent's Slack channel, for the summary printout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelOutcome {
    /// A new agent was created bound to this channel.
    Created(String),
    /// An existing agent gained a binding it did not hold. Never a move: a
    /// deploy only ever ADDS (ADR-0118), so the agent's other channels stay
    /// routed.
    Added { address: String },
    /// An existing agent's binding set was left as-is, and carries every
    /// address in it. `passed` records whether a `--slack-channel` was supplied
    /// (and merely matched) so the caller can hint how to bind another when
    /// none was given.
    Unchanged { channels: Vec<String>, passed: bool },
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

/// Provenance nested on ``MemoryEntryOut`` (`MemoryProvenanceOut`).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct MemoryProvenance {
    #[serde(default)]
    pub learned_from_session_id: Option<String>,
    #[serde(default)]
    pub source_trace_ids: Vec<String>,
    #[serde(default)]
    pub recorded_at: String,
    #[serde(default)]
    pub source: Option<String>,
}

/// One learned memory entry (`MemoryEntryOut`) for the `memory` listing verb.
#[derive(Debug, Clone, Deserialize)]
pub struct MemoryEntry {
    pub index: u64,
    pub content: String,
    pub version: u64,
    #[serde(default)]
    pub provenance: MemoryProvenance,
}

/// One row returned by `GET /langfuse/traces`.
///
/// That API route deliberately exposes Langfuse's open-ended trace object
/// (`list[dict[str, object]]`) rather than a named OpenAPI DTO. Keep the row as
/// a typed map wrapper so the CLI can bound the collection without projecting
/// away trace/session/outcome fields a newer platform adds.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TraceListRow {
    pub id: String,
    pub name: Option<String>,
    pub timestamp: String,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// One node in the existing API's reconstructed observation tree.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ObservationNode {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(rename = "startTime", default)]
    pub start_time: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(rename = "usageDetails", default)]
    pub usage_details: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(default)]
    pub children: Vec<ObservationNode>,
}

/// The complete existing `TraceTree` response from `GET
/// /langfuse/traces/{trace_id}`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TraceTree {
    pub trace: serde_json::Map<String, serde_json::Value>,
    pub tree: Vec<ObservationNode>,
    #[serde(default)]
    pub sandbox_id: Option<String>,
    #[serde(default)]
    pub approval_decision: Option<String>,
}

fn default_true() -> bool {
    true
}

/// Scalar totals returned by the existing observability summary route.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MetricsSummary {
    pub start: String,
    pub end: String,
    pub runs: u64,
    pub latency_p95_ms: f64,
    pub tokens: u64,
    pub cost_usd: f64,
    #[serde(default = "default_true")]
    pub cost_known: bool,
    pub error_rate: f64,
}

/// One point in an existing observability metric series.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MetricPoint {
    pub ts: String,
    pub value: f64,
}

/// The existing observability metric-series DTO.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MetricSeries {
    pub metric: String,
    pub granularity: String,
    pub start: String,
    pub end: String,
    pub points: Vec<MetricPoint>,
}

/// Maximum number of metric points a CLI result may carry. This defensive
/// response check keeps a skewed backend from violating the public result bound.
pub const MAX_OBSERVABILITY_METRIC_POINTS: usize = 1000;

/// Match one agent by exact name or id. Free-standing so the deploy flow's
/// [`ApiClient::find_agent`] and the observability lookup can never
/// grow different identity rules; the no-match case is the typed
/// [`AgentLookupNotFound`] marker.
fn match_agent(
    agents: Vec<Agent>,
    identifier: &str,
) -> std::result::Result<Agent, AgentLookupNotFound> {
    agents
        .into_iter()
        .find(|a| a.name == identifier || a.id == identifier)
        .ok_or_else(|| AgentLookupNotFound(identifier.to_string()))
}

/// The Langfuse timeDimension bucket each shipped granularity maps to, and the
/// number of buckets a window touches. Buckets are wall-clock aligned: hour and
/// day boundaries are epoch-aligned in UTC, so the touched count is the
/// difference of the epoch bucket floors over the half-open `[start, end)`
/// window, which charges the window for BOTH partial end buckets (#1948
/// review). Langfuse's week boundary anchoring is not the epoch, so week adds
/// one safety bucket against the alignment skew. An unknown granularity has no
/// client-side estimate; the server answers it with a 422, which the CLI
/// preserves.
fn series_bucket_estimate(
    granularity: &str,
    start: time::OffsetDateTime,
    end: time::OffsetDateTime,
) -> Option<i64> {
    let bucket_seconds: i64 = match granularity {
        "hour" => 3600,
        "day" => 86_400,
        "week" => 604_800,
        _ => return None,
    };
    // Bucket floors are computed at nanosecond precision: whole_seconds()
    // truncates toward zero, which on a negative (pre-epoch) fractional
    // duration rounds UP and under-counts the window's first partial bucket
    // (#1948 review, round 5). div_euclid on the nanosecond remainder is a
    // true floor for every sign.
    let epoch = time::OffsetDateTime::UNIX_EPOCH;
    let bucket_nanos = bucket_seconds as i128 * 1_000_000_000;
    let bucket_floor =
        |dt: time::OffsetDateTime| ((dt - epoch).whole_nanoseconds()).div_euclid(bucket_nanos);
    // The half-open window's last instant is end minus an arbitrarily small
    // epsilon, so an exactly bucket-aligned end contributes its previous
    // bucket, while an end even a fraction of a second past a boundary (RFC
    // 3339 accepts fractional seconds) still touches the bucket that boundary
    // opens (#1948 review, round 3).
    let end_nanos = (end - epoch).whole_nanoseconds();
    let end_aligned = end_nanos.rem_euclid(bucket_nanos) == 0;
    let end_floor = if end_aligned {
        end_nanos.div_euclid(bucket_nanos) - 1
    } else {
        end_nanos.div_euclid(bucket_nanos)
    };
    let touched = (end_floor - bucket_floor(start) + 1) as i64;
    Some(if granularity == "week" {
        touched + 1
    } else {
        touched
    })
}

/// #1948: enforce the CLI's series point cap on the requested span before the
/// upstream query is dispatched, so an over-cap window never reaches Langfuse
/// and ClickHouse. Absent `--start` is bounded server-side by the metrics
/// default window (168 hours, `apps/api` config), under the cap at every
/// shipped granularity, so no estimate is attempted there; the post-dispatch
/// bound still catches an operator-raised default. Absent `--end` defaults to
/// now on the server (`apps/api/src/curie_api/metrics.py::resolve_window`), so
/// `now` is the effective bound here too. Values the CLI cannot parse stay the
/// server's 422 business rather than a stricter client-side format contract.
/// Parse a `--start`/`--end` value with the same leniency the platform API's
/// `datetime.fromisoformat` accepts for its documented common forms (#1948
/// scope review): strict RFC 3339, a date-only value (midnight UTC), a space
/// separator between date and time, a time without seconds, and a naive
/// timestamp (assumed UTC for the estimate). A value none of these cover is
/// `None`: the guard skips it and the API's own 422 or the post-dispatch bound
/// answers, because the CLI must not become stricter than the API on formats
/// neither documents.
fn parse_api_timestamp(raw: &str) -> Option<time::OffsetDateTime> {
    use time::format_description::well_known::Rfc3339;
    use time::OffsetDateTime;

    let trimmed = raw.trim();
    if let Ok(parsed) = OffsetDateTime::parse(trimmed, &Rfc3339) {
        return Some(parsed);
    }
    let mut normalized = String::from(trimmed);
    let bytes = normalized.as_bytes();
    if bytes.len() == 10 && bytes[4] == b'-' && bytes[7] == b'-' {
        // Date-only: the API resolves it to midnight.
        normalized.push_str("T00:00:00Z");
    } else {
        normalized = normalized.replacen(' ', "T", 1);
        let bytes = normalized.as_bytes();
        if bytes.len() >= 16 && bytes[10] == b'T' {
            // Where does the time end? The time-of-day segment runs from
            // index 11 to the first offset marker (Z, +, -) if any.
            let offset_start = bytes[11..]
                .iter()
                .position(|byte| *byte == b'Z' || *byte == b'+' || *byte == b'-')
                .map(|index| 11 + index)
                .unwrap_or(bytes.len());
            let time_len = offset_start - 11;
            if time_len == 5 {
                // A time truncated at the minute (HH:MM), with or without an
                // offset: fromisoformat accepts it, RFC 3339 does not, so the
                // seconds segment is inserted before any offset marker.
                normalized.insert_str(offset_start, ":00");
            }
        }
        // Python 3.11+ datetime.fromisoformat accepts a compact ±HHMM offset
        // (no colon). RFC 3339 does not, so an over-cap span in that form
        // would skip the pre-dispatch cap and still execute upstream (#1948
        // code review). Insert the colon so the estimate sees the same window.
        let bytes = normalized.as_bytes();
        if let Some(sign_at) = bytes[11.min(bytes.len())..]
            .iter()
            .position(|byte| *byte == b'+' || *byte == b'-')
            .map(|index| 11 + index)
        {
            let offset = &normalized[sign_at + 1..];
            if offset.len() == 4 && offset.as_bytes().iter().all(|byte| byte.is_ascii_digit()) {
                normalized.insert(sign_at + 3, ':');
            }
        }
        let bytes = normalized.as_bytes();
        // Nothing after byte 10 can carry an offset when the value is this
        // short, and slicing past the end would panic on an arbitrary
        // --start/--end value; the parse below answers instead (scope review
        // round 3).
        let has_offset = normalized.ends_with('Z')
            || (bytes.len() > 11
                && bytes[11..]
                    .iter()
                    .any(|byte| *byte == b'+' || *byte == b'-'));
        if !has_offset {
            // A naive timestamp has no offset in fromisoformat; the estimate
            // assumes UTC and the post-dispatch bound stays the backstop.
            normalized.push('Z');
        }
    }
    OffsetDateTime::parse(&normalized, &Rfc3339).ok()
}

fn prevalidate_series_span(
    granularity: &str,
    start: Option<&str>,
    end: Option<&str>,
) -> Result<()> {
    let Some(start) = start else {
        return Ok(());
    };
    let start_dt = match parse_api_timestamp(start) {
        Some(parsed) => parsed,
        None => return Ok(()),
    };
    let end_dt = match end {
        Some(end) => match parse_api_timestamp(end) {
            Some(parsed) => parsed,
            None => return Ok(()),
        },
        None => time::OffsetDateTime::now_utc(),
    };
    if end_dt <= start_dt {
        // An inverted or empty window is the server's business.
        return Ok(());
    }
    let Some(buckets) = series_bucket_estimate(granularity, start_dt, end_dt) else {
        return Ok(());
    };
    if buckets > MAX_OBSERVABILITY_METRIC_POINTS as i64 {
        return Err(anyhow::Error::from(
            crate::exit::CliError::failure(format!(
                "--start/--end span {start} to {} is about {buckets} {granularity} buckets, above the CLI maximum of {MAX_OBSERVABILITY_METRIC_POINTS}",
                end.unwrap_or("now")
            ))
            .with_fix("narrow --start/--end or choose a coarser --granularity"),
        ));
    }
    Ok(())
}

#[derive(Debug)]
struct ObservabilityApiUnavailable(String);

impl std::fmt::Display for ObservabilityApiUnavailable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ObservabilityApiUnavailable {}

pub fn is_observability_api_unavailable(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<ObservabilityApiUnavailable>()
            .is_some()
    })
}

/// The agent lookup found no record matching the identifier. A typed marker
/// rather than a bare message so the observability query path can tell a
/// genuine no-match (operator input, a usage refusal) from a transport or
/// availability failure, which must keep its transient or failure class
/// (#1948 review). The display text is unchanged from the message the deploy
/// flow's callers already render.
#[derive(Debug)]
struct AgentLookupNotFound(String);

impl std::fmt::Display for AgentLookupNotFound {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "no agent found matching {:?} (by name or id); deploy it first with `curie cluster deploy`",
            self.0
        )
    }
}

impl std::error::Error for AgentLookupNotFound {}

pub fn is_agent_lookup_not_found(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.downcast_ref::<AgentLookupNotFound>().is_some())
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
    /// Deployment-level runtime workspace capability; absent defaults to disabled.
    #[serde(default)]
    pub workspace_enabled: bool,
}

/// Tri-state deployment intent. Preserve omits the request key, Disable sends
/// `false`, and Enable sends `true`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceIntent {
    Preserve,
    Enable,
    Disable,
}

impl WorkspaceIntent {
    pub fn from_flags(workspace: bool, no_workspace: bool) -> Self {
        match (workspace, no_workspace) {
            (true, false) => Self::Enable,
            (false, true) => Self::Disable,
            _ => Self::Preserve,
        }
    }
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

/// One case and version verdict in the platform eval matrix.
#[derive(Debug, Clone, Deserialize)]
pub struct EvalCell {
    pub version: String,
    pub status: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub stream_id: Option<String>,
    #[serde(default)]
    pub scorer: Option<String>,
    #[serde(default)]
    pub case_count: Option<u64>,
}

/// One case row across the versions returned by the eval matrix.
#[derive(Debug, Clone, Deserialize)]
pub struct EvalMatrixRow {
    pub case_id: String,
    pub cells: Vec<EvalCell>,
}

/// The eval matrix grid (`EvalMatrix` in openapi.json): `GET /evals/matrix`. The
/// sweep reads `model_version_summaries` (the per-`(version, model)` dimension)
/// plus `versions` (the shown version columns, newest first): a `--model` sweep
/// uses `versions` to scope readiness to the run it just triggered, so a prior
/// run's rows cannot satisfy the exit condition on the first poll (issue #608),
/// and reads the per-version rollup scoped to the triggered sha so a prior sha's
/// completions cannot mask the triggered sha's zero-completed outcome (#814). The
/// window-blended `model_summaries` and the `cases`/`models` labels are carried
/// by the endpoint but unused here. The sidecar selected eval path reads `rows`
/// so it can report the worker scorer's exact verdict and detail.
#[derive(Debug, Clone, Deserialize)]
pub struct EvalMatrix {
    pub suite: String,
    /// The shown version columns (commit shas), most recent first. A triggered
    /// run's sha appears here only once at least one of its traces has landed.
    #[serde(default)]
    pub versions: Vec<String>,
    #[serde(default)]
    pub model_version_summaries: Vec<EvalModelVersionSummary>,
    #[serde(default)]
    pub rows: Vec<EvalMatrixRow>,
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

/// Artifacts prepared before deployment activation.
pub struct PreparedDeployOutcome {
    pub(crate) agent: Agent,
    pub(crate) version: Version,
    pub(crate) bundle: Bundle,
    pub(crate) channel: ChannelOutcome,
    pub(crate) repo_note: Option<String>,
    pub(crate) commit_sha: Option<String>,
    pub(crate) workspace_enabled: Option<bool>,
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
    let Ok(endpoint) = reqwest::Url::parse(base_url.trim()) else {
        return true;
    };
    match endpoint.scheme() {
        "https" => !endpoint.has_host(),
        "http" => !has_local_host(&endpoint),
        _ => true,
    }
}

fn has_local_host(endpoint: &reqwest::Url) -> bool {
    let Some(host) = endpoint.host_str() else {
        return false;
    };
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    match host.parse::<std::net::IpAddr>() {
        Ok(address) => address.is_loopback() || address.is_unspecified(),
        Err(_) => host.eq_ignore_ascii_case("localhost") || host.ends_with(".localhost"),
    }
}

/// Build the one kind of HTTP client this CLI makes requests with.
///
/// The loopback `no_proxy` rule is a SECURITY property, not a convenience: a
/// system HTTP proxy must never see traffic on a tunnel that exists precisely to
/// keep that traffic on the loopback. Both the authenticated [`ApiClient`] and
/// the unauthenticated `/health` probe need it, so it has exactly one
/// implementation here rather than two copies that have to be kept in step.
///
/// Redirects are NOT followed, and that is a SECURITY property too. reqwest
/// strips `Authorization`, cookies, and `Proxy-Authorization` when a redirect
/// crosses to another host (`reqwest/src/redirect.rs`), but it does NOT strip
/// custom headers -- and this CLI authenticates with the custom `X-API-Key`
/// header. Under reqwest's default policy (follow up to 10 hops) an endpoint
/// that answers `307`/`308 Location: http://attacker.example/...` would be
/// handed the release's auto-discovered strong key, with method and body
/// preserved. It would also defeat #705's cleartext refusal, which classifies
/// only the INITIAL `--api-url`: an `https://` URL that redirects to `http://`
/// would sail past it. And it would weaken the `/health` verification below,
/// which exists to certify THIS listener -- a redirect would let a wrong
/// listener point the probe at something that does answer `{"status": "ok"}`.
///
/// Nothing should depend on a silent redirect here: the platform API is a
/// direct JSON API reached over a loopback tunnel or an operator-supplied URL.
/// Do not "helpfully" restore following; a 3xx is surfaced to the operator as
/// its own diagnosis instead (see `verify_is_curie_api` and `ApiClient::expect_ok`).
///
/// `connect_timeout` is optional because a caller that bounds the WHOLE request
/// (`RequestBuilder::timeout`) has already bounded the connect inside it; a
/// second, equal bound would only be a second number to keep in step.
///
/// Returns reqwest's own error so each caller keeps its own `.context`.
fn http_client(
    base_url: &str,
    connect_timeout: Option<std::time::Duration>,
) -> std::result::Result<reqwest::Client, reqwest::Error> {
    let mut builder = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());
    if let Some(connect_timeout) = connect_timeout {
        builder = builder.connect_timeout(connect_timeout);
    }
    if reqwest::Url::parse(base_url.trim()).is_ok_and(|endpoint| {
        matches!(endpoint.scheme(), "http" | "https") && has_local_host(&endpoint)
    }) {
        builder = builder.no_proxy();
    }
    builder.build()
}

/// Render a 3xx response's `Location` for an operator-facing message.
///
/// A redirect is never followed by this CLI (see [`http_client`]), so where it
/// pointed is the whole diagnosis: it names what the endpoint wanted to hand the
/// request -- and, under a following client, the `X-API-Key` header -- to.
/// A 3xx with no `Location` is legal, hence the fallback rather than an unwrap.
fn redirect_target(headers: &reqwest::header::HeaderMap) -> String {
    headers
        .get(reqwest::header::LOCATION)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!("`{}`", value.trim()))
        .unwrap_or_else(|| "an unstated location (no `Location` header)".to_string())
}

/// The operator-facing diagnosis for a 3xx on a KEY-CARRYING request, or `None`
/// when the status is not a redirect.
///
/// Every authenticated path shares it, because redirects are never followed (see
/// [`http_client`]) and so a 3xx reaches all of them as an ordinary response
/// rather than as a transport error.
fn redirect_refusal(
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
) -> Option<String> {
    status.is_redirection().then(|| {
        format!(
            "the endpoint answered {status} redirecting to {}. This CLI never follows \
             redirects, because a redirect would carry the API key's `X-API-Key` header to \
             the host it names, including a downgrade from https to cleartext http (#705). \
             Point --api-url at the platform API's own address.",
            redirect_target(headers)
        )
    })
}

/// How long the `/health` probe waits. Deliberately short: `start_port_forward`
/// has already waited for kubectl's readiness line and TCP-proved the local
/// port, so by the time this runs a slow answer is a real signal about what is
/// listening, not a cold start.
const HEALTH_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// Prove that whatever answers on `base_url` really is the Curie platform API,
/// by reading its `GET /health` (`apps/api/src/curie_api/main.py`: HTTP 200 with
/// a JSON body whose `status` is `"ok"`; the route is unauthenticated, per
/// `apps/api/tests/test_channels.py`).
///
/// **Unauthenticated by construction, and that is the whole point (#705).** The
/// caller (`cluster deploy`) is already holding the release's auto-discovered
/// strong key when it calls this, and the endpoint is not yet known to be Curie.
/// Sending the key to an unproven listener is exactly the egress the cleartext
/// refusal exists to prevent. So this is a FREE function, not an [`ApiClient`]
/// method: no key needs to exist for it to run, and a 401/403 is a refusal, never
/// a prompt to retry with the key. A committed test asserts the request carries no
/// `x-api-key`.
///
/// Why it is needed at all: a squatted local port and a port-forward pointed at
/// the wrong workload are both TCP-alive and readable, so the bind and readiness
/// checks in `start_port_forward` cannot see either. This is the only check that
/// can, and it is the compensating control for release-name resolution falling
/// back to the chart rule.
///
/// The error's `Display` is the class-specific OBSERVATION ("what answered, and
/// how it differed"), with no recovery text: only the caller knows which Service
/// it forwarded and on which local port, so the caller composes the recovery
/// around this sentence.
pub async fn verify_is_curie_api(base_url: &str) -> Result<()> {
    let url = format!("{}/health", base_url.trim().trim_end_matches('/'));
    // No connect timeout: the per-request `.timeout` below bounds the whole
    // request, the connect included, at the same `HEALTH_PROBE_TIMEOUT`.
    let http = http_client(base_url, None).context("building the health-probe HTTP client")?;

    // No `X-API-Key` header. See the #705 note above.
    let response = match http.get(&url).timeout(HEALTH_PROBE_TIMEOUT).send().await {
        Ok(response) => response,
        Err(err) if err.is_timeout() => bail!(
            "GET {url} did not answer within {}s, and the local port had already been \
             TCP-proved, so something is holding it without serving the API",
            HEALTH_PROBE_TIMEOUT.as_secs()
        ),
        Err(err) if err.is_connect() => bail!(
            "GET {url} could not connect, even though the port-forward reported the port \
             ready, so the tunnel died between readiness and this check"
        ),
        Err(err) => bail!("GET {url} failed before any response arrived: {err}"),
    };

    let status = response.status();
    // Redirects are not followed (see `http_client`), so a 3xx reaches here as an
    // ordinary response. It is one of the answers this probe exists to catch: the
    // thing on the port is not the Curie API, it is something pointing elsewhere,
    // and following it would have certified the redirect target instead of the
    // tunnel endpoint.
    if status.is_redirection() {
        bail!(
            "GET {url} answered {status} redirecting to {}, and redirects are never followed, \
             so the port is held by something that forwards elsewhere rather than by the \
             Curie API, whose /health answers 200 in place",
            redirect_target(response.headers())
        );
    }
    if matches!(
        status,
        reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
    ) {
        bail!(
            "GET {url} answered {status}, but the Curie API serves /health unauthenticated, \
             so an auth challenge means a different service holds the port (the release key \
             was not sent, and is not retried)"
        );
    }
    if status == reqwest::StatusCode::NOT_FOUND {
        bail!(
            "GET {url} answered 404, so something is listening there but it does not route \
             the Curie API's /health"
        );
    }
    if status != reqwest::StatusCode::OK {
        bail!("GET {url} answered {status}, not the 200 the Curie API's /health returns");
    }

    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let is_json = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .eq_ignore_ascii_case("application/json");
    let body = response.text().await.unwrap_or_default();

    // Deliberately NOT parsed as JSON first: a dev server answering 200 with
    // HTML is the squatted-port case the issue reported, and surfacing a serde
    // decode error for it would read as a Curie bug rather than as the
    // wrong-listener fact it is.
    if !is_json {
        bail!(
            "GET {url} answered 200 with {} content rather than JSON, so a different service \
             is holding the port",
            if content_type.trim().is_empty() {
                "untyped".to_string()
            } else {
                content_type
            }
        );
    }
    let Ok(payload) = serde_json::from_str::<serde_json::Value>(&body) else {
        bail!(
            "GET {url} answered 200 and claimed JSON, but the body does not parse as JSON, \
             so it is not the Curie API's /health"
        );
    };
    match payload.get("status").and_then(|status| status.as_str()) {
        Some("ok") => Ok(()),
        Some(other) => bail!(
            "GET {url} answered 200 with JSON status {other:?} rather than \"ok\", so either \
             this is not the Curie API or the release is unhealthy"
        ),
        None => bail!(
            "GET {url} answered 200 with JSON carrying no `status` field, while the Curie \
             API's /health answers {{\"status\": \"ok\"}}"
        ),
    }
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

fn valid_github_owner(owner: &str) -> bool {
    (1..=39).contains(&owner.len())
        && owner
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        && !owner.starts_with('-')
        && !owner.ends_with('-')
        && !owner.contains("--")
}

fn validate_repo_full_name(repo_full_name: &str) -> Result<()> {
    let Some((owner, repository)) = repo_full_name.split_once('/') else {
        bail!(
            "invalid repo_full_name: expected one owner/name pair; the owner must use 1 to 39 \
             ASCII letters or digits with single nonterminal hyphens, and the name must use 1 \
             to 100 ASCII letters, digits, dots, underscores, or hyphens other than `.` or `..`"
        );
    };

    let repository_valid = (1..=100).contains(&repository.len())
        && repository != "."
        && repository != ".."
        && repository
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));

    if !valid_github_owner(owner) || !repository_valid {
        bail!(
            "invalid repo_full_name: expected one owner/name pair; the owner must use 1 to 39 \
             ASCII letters or digits with single nonterminal hyphens, and the name must use 1 \
             to 100 ASCII letters, digits, dots, underscores, or hyphens other than `.` or `..`"
        );
    }

    Ok(())
}

/// Accept exact `owner/repository` or owner-wide `owner/*` chart allowlist entries.
pub(crate) fn validate_allowlist_entry(entry: &str) -> Result<()> {
    if let Some((owner, rest)) = entry.split_once('/') {
        if rest == "*" && valid_github_owner(owner) {
            return Ok(());
        }
    }
    validate_repo_full_name(entry).map_err(|_| {
        anyhow::anyhow!(
            "invalid --workspace-repo {entry}: expected `owner/repo` or `owner/*` in the \
             api.githubRepoAllowlist shape"
        )
    })
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
fn agent_update_body(repo_full_name: Option<&str>) -> serde_json::Value {
    let mut body = json!({});
    if let Some(repo) = repo_full_name {
        body["repo_full_name"] = json!(repo);
    }
    body
}

/// The `POST /agents/{id}/channels` body: the binding PAIR and nothing else.
/// Pure so the shape is testable without a live API. The kind is never
/// inferred -- a channel-neutral binding carries it explicitly.
fn add_channel_body(
    kind: &str,
    address: &str,
    endpoint: Option<&str>,
    adapter: Option<&str>,
) -> serde_json::Value {
    let mut body = json!({"kind": kind, "address": address});
    if let (Some(endpoint), Some(adapter)) = (endpoint, adapter) {
        body["endpoint"] = json!(endpoint);
        body["adapter"] = json!(adapter);
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

/// Validate one trace id as the single safe path segment accepted by the API.
///
/// Keeping this byte-for-byte shape at the CLI boundary means a malformed id
/// is a usage error before HTTP, while a well-formed id that the API does not
/// know remains a distinct runtime failure.
pub fn parse_trace_id(raw: &str) -> std::result::Result<String, String> {
    if (1..=128).contains(&raw.len())
        && raw
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        Ok(raw.to_string())
    } else {
        Err("trace id must be 1-128 ASCII letters, digits, underscores, or hyphens".to_string())
    }
}

impl ApiClient {
    /// The server caps `/approvals` results at this many rows
    /// (`apps/api/.../routers/approvals.py`: `min(max(limit, 1), 200)`); the CLI
    /// requests exactly the cap so hitting it is how the caller detects possible
    /// truncation of the pending list (#670).
    pub const APPROVALS_LIST_LIMIT: usize = 200;

    pub fn new(base_url: &str, api_key: &str) -> Result<Self> {
        warn_if_insecure(base_url);
        let http = http_client(base_url, Some(std::time::Duration::from_secs(5)))
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
        // Redirects are never followed, because a followed one would carry the
        // `X-API-Key` header to whatever host it named (see [`http_client`]), so a
        // 3xx lands here as an ordinary response. Say what it is: the configured
        // endpoint is not the platform API itself but something in front of it,
        // which is an operator configuration fact, not an API failure.
        if let Some(refusal) = redirect_refusal(status, resp.headers()) {
            bail!("{what} failed: {refusal}");
        }
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

    async fn send_request(
        &self,
        request: reqwest::RequestBuilder,
        operation: &'static str,
    ) -> Result<reqwest::Response> {
        match request.send().await {
            Ok(response) => Ok(response),
            Err(err) => {
                let transient = err.is_connect() || err.is_timeout();
                let source = anyhow::Error::new(err).context(operation);
                let message = if transient {
                    format!("the platform API at {} is unreachable", self.base_url)
                } else {
                    format!(
                        "the platform API at {} could not send the requested operation",
                        self.base_url
                    )
                };
                let remedy = if transient {
                    format!(
                        "Run `curl -fsS {}/health` to check it, then retry.",
                        self.base_url
                    )
                } else {
                    "Set --api-url to an absolute http(s) URL, then retry.".to_string()
                };
                Err(crate::exit::operator_context(source, message, Some(remedy)))
            }
        }
    }

    async fn expect_observability_ok(
        resp: reqwest::Response,
        what: &str,
    ) -> Result<reqwest::Response> {
        let status = resp.status();
        if matches!(
            status,
            reqwest::StatusCode::INTERNAL_SERVER_ERROR
                | reqwest::StatusCode::BAD_GATEWAY
                | reqwest::StatusCode::SERVICE_UNAVAILABLE
                | reqwest::StatusCode::GATEWAY_TIMEOUT
        ) {
            let body = resp.text().await.unwrap_or_default();
            return Err(ObservabilityApiUnavailable(format!(
                "{what} failed with {status}: {}",
                body.trim()
            ))
            .into());
        }
        if matches!(
            status,
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
        ) {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow::Error::from(
                crate::exit::CliError::failure(format!(
                    "{what} failed with {status}: {}",
                    body.trim()
                ))
                .with_fix("verify --api-key or CURIE_API_KEY matches the selected platform API"),
            ));
        }
        if matches!(
            status,
            reqwest::StatusCode::BAD_REQUEST | reqwest::StatusCode::UNPROCESSABLE_ENTITY
        ) {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(format!(
                    "{what} failed with {status}: {}",
                    body.trim()
                ))
                .with_fix("review the observability query filters and retry with valid values"),
            ));
        }
        Self::expect_ok(resp, what).await
    }

    async fn get_observability_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
        what: &str,
    ) -> Result<T> {
        let resp = self
            .send_request(
                self.http
                    .get(format!("{}{path}", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    .query(query)
                    .timeout(std::time::Duration::from_secs(30)),
                "GET observability",
            )
            .await?;
        Self::expect_observability_ok(resp, what)
            .await?
            .json()
            .await
            .with_context(|| format!("decoding {what}"))
    }

    pub async fn list_agents(&self) -> Result<Vec<Agent>> {
        let resp = self
            .send_request(
                self.http
                    .get(format!("{}/agents", self.base_url))
                    .header("X-API-Key", &self.api_key),
                "GET /agents",
            )
            .await?;
        Self::expect_ok(resp, "listing agents")
            .await?
            .json()
            .await
            .context("decoding agent list")
    }

    /// Poll one opaque disconnected cluster-message reply bucket.
    ///
    /// `reply_ref` is a UUID rather than an arbitrary string so caller input can
    /// never become a path segment. A valid but not-yet-written bucket is a
    /// normal race between enqueue and the worker's first reply event, exposed
    /// as `None`; every other non-success remains a real API failure.
    pub(crate) async fn cluster_message_replies(
        &self,
        reply_ref: &uuid::Uuid,
        after: usize,
    ) -> Result<Option<ClusterMessageReplyPage>> {
        let resp = match self
            .http
            .get(format!(
                "{}/cluster-message-replies/{}",
                self.base_url,
                reply_ref.hyphenated()
            ))
            .header("X-API-Key", &self.api_key)
            .query(&[("after", after)])
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err) if err.is_connect() || err.is_timeout() => return Ok(None),
            Err(err) => {
                return Err(err).context("GET /cluster-message-replies/{reply_ref}");
            }
        };
        if resp.status().is_server_error() {
            return Ok(None);
        }
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            let body = resp.text().await.unwrap_or_default();
            if is_unrouted(reqwest::StatusCode::NOT_FOUND, &body) {
                bail!(
                    "reading cluster-message replies failed: this platform release does not have \
                     the cluster-message-replies endpoint, so it is older than this CLI. Upgrade \
                     the release, or use a CLI matching it."
                );
            }
            return Ok(None);
        }
        Ok(Some(
            Self::expect_ok(resp, "reading cluster-message replies")
                .await?
                .json()
                .await
                .context("decoding cluster-message reply page")?,
        ))
    }

    /// Read the newest trace rows through the platform API proxy. The caller
    /// supplies the public bound and also truncates defensively after decoding;
    /// the latter keeps a skewed or older server from violating the CLI result
    /// contract even if it ignores `limit`.
    pub async fn list_observability_runs(
        &self,
        limit: usize,
        agent_id: Option<&str>,
    ) -> Result<Vec<TraceListRow>> {
        let mut query = vec![("limit", limit.to_string())];
        if let Some(agent_id) = agent_id {
            query.push(("agent_id", agent_id.to_string()));
        }
        self.get_observability_json("/langfuse/traces", &query, "listing observability runs")
            .await
    }

    /// Read one complete trace tree through the platform API proxy. A handler
    /// 404 is `Ok(None)` so the command can classify an unknown trace as exit
    /// 1; FastAPI's generic unrouted 404 remains the existing stale-platform
    /// error with its upgrade guidance.
    pub async fn observability_run(&self, trace_id: &str) -> Result<Option<TraceTree>> {
        parse_trace_id(trace_id).map_err(anyhow::Error::msg)?;
        let resp = self
            .http
            .get(format!("{}/langfuse/traces/{trace_id}", self.base_url))
            .header("X-API-Key", &self.api_key)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .context("GET /langfuse/traces/{trace_id}")?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            let body = resp.text().await.unwrap_or_default();
            if is_unrouted(reqwest::StatusCode::NOT_FOUND, &body) {
                bail!(
                    "reading observability run failed: this platform release does not have that \
                     endpoint, so it is older than this CLI. Upgrade the release, or use a CLI \
                     matching it."
                );
            }
            return Ok(None);
        }
        let run = Self::expect_observability_ok(resp, "reading observability run")
            .await?
            .json()
            .await
            .context("decoding observability run")?;
        Ok(Some(run))
    }

    /// Read the complete existing metrics-summary DTO through the platform API.
    pub async fn observability_metrics_summary(
        &self,
        start: Option<&str>,
        end: Option<&str>,
        environment: Option<&str>,
        agent: Option<&str>,
    ) -> Result<MetricsSummary> {
        let mut query = Vec::new();
        for (key, value) in [
            ("start", start),
            ("end", end),
            ("environment", environment),
            ("agent", agent),
        ] {
            if let Some(value) = value {
                query.push((key, value.to_string()));
            }
        }
        self.get_observability_json(
            "/observability/metrics/summary",
            &query,
            "reading observability metrics summary",
        )
        .await
    }

    /// Read the complete existing metric-series DTO through the platform API.
    #[allow(clippy::too_many_arguments)]
    pub async fn observability_metric_series(
        &self,
        metric: &str,
        granularity: &str,
        start: Option<&str>,
        end: Option<&str>,
        environment: Option<&str>,
        agent: Option<&str>,
    ) -> Result<MetricSeries> {
        // #1948: refuse an over-cap span before the request, because the
        // backend has already executed the query by the time a response can be
        // buffered and rejected. The post-dispatch bound below stays as the
        // defense against a server that returns more points than the
        // requested span implies.
        prevalidate_series_span(granularity, start, end)?;
        let mut query = vec![
            ("metric", metric.to_string()),
            ("granularity", granularity.to_string()),
        ];
        for (key, value) in [
            ("start", start),
            ("end", end),
            ("environment", environment),
            ("agent", agent),
        ] {
            if let Some(value) = value {
                query.push((key, value.to_string()));
            }
        }
        let series: MetricSeries = self
            .get_observability_json(
                "/observability/metrics/series",
                &query,
                "reading observability metric series",
            )
            .await?;
        if series.points.len() > MAX_OBSERVABILITY_METRIC_POINTS {
            return Err(anyhow::Error::from(
                crate::exit::CliError::failure(format!(
                    "the API returned {} metric points, above the CLI maximum of {}",
                    series.points.len(),
                    MAX_OBSERVABILITY_METRIC_POINTS
                ))
                .with_fix("narrow --start/--end or choose a coarser --granularity"),
            ));
        }
        Ok(series)
    }

    pub async fn create_agent(
        &self,
        name: &str,
        slack_channel: &str,
        repo_full_name: Option<&str>,
    ) -> Result<Agent> {
        let body = agent_create_body(name, slack_channel, repo_full_name);
        let resp = self
            .send_request(
                self.http
                    .post(format!("{}/agents", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    .json(&body),
                "POST /agents",
            )
            .await?;
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
            .send_request(
                self.http
                    .patch(format!("{}/agents/{agent_id}", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    .json(body),
                "PATCH /agents/{id}",
            )
            .await?;
        Self::expect_ok(resp, "updating the agent")
            .await?
            .json()
            .await
            .context("decoding updated agent")
    }

    /// `GET /agents/{id}`: the agent row as the API holds it right now. Used by
    /// the ensure-bound recheck, which must read a FRESH row rather than the
    /// one resolution already had.
    pub async fn get_agent(&self, agent_id: &str) -> Result<Agent> {
        let resp = self
            .http
            .get(format!("{}/agents/{agent_id}", self.base_url))
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .context("GET /agents/{id}")?;
        Self::expect_ok(resp, "reading the agent")
            .await?
            .json()
            .await
            .context("decoding the agent")
    }

    /// Add one channel binding: `POST /agents/{id}/channels` (201 with the
    /// agent as stored).
    ///
    /// A 409 is AMBIGUOUS: the pair's uniqueness is platform-wide, so the
    /// conflict may be another agent holding it (a real error) or this very
    /// agent, when a concurrent deploy won the race to add the same pair. This
    /// is ensure-bound, a statement about the END STATE, so the conflict is
    /// rechecked against a fresh read and answered as success only when this
    /// agent now owns the pair.
    pub async fn add_agent_channel(
        &self,
        agent_id: &str,
        kind: &str,
        address: &str,
        endpoint: Option<&str>,
        adapter: Option<&str>,
    ) -> Result<Agent> {
        let resp = self
            .http
            .post(format!("{}/agents/{agent_id}/channels", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(&add_channel_body(kind, address, endpoint, adapter))
            .send()
            .await
            .context("POST /agents/{id}/channels")?;
        if resp.status() == reqwest::StatusCode::CONFLICT {
            let conflict = Self::expect_ok(resp, "adding the channel binding")
                .await
                .expect_err("a 409 is never a success");
            let agent = self.get_agent(agent_id).await?;
            if agent
                .channels
                .iter()
                .any(|b| b.kind == kind && b.address == address)
            {
                return Ok(agent);
            }
            return Err(conflict);
        }
        Self::expect_ok(resp, "adding the channel binding")
            .await?
            .json()
            .await
            .context("decoding the agent after the channel add")
    }

    /// Remove one channel binding: `DELETE /agents/{id}/channels?kind=&address=`
    /// (204, no body). The PAIR travels, never the address alone: on a
    /// multi-binding agent an address-only removal would drop the wrong row.
    ///
    /// The API refuses to remove an agent's LAST binding with a 409, which
    /// [`Self::expect_ok`] surfaces with the reason intact.
    pub async fn remove_agent_channel(
        &self,
        agent_id: &str,
        kind: &str,
        address: &str,
    ) -> Result<()> {
        let resp = self
            .http
            .delete(format!("{}/agents/{agent_id}/channels", self.base_url))
            .query(&[("kind", kind), ("address", address)])
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .context("DELETE /agents/{id}/channels")?;
        Self::expect_ok(resp, "removing the channel binding").await?;
        Ok(())
    }

    /// Mint a scoped `chn` token for one binding (`POST /channels/token`).
    ///
    /// Returns the token string. Callers must not print it; decode `exp` from
    /// the `chn.` payload instead. An empty token is refused so a success
    /// cannot carry nothing.
    pub async fn mint_channel_token(
        &self,
        kind: &str,
        address: &str,
        ttl_s: i64,
    ) -> Result<String> {
        let resp = self
            .http
            .post(format!("{}/channels/token", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(&json!({
                "kind": kind,
                "address": address,
                "ttl_s": ttl_s,
            }))
            .send()
            .await
            .context("POST /channels/token")?;
        let resp = Self::expect_ok(resp, "minting the channel token").await?;
        let body: serde_json::Value = resp.json().await.context("decoding the channel token")?;
        let token = body
            .get("token")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                crate::exit::CliError::failure("platform returned an empty channel token")
                    .with_fix("retry; if it persists, the binding may be missing or unroutable")
            })?;
        Ok(token.to_string())
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
            .send_request(
                self.http
                    .patch(format!("{}/agents/{agent_id}", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    .json(&json!({ "secrets": secrets })),
                "PATCH /agents/{id}",
            )
            .await?;
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
        if let Some(repo_full_name) = repo_full_name {
            validate_repo_full_name(repo_full_name)?;
        }

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
                // ENSURE-BOUND (ADR-0118): `--slack-channel` states a binding
                // the agent must HOLD, not the one binding it may have. An
                // address it already answers on is nothing to do; anything else
                // is added beside what is there, never on top of it.
                let channel_add = slack_channel.filter(|c| {
                    !agent
                        .channels
                        .iter()
                        .any(|b| b.kind == "slack" && b.address == **c)
                });
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
                // TWO requests, not one: the binding lives on its own
                // subresource now that `AgentUpdate.channel` is retired
                // (ADR-0118), so the repo bind stays a separate PATCH. A
                // failure between them therefore leaves the channel added and
                // the repo unbound -- which is why the channel goes FIRST. A
                // bound channel with no repo still answers a turn; a bound repo
                // with no channel does not.
                let agent = match channel_add {
                    Some(address) => {
                        self.add_agent_channel(&agent.id, "slack", address, None, None)
                            .await?
                    }
                    None => agent,
                };
                let agent = if repo_bind.is_some() {
                    let body = agent_update_body(repo_bind);
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
                let outcome = match channel_add {
                    Some(address) => ChannelOutcome::Added {
                        address: address.to_string(),
                    },
                    None => ChannelOutcome::Unchanged {
                        channels: agent.channels.iter().map(|b| b.address.clone()).collect(),
                        passed: slack_channel.is_some(),
                    },
                };
                Ok((agent, outcome, repo_note))
            }
            None => {
                let channel = slack_channel.unwrap_or(DEFAULT_SLACK_CHANNEL);
                let agent = self.create_agent(name, channel, repo_full_name).await?;
                // `AgentCreate` still carries the singular channel, so a created
                // agent holds exactly the one binding it was created with.
                let outcome = ChannelOutcome::Created(
                    agent
                        .channels
                        .first()
                        .map_or_else(|| channel.to_string(), |b| b.address.clone()),
                );
                Ok((agent, outcome, None))
            }
        }
    }

    pub async fn create_version(
        &self,
        agent_id: &str,
        version_label: &str,
        created_by: &str,
        commit_sha: Option<&str>,
    ) -> Result<Version> {
        let resp = self
            .send_request(
                self.http
                    .post(format!("{}/agents/{agent_id}/versions", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    .json(&json!({
                        "version_label": version_label,
                        "created_by": created_by,
                        "commit_sha": commit_sha,
                    })),
                "POST /agents/{id}/versions",
            )
            .await?;
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
            .send_request(
                self.http
                    .put(format!(
                        "{}/agents/{agent_id}/versions/{version_id}/bundle",
                        self.base_url
                    ))
                    .header("X-API-Key", &self.api_key)
                    .multipart(form),
                "PUT bundle",
            )
            .await?;
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
        commit_sha: Option<&str>,
        workspace_enabled: Option<bool>,
    ) -> Result<Deployment> {
        let mut body = json!({
            "agent_id": agent_id,
            "version_id": version_id,
            "environment": environment,
            "commit_sha": commit_sha,
        });
        if let Some(enabled) = workspace_enabled {
            body["workspace_enabled"] = json!(enabled);
        }
        let resp = self
            .send_request(
                self.http
                    .post(format!("{}/deployments", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    .json(&body),
                "POST /deployments",
            )
            .await?;
        Self::expect_ok(resp, "creating the deployment")
            .await?
            .json()
            .await
            .context("decoding created deployment")
    }

    /// Prepare a deploy through bundle upload without creating its deployment.
    #[allow(clippy::too_many_arguments)] // one cohesive deploy call; a struct would not clarify it
    pub async fn prepare_deploy(
        &self,
        agent_name: &str,
        slack_channel: Option<&str>,
        version_label: &str,
        created_by: &str,
        archive: Vec<u8>,
        secrets: &std::collections::BTreeMap<String, String>,
        repo_full_name: Option<&str>,
        commit_sha: Option<&str>,
        workspace: WorkspaceIntent,
    ) -> Result<PreparedDeployOutcome> {
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
            .create_version(&agent.id, version_label, created_by, commit_sha)
            .await?;
        let bundle = self.upload_bundle(&agent.id, &version.id, archive).await?;
        Ok(PreparedDeployOutcome {
            agent,
            version,
            bundle,
            channel,
            repo_note,
            commit_sha: commit_sha.map(str::to_string),
            workspace_enabled: match workspace {
                WorkspaceIntent::Preserve => None,
                WorkspaceIntent::Disable => Some(false),
                WorkspaceIntent::Enable => Some(true),
            },
        })
    }

    pub async fn activate_deploy(
        &self,
        prepared: PreparedDeployOutcome,
        environment: &str,
    ) -> Result<DeployOutcome> {
        let commit_sha = prepared.commit_sha.clone();
        let workspace_enabled = prepared.workspace_enabled;
        let deployment = self
            .create_deployment(
                &prepared.agent.id,
                &prepared.version.id,
                environment,
                commit_sha.as_deref(),
                workspace_enabled,
            )
            .await?;
        Ok(DeployOutcome {
            agent: prepared.agent,
            version: prepared.version,
            bundle: prepared.bundle,
            deployment,
            channel: prepared.channel,
            repo_note: prepared.repo_note,
        })
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
        commit_sha: Option<&str>,
        workspace: WorkspaceIntent,
    ) -> Result<DeployOutcome> {
        let prepared = self
            .prepare_deploy(
                agent_name,
                slack_channel,
                version_label,
                created_by,
                archive,
                secrets,
                repo_full_name,
                commit_sha,
                workspace,
            )
            .await?;
        self.activate_deploy(prepared, environment).await
    }

    /// Resolve an agent identifier (its `name`, or its `id`) to the full record
    /// by listing agents and matching -- the same name-based resolution the
    /// deploy flow uses (`resolve_agent`), so the lifecycle verbs never grow a
    /// second resolution path. Errors when nothing matches; never creates.
    pub async fn find_agent(&self, identifier: &str) -> Result<Agent> {
        match_agent(self.list_agents().await?, identifier).map_err(anyhow::Error::new)
    }

    /// The observability query path's agent lookup (#1948): the same exact
    /// name-or-id match as [`Self::find_agent`], but the `/agents` GET is
    /// classified with the observability status semantics, so a 5xx stays
    /// transient, an auth failure keeps its credential fix, and only a genuine
    /// no-match reaches the caller's usage refusal.
    pub(crate) async fn find_agent_observability(&self, identifier: &str) -> Result<Agent> {
        let resp = self
            .send_request(
                self.http
                    .get(format!("{}/agents", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    // The same request budget the observability reads carry:
                    // the client default is a connect timeout only, so a
                    // stalled-but-accepted connection must not hang the query
                    // indefinitely (#1948 review, round 4).
                    .timeout(std::time::Duration::from_secs(30)),
                "GET /agents",
            )
            .await?;
        let agents: Vec<Agent> = Self::expect_observability_ok(resp, "listing agents")
            .await?
            .json()
            .await
            .context("decoding agent list")?;
        match_agent(agents, identifier).map_err(anyhow::Error::new)
    }

    /// Flip the agent kill switch on: `POST /agents/{id}/kill` (no request body).
    pub async fn kill_agent(&self, agent_id: &str) -> Result<KillState> {
        let resp = self
            .send_request(
                self.http
                    .post(format!("{}/agents/{agent_id}/kill", self.base_url))
                    .header("X-API-Key", &self.api_key),
                "POST /agents/{id}/kill",
            )
            .await?;
        Self::expect_ok(resp, "killing the agent")
            .await?
            .json()
            .await
            .context("decoding kill state")
    }

    /// Flip the agent kill switch off: `POST /agents/{id}/resume` (no request body).
    pub async fn resume_agent(&self, agent_id: &str) -> Result<KillState> {
        let resp = self
            .send_request(
                self.http
                    .post(format!("{}/agents/{agent_id}/resume", self.base_url))
                    .header("X-API-Key", &self.api_key),
                "POST /agents/{id}/resume",
            )
            .await?;
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
            .send_request(
                self.http
                    .post(format!(
                        "{}/agents/{agent_id}/threads/{thread_key}/reset",
                        self.base_url
                    ))
                    .header("X-API-Key", &self.api_key),
                "POST /agents/{id}/threads/{thread_key}/reset",
            )
            .await?;
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
            .send_request(
                self.http
                    .get(format!(
                        "{}/agents/{agent_id}/threads/{thread_key}/reset",
                        self.base_url
                    ))
                    .header("X-API-Key", &self.api_key),
                "GET /agents/{id}/threads/{thread_key}/reset",
            )
            .await?;
        Self::expect_ok(resp, "polling the thread reset state")
            .await?
            .json()
            .await
            .context("decoding thread reset state")
    }

    /// Set the agent budget: `PUT /agents/{id}/budget` with a `BudgetConfig` body.
    pub async fn set_budget(&self, agent_id: &str, budget: &BudgetConfig) -> Result<BudgetConfig> {
        let resp = self
            .send_request(
                self.http
                    .put(format!("{}/agents/{agent_id}/budget", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    .json(budget),
                "PUT /agents/{id}/budget",
            )
            .await?;
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
            .send_request(
                self.http
                    .post(format!("{}/deploy-targets/resolve", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    .json(&serde_json::json!({"content": content, "target": target})),
                "resolving the deploy target",
            )
            .await?;
        let status = resp.status();
        if let Some(refusal) = redirect_refusal(status, resp.headers()) {
            anyhow::bail!("resolving target `{target}` failed: {refusal}");
        }
        let body = resp.text().await.unwrap_or_default();
        if is_unrouted(status, &body) {
            anyhow::bail!(
                "this platform release has no deploy-target resolver, so `--target` cannot \
                 work against it. The API predates ADR-0089. Upgrade the release, or drop \
                 `--target` and pass `--agent`/`--env`/`--slack-channel` directly."
            );
        }
        if status == reqwest::StatusCode::BAD_REQUEST {
            let detail = serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|value| value["detail"].as_str().map(str::to_string));
            if let Some(detail) = detail.filter(|detail| {
                detail
                    .split_once(':')
                    .is_some_and(|(code, _)| code == "deploy.placeholder_channel")
            }) {
                return Err(crate::exit::CliError::usage(format!(
                    "resolving target `{target}` failed: {detail}"
                ))
                .with_fix(
                    "replace the target's slack_channel with a real Slack channel ID, or remove \
                     slack_channel from deploy.yaml",
                )
                .into());
            }
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
            .send_request(
                self.http
                    .post(format!("{}/deploy-targets/list", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    .json(&serde_json::json!({"content": content, "target": ""})),
                "listing the deploy targets",
            )
            .await?;
        let status = resp.status();
        if let Some(refusal) = redirect_refusal(status, resp.headers()) {
            anyhow::bail!("listing the deploy targets failed: {refusal}");
        }
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

    /// Ask the API whether this repository's pushes still route (#1221).
    ///
    /// ADVISORY, and deliberately incapable of failing a deploy: binding an
    /// agent to a repository already succeeded by the time this is called, so
    /// an error here would report failure for work that landed. Every
    /// non-success -- an older platform with no such route, a 500, a body that
    /// does not decode -- returns `Ok(None)`, meaning "no answer", which the
    /// caller renders as no output at all.
    ///
    /// The `deploy.yaml` text goes over the wire unparsed for the same reason
    /// `resolve_deploy_target` sends it: ADR-0089 keeps exactly one parser for
    /// this format, and the resolver rule the answer depends on is the
    /// platform's, not a copy of it living here.
    ///
    /// This is the ONE call with a per-request deadline, and it is set here
    /// rather than on the shared client on purpose. `ApiClient::new` bounds
    /// only `connect_timeout`, so a peer that accepts the TCP connection and
    /// then stalls before sending headers leaves the caller waiting forever --
    /// and because this runs AFTER the deploy has already landed, that
    /// unbounded wait turns a successful deploy into an apparent hang, which is
    /// exactly the failure this check promises never to cause. A GLOBAL timeout
    /// is the wrong fix: bundle upload, deploy activation, and the streaming
    /// reads legitimately run longer than any bound this call wants, and
    /// capping them would break work that must not be interrupted. 10s is far
    /// past a healthy answer and far short of an operator's patience; expiry
    /// lands in the non-success path below as `Ok(None)` -- a timeout is "no
    /// answer", indistinguishable from a 500, and prints nothing.
    pub async fn check_git_flow_routing(
        &self,
        repo_full_name: &str,
        content: Option<&str>,
    ) -> Result<Option<RoutingCheck>> {
        let sent = self
            .send_request(
                self.http
                    .post(format!("{}/git-flow/routing-check", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    .json(&serde_json::json!({
                        "repo_full_name": repo_full_name,
                        "content": content,
                    }))
                    .timeout(std::time::Duration::from_secs(10)),
                "checking git-flow routing",
            )
            .await;
        // A send that never completed -- the 10s deadline above, or an
        // unreachable API -- is "no answer" like any other, so it is absorbed
        // here rather than bubbled with `?`. Propagating would make the
        // function's advisory contract depend on every caller remembering to
        // discard the error.
        let Ok(resp) = sent else {
            return Ok(None);
        };
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        // A platform predating this endpoint answers FastAPI's bare 404. That is
        // skew, not a fault: an operator on an older release is not doing
        // anything wrong and must not be told to upgrade by an advisory check.
        if is_unrouted(status, &body) || !status.is_success() {
            return Ok(None);
        }
        Ok(serde_json::from_str(&body).ok())
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
            .send_request(
                self.http
                    .get(format!(
                        "{}/agents/{agent_id}/versions/{version_id}/connectors",
                        self.base_url
                    ))
                    .query(&[
                        ("release", release),
                        ("namespace", namespace),
                        ("app_name", app_name),
                    ])
                    .header("X-API-Key", &self.api_key),
                "GET /agents/{id}/versions/{vid}/connectors",
            )
            .await?;
        Self::expect_ok(resp, "rendering declared connectors")
            .await?
            .json()
            .await
            .context("decoding connector manifests")
    }

    pub async fn list_versions(&self, agent_id: &str) -> Result<Vec<Version>> {
        let resp = self
            .send_request(
                self.http
                    .get(format!("{}/agents/{agent_id}/versions", self.base_url))
                    .header("X-API-Key", &self.api_key),
                "GET /agents/{id}/versions",
            )
            .await?;
        Self::expect_ok(resp, "listing versions")
            .await?
            .json()
            .await
            .context("decoding version list")
    }

    /// List an agent's learned memory, oldest first: `GET /agents/{id}/memory`.
    pub async fn list_memory(&self, agent_id: &str) -> Result<Vec<MemoryEntry>> {
        let resp = self
            .send_request(
                self.http
                    .get(format!("{}/agents/{agent_id}/memory", self.base_url))
                    .header("X-API-Key", &self.api_key),
                "GET /agents/{id}/memory",
            )
            .await?;
        Self::expect_ok(resp, "listing memory")
            .await?
            .json()
            .await
            .context("decoding memory list")
    }

    /// Append an operator-authored memory record: `POST /agents/{id}/memory`.
    pub async fn create_memory(&self, agent_id: &str, content: &str) -> Result<MemoryEntry> {
        let resp = self
            .send_request(
                self.http
                    .post(format!("{}/agents/{agent_id}/memory", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    .json(&json!({ "content": content })),
                "POST /agents/{id}/memory",
            )
            .await?;
        Self::expect_ok(resp, "creating memory")
            .await?
            .json()
            .await
            .context("decoding created memory entry")
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
            .send_request(
                self.http
                    .get(format!("{}/approvals", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    .query(&[
                        ("status_filter", "pending"),
                        ("agent_id", agent_id),
                        ("limit", limit.as_str()),
                    ]),
                "GET /approvals",
            )
            .await?;
        Self::expect_ok(resp, "listing pending approvals")
            .await?
            .json()
            .await
            .context("decoding approvals")
    }

    /// One approval by id: `GET /approvals/{approval_id}`, mirroring the
    /// endpoint at `apps/api/src/curie_api/routers/approvals.py:92-98`. Answers
    /// 404 when the id names no approval, which a resolve or an expiry by
    /// another operator makes a real answer rather than a theoretical one.
    ///
    /// The CLI reads this for `card_channel` (#1531 finding 3): a route binding
    /// can put the card in a channel other than the one the turn spoke in, and
    /// the printed resolve hint has to name the authenticated card's real
    /// location. The single-record read is what the hint needs rather than
    /// [`Self::list_pending_approvals`], whose page is capped at the server max
    /// and can simply not contain the approval being waited on (#670).
    ///
    /// Deliberately UNBOUNDED here, unlike [`Self::check_git_flow_routing`]:
    /// this stays a plain mirror of the endpoint like every other method, so a
    /// future non-advisory caller is not silently capped. The deadline belongs
    /// to the advisory caller, which is where the reasoning for a specific
    /// budget lives (`message::hint_channel`).
    pub async fn get_approval(&self, approval_id: &str) -> Result<ApprovalRecord> {
        let resp = self
            .send_request(
                self.http
                    .get(format!("{}/approvals/{approval_id}", self.base_url))
                    .header("X-API-Key", &self.api_key),
                "GET /approvals/{id}",
            )
            .await?;
        Self::expect_ok(resp, "reading an approval")
            .await?
            .json()
            .await
            .context("decoding the approval")
    }

    /// Resolve one approval as the authenticated principal carried by
    /// `principal_token`: `POST /approvals/{id}/resolve`.
    ///
    /// The body carries policy input only. The API derives `resolved_by` and any
    /// channel evidence from the credential, then owns the membership check,
    /// resolve-once CAS, audit entry and resume-turn enqueue (ADR-0106, #1531).
    pub async fn resolve_approval(
        &self,
        approval_id: &str,
        decision: &str,
        principal_token: &str,
        note: Option<&str>,
    ) -> Result<ApprovalRecord> {
        let mut body = json!({ "decision": decision });
        if let Some(note) = note {
            body["note"] = json!(note);
        }
        let resp = self
            .send_request(
                self.http
                    .post(format!("{}/approvals/{approval_id}/resolve", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    .header("X-Curie-Approval-Principal", principal_token)
                    .json(&body),
                "POST /approvals/{id}/resolve",
            )
            .await?;
        Self::expect_ok(resp, "resolving approval")
            .await?
            .json()
            .await
            .context("decoding resolved approval")
    }

    /// Mint a reusable operator approval principal under the platform key.
    /// The returned token is a one-time delivery and is never cached by the CLI.
    pub async fn mint_operator_principal(
        &self,
        subject: &str,
    ) -> Result<OperatorPrincipalDelivery> {
        let resp = self
            .send_request(
                self.http
                    .post(format!("{}/approvals/principals/operator", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    .json(&json!({ "subject": subject })),
                "POST /approvals/principals/operator",
            )
            .await?;
        let body: serde_json::Value = Self::expect_ok(resp, "minting operator principal")
            .await?
            .json()
            .await
            .context("decoding operator principal delivery")?;
        Ok(OperatorPrincipalDelivery {
            token: required_response_string(&body, "token", "operator principal delivery")?,
            subject: required_response_string(&body, "subject", "operator principal delivery")?,
            expires_at: required_response_string(
                &body,
                "expires_at",
                "operator principal delivery",
            )?,
        })
    }

    /// Mint a subject-bound, single-use console login code under the platform
    /// key. The plaintext code is returned once and is never cached by the CLI.
    pub async fn mint_console_login_code(&self, subject: &str) -> Result<ConsoleLoginCodeDelivery> {
        let resp = self
            .send_request(
                self.http
                    .post(format!("{}/console/login-codes", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    .json(&json!({ "subject": subject })),
                "POST /console/login-codes",
            )
            .await?;
        let body: serde_json::Value = Self::expect_ok(resp, "minting console login code")
            .await?
            .json()
            .await
            .context("decoding console login-code delivery")?;
        Ok(ConsoleLoginCodeDelivery {
            code: required_response_string(&body, "code", "console login-code delivery")?,
            subject: required_response_string(&body, "subject", "console login-code delivery")?,
            expires_at: required_response_string(
                &body,
                "expires_at",
                "console login-code delivery",
            )?,
        })
    }

    /// Set the agent's approval-required tool gates: `PATCH /agents/{id}` with
    /// `approval_required_tools` (an empty list clears them). Returns the updated
    /// agent so the caller can echo the effective gates.
    pub async fn set_approval_tools(&self, agent_id: &str, tools: &[String]) -> Result<Agent> {
        let resp = self
            .send_request(
                self.http
                    .patch(format!("{}/agents/{agent_id}", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    .json(&json!({ "approval_required_tools": tools })),
                "PATCH /agents/{id} (approval gates)",
            )
            .await?;
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
        routes: &std::collections::BTreeMap<String, ApprovalRouteBindingWrite>,
    ) -> Result<Agent> {
        let resp = self
            .send_request(
                self.http
                    .patch(format!("{}/agents/{agent_id}", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    .json(&json!({ "approval_routes": routes })),
                "PATCH /agents/{id} (approval routes)",
            )
            .await?;
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
            .send_request(
                self.http
                    .post(format!("{}/evals/trigger", self.base_url))
                    .header("X-API-Key", &self.api_key)
                    .json(&body),
                "POST /evals/trigger",
            )
            .await?;
        Self::expect_ok(resp, "triggering the eval")
            .await?
            .json()
            .await
            .context("decoding eval trigger result")
    }

    /// Read the eval matrix for a suite, optionally scoped to one triggered run.
    pub async fn eval_matrix(
        &self,
        suite: &str,
        versions: u32,
        stream_id: Option<&str>,
    ) -> Result<EvalMatrix> {
        let mut query = vec![
            ("suite", suite.to_string()),
            ("versions", versions.to_string()),
        ];
        if let Some(stream_id) = stream_id {
            query.push(("stream_id", stream_id.to_string()));
        }
        let resp = self
            .send_request(
                self.http
                    .get(format!("{}/evals/matrix", self.base_url))
                    .query(&query)
                    .header("X-API-Key", &self.api_key),
                "GET /evals/matrix",
            )
            .await?;
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
            .send_request(
                self.http
                    .get(format!("{}/deployments", self.base_url))
                    .query(&[("agent_id", agent_id)])
                    .header("X-API-Key", &self.api_key),
                "GET /deployments",
            )
            .await?;
        Self::expect_ok(resp, "listing deployments")
            .await?
            .json()
            .await
            .context("decoding deployment list")
    }

    /// End the deployment: `DELETE /deployments/{id}` (204 No Content on success).
    pub async fn end_deployment(&self, deployment_id: &str) -> Result<()> {
        let resp = self
            .http
            .delete(format!("{}/deployments/{deployment_id}", self.base_url))
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .context("DELETE /deployments/{id}")?;
        Self::expect_ok(resp, "ending the deployment").await?;
        Ok(())
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
            .send_request(
                self.http
                    .get(format!(
                        "{}/agents/{agent_id}/versions/{version_id}/files",
                        self.base_url
                    ))
                    .header("X-API-Key", &self.api_key),
                "GET /agents/{id}/versions/{version_id}/files",
            )
            .await?;
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
            .send_request(
                self.http
                    .delete(format!("{}/agents/{agent_id}", self.base_url))
                    .header("X-API-Key", &self.api_key),
                "DELETE /agents/{id}",
            )
            .await?;
        Self::expect_ok(resp, "deleting the agent").await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        add_channel_body, agent_create_body, agent_update_body, is_insecure_endpoint,
        prevalidate_series_span, validate_allowlist_entry, MAX_OBSERVABILITY_METRIC_POINTS,
    };

    /// The pre-dispatch span guard allows exactly the cap (#1948): 1,000 hour
    /// buckets from 1970-01-01T00:00:00Z is legal, one more is not.
    #[test]
    fn series_span_guard_enforces_the_point_cap_boundary() {
        prevalidate_series_span(
            "hour",
            Some("1970-01-01T00:00:00Z"),
            Some("1970-02-11T16:00:00Z"),
        )
        .expect("exactly the cap dispatches");
        let error = prevalidate_series_span(
            "hour",
            Some("1970-01-01T00:00:00Z"),
            Some("1970-02-11T17:00:00Z"),
        )
        .expect_err("one bucket over the cap is refused");
        assert!(error.to_string().contains("buckets"));
    }

    /// Wall-clock buckets charge an unaligned window for its partial leading
    /// and trailing buckets (#1948 review): a span of exactly 1,000 hours that
    /// starts mid-hour touches 1,001 hour buckets and must be refused before
    /// dispatch, while the aligned same-length span stays at the cap.
    #[test]
    fn series_span_guard_charges_partial_boundary_buckets() {
        prevalidate_series_span(
            "hour",
            Some("1970-01-01T00:00:00Z"),
            Some("1970-02-11T16:00:00Z"),
        )
        .expect("the aligned cap-length window dispatches");
        let error = prevalidate_series_span(
            "hour",
            Some("1970-01-01T00:30:00Z"),
            Some("1970-02-11T16:30:00Z"),
        )
        .expect_err("the unaligned same-length window touches one more bucket");
        assert!(error.to_string().contains("1001"));

        // A fractional-second end past the cap boundary still opens the
        // boundary's bucket (RFC 3339 accepts fractional seconds, round 3).
        let error = prevalidate_series_span(
            "hour",
            Some("1970-01-01T00:00:00Z"),
            Some("1970-02-11T16:00:00.500Z"),
        )
        .expect_err("a half-second-past-cap end touches one more bucket");
        assert!(error.to_string().contains("1001"));
        prevalidate_series_span(
            "hour",
            Some("1970-01-01T00:00:00Z"),
            Some("1970-02-11T15:00:00.500Z"),
        )
        .expect("a fractional end still under the cap dispatches");

        // A fractional pre-epoch start floors DOWN at nanosecond precision:
        // epoch minus half a second sits in bucket -1, so the same cap-length
        // window touches 1,001 buckets (round 5).
        let error = prevalidate_series_span(
            "hour",
            Some("1969-12-31T23:59:59.500Z"),
            Some("1970-02-11T16:00:00Z"),
        )
        .expect_err("a pre-epoch fractional start opens its partial bucket");
        assert!(error.to_string().contains("1001"));

        // Langfuse's week anchoring is not the epoch, so week carries one
        // safety bucket: an aligned 999-week window is the last allowed.
        prevalidate_series_span(
            "week",
            Some("1970-01-01T00:00:00Z"),
            Some("1989-02-23T00:00:00Z"),
        )
        .expect("999 aligned weeks stay at the cap");
        prevalidate_series_span(
            "week",
            Some("1970-01-01T00:00:00Z"),
            Some("1989-03-02T00:00:00Z"),
        )
        .expect_err("1,000 aligned weeks exceed the cap with the safety bucket");
    }

    /// An absent --start is bounded by the server's default window and an
    /// unparseable bound stays the server's 422 business: neither may invent a
    /// client-side refusal.
    #[test]
    fn series_span_guard_skips_what_the_server_owns() {
        prevalidate_series_span("hour", None, None).expect("absent start stays server-owned");
        prevalidate_series_span(
            "hour",
            Some("not-a-timestamp"),
            Some("2026-01-01T00:00:00Z"),
        )
        .expect("unparseable start stays server-owned");
        prevalidate_series_span("hour", Some("x"), Some("2026-01-01T00:00:00Z"))
            .expect("a too-short value must never panic the guard");
        prevalidate_series_span("hour", Some("19700101"), Some("2026-01-01T00:00:00Z"))
            .expect("a basic-format date this parser does not model stays server-owned");
        prevalidate_series_span(
            "fortnight",
            Some("1970-01-01T00:00:00Z"),
            Some("2026-01-01T00:00:00Z"),
        )
        .expect("unknown granularity stays server-owned");
    }

    /// The API's datetime.fromisoformat accepts lenient timestamp forms the
    /// strict RFC 3339 parser rejects (#1948 scope review). The guard must
    /// parse those forms too, or an over-cap span in an accepted format
    /// reaches the backend unprevalidated. Naive values are assumed UTC for
    /// the estimate; the post-dispatch bound is the backstop for anything
    /// else fromisoformat accepts that this parser does not.
    #[test]
    fn series_span_guard_parses_the_api_lenient_timestamp_forms() {
        for (start, end) in [
            ("1970-01-01", "2026-01-01"),
            ("1970-01-01 00:00:00", "2026-01-01 00:00:00"),
            ("1970-01-01T00:00", "2026-01-01T00:00"),
            ("1970-01-01 00:00", "2026-01-01 00:00"),
            ("1970-01-01T00:00:00", "2026-01-01T00:00:00"),
            ("1970-01-01T00:00Z", "2026-01-01T00:00Z"),
            ("1970-01-01T00:00+00:00", "2026-01-01T00:00+00:00"),
            ("1970-01-01T00:00:00+0000", "2026-01-01T00:00:00+0000"),
        ] {
            let error = prevalidate_series_span("hour", Some(start), Some(end))
                .expect_err("a 56 year hourly window must be refused in every API-accepted form");
            assert!(error.to_string().contains("buckets"), "{start} to {end}");
        }
    }

    /// A --start with no --end is unbounded server-side (the API defaults end
    /// to now), so the guard validates it against the current clock.
    #[test]
    fn series_span_guard_bounds_a_one_sided_start_window() {
        let error = prevalidate_series_span("hour", Some("1970-01-01T00:00:00Z"), None)
            .expect_err("a start of 1970 with no end exceeds the cap against now");
        assert!(error.to_string().contains("now"));
        assert_eq!(
            MAX_OBSERVABILITY_METRIC_POINTS, 1000,
            "the pre-dispatch guard and the post-dispatch bound share one cap"
        );
    }

    #[test]
    fn allowlist_entries_accept_owner_repo_and_owner_wildcard() {
        validate_allowlist_entry("acme-corp/acme-bot").expect("exact repo");
        validate_allowlist_entry("acme-labs/*").expect("owner wildcard");
        assert!(validate_allowlist_entry("not a repo").is_err());
        assert!(validate_allowlist_entry("acme-corp/").is_err());
        assert!(validate_allowlist_entry("*/acme-bot").is_err());
    }

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
        let body = agent_update_body(None);
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
        let repo = agent_update_body(Some("acme/bundle"));
        assert_eq!(repo["repo_full_name"], "acme/bundle");
    }

    #[test]
    fn agent_update_body_never_emits_channel() {
        // `AgentUpdate.channel` is retired: bindings move through
        // `POST /agents/{id}/channels`. A body that still carries the key does
        // not merely send something unnecessary, it 422s the whole PATCH --
        // taking the repo bind travelling beside it down with it.
        for body in [
            agent_update_body(None),
            agent_update_body(Some("acme/bundle")),
        ] {
            assert!(
                body.get("channel").is_none() && body.get("channels").is_none(),
                "no binding key belongs in an AgentUpdate body: {body}"
            );
        }
        assert_eq!(
            agent_update_body(Some("acme/bundle")),
            serde_json::json!({"repo_full_name": "acme/bundle"}),
            "the repo bind is the only thing left in this body"
        );
    }

    #[test]
    fn add_channel_body_shape() {
        // The subresource takes the PAIR and nothing else. Exact equality is
        // the assertion, so a stray `agent_id` echoed back into the body, or a
        // bare address string standing in for the pair, both fail.
        assert_eq!(
            add_channel_body("slack", "C0EXAMPLE1", None, None),
            serde_json::json!({"kind": "slack", "address": "C0EXAMPLE1"})
        );
        // The kind is never inferred: a non-Slack ingress passes through
        // verbatim, which is the whole point of a channel-neutral binding.
        assert_eq!(
            add_channel_body("email", "ops@example.com", None, None),
            serde_json::json!({"kind": "email", "address": "ops@example.com"})
        );
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
