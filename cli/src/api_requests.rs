//! Typed request bodies for every platform JSON send in [`crate::api`] (#3834).
//!
//! Each struct mirrors one OpenAPI request schema in the committed
//! `apps/api/openapi.json` and is declared in `cli/api-mirrors.json`
//! (`request_mirrors` for the struct, `requests` for the client fn, method and
//! path that send it). The field-parity gate binds every
//! `.json::<T>(..)` send to its operation and field-compares these structs,
//! including optionality (`cli/tests/support/field_parity.rs` documents the
//! exact rules).
//!
//! Conventions:
//!
//! * Request mirrors derive `Serialize` only. Response decoding lives in
//!   `crate::api`; a struct here is never read back from the API.
//! * A key the request may omit is `Option<T>` with
//!   `skip_serializing_if = "Option::is_none"`: `None` sends nothing. A nullable
//!   field whose explicit `null` means something different from omission (the
//!   PATCH clears in `update_agent`, which reads `model_fields_set`) is
//!   `Option<Option<T>>` with the same skip: `None` omits the key (unchanged),
//!   `Some(None)` sends `null` (clear), `Some(Some(v))` sends the value (set).
//! * A nullable field without that skip always sends its key, `null` when
//!   `None`, which is the wire shape those sends have always had.
//! * Fields are declared in wire-name order. The bodies these structs replaced
//!   were `serde_json::Value` objects, which serialize their keys sorted, so the
//!   same order keeps every request byte-identical on the wire.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::api::{ApprovalRouteBindingWrite, HookPartitionInput, SourceBindingInput};

/// `POST /agents` (`AgentCreate`). Deploy creates an agent with its one
/// channel and, when asked, its repository binding; every other setting is
/// written afterwards by its own verb.
#[derive(Debug, Clone, Serialize)]
pub struct AgentCreate {
    pub channel: ChannelBindingWrite,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_full_name: Option<String>,
}

/// One channel binding as written (`ChannelBindingWrite`): the
/// `AgentCreate.channel` value and the `POST /agents/{id}/channels` body.
#[derive(Debug, Clone, Serialize)]
pub struct ChannelBindingWrite {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adapter: Option<String>,
    pub address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    pub kind: String,
}

/// `PATCH /agents/{id}` (`AgentUpdate`). Every field is optional: an omitted
/// key leaves the stored value unchanged. `model`, `thinking`,
/// `execution_deadline_seconds`, `runner_resources` and
/// `publication_branch_prefix` are three-state because the router reads
/// `model_fields_set` for them, so an explicit `null` clears the value.
///
/// Deliberately not `Debug`: `secrets` carries connector secret values.
#[derive(Clone, Default, Serialize)]
pub struct AgentUpdate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_required_tools: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_routes: Option<BTreeMap<String, ApprovalRouteBindingWrite>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_deadline_seconds: Option<Option<u32>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hook_partitions: Option<BTreeMap<String, HookPartitionInput>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_writes: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_branch_prefix: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_draft: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_full_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_resources: Option<Option<serde_json::Map<String, serde_json::Value>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secrets: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_bindings: Option<BTreeMap<String, SourceBindingInput>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<Option<String>>,
}

/// `PUT /agents/{id}/channels/callers` (`ChannelCallersWrite`). The key is
/// required; `null` clears the list so everyone may talk to the bot again.
#[derive(Debug, Clone, Serialize)]
pub struct ChannelCallersWrite {
    pub allowed_callers: Option<Vec<String>>,
}

/// `POST /channels/token` (`ChannelTokenRequest`).
#[derive(Debug, Clone, Serialize)]
pub struct ChannelTokenRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adapter: Option<String>,
    pub address: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_s: Option<i64>,
}

/// `POST /agents/{id}/versions` (`VersionCreate`). `commit_sha` is always sent,
/// `null` when the deploy has no commit.
#[derive(Debug, Clone, Serialize)]
pub struct VersionCreate {
    pub commit_sha: Option<String>,
    pub created_by: String,
    pub version_label: String,
}

/// `POST /deployments` (`DeploymentCreate`). `workspace_enabled` is omitted to
/// preserve the agent's current workspace capability.
#[derive(Debug, Clone, Serialize)]
pub struct DeploymentCreate {
    pub agent_id: String,
    pub commit_sha: Option<String>,
    pub environment: String,
    pub version_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_enabled: Option<bool>,
}

/// `POST /deploy-targets/resolve` and `POST /deploy-targets/list`
/// (`ResolveTargetRequest`): the `deploy.yaml` text, never parsed by the CLI.
#[derive(Debug, Clone, Serialize)]
pub struct ResolveTargetRequest {
    pub content: String,
    pub target: String,
}

/// `POST /git-flow/routing-check` (`RoutingCheckRequest`).
#[derive(Debug, Clone, Serialize)]
pub struct RoutingCheckRequest {
    pub content: Option<String>,
    pub repo_full_name: String,
}

/// `POST /agents/{id}/memory` (`MemoryEntryCreate`).
#[derive(Debug, Clone, Serialize)]
pub struct MemoryEntryCreate {
    pub content: String,
}

/// `PUT /agents/{id}/memory/guidance` (`MemoryGuidanceIn`).
#[derive(Debug, Clone, Serialize)]
pub struct MemoryGuidanceIn {
    pub text: String,
}

/// `POST /approvals/{id}/resolve` (`ApprovalResolve`).
#[derive(Debug, Clone, Serialize)]
pub struct ApprovalResolve {
    pub decision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// `POST /approvals/{id}/recover` (`ApprovalRecover`).
#[derive(Debug, Clone, Serialize)]
pub struct ApprovalRecover {
    pub disposition: String,
    pub reason: String,
    pub recovery_key: String,
}

/// `POST /approvals/principals/operator` (`ApprovalPrincipalMint`).
#[derive(Debug, Clone, Serialize)]
pub struct ApprovalPrincipalMint {
    pub subject: String,
}

/// `POST /console/login-codes` (`ConsoleLoginCodeMint`).
#[derive(Debug, Clone, Serialize)]
pub struct ConsoleLoginCodeMint {
    pub subject: String,
}

/// `POST /evals/trigger` (`EvalTriggerRequest`).
#[derive(Debug, Clone, Serialize)]
pub struct EvalTriggerRequest {
    pub agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suite: Option<String>,
}

// @spec ACTION-EXECUTOR-23 @spec ACTION-EXECUTOR-3
/// `POST /actions/{action_id}/undo` (`ActionUndo`). Always sent empty: the
/// actor is the authenticated principal, never a claim in the body, and the
/// platform observes the live state itself. Braced, not a unit struct, so it
/// serializes as `{}` rather than `null`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ActionUndo {}
