//! `curie <local|cluster> actions`: the operator surface over the action
//! ledger and its executions (ACTION-EXECUTOR-23).
//!
//! `list` and `show` read the ledger, `undo` asks for a restore and prints the
//! execution it created, and `execution` is the receipt of ACTION-EXECUTOR-18.
//! Unrelated to `agent_actions.rs`, which is the agent lifecycle verbs.
//!
//! Nothing here prints snapshot material: the typed mirrors in `crate::api`
//! do not carry `prior_state`/`post_state`, so serde drops them on decode.

use anyhow::Result;

use crate::api::{ActionExecution, ActionRecord, ActionUndoReceipt, ApiClient};

// @spec ACTION-EXECUTOR-23
/// The verb an `actions` invocation runs, as the dispatcher hands it over.
pub enum ActionsVerb {
    List { agent: Option<String> },
    Show { id: String },
    Undo { id: String },
    Execution { id: String },
}

// @spec ACTION-EXECUTOR-23
/// The connection an `actions` verb uses, already resolved for its tier.
pub struct ActionsOpts {
    pub api_url: String,
    pub api_key: String,
    /// "local" or "cluster": the tier named in follow-up hints.
    pub tier: &'static str,
}

// @spec ACTION-EXECUTOR-23
/// Output of `<tier> actions <verb>`: one JSON object per verb under `--json`.
pub enum ActionsOutput {
    List {
        agent: Option<String>,
        actions: Vec<ActionRecord>,
        truncated: bool,
    },
    Show {
        action: Box<ActionRecord>,
    },
    Undo {
        action_id: String,
        receipt: ActionUndoReceipt,
        tier: &'static str,
    },
    Execution {
        execution: Box<ActionExecution>,
    },
}

// @spec ACTION-EXECUTOR-18
/// The code a receipt names: the refusal code of a refused execution, the
/// failure code of a failed or indeterminate one, none for a confirmed one.
fn receipt_code(execution: &ActionExecution) -> Option<&str> {
    execution
        .refusal_code
        .as_deref()
        .or(execution.failure_code.as_deref())
}

impl crate::ui::CliOutput for ActionsOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ActionsOutput::List {
                agent,
                actions,
                truncated,
            } => serde_json::json!({
                "agent": agent,
                "actions": actions,
                "truncated": truncated,
            }),
            ActionsOutput::Show { action } => serde_json::json!({ "action": action }),
            // @spec ACTION-EXECUTOR-23: the execution id and state, never a
            // snapshot or a target.
            ActionsOutput::Undo {
                action_id, receipt, ..
            } => serde_json::json!({
                "action_id": action_id,
                "execution_id": receipt.execution_id,
                "state": receipt.state,
            }),
            // @spec ACTION-EXECUTOR-18: the receipt names state and code.
            ActionsOutput::Execution { execution } => serde_json::json!({
                "execution_id": execution.id,
                "kind": execution.kind,
                "state": execution.state,
                "code": receipt_code(execution),
                "agent_id": execution.agent_id,
                "connector": execution.connector,
                "tool": execution.tool,
                "subject_action_id": execution.subject_action_id,
                "requested_by": execution.requested_by,
                "attempt": execution.attempt,
                "dispatched_at": execution.dispatched_at,
                "finished_at": execution.finished_at,
                "created_at": execution.created_at,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        let line = |key: &str, value: &str| ui.payload_plain(&format!("{key:<12} {value}"));
        match self {
            ActionsOutput::List {
                actions, truncated, ..
            } => {
                if actions.is_empty() {
                    ui.payload("no actions");
                    return;
                }
                let rows: Vec<Vec<String>> = actions
                    .iter()
                    .map(|action| {
                        vec![
                            action.id.clone(),
                            action.tool.clone(),
                            action.status.clone(),
                            if action.undoable { "yes" } else { "no" }.to_string(),
                            action.created_at.clone(),
                        ]
                    })
                    .collect();
                let table =
                    crate::ui::table(&["ID", "TOOL", "STATUS", "UNDOABLE", "CREATED"], &rows, &[]);
                let trimmed: Vec<&str> = table.lines().map(str::trim_end).collect();
                ui.payload_plain(&trimmed.join("\n"));
                if *truncated {
                    ui.payload_plain(&format!(
                        "(showing the first {} actions; more exist)",
                        ApiClient::ACTIONS_LIST_LIMIT
                    ));
                }
            }
            ActionsOutput::Show { action } => {
                line("id", &action.id);
                line("tool", &action.tool);
                line("status", &action.status);
                line("undoable", if action.undoable { "yes" } else { "no" });
                line("agent", action.agent_id.as_deref().unwrap_or("-"));
                line("thread", &action.conversation_id);
                line("created", &action.created_at);
                line("completed", action.completed_at.as_deref().unwrap_or("-"));
                if let Some(undone_at) = &action.undone_at {
                    line(
                        "undone",
                        &format!(
                            "{undone_at} by {}",
                            action.undone_by.as_deref().unwrap_or("-")
                        ),
                    );
                }
                if let Some(detail) = &action.detail {
                    line("detail", detail);
                }
            }
            ActionsOutput::Undo {
                action_id,
                receipt,
                tier,
            } => {
                line("action", action_id);
                line("execution", &receipt.execution_id);
                line("state", &receipt.state);
                ui.payload_plain(&format!(
                    "Read the receipt with `curie {tier} actions execution {}`.",
                    receipt.execution_id
                ));
            }
            ActionsOutput::Execution { execution } => {
                line("execution", &execution.id);
                line("kind", &execution.kind);
                line("state", &execution.state);
                line("code", receipt_code(execution).unwrap_or("-"));
                line("connector", &execution.connector);
                line(
                    "action",
                    execution.subject_action_id.as_deref().unwrap_or("-"),
                );
                line(
                    "requested",
                    execution.requested_by.as_deref().unwrap_or("-"),
                );
                line("attempt", &execution.attempt.to_string());
                line("finished", execution.finished_at.as_deref().unwrap_or("-"));
            }
        }
    }
}

// @spec ACTION-EXECUTOR-23
/// A malformed id is a usage error before any request, so it never reaches
/// the API (whose 422 would carry the same class).
fn validated_id(kind: &str, raw: &str) -> Result<String> {
    uuid::Uuid::parse_str(raw)
        .map(|_| raw.to_string())
        .map_err(|_| crate::exit::usage(format!("{kind} id must be a UUID, got {raw:?}")))
}

// @spec ACTION-EXECUTOR-23 @spec ACTION-EXECUTOR-3
/// The ADR-0106 operator principal that rules an undo, read from the same
/// environment variable `approvals --resolve` reads. Never echoed.
fn undo_principal_token() -> Result<String> {
    std::env::var("CURIE_APPROVAL_PRINCIPAL_TOKEN")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            anyhow::Error::from(
                crate::exit::CliError::usage(
                    "undo requires CURIE_APPROVAL_PRINCIPAL_TOKEN, which names WHO ruled the \
                     undo (the platform key authorizes reads but identifies nobody)",
                )
                .with_fix(
                    "mint a reusable operator credential with `curie <local|cluster> approvals \
                     <AGENT> --mint-operator-principal <SUBJECT>`, export the one-time result \
                     as CURIE_APPROVAL_PRINCIPAL_TOKEN, and retry",
                ),
            )
        })
}

// @spec ACTION-EXECUTOR-23
/// `<tier> actions <verb>`: list or read ledger actions, ask for an undo, or
/// read an execution's receipt.
pub async fn actions(opts: ActionsOpts, verb: ActionsVerb) -> Result<ActionsOutput> {
    // Inputs are validated before the client exists, so a malformed request
    // makes no network call at all.
    let verb = match verb {
        ActionsVerb::List { agent } => ActionsVerb::List { agent },
        ActionsVerb::Show { id } => ActionsVerb::Show {
            id: validated_id("action", &id)?,
        },
        ActionsVerb::Undo { id } => ActionsVerb::Undo {
            id: validated_id("action", &id)?,
        },
        ActionsVerb::Execution { id } => ActionsVerb::Execution {
            id: validated_id("execution", &id)?,
        },
    };
    let principal = match &verb {
        ActionsVerb::Undo { .. } => Some(undo_principal_token()?),
        _ => None,
    };
    // A cluster tunnel is loopback too; its key is already the release's, so
    // it must not trigger local key discovery.
    let client = if opts.tier == "cluster" {
        ApiClient::with_resolved_key(&opts.api_url, &opts.api_key)?
    } else {
        ApiClient::new(&opts.api_url, &opts.api_key)?
    };
    match verb {
        ActionsVerb::List { agent } => {
            let resolved = match &agent {
                Some(agent) => Some(client.find_agent(agent).await?),
                None => None,
            };
            let actions = client
                .list_actions(resolved.as_ref().map(|agent| agent.id.as_str()))
                .await?;
            let truncated = actions.len() >= ApiClient::ACTIONS_LIST_LIMIT;
            Ok(ActionsOutput::List {
                agent: resolved.map(|agent| agent.name),
                actions,
                truncated,
            })
        }
        ActionsVerb::Show { id } => Ok(ActionsOutput::Show {
            action: Box::new(client.get_action(&id).await?),
        }),
        ActionsVerb::Undo { id } => {
            let principal = principal.expect("undo read its principal above");
            let receipt = client.undo_action(&id, &principal).await?;
            Ok(ActionsOutput::Undo {
                action_id: id,
                receipt,
                tier: opts.tier,
            })
        }
        ActionsVerb::Execution { id } => Ok(ActionsOutput::Execution {
            execution: Box::new(client.get_action_execution(&id).await?),
        }),
    }
}
