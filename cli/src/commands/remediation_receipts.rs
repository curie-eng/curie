//! `curie <local|cluster> remediation`: the operator receipt of each nomination
//! (AUTOMATED-REMEDIATION-20).
//!
//! `list` reads `GET /remediation-nominations` (newest first, optionally one
//! agent, one state and a page size) and `show` reads one nomination's row; the
//! row is the API's, unchanged. Both are reads on the platform key: no
//! principal, and no request is made for a malformed id, an unknown state or a
//! limit outside 1 to 200. With no agent, `list` makes no agent lookup.

use anyhow::Result;

use super::remediation_policy::RemediationPolicyOpts;
use crate::api::RemediationNomination;
use crate::exit::CliError;
use crate::remediation::NOMINATION_STATES;

/// The API's page ceiling.
const LIMIT_MAXIMUM: u32 = 200;

// @spec AUTOMATED-REMEDIATION-20
/// The verb a `remediation` invocation runs, as the dispatcher hands it over.
pub enum RemediationVerb {
    List {
        /// A name or id; `None` lists every agent's nominations.
        agent: Option<String>,
        state: Option<String>,
        limit: Option<u32>,
    },
    Show {
        nomination_id: String,
    },
}

enum Validated {
    List {
        agent: Option<String>,
        state: Option<String>,
        limit: Option<u32>,
    },
    Show(String),
}

// @spec AUTOMATED-REMEDIATION-20
/// A `remediation` verb whose inputs passed [`RemediationVerb::validate`].
pub struct ValidatedRemediation(Validated);

/// A canonical (lowercase, hyphenated) UUID.
pub(super) fn canonical_uuid(raw: &str, what: &str) -> Result<String> {
    match uuid::Uuid::parse_str(raw) {
        Ok(parsed) if parsed.hyphenated().to_string() == raw => Ok(raw.to_string()),
        _ => Err(anyhow::Error::from(
            CliError::usage(format!(
                "{what} must be a lowercase hyphenated UUID, got {raw:?}"
            ))
            .with_fix("copy the id from `remediation list` or the command that printed it"),
        )),
    }
}

impl RemediationVerb {
    // @spec AUTOMATED-REMEDIATION-20
    /// Check the invocation's own inputs; pure, with no network or discovery.
    pub fn validate(self) -> Result<ValidatedRemediation> {
        Ok(ValidatedRemediation(match self {
            RemediationVerb::Show { nomination_id } => {
                Validated::Show(canonical_uuid(&nomination_id, "the nomination id")?)
            }
            RemediationVerb::List {
                agent,
                state,
                limit,
            } => {
                if let Some(state) = &state {
                    if !NOMINATION_STATES.contains(&state.as_str()) {
                        return Err(anyhow::Error::from(
                            CliError::usage(format!(
                                "--state must be one of {}, got {state:?}",
                                NOMINATION_STATES.join(", ")
                            ))
                            .with_fix("use one of the nomination states named above"),
                        ));
                    }
                }
                if let Some(limit) = limit {
                    if !(1..=LIMIT_MAXIMUM).contains(&limit) {
                        return Err(anyhow::Error::from(
                            CliError::usage(format!(
                                "--limit must be between 1 and {LIMIT_MAXIMUM}, got {limit}"
                            ))
                            .with_fix("pass a page size from 1 to 200"),
                        ));
                    }
                }
                Validated::List {
                    agent,
                    state,
                    limit,
                }
            }
        }))
    }
}

// @spec AUTOMATED-REMEDIATION-20
/// Output of `<tier> remediation <verb>`: `{"nominations":[..]}` for `list`, the
/// API row unchanged for `show`.
pub struct RemediationOutput(Receipt);

enum Receipt {
    List(Vec<RemediationNomination>),
    Show(Box<RemediationNomination>),
}

impl crate::ui::CliOutput for RemediationOutput {
    fn to_json(&self) -> serde_json::Value {
        match &self.0 {
            Receipt::List(rows) => serde_json::json!({ "nominations": rows }),
            Receipt::Show(row) => serde_json::to_value(row).unwrap_or_default(),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        let dash = |value: &Option<String>| value.clone().unwrap_or_else(|| "-".to_string());
        match &self.0 {
            Receipt::List(rows) => {
                if rows.is_empty() {
                    ui.payload_plain("no nominations");
                }
                for row in rows {
                    ui.payload_plain(&format!(
                        "{}  {:<20} {:<8} {}  {}",
                        row.id,
                        row.stage,
                        row.authority,
                        dash(&row.action),
                        dash(&row.target),
                    ));
                }
            }
            Receipt::Show(row) => {
                let line = |key: &str, value: &str| ui.payload_plain(&format!("{key:<12} {value}"));
                line("nomination", &row.id);
                line("agent", &row.agent_id);
                line("hook", &row.hook);
                line("kind", &dash(&row.kind));
                line("action", &dash(&row.action));
                line("target", &dash(&row.target));
                line("state", &row.state);
                line("stage", &row.stage);
                line("authority", &row.authority);
                line("code", &dash(&row.code));
                line("outcome", &dash(&row.verification_outcome));
                line("approval", &dash(&row.approval_id));
                line("execution", &dash(&row.execution_id));
                line("created", &row.created_at);
                line("decided", &dash(&row.decided_at));
            }
        }
    }
}

// @spec AUTOMATED-REMEDIATION-20
/// `<tier> remediation <verb>`: read the receipt(s) and print them.
pub async fn remediation(
    opts: RemediationPolicyOpts,
    validated: ValidatedRemediation,
) -> Result<RemediationOutput> {
    let client = super::remediation_policy::api_client(&opts)?;
    Ok(RemediationOutput(match validated.0 {
        Validated::Show(id) => {
            Receipt::Show(Box::new(client.get_remediation_nomination(&id).await?))
        }
        Validated::List {
            agent,
            state,
            limit,
        } => {
            let agent_id = match agent {
                Some(agent) => Some(client.find_agent(&agent).await?.id),
                None => None,
            };
            Receipt::List(
                client
                    .list_remediation_nominations(agent_id.as_deref(), state.as_deref(), limit)
                    .await?,
            )
        }
    }))
}
