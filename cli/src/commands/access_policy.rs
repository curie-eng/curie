//! `curie cluster access-policy <agent>`: show or change which IdP teams may
//! invoke an agent and approve for it (ADR 0220 section 8,
//! `GET`/`PUT /agents/{id}/access-policy`).
//!
//! Storage and administration only: nothing enforces the policy yet. A write
//! reads the stored policy first and sends its version back as
//! `expected_version`, so a concurrent change by another operator is refused
//! (409) instead of silently overwritten. A role left unmentioned keeps its
//! stored team.

use anyhow::Result;

use crate::api::{AgentAccessPolicy, ApiClient};
use crate::api_requests::AgentAccessPolicyWrite;

use super::AgentActionOpts;

/// One role's requested change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TeamChange {
    /// Name this team (an IdP group team id of the agent's tenant).
    Set(String),
    /// Remove the team, so the role keeps today's behavior.
    Clear,
}

/// What one invocation does: show the policy, or change one or both roles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessPolicyChange {
    pub invocation: Option<TeamChange>,
    pub approver: Option<TeamChange>,
}

impl AccessPolicyChange {
    /// Resolve the flags into one intent, before any I/O.
    ///
    /// clap refuses a role's set and clear flags together. A team is named by
    /// its id, which must be a UUID; whether it is an IdP group team of the
    /// agent's tenant stays the API's call.
    ///
    /// Args:
    ///   invocation_team: `--invocation-team`, when given.
    ///   approver_team: `--approver-team`, when given.
    ///   clear_invocation: whether `--clear-invocation-team` was passed.
    ///   clear_approver: whether `--clear-approver-team` was passed.
    ///
    /// Returns:
    ///   The intent, or a usage error naming the malformed id.
    pub fn resolve(
        invocation_team: Option<String>,
        approver_team: Option<String>,
        clear_invocation: bool,
        clear_approver: bool,
    ) -> Result<Self> {
        Ok(AccessPolicyChange {
            invocation: role(invocation_team, clear_invocation, "--invocation-team")?,
            approver: role(approver_team, clear_approver, "--approver-team")?,
        })
    }

    /// No role changes: read and print the policy.
    pub fn is_show(&self) -> bool {
        self.invocation.is_none() && self.approver.is_none()
    }
}

fn role(team: Option<String>, clear: bool, flag: &str) -> Result<Option<TeamChange>> {
    if clear {
        return Ok(Some(TeamChange::Clear));
    }
    match team {
        None => Ok(None),
        Some(id) if uuid::Uuid::parse_str(&id).is_ok() => Ok(Some(TeamChange::Set(id))),
        Some(id) => Err(crate::exit::usage(format!(
            "{flag} takes a team id (a UUID, as `GET /teams` lists it); got {id:?}"
        ))),
    }
}

fn merged(change: &Option<TeamChange>, stored: &Option<String>) -> Option<String> {
    match change {
        None => stored.clone(),
        Some(TeamChange::Set(id)) => Some(id.clone()),
        Some(TeamChange::Clear) => None,
    }
}

/// Output of `cluster access-policy <agent>`: the dry-run plan, or the policy
/// as the API holds it after this invocation. A role with no team is emitted
/// as JSON `null`, never omitted. `changed` distinguishes a show from a write.
#[derive(Debug)]
pub enum AccessPolicyOutput {
    DryRun(crate::ui::DryRunPlan),
    Done {
        agent: String,
        policy: AgentAccessPolicy,
        changed: bool,
    },
}

impl crate::ui::CliOutput for AccessPolicyOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            AccessPolicyOutput::DryRun(plan) => plan.to_json(),
            AccessPolicyOutput::Done {
                agent,
                policy,
                changed,
            } => serde_json::json!({
                "agent": agent,
                "agent_id": policy.agent_id,
                "invocation_team_id": policy.invocation_team_id,
                "approver_team_id": policy.approver_team_id,
                "version": policy.version,
                "changed": changed,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            AccessPolicyOutput::DryRun(plan) => plan.render(ui),
            AccessPolicyOutput::Done {
                agent,
                policy,
                changed,
            } => {
                let verb = if *changed { " now" } else { "" };
                let invocation = policy
                    .invocation_team_id
                    .as_deref()
                    .unwrap_or("none (today's caller rules)");
                let approver = policy
                    .approver_team_id
                    .as_deref()
                    .unwrap_or("none (each route's approvers)");
                ui.payload(&format!(
                    "access policy for {agent}{verb} (version {}): invocation team {invocation}; \
                     approver team {approver}",
                    policy.version
                ));
            }
        }
    }
}

/// Show or change one agent's access policy.
///
/// Args:
///   opts: the connection, the agent name or id, and `--dry-run`.
///   change: the resolved intent.
///
/// Returns:
///   The policy as stored, or the dry-run plan.
pub async fn agent_access_policy(
    opts: AgentActionOpts,
    change: AccessPolicyChange,
) -> Result<AccessPolicyOutput> {
    if opts.dry_run {
        let mut lines = vec![format!(
            "GET {}/agents/<id>/access-policy  (would resolve agent {:?} first)",
            opts.api_url, opts.agent
        )];
        if !change.is_show() {
            let describe = |change: &Option<TeamChange>| match change {
                None => "unchanged".to_string(),
                Some(TeamChange::Set(id)) => id.clone(),
                Some(TeamChange::Clear) => "null".to_string(),
            };
            lines.push(format!(
                "PUT {}/agents/<id>/access-policy  invocation_team_id={} approver_team_id={} \
                 expected_version=<the version just read>",
                opts.api_url,
                describe(&change.invocation),
                describe(&change.approver)
            ));
        }
        return Ok(AccessPolicyOutput::DryRun(crate::ui::DryRunPlan { lines }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let stored = client.get_agent_access_policy(&agent.id).await?;
    if change.is_show() {
        return Ok(AccessPolicyOutput::Done {
            agent: agent.name,
            policy: stored,
            changed: false,
        });
    }
    let body = AgentAccessPolicyWrite {
        invocation_team_id: merged(&change.invocation, &stored.invocation_team_id),
        approver_team_id: merged(&change.approver, &stored.approver_team_id),
        expected_version: stored.version,
    };
    let ui = crate::ui::ui();
    let cl = ui.checklist();
    let step = cl.step(&format!("setting the access policy of {}", agent.name));
    let saved = match client.put_agent_access_policy(&agent.id, &body).await {
        Ok(saved) => {
            step.done("set");
            saved
        }
        Err(err) => {
            step.fail("failed");
            return Err(err);
        }
    };
    Ok(AccessPolicyOutput::Done {
        agent: agent.name,
        policy: saved,
        changed: true,
    })
}

#[cfg(test)]
mod access_policy_tests {
    use super::{merged, AccessPolicyChange, TeamChange};

    const TEAM: &str = "00000000-0000-0000-0000-0000000000aa";

    #[test]
    fn no_flags_is_a_show() {
        let change = AccessPolicyChange::resolve(None, None, false, false).unwrap();
        assert!(change.is_show());
    }

    #[test]
    fn a_team_id_must_be_a_uuid() {
        assert!(AccessPolicyChange::resolve(Some("finance".into()), None, false, false).is_err());
        let change = AccessPolicyChange::resolve(Some(TEAM.into()), None, false, true).unwrap();
        assert_eq!(change.invocation, Some(TeamChange::Set(TEAM.into())));
        assert_eq!(change.approver, Some(TeamChange::Clear));
    }

    #[test]
    fn an_unmentioned_role_keeps_its_stored_team() {
        let stored = Some(TEAM.to_string());
        assert_eq!(merged(&None, &stored), stored);
        assert_eq!(merged(&Some(TeamChange::Clear), &stored), None);
    }
}
