//! `<tier> factory-progress`: where one agent's factory runs post their Slack
//! progress.
//!
//! This is a per-agent setting written through `PATCH /agents/{id}`, which is
//! why it is its own verb and not a flag on `curie cluster factory`: that
//! command configures release-wide intake and has no agent argument.

use super::*;
use crate::api_requests::AgentUpdate;

/// What one `factory-progress` invocation intends to do to the stored channel.
///
/// Three states because the API's PATCH has three: an omitted key leaves the
/// channel alone, an explicit JSON null clears it, and a channel id sets it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FactoryProgressChange {
    /// Neither flag: read the stored channel and write nothing.
    Inspect,
    /// `--no-slack-channel`: send an explicit null.
    Clear,
    /// `--slack-channel <ID>`: set this channel.
    Set(String),
}

/// Whether `id` has the shape the API accepts, `^[CG][A-Z0-9]{6,}$`.
fn is_slack_channel_id(id: &str) -> bool {
    let mut chars = id.chars();
    matches!(chars.next(), Some('C' | 'G'))
        && id.len() >= 7
        && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
}

impl FactoryProgressChange {
    /// Resolve the `--slack-channel` / `--no-slack-channel` pair into one intent.
    ///
    /// Args:
    ///   slack_channel: the `--slack-channel` value, if one was passed.
    ///   no_slack_channel: whether `--no-slack-channel` was passed.
    ///
    /// Returns:
    ///   The intent, or a usage error naming the expected shape when the id is
    ///   malformed or both flags were passed. Surrounding whitespace is
    ///   trimmed; nothing else is altered, so a lowercase id is refused rather
    ///   than quietly uppercased into a different channel.
    pub fn resolve(slack_channel: Option<String>, no_slack_channel: bool) -> Result<Self> {
        match (slack_channel, no_slack_channel) {
            (Some(_), true) => Err(crate::exit::usage(
                "--slack-channel and --no-slack-channel contradict each other; pass one",
            )),
            (Some(raw), false) => {
                let id = raw.trim();
                if is_slack_channel_id(id) {
                    Ok(Self::Set(id.to_string()))
                } else {
                    Err(anyhow::Error::from(
                        crate::exit::CliError::usage(format!(
                            "{id:?} is not a Slack channel id; expected a public or private channel id \
                             matching ^[CG][A-Z0-9]{{6,}}$, such as C0EXAMPLE1"
                        ))
                        .with_fix("copy the channel id from Slack: channel details, About, bottom of the pane"),
                    ))
                }
            }
            (None, true) => Ok(Self::Clear),
            (None, false) => Ok(Self::Inspect),
        }
    }

    /// The channel this change would write, if it writes one.
    fn target(&self) -> Option<&str> {
        match self {
            Self::Set(id) => Some(id),
            _ => None,
        }
    }
}

/// The PATCH body for a change, or `None` when this invocation only inspects.
pub fn factory_progress_patch_body(change: &FactoryProgressChange) -> Option<AgentUpdate> {
    let factory_slack_channel = match change {
        FactoryProgressChange::Inspect => return None,
        FactoryProgressChange::Clear => Some(None),
        FactoryProgressChange::Set(id) => Some(Some(id.clone())),
    };
    Some(AgentUpdate {
        factory_slack_channel,
        ..AgentUpdate::default()
    })
}

/// The one-line human summary of a `factory-progress` result.
///
/// Pure so the wording is assertable. `changed` adds ` now`, as
/// `publication-policy` does after a write.
pub fn factory_progress_summary(
    agent: &str,
    factory_slack_channel: Option<&str>,
    changed: bool,
) -> String {
    let verb = if changed { " now" } else { "" };
    let value = factory_slack_channel.unwrap_or("none (factory runs post no Slack progress)");
    format!("factory progress channel for {agent}{verb}: {value}")
}

/// Output of `<tier> factory-progress`.
#[derive(Debug)]
pub enum FactoryProgressOutput {
    DryRun(crate::ui::DryRunPlan),
    Done {
        agent: String,
        factory_slack_channel: Option<String>,
        changed: bool,
    },
}

impl crate::ui::CliOutput for FactoryProgressOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            FactoryProgressOutput::DryRun(plan) => plan.to_json(),
            FactoryProgressOutput::Done {
                agent,
                factory_slack_channel,
                changed,
            } => serde_json::json!({
                "agent": agent,
                "factory_slack_channel": factory_slack_channel,
                "changed": changed,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            FactoryProgressOutput::DryRun(plan) => plan.render(ui),
            FactoryProgressOutput::Done {
                agent,
                factory_slack_channel,
                changed,
            } => ui.payload(&factory_progress_summary(
                agent,
                factory_slack_channel.as_deref(),
                *changed,
            )),
        }
    }
}

/// Read or set one agent's factory Slack progress channel.
///
/// Args:
///   opts: the connection, agent and dry-run flag.
///   tier: `cluster` or `local`, used to name the exact `surfaces` command in
///     the unbound refusal.
///   change: the intent resolved from the flags.
///
/// Returns:
///   The channel as the API stored it, or the dry-run plan.
pub async fn factory_progress(
    opts: AgentActionOpts,
    tier: &'static str,
    change: FactoryProgressChange,
) -> Result<FactoryProgressOutput> {
    let ui = crate::ui::ui();
    let body = factory_progress_patch_body(&change);
    if opts.dry_run {
        let plan = match &body {
            Some(b) => format!(
                "PATCH {}/agents/<id>  {}  (would resolve agent {:?} first)",
                opts.api_url,
                serde_json::to_value(b).context("encoding the PATCH body")?,
                opts.agent
            ),
            None => format!(
                "GET {}/agents  (read-only: would resolve agent {:?} and print its factory progress channel)",
                opts.api_url, opts.agent
            ),
        };
        return Ok(FactoryProgressOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![plan],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    if body.is_none() {
        // Inspect: `find_agent` already returns the full agent row.
        return Ok(FactoryProgressOutput::Done {
            agent: agent.name,
            factory_slack_channel: agent.factory_slack_channel,
            changed: false,
        });
    }
    let cl = ui.checklist();
    let step = cl.step(&format!(
        "updating factory progress channel for {}",
        agent.name
    ));
    let saved = match client
        .update_agent_factory_slack_channel(&agent, change.target(), tier)
        .await
    {
        Ok(saved) => {
            step.done("updated");
            saved
        }
        Err(err) => {
            step.fail("failed");
            return Err(err);
        }
    };
    Ok(FactoryProgressOutput::Done {
        agent: saved.name,
        factory_slack_channel: saved.factory_slack_channel,
        changed: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(change: &FactoryProgressChange) -> Option<serde_json::Value> {
        factory_progress_patch_body(change).map(|b| serde_json::to_value(b).expect("encodes"))
    }

    #[test]
    fn an_inspect_sends_no_patch_body() {
        assert!(body(&FactoryProgressChange::Inspect).is_none());
    }

    #[test]
    fn a_set_sends_only_the_channel() {
        assert_eq!(
            body(&FactoryProgressChange::Set("C0EXAMPLE1".to_string())),
            Some(serde_json::json!({"factory_slack_channel": "C0EXAMPLE1"}))
        );
    }

    #[test]
    fn a_clear_sends_the_key_as_an_explicit_null() {
        let sent = body(&FactoryProgressChange::Clear).expect("a write");
        let object = sent.as_object().expect("an object");
        assert_eq!(object.len(), 1);
        assert_eq!(
            object.get("factory_slack_channel"),
            Some(&serde_json::Value::Null)
        );
    }

    #[test]
    fn resolve_accepts_the_api_shape_and_trims_whitespace() {
        for id in ["C0EXAMPLE1", "G0EXAMPLE1", "CABCDEF", "  C0EXAMPLE1\n"] {
            let change = FactoryProgressChange::resolve(Some(id.to_string()), false)
                .unwrap_or_else(|e| panic!("{id:?} must be accepted: {e}"));
            assert_eq!(change, FactoryProgressChange::Set(id.trim().to_string()));
        }
    }

    #[test]
    fn resolve_refuses_ids_the_api_would_refuse() {
        for id in [
            "c0example1",
            "C0EXAM",
            "D0EXAMPLE1",
            "C0EXAMPLE-1",
            "",
            "C0 EXAMPLE1",
            "U0EXAMPLE1",
        ] {
            let err = FactoryProgressChange::resolve(Some(id.to_string()), false)
                .expect_err(&format!("{id:?} must be refused"));
            assert!(
                err.to_string().contains("^[CG][A-Z0-9]{6,}$"),
                "the error names the expected shape: {err}"
            );
        }
    }

    #[test]
    fn resolve_maps_the_flag_pair_to_one_intent() {
        assert_eq!(
            FactoryProgressChange::resolve(None, false).expect("inspect"),
            FactoryProgressChange::Inspect
        );
        assert_eq!(
            FactoryProgressChange::resolve(None, true).expect("clear"),
            FactoryProgressChange::Clear
        );
        assert!(FactoryProgressChange::resolve(Some("C0EXAMPLE1".to_string()), true).is_err());
    }

    #[test]
    fn the_summary_names_the_channel_or_says_none() {
        assert_eq!(
            factory_progress_summary("acme-bot", Some("C0EXAMPLE1"), false),
            "factory progress channel for acme-bot: C0EXAMPLE1"
        );
        assert_eq!(
            factory_progress_summary("acme-bot", None, false),
            "factory progress channel for acme-bot: none (factory runs post no Slack progress)"
        );
        assert_eq!(
            factory_progress_summary("acme-bot", Some("C0EXAMPLE1"), true),
            "factory progress channel for acme-bot now: C0EXAMPLE1"
        );
        assert_eq!(
            factory_progress_summary("acme-bot", None, true),
            "factory progress channel for acme-bot now: none (factory runs post no Slack progress)"
        );
    }
}
