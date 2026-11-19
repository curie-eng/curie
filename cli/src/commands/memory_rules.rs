//! `curie cluster memory --rules | --accept | --reject | --remove`: list an
//! agent's learned rules, and accept, reject or remove one as the signed-in
//! person. Listing uses the cluster connection (platform key or session). An
//! action reads a private session file and never the platform key.

use std::path::PathBuf;

use anyhow::Result;
use serde_json::Value;

use super::AgentActionOpts;
use crate::api::{ApiClient, OwnerApiClient};
use crate::exit::CliError;
use crate::ui::{CliOutput, DryRunPlan, Ui};

/// A change to one learned rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryRuleVerb {
    Accept,
    Reject,
    Remove,
}

impl MemoryRuleVerb {
    fn path_segment(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Reject => "reject",
            Self::Remove => "remove",
        }
    }

    fn past_tense(self) -> &'static str {
        match self {
            Self::Accept => "accepted",
            Self::Reject => "rejected",
            Self::Remove => "removed",
        }
    }
}

/// Output of the learned rule verbs: the API's own objects, as sent, or a plan.
#[derive(Debug)]
pub enum MemoryRulesOutput {
    DryRun(DryRunPlan),
    /// `--rules`: the API's rule list.
    Rules(Value),
    /// `--accept`, `--reject` or `--remove`: the API's changed rule.
    Changed {
        verb: MemoryRuleVerb,
        change: Value,
    },
}

impl CliOutput for MemoryRulesOutput {
    fn to_json(&self) -> Value {
        match self {
            Self::DryRun(plan) => plan.to_json(),
            Self::Rules(rules) => rules.clone(),
            Self::Changed { change, .. } => change.clone(),
        }
    }

    fn render(&self, ui: &Ui) {
        match self {
            Self::DryRun(plan) => plan.render(ui),
            Self::Rules(rules) => render_rules(ui, rules),
            Self::Changed { verb, change } => render_change(ui, *verb, change),
        }
    }
}

/// Options for a rule change: the session file is the only credential.
pub struct MemoryRuleActOpts {
    pub agent: String,
    pub verb: MemoryRuleVerb,
    pub rule_id: String,
    pub expected_version: i64,
    pub session_file: PathBuf,
    pub api_url: Option<String>,
    pub namespace: String,
    pub release: String,
    pub dry_run: bool,
}

fn rule_id_usage() -> anyhow::Error {
    let fix = "pass the rule id exactly as `curie cluster memory <agent> --rules` prints it";
    CliError::usage(format!("the rule id is not a valid id; {fix}"))
        .with_fix(fix)
        .into()
}

/// A rule id goes into a URL path, so only the characters of a UUID pass.
fn checked_rule_id(rule_id: &str) -> Result<&str> {
    let ok = !rule_id.is_empty()
        && rule_id.len() <= 64
        && rule_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-');
    if ok {
        Ok(rule_id)
    } else {
        Err(rule_id_usage())
    }
}

/// `memory --rules`: list rules through the cluster connection.
pub async fn memory_rules_list(
    opts: AgentActionOpts,
    state: Option<String>,
    scope_key: Option<String>,
) -> Result<MemoryRulesOutput> {
    if opts.dry_run {
        let mut query = Vec::new();
        if let Some(state) = &state {
            query.push(format!("state={state}"));
        }
        if let Some(scope_key) = &scope_key {
            query.push(format!("scope_key={scope_key}"));
        }
        let query = if query.is_empty() {
            String::new()
        } else {
            format!("?{}", query.join("&"))
        };
        return Ok(MemoryRulesOutput::DryRun(DryRunPlan {
            lines: vec![
                format!(
                    "GET {}/agents/<id>/memory/rules{query}  (would resolve agent {:?} first)",
                    opts.api_url.trim_end_matches('/'),
                    opts.agent
                ),
                "credential: the cluster connection (platform key or session)".to_string(),
            ],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let rules = client
        .list_memory_rules(&agent.id, state.as_deref(), scope_key.as_deref())
        .await?;
    Ok(MemoryRulesOutput::Rules(rules))
}

/// `memory --accept | --reject | --remove`: change one rule as the person whose
/// session file is given.
pub async fn memory_rule_act(opts: MemoryRuleActOpts) -> Result<MemoryRulesOutput> {
    let rule_id = checked_rule_id(&opts.rule_id)?.to_string();
    let cookie = super::subscription::read_session_cookie(&opts.session_file)?;
    let (api_url, _tunnel) = super::resolve_cluster_api(
        opts.api_url.clone(),
        &opts.namespace,
        &opts.release,
        opts.dry_run,
    )
    .await?;
    let action = opts.verb.path_segment();
    if opts.dry_run {
        let base = if opts.api_url.is_none() {
            "http://127.0.0.1:<tunnel port>".to_string()
        } else {
            api_url.trim_end_matches('/').to_string()
        };
        return Ok(MemoryRulesOutput::DryRun(DryRunPlan {
            lines: vec![
                format!(
                    "POST {base}/agents/<id>/memory/rules/{rule_id}/{action}  (would resolve agent {:?} first)",
                    opts.agent
                ),
                format!("body: {{\"expected_version\":{}}}", opts.expected_version),
                format!(
                    "credential: Console session cookie from {} (value not shown); no platform key",
                    opts.session_file.display()
                ),
            ],
        }));
    }
    let client = OwnerApiClient::new(&api_url, cookie)?;
    let agent = client.find_agent(&opts.agent).await?;
    let change = client
        .act_memory_rule(&agent.id, &rule_id, action, opts.expected_version)
        .await?;
    Ok(MemoryRulesOutput::Changed {
        verb: opts.verb,
        change,
    })
}

/// Rule text and labels come from pull requests; show no control characters.
fn plain(text: &str) -> String {
    text.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

fn field<'a>(rule: &'a Value, key: &str) -> &'a str {
    rule.get(key).and_then(Value::as_str).unwrap_or("")
}

fn rule_line(rule: &Value) -> String {
    format!(
        "{}  {}  v{}  {}  {}",
        plain(field(rule, "label")),
        plain(field(rule, "state")),
        rule.get("version").and_then(Value::as_i64).unwrap_or(0),
        plain(field(rule, "scope_label")),
        plain(field(rule, "text")),
    )
}

fn render_rules(ui: &Ui, rules: &Value) {
    let list = rules
        .get("rules")
        .and_then(Value::as_array)
        .map_or(&[][..], Vec::as_slice);
    if list.is_empty() {
        ui.payload("no learned rules");
        return;
    }
    ui.payload(&format!("{} learned rules:", list.len()));
    for rule in list {
        ui.payload_plain(&rule_line(rule));
    }
}

fn render_change(ui: &Ui, verb: MemoryRuleVerb, change: &Value) {
    let rule = change.get("rule").unwrap_or(&Value::Null);
    ui.payload(&format!(
        "{} rule {} ({}); applies from the next ticket",
        verb.past_tense(),
        plain(field(rule, "label")),
        plain(field(rule, "scope_label")),
    ));
}
