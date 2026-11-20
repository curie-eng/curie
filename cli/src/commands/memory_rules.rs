//! `curie cluster memory --rules | --accept | --reject | --remove`: list an
//! agent's learned rules, and accept, reject or remove one as the signed-in
//! person. Listing uses the platform key, or the session file when one is given. An
//! action reads a private session file and never the platform key.

use std::path::PathBuf;

use anyhow::Result;
use serde_json::Value;

use super::AgentActionOpts;
use crate::api::{
    ApiClient, MemoryRule, MemoryRuleAppliesFrom, MemoryRuleChange, MemoryRuleState, MemoryRules,
    OwnerApiClient,
};
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

/// Output of the learned rule verbs: the API's validated objects, or a plan.
#[derive(Debug)]
pub enum MemoryRulesOutput {
    DryRun(DryRunPlan),
    /// `--rules`: the API's rule list.
    Rules(MemoryRules),
    /// `--accept`, `--reject` or `--remove`: the API's changed rule.
    Changed {
        verb: MemoryRuleVerb,
        change: Box<MemoryRuleChange>,
    },
}

impl CliOutput for MemoryRulesOutput {
    fn to_json(&self) -> Value {
        match self {
            Self::DryRun(plan) => plan.to_json(),
            Self::Rules(rules) => serde_json::to_value(rules).unwrap_or_default(),
            Self::Changed { change, .. } => serde_json::to_value(change).unwrap_or_default(),
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
    /// A scope the rule must be in; empty means no check and no extra request.
    pub scope: RuleScopeFilter,
    pub api_url: Option<String>,
    pub namespace: String,
    pub release: String,
    pub dry_run: bool,
}

fn rule_id_usage() -> anyhow::Error {
    let fix = "copy the rule id (a 36-character UUID, not the R label) from `curie cluster memory <agent> --rules`";
    CliError::usage(format!("the rule id is not a valid id; {fix}"))
        .with_fix(fix)
        .into()
}

/// A rule id goes into a URL path, so only a canonical hyphenated UUID passes.
/// It is sent lowercased.
fn checked_rule_id(rule_id: &str) -> Result<String> {
    match uuid::Uuid::parse_str(rule_id) {
        Ok(id) if rule_id.len() == 36 => Ok(id.hyphenated().to_string()),
        _ => Err(rule_id_usage()),
    }
}

/// Which scope a `--rules` list is narrowed to: the API's `scope_kind` and the
/// exact `scope_key` (`agent` has the empty key, a bound channel is
/// `<kind>:<address>`, the string the API's binding claim carries).
#[derive(Debug, Clone, Default)]
pub struct RuleScopeFilter {
    pub kind: Option<String>,
    pub key: Option<String>,
}

impl RuleScopeFilter {
    /// Whether any scope flag narrowed the filter.
    pub fn is_set(&self) -> bool {
        self.kind.is_some() || self.key.is_some()
    }

    /// Resolve the `--channel`, `--agent-scope` and `--scope-key` flags.
    pub fn resolve(
        channel: Option<String>,
        agent_scope: bool,
        scope_key: Option<String>,
    ) -> Result<Self> {
        if agent_scope {
            return Ok(Self {
                kind: Some("agent".to_string()),
                key: Some(String::new()),
            });
        }
        if let Some(channel) = super::memory_channel(channel)? {
            return Ok(Self {
                kind: Some("binding".to_string()),
                key: Some(format!("{}:{}", channel.kind, channel.address)),
            });
        }
        Ok(Self {
            kind: None,
            key: scope_key,
        })
    }
}

fn query_string(state: &Option<String>, scope: &RuleScopeFilter) -> String {
    let mut query = Vec::new();
    if let Some(state) = state {
        query.push(format!("state={state}"));
    }
    if let Some(kind) = &scope.kind {
        query.push(format!("scope_kind={kind}"));
    }
    let scope_key = &scope.key;
    if let Some(scope_key) = scope_key {
        query.push(format!("scope_key={scope_key}"));
    }
    if query.is_empty() {
        String::new()
    } else {
        format!("?{}", query.join("&"))
    }
}

/// Options for listing with a session file instead of the platform key.
pub struct MemoryRuleListSessionOpts {
    pub agent: String,
    pub state: Option<String>,
    pub scope: RuleScopeFilter,
    pub session_file: PathBuf,
    pub api_url: Option<String>,
    pub namespace: String,
    pub release: String,
    pub dry_run: bool,
}

/// `memory --rules --session-file`: list rules as the signed-in person.
pub async fn memory_rules_list_session(
    opts: MemoryRuleListSessionOpts,
) -> Result<MemoryRulesOutput> {
    let cookie = super::subscription::read_session_cookie(&opts.session_file)?;
    let (api_url, _tunnel) = super::resolve_cluster_api(
        opts.api_url.clone(),
        &opts.namespace,
        &opts.release,
        opts.dry_run,
    )
    .await?;
    if opts.dry_run {
        let base = if opts.api_url.is_none() {
            "http://127.0.0.1:<tunnel port>".to_string()
        } else {
            api_url.trim_end_matches('/').to_string()
        };
        return Ok(MemoryRulesOutput::DryRun(DryRunPlan {
            lines: vec![
                format!(
                    "GET {base}/agents/<id>/memory/rules{}  (would resolve agent {:?} first)",
                    query_string(&opts.state, &opts.scope),
                    opts.agent
                ),
                format!(
                    "credential: Console session cookie from {} (value not shown); no platform key",
                    opts.session_file.display()
                ),
            ],
        }));
    }
    let client = OwnerApiClient::new(&api_url, cookie)?;
    let agent = client.find_agent(&opts.agent).await?;
    let rules = client
        .list_memory_rules(
            &agent.id,
            opts.state.as_deref(),
            opts.scope.kind.as_deref(),
            opts.scope.key.as_deref(),
        )
        .await?;
    Ok(MemoryRulesOutput::Rules(rules))
}

/// `memory --rules`: list rules through the cluster connection.
pub async fn memory_rules_list(
    opts: AgentActionOpts,
    state: Option<String>,
    scope: RuleScopeFilter,
) -> Result<MemoryRulesOutput> {
    if opts.dry_run {
        let query = query_string(&state, &scope);
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
        .list_memory_rules(
            &agent.id,
            state.as_deref(),
            scope.kind.as_deref(),
            scope.key.as_deref(),
        )
        .await?;
    Ok(MemoryRulesOutput::Rules(rules))
}

/// `memory --accept | --reject | --remove`: change one rule as the person whose
/// session file is given.
pub async fn memory_rule_act(opts: MemoryRuleActOpts) -> Result<MemoryRulesOutput> {
    let rule_id = checked_rule_id(&opts.rule_id)?;
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
        let mut lines = Vec::new();
        if opts.scope.is_set() {
            lines.push(format!(
                "GET {base}/agents/<id>/memory/rules{}  (scope check: refuse unless the rule is listed)",
                query_string(&None, &opts.scope)
            ));
        }
        lines.extend([
                format!(
                    "POST {base}/agents/<id>/memory/rules/{rule_id}/{action}  (would resolve agent {:?} first)",
                    opts.agent
                ),
                format!("body: {{\"expected_version\":{}}}", opts.expected_version),
                format!(
                    "credential: Console session cookie from {} (value not shown); no platform key",
                    opts.session_file.display()
                ),
        ]);
        return Ok(MemoryRulesOutput::DryRun(DryRunPlan { lines }));
    }
    let client = OwnerApiClient::new(&api_url, cookie)?;
    let agent = client.find_agent(&opts.agent).await?;
    if opts.scope.is_set() {
        let listed = client
            .list_memory_rules(
                &agent.id,
                None,
                opts.scope.kind.as_deref(),
                opts.scope.key.as_deref(),
            )
            .await?;
        if !listed
            .rules
            .iter()
            .any(|r| r.id.hyphenated().to_string() == rule_id)
        {
            let fix = "list that scope with `curie cluster memory <agent> --rules` and copy the rule id from it";
            return Err(CliError::usage(format!(
                "rule {rule_id} is not in the requested scope; no decision was sent"
            ))
            .with_fix(fix)
            .into());
        }
    }
    let body = crate::api_requests::MemoryRuleDecision {
        expected_version: opts.expected_version,
    };
    let change = match opts.verb {
        MemoryRuleVerb::Accept => client.accept_memory_rule(&agent.id, &rule_id, &body).await,
        MemoryRuleVerb::Reject => client.reject_memory_rule(&agent.id, &rule_id, &body).await,
        MemoryRuleVerb::Remove => client.remove_memory_rule(&agent.id, &rule_id, &body).await,
    }?;
    Ok(MemoryRulesOutput::Changed {
        verb: opts.verb,
        change: Box::new(change),
    })
}

/// Rule text and labels come from pull requests; show no control characters.
fn plain(text: &str) -> String {
    text.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

fn state_name(state: MemoryRuleState) -> &'static str {
    match state {
        MemoryRuleState::Proposed => "proposed",
        MemoryRuleState::Accepted => "accepted",
        MemoryRuleState::Rejected => "rejected",
        MemoryRuleState::Removed => "removed",
    }
}

fn rule_line(rule: &MemoryRule) -> String {
    format!(
        "{}  {}  {}  v{}  {}  {}",
        plain(&rule.label),
        rule.id,
        state_name(rule.state),
        rule.version,
        plain(&rule.scope_label),
        plain(&rule.text),
    )
}

fn render_rules(ui: &Ui, rules: &MemoryRules) {
    let list = &rules.rules;
    if list.is_empty() {
        ui.payload("no learned rules");
        return;
    }
    ui.payload(&format!("{} learned rules:", list.len()));
    for rule in list {
        ui.payload_plain(&rule_line(rule));
    }
}

fn render_change(ui: &Ui, verb: MemoryRuleVerb, change: &MemoryRuleChange) {
    let rule = &change.rule;
    let when = match change.applies_from {
        MemoryRuleAppliesFrom::NextTicket => "applies from the next ticket",
        MemoryRuleAppliesFrom::NextSession => {
            "takes effect at the agent's next session in that channel; a running session keeps the rules it started with"
        }
    };
    ui.payload(&format!(
        "{} rule {} ({}); {when}",
        verb.past_tense(),
        plain(&rule.label),
        plain(&rule.scope_label),
    ));
}
