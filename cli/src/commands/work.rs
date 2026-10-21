//! Hook fires, schedules, and work items for a deployed agent.

use super::*;
use crate::api_requests::{
    AgentScheduleCancel, AgentScheduleRevert, AgentScheduleUpdate, AgentUpdate,
};

/// The prompt of one cron hook declared in `.claude-plugin/plugin.json`.
pub fn hook_prompt(plugin_dir: &Path, name: &str) -> Result<String> {
    let path = plugin_dir.join(".claude-plugin/plugin.json");
    let raw =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let value: serde_json::Value = serde_json::from_str(&raw).context("parsing plugin.json")?;
    let triggers = value
        .get("triggers")
        .and_then(|item| item.as_array())
        .context("plugin.json has no triggers array")?;
    for trigger in triggers {
        if trigger.get("type").and_then(|item| item.as_str()) != Some("cron") {
            continue;
        }
        if trigger.get("name").and_then(|item| item.as_str()) != Some(name) {
            continue;
        }
        let prompt = trigger
            .get("prompt")
            .and_then(|item| item.as_str())
            .map(str::trim)
            .filter(|prompt| !prompt.is_empty())
            .context("cron hook has no prompt")?;
        return Ok(prompt.to_string());
    }
    bail!("no cron hook named {name}")
}

/// `skill hook fire`: run the named hook against the local runner. No run row.
pub async fn skill_hook_fire(
    plugin_dir: &Path,
    name: &str,
    url: Option<String>,
) -> Result<SkillMessageOutput> {
    let prompt = hook_prompt(plugin_dir, name)?;
    send(&prompt, "hook", SendType::Job.into(), url, false).await
}

/// Inputs for `<tier> hook fire`.
pub struct HookFireOpts {
    pub api_url: String,
    pub api_key: String,
    pub agent: String,
    pub name: String,
    pub dry_run: bool,
    pub wait_secs: u64,
    pub tier: &'static str,
}

/// Inputs for `<tier> hook record`.
pub struct HookRecordOpts {
    pub api_url: String,
    pub api_key: String,
    pub agent: String,
    pub name: String,
    pub run_id: String,
    pub dry_run: bool,
}

/// Output of `<tier> hook fire` and `<tier> hook record`.
pub enum HookFireOutput {
    DryRun(crate::ui::DryRunPlan),
    Record(Box<crate::api::HookFireRecord>),
}

impl crate::ui::CliOutput for HookFireOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            HookFireOutput::DryRun(plan) => plan.to_json(),
            HookFireOutput::Record(record) => serde_json::to_value(record).unwrap_or_default(),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            HookFireOutput::DryRun(plan) => plan.render(ui),
            HookFireOutput::Record(record) => {
                let outcome = record.outcome.as_deref().unwrap_or("-");
                let reason = record.reason.as_deref().unwrap_or("");
                if reason.is_empty() {
                    ui.payload(&format!(
                        "{} {} {} {} {}",
                        record.agent, record.name, record.slot_utc, outcome, record.id
                    ));
                } else {
                    ui.payload(&format!(
                        "{} {} {} {} {} {}",
                        record.agent, record.name, record.slot_utc, outcome, reason, record.id
                    ));
                }
            }
        }
    }
}

pub(super) fn hook_fire_path(agent: &str, name: &str) -> String {
    // Delegate to the same builder `fire_hook` encodes its request with, so the
    // dry-run plan cannot drift from the path the real request uses (#3731).
    crate::api::hook_fire_path(agent, name)
}

/// `<tier> hook fire`: run the hook now and print the record once it settles.
pub async fn hook_fire(opts: HookFireOpts) -> Result<HookFireOutput> {
    let path = hook_fire_path(&opts.agent, &opts.name);
    if opts.dry_run {
        return Ok(HookFireOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!("POST {}{path}", opts.api_url.trim_end_matches('/'))],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let mut record = client.fire_hook(&opts.agent, &opts.name).await?;
    let deadline = Instant::now() + Duration::from_secs(opts.wait_secs);
    while matches!(record.outcome.as_deref(), None | Some("deferred")) {
        if Instant::now() >= deadline {
            return Err(crate::exit::transient(format!(
                "hook {} did not settle within {}s; inspect with `curie {} hook record {} {} {}`",
                opts.name, opts.wait_secs, opts.tier, opts.agent, opts.name, record.id
            )));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        record = client
            .get_hook_run(&opts.agent, &opts.name, &record.id)
            .await?;
    }
    if record.outcome.as_deref() != Some("ran") {
        let mut message = format!(
            "hook {} ran and recorded {}",
            record.name,
            record.outcome.as_deref().unwrap_or("-")
        );
        if let Some(reason) = record.reason.as_deref().filter(|reason| !reason.is_empty()) {
            message.push_str(&format!(": {reason}"));
        }
        let output = HookFireOutput::Record(Box::new(record));
        return Err(
            crate::ui::ui().failed_report(&output, crate::exit::CliError::failure(message).into())
        );
    }
    Ok(HookFireOutput::Record(Box::new(record)))
}

/// `<tier> hook record`: read and print a durable run without waiting for settlement.
pub async fn hook_record(opts: HookRecordOpts) -> Result<HookFireOutput> {
    if opts.dry_run {
        let path = crate::api::hook_run_path(&opts.agent, &opts.name, &opts.run_id);
        return Ok(HookFireOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!("GET {}{path}", opts.api_url.trim_end_matches('/'))],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let record = client
        .get_hook_run(&opts.agent, &opts.name, &opts.run_id)
        .await?;
    Ok(HookFireOutput::Record(Box::new(record)))
}

/// Inputs for `<tier> schedules [--agent NAME_OR_ID]`.
pub struct SchedulesOpts {
    pub api_url: String,
    pub api_key: String,
    pub agent: Option<String>,
    pub pause: Option<String>,
    pub resume: Option<String>,
    pub dry_run: bool,
}

/// Inputs for an agent-schedule verb of `<tier> schedules --agent X` (ADR 0238).
pub struct AgentScheduleOpts {
    pub api_url: String,
    pub api_key: String,
    pub agent: Option<String>,
    pub verb: AgentScheduleVerb,
    pub dry_run: bool,
}

/// Output of an agent-schedule verb: the API body as sent (one schedule, or
/// `{"revisions": [..]}`), or `{"agent", "policy"}` for a policy setter.
pub struct AgentScheduleOutput {
    pub body: serde_json::Value,
}

impl crate::ui::CliOutput for AgentScheduleOutput {
    fn to_json(&self) -> serde_json::Value {
        self.body.clone()
    }

    fn render(&self, ui: &crate::ui::Ui) {
        if let Some(items) = self
            .body
            .get("revisions")
            .and_then(serde_json::Value::as_array)
        {
            for item in items {
                ui.payload(&format!(
                    "{} {}",
                    item.get("revision")
                        .map_or_else(|| "-".to_string(), ToString::to_string),
                    agent_schedule_line(item)
                ));
            }
        } else if let Some(policy) = self.body.get("policy") {
            ui.payload(&format!(
                "{} schedule policy: {policy}",
                self.body["agent"].as_str().unwrap_or("-")
            ));
        } else {
            ui.payload(&agent_schedule_line(&self.body));
        }
    }
}

/// One agent-schedule verb of `<tier> schedules --agent X` (ADR 0238). The
/// schedule verbs hit `/agents/{id}/schedules/..`; the `Set*` verbs patch the
/// agent's schedule policy columns.
#[derive(Debug, Clone)]
pub enum AgentScheduleVerb {
    Show(String),
    Cancel(String),
    History(String),
    Update {
        id: String,
        prompt: Option<String>,
        cron: Option<String>,
        run_at: Option<String>,
        timezone: Option<String>,
        expected_revision: u32,
    },
    Revert {
        id: String,
        to_revision: Option<u32>,
        expected_revision: u32,
    },
    SetTimezone(String),
    SetMaxActive(u32),
    SetMinIntervalMinutes(u32),
}

/// What a verb sends, with its typed body.
enum AgentScheduleRequest {
    Show(String),
    History(String),
    Cancel(String, AgentScheduleCancel),
    Update(String, AgentScheduleUpdate),
    Revert(String, AgentScheduleRevert),
    Policy(Box<AgentUpdate>),
}

impl AgentScheduleRequest {
    /// Method, path after the agent id, and JSON body of the planned request.
    fn plan(&self) -> Result<(&'static str, Vec<String>, Option<serde_json::Value>)> {
        let route = |id: &String, more: Option<&str>| {
            let mut tail = vec!["schedules".to_string(), id.clone()];
            tail.extend(more.map(str::to_string));
            tail
        };
        Ok(match self {
            AgentScheduleRequest::Show(id) => ("GET", route(id, None), None),
            AgentScheduleRequest::History(id) => ("GET", route(id, Some("revisions")), None),
            AgentScheduleRequest::Cancel(id, body) => (
                "POST",
                route(id, Some("cancel")),
                Some(serde_json::to_value(body)?),
            ),
            AgentScheduleRequest::Update(id, body) => {
                ("PATCH", route(id, None), Some(serde_json::to_value(body)?))
            }
            AgentScheduleRequest::Revert(id, body) => (
                "POST",
                route(id, Some("revert")),
                Some(serde_json::to_value(body)?),
            ),
            AgentScheduleRequest::Policy(body) => {
                ("PATCH", Vec::new(), Some(serde_json::to_value(body)?))
            }
        })
    }
}

/// A schedule id is a UUID; refused locally before it can shape a URL path.
fn schedule_id(id: &str) -> Result<String> {
    match uuid::Uuid::parse_str(id) {
        Ok(_) => Ok(id.to_string()),
        Err(_) => Err(crate::exit::usage(format!(
            "schedule ID must be a UUID, got {id:?}"
        ))),
    }
}

impl AgentScheduleVerb {
    fn request(&self) -> Result<AgentScheduleRequest> {
        let policy = |update: AgentUpdate| AgentScheduleRequest::Policy(Box::new(update));
        Ok(match self {
            AgentScheduleVerb::Show(id) => AgentScheduleRequest::Show(schedule_id(id)?),
            AgentScheduleVerb::History(id) => AgentScheduleRequest::History(schedule_id(id)?),
            AgentScheduleVerb::Cancel(id) => AgentScheduleRequest::Cancel(
                schedule_id(id)?,
                AgentScheduleCancel {
                    expected_revision: None,
                },
            ),
            AgentScheduleVerb::Update {
                id,
                prompt,
                cron,
                run_at,
                timezone,
                expected_revision,
            } => {
                if prompt.is_none() && cron.is_none() && run_at.is_none() && timezone.is_none() {
                    return Err(crate::exit::usage(
                        "--update needs at least one of --prompt, --cron, --run-at, --timezone",
                    ));
                }
                AgentScheduleRequest::Update(
                    schedule_id(id)?,
                    AgentScheduleUpdate {
                        expected_revision: *expected_revision,
                        prompt: prompt.clone(),
                        cron: cron.clone(),
                        run_at: run_at.clone(),
                        timezone: timezone.clone(),
                    },
                )
            }
            AgentScheduleVerb::Revert {
                id,
                to_revision,
                expected_revision,
            } => AgentScheduleRequest::Revert(
                schedule_id(id)?,
                AgentScheduleRevert {
                    expected_revision: *expected_revision,
                    to_revision: *to_revision,
                },
            ),
            AgentScheduleVerb::SetTimezone(zone) => policy(AgentUpdate {
                schedule_timezone: Some(zone.clone()),
                ..Default::default()
            }),
            AgentScheduleVerb::SetMaxActive(max) => policy(AgentUpdate {
                schedule_max_active: Some(*max),
                ..Default::default()
            }),
            AgentScheduleVerb::SetMinIntervalMinutes(minutes) => policy(AgentUpdate {
                schedule_min_interval_minutes: Some(*minutes),
                ..Default::default()
            }),
        })
    }
}

/// Output of `<tier> schedules`. `List` is the API body, not a reshaped copy.
pub enum SchedulesOutput {
    DryRun(crate::ui::DryRunPlan),
    List(crate::api::ScheduleList),
    Control(crate::api::ScheduleControl),
}

/// One text line for an agent schedule JSON object.
fn agent_schedule_line(item: &serde_json::Value) -> String {
    let text = |key: &str| {
        item.get(key)
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-")
            .to_string()
    };
    let when = match item.get("cron").and_then(serde_json::Value::as_str) {
        Some(cron) => cron.to_string(),
        None => text("run_at"),
    };
    let next = item
        .get("next_run_local")
        .and_then(serde_json::Value::as_str)
        .map_or_else(|| text("next_run_at"), str::to_string);
    format!(
        "{} {} {} {} {} {}",
        text("id"),
        text("state"),
        when,
        text("timezone"),
        next,
        text("created_by")
    )
}

impl crate::ui::CliOutput for SchedulesOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            SchedulesOutput::DryRun(plan) => plan.to_json(),
            SchedulesOutput::List(list) => serde_json::to_value(list).unwrap_or_default(),
            SchedulesOutput::Control(control) => serde_json::to_value(control).unwrap_or_default(),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            SchedulesOutput::DryRun(plan) => plan.render(ui),
            SchedulesOutput::Control(control) => ui.payload(&format!(
                "{} {} {}",
                control.agent,
                control.name,
                if control.paused { "paused" } else { "resumed" }
            )),
            SchedulesOutput::List(list) => {
                if list.schedules.is_empty() && list.agent_schedules.is_empty() {
                    ui.payload("no schedules");
                    return;
                }
                for agent in &list.schedules {
                    ui.payload(&agent.agent);
                    if let Some(error) = &agent.bundle_error {
                        ui.payload(error);
                    }
                    for hook in &agent.hooks {
                        let fire = hook.last_fire_at.as_deref().unwrap_or("-");
                        let outcome = hook.last_outcome.as_deref().unwrap_or("-");
                        let reason = hook.last_reason.as_deref().unwrap_or("");
                        let manual_fire = hook.last_manual_fire_at.as_deref().unwrap_or("-");
                        let manual_outcome = hook.last_manual_outcome.as_deref().unwrap_or("-");
                        if reason.is_empty() {
                            ui.payload(&format!(
                                "{} {} {} {} {} {} {} {} {}",
                                hook.name,
                                hook.trigger,
                                hook.schedule,
                                hook.zone,
                                fire,
                                outcome,
                                if hook.paused { "paused" } else { "active" },
                                manual_fire,
                                manual_outcome
                            ));
                        } else {
                            ui.payload(&format!(
                                "{} {} {} {} {} {} {} {} {} {}",
                                hook.name,
                                hook.trigger,
                                hook.schedule,
                                hook.zone,
                                fire,
                                outcome,
                                reason,
                                if hook.paused { "paused" } else { "active" },
                                manual_fire,
                                manual_outcome
                            ));
                        }
                    }
                }
                for item in &list.agent_schedules {
                    ui.payload(&agent_schedule_line(item));
                }
            }
        }
    }
}

/// `<tier> schedules`: list cron hooks and their scheduled and manual history (`GET /schedules`).
pub async fn schedules(opts: SchedulesOpts) -> Result<SchedulesOutput> {
    let action = match (opts.pause.as_deref(), opts.resume.as_deref()) {
        (Some(name), None) => Some((name, true)),
        (None, Some(name)) => Some((name, false)),
        (None, None) => None,
        (Some(_), Some(_)) => return Err(crate::exit::usage("choose pause or resume")),
    };
    if action.is_some() && opts.agent.is_none() {
        return Err(crate::exit::usage("schedule control requires --agent"));
    }
    let agent_query = opts
        .agent
        .as_ref()
        .map(|agent| format!("?agent={agent}"))
        .unwrap_or_default();
    if opts.dry_run {
        let line = if let Some((name, pause)) = action {
            format!(
                "POST {}/schedules/{}/{}/{}",
                opts.api_url.trim_end_matches('/'),
                opts.agent.as_deref().unwrap_or_default(),
                name,
                if pause { "pause" } else { "resume" }
            )
        } else {
            format!(
                "GET {}/schedules{agent_query}",
                opts.api_url.trim_end_matches('/')
            )
        };
        return Ok(SchedulesOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![line],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    if let Some((name, pause)) = action {
        return Ok(SchedulesOutput::Control(
            client
                .control_schedule(opts.agent.as_deref().unwrap_or_default(), name, pause)
                .await?,
        ));
    }
    Ok(SchedulesOutput::List(
        client.list_schedules(opts.agent.as_deref()).await?,
    ))
}

/// Run (or plan) one agent-schedule verb. The real run resolves `--agent` to
/// the agent id first; the dry run shows the agent as given.
pub async fn agent_schedule(opts: AgentScheduleOpts) -> Result<Box<dyn crate::ui::CliOutput>> {
    let verb = &opts.verb;
    let agent = opts
        .agent
        .as_deref()
        .ok_or_else(|| crate::exit::usage("agent schedule verbs require --agent"))?;
    let request = verb.request()?;
    let base = opts.api_url.trim_end_matches('/');
    if opts.dry_run {
        let (method, tail, body) = request.plan()?;
        let tail: Vec<&str> = tail.iter().map(String::as_str).collect();
        let mut line = format!(
            "{method} {base}{}",
            crate::api::agent_schedule_path(agent, &tail)
        );
        if let Some(body) = body {
            line.push(' ');
            line.push_str(&body.to_string());
        }
        return Ok(Box::new(SchedulesOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![line],
        })));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let found = client.find_agent(agent).await?;
    let body = match request {
        AgentScheduleRequest::Show(id) => client.get_agent_schedule(&found.id, &id).await?,
        AgentScheduleRequest::History(id) => {
            serde_json::to_value(client.get_agent_schedule_revisions(&found.id, &id).await?)?
        }
        AgentScheduleRequest::Cancel(id, body) => {
            client.cancel_agent_schedule(&found.id, &id, &body).await?
        }
        AgentScheduleRequest::Update(id, body) => {
            client.update_agent_schedule(&found.id, &id, &body).await?
        }
        AgentScheduleRequest::Revert(id, body) => {
            client.revert_agent_schedule(&found.id, &id, &body).await?
        }
        AgentScheduleRequest::Policy(update) => {
            // The stored settings from the PATCH response, not the request.
            let saved = client.update_agent(&found.id, &update).await?;
            serde_json::json!({
                "agent": saved.name,
                "policy": saved.schedule_policy(),
            })
        }
    };
    Ok(Box::new(AgentScheduleOutput { body }))
}

/// A work item ID must be a UUID. Refused locally as a usage error (exit 2)
/// before any request is made, through the centralized error path so `--json`
/// still gets the structured `{error, ...}` payload.
pub fn validated_work_item_id(id: Option<String>) -> Result<Option<String>> {
    match id {
        None => Ok(None),
        Some(raw) => match uuid::Uuid::parse_str(&raw) {
            Ok(_) => Ok(Some(raw)),
            Err(_) => Err(crate::exit::usage(format!(
                "work item ID must be a UUID, got {raw:?}"
            ))),
        },
    }
}

/// Inputs for `<tier> work-items [ID] [--agent NAME_OR_ID]` (#2577).
pub struct WorkItemsOpts {
    pub api_url: String,
    pub api_key: String,
    pub id: Option<String>,
    pub agent: Option<String>,
    pub dry_run: bool,
    /// "local" or "cluster", for the transient-error remediation hint.
    pub tier: &'static str,
}

/// Output of `<tier> work-items`. The list and detail carry the API payload
/// through typed mirrors under a stable envelope; `state` and
/// `actionable_cause` are the API's strings, never recomputed here.
pub enum WorkItemsOutput {
    List {
        list: crate::api::WorkItemList,
    },
    Detail {
        item: Box<crate::api::WorkItemOutcome>,
    },
    DryRun(crate::ui::DryRunPlan),
}

/// How an operator names the issue: the tracker's display key (a Jira key)
/// when it has one, else `<repository path>#<issue id>`.
pub(super) fn work_item_issue_label(item: &crate::api::WorkItemOutcome) -> String {
    item.tracker
        .display_key
        .clone()
        .unwrap_or_else(|| format!("{}#{}", item.repository.path, item.tracker.issue_id))
}

pub(super) fn work_item_pr_cell(item: &crate::api::WorkItemOutcome) -> String {
    item.pr
        .as_ref()
        .map(|pr| format!("#{} {}", pr.number, pr.status))
        .unwrap_or_else(|| "-".to_string())
}

impl crate::ui::CliOutput for WorkItemsOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            WorkItemsOutput::List { list } => serde_json::to_value(list).unwrap_or_default(),
            WorkItemsOutput::Detail { item } => serde_json::json!({ "item": item }),
            WorkItemsOutput::DryRun(plan) => plan.to_json(),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            WorkItemsOutput::DryRun(plan) => plan.render(ui),
            WorkItemsOutput::List { list } => {
                if list.items.is_empty() {
                    ui.payload("no work items");
                    return;
                }
                let rows: Vec<Vec<String>> = list
                    .items
                    .iter()
                    .map(|item| {
                        vec![
                            item.id.clone(),
                            work_item_issue_label(item),
                            item.state.clone(),
                            work_item_pr_cell(item),
                            item.tracker.url.clone(),
                            item.actionable_cause.clone(),
                        ]
                    })
                    .collect();
                // The last column is never padded: a long cause must not
                // trail every row with spaces out to the widest cause.
                let table =
                    crate::ui::table(&["ID", "ISSUE", "STATE", "PR", "LINK", "CAUSE"], &rows, &[]);
                let trimmed: Vec<&str> = table.lines().map(str::trim_end).collect();
                ui.payload_plain(&trimmed.join("\n"));
                if list.truncated {
                    ui.payload_plain(&format!(
                        "(showing the first {} work items; more exist)",
                        list.limit
                    ));
                }
            }
            WorkItemsOutput::Detail { item } => {
                let line = |key: &str, value: &str| ui.payload_plain(&format!("{key:<12} {value}"));
                line("id", &item.id);
                line(
                    "issue",
                    &format!("{} {}", work_item_issue_label(item), item.tracker.url),
                );
                line("state", &item.state);
                line("cause", &item.actionable_cause);
                line(
                    "pr",
                    &item
                        .pr
                        .as_ref()
                        .map(|pr| format!("#{} {} {}", pr.number, pr.status, pr.url))
                        .unwrap_or_else(|| "-".into()),
                );
                if let Some(publication) = &item.publication {
                    line(
                        "publication",
                        &format!(
                            "{} (approval {})",
                            publication.status,
                            publication.approval_status.as_deref().unwrap_or("-")
                        ),
                    );
                }
                match &item.ci {
                    Some(ci) => line(
                        "ci",
                        &match ci.reason.as_deref() {
                            Some(reason) => format!("{} ({reason})", ci.state),
                            None => ci.state.clone(),
                        },
                    ),
                    None => line("ci", "-"),
                }
                line(
                    "correctness",
                    "not asserted by the platform (owned by the bundle)",
                );
                if let Some(objective) = &item.objective {
                    line("objective", objective);
                }
                for request in &item.requests {
                    line(
                        "request",
                        &format!(
                            "#{} {} {}",
                            request.sequence,
                            request.status,
                            request.terminal_cause.as_deref().unwrap_or("")
                        ),
                    );
                }
            }
        }
    }
}

/// `<tier> work-items [ID]`: list or read factory work item outcomes
/// (`GET /work-items`, `GET /work-items/{id}`, #2577). An `--agent` that
/// matches no agent is a failure (exit 1) and never falls back to the
/// unfiltered list.
pub async fn work_items(opts: WorkItemsOpts) -> Result<WorkItemsOutput> {
    if opts.dry_run {
        let path = match &opts.id {
            Some(id) => format!("GET {}/work-items/{id}", opts.api_url),
            None => format!(
                "GET {}/work-items?limit={}",
                opts.api_url,
                ApiClient::WORK_ITEMS_LIST_LIMIT
            ),
        };
        let mut lines = vec![path];
        if let Some(agent) = &opts.agent {
            lines.push(format!("(would resolve agent {agent:?} to its id first)"));
        }
        return Ok(WorkItemsOutput::DryRun(crate::ui::DryRunPlan { lines }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent_id = match &opts.agent {
        // The observability-classified lookup (#1948): a 5xx/unreachable/hang
        // on `GET /agents` must exit 3 (transient) and never fall through to
        // an unfiltered `/work-items` call (#2577).
        Some(agent) => Some(
            client
                .find_agent_observability(agent)
                .await
                .map_err(|error| crate::observability::classify_api_error(error, opts.tier))?
                .id,
        ),
        None => None,
    };
    match opts.id {
        Some(id) => Ok(WorkItemsOutput::Detail {
            item: Box::new(client.get_work_item(&id, agent_id.as_deref()).await?),
        }),
        None => Ok(WorkItemsOutput::List {
            list: client
                .list_work_items(agent_id.as_deref(), ApiClient::WORK_ITEMS_LIST_LIMIT)
                .await?,
        }),
    }
}
