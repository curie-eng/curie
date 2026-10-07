//! Hook fires, schedules, and work items for a deployed agent.

use super::*;

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

/// Output of `<tier> schedules`. `List` is the API body, not a reshaped copy.
pub enum SchedulesOutput {
    DryRun(crate::ui::DryRunPlan),
    List(crate::api::ScheduleList),
    Control(crate::api::ScheduleControl),
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
                if list.schedules.is_empty() {
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
                            format!("{}#{}", item.repo_full_name, item.github_issue_number),
                            item.state.clone(),
                            work_item_pr_cell(item),
                            item.actionable_cause.clone(),
                        ]
                    })
                    .collect();
                // The last column is never padded: a long cause must not
                // trail every row with spaces out to the widest cause.
                let table = crate::ui::table(&["ID", "ISSUE", "STATE", "PR", "CAUSE"], &rows, &[]);
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
                    &format!("{}#{}", item.repo_full_name, item.github_issue_number),
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
