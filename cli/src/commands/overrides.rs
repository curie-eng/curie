//! `<tier> overrides` and `<tier> publication-policy`: per-agent operator settings.

use super::*;
use crate::api_requests::AgentUpdate;

/// Which nullable override a `<tier> overrides` invocation intends to change.
///
/// Three states, because the API's PATCH semantics have three (#1310): omitted
/// leaves the stored value alone, an explicit JSON null clears it back to the
/// platform default, and a string pins it. A plain `Option<String>` can only
/// express two of those, and collapsing "clear" onto the empty string is the
/// exact defect #1355 fixed on the console -- an empty override reaches the
/// worker falsy, emits no boot key, and skips the platform default that
/// clearing is supposed to restore. So the CLI never sends `""` either; the
/// operator says `--clear-model` and the wire carries `null`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OverrideChange {
    /// Not mentioned on the command line: leave whatever is stored.
    Unchanged,
    /// `--clear-<field>`: send explicit null, restoring the platform default.
    Clear,
    /// `--<field> <value>`: pin this value for this agent.
    Set(String),
}

impl OverrideChange {
    /// Resolve the `--<field>` / `--clear-<field>` pair into one intent.
    ///
    /// Args:
    ///   field: the field name, used only in the usage error.
    ///   value: the `--<field>` value, if the operator passed one.
    ///   clear: whether `--clear-<field>` was passed.
    ///
    /// Returns:
    ///   The intent, or a usage error when both were passed or the value is
    ///   blank.
    pub fn resolve(field: &str, value: Option<String>, clear: bool) -> Result<Self> {
        match (value, clear) {
            (Some(_), true) => Err(crate::exit::usage(format!(
                "--{field} and --clear-{field} contradict each other; pass one. \
                 --clear-{field} restores the platform default"
            ))),
            // Refused here rather than forwarded, so the operator gets the fix
            // from the CLI instead of a 422 from the API. The API refuses it
            // too (#1355), and for the reason the hint states.
            (Some(v), false) if v.trim().is_empty() => Err(crate::exit::usage(format!(
                "--{field} must not be blank: an empty value skips the platform \
                 default instead of restoring it. Pass --clear-{field} to clear \
                 the override, or a real value"
            ))),
            // Trimmed before it becomes an intent (#1392). The API strips too
            // and is the authoritative gate, but doing it here as well is what
            // makes `--dry-run` honest: it must print the body that would
            // actually be sent, not the argv the operator typed.
            (Some(v), false) => Ok(OverrideChange::Set(v.trim().to_string())),
            (None, true) => Ok(OverrideChange::Clear),
            (None, false) => Ok(OverrideChange::Unchanged),
        }
    }

    /// The three-state [`AgentUpdate`] value this intent contributes to a
    /// `PATCH /agents/{id}` body.
    ///
    /// Returns:
    ///   `Some(None)` to clear (the wire carries `null`), `Some(Some(v))` to
    ///   pin, `None` to leave the key out so the API's `model_fields_set` check
    ///   reads it as unchanged.
    fn patch_value(&self) -> Option<Option<String>> {
        match self {
            OverrideChange::Unchanged => None,
            OverrideChange::Clear => Some(None),
            OverrideChange::Set(v) => Some(Some(v.clone())),
        }
    }

    /// Same as [`patch_value`](Self::patch_value), but for `Set` the stored
    /// string is parsed into the integer the wire carries.
    ///
    /// `execution_deadline_seconds` is an int on the wire, unlike `model`/
    /// `thinking`, which are always strings. The value has already been range
    /// checked by [`resolve_execution_deadline`](Self::resolve_execution_deadline),
    /// so the parse here cannot fail for a `Set` this function is meant to see.
    fn patch_value_as_seconds(&self) -> Option<Option<u32>> {
        match self {
            OverrideChange::Unchanged => None,
            OverrideChange::Clear => Some(None),
            OverrideChange::Set(v) => Some(Some(
                v.parse::<u32>()
                    .expect("execution deadline Set must already be a validated integer"),
            )),
        }
    }

    /// Resolve the `--execution-deadline`/`--clear-execution-deadline` pair
    /// into one intent, with the same three-way semantics as
    /// [`resolve`](Self::resolve) plus client-side range validation.
    ///
    /// Args:
    ///   value: the `--execution-deadline` value, if the operator passed one.
    ///   clear: whether `--clear-execution-deadline` was passed.
    ///
    /// Returns:
    ///   The intent, or a usage error when both were passed, the value is not
    ///   an integer, or it falls outside 60..=10800 seconds.
    pub fn resolve_execution_deadline(value: Option<String>, clear: bool) -> Result<Self> {
        const MIN_SECONDS: i64 = 60;
        const MAX_SECONDS: i64 = 10800;
        match (value, clear) {
            (Some(_), true) => Err(crate::exit::usage(
                "--execution-deadline and --clear-execution-deadline contradict each other; \
                 pass one. --clear-execution-deadline restores the platform default"
                    .to_string(),
            )),
            (Some(v), false) => {
                let trimmed = v.trim();
                let seconds: i64 = trimmed.parse().map_err(|_| {
                    crate::exit::usage(format!(
                        "--execution-deadline must be a whole number of seconds \
                         between {MIN_SECONDS} and {MAX_SECONDS}, got {v:?}"
                    ))
                })?;
                if !(MIN_SECONDS..=MAX_SECONDS).contains(&seconds) {
                    return Err(crate::exit::usage(format!(
                        "--execution-deadline must be between {MIN_SECONDS} and \
                         {MAX_SECONDS} seconds, got {seconds}"
                    )));
                }
                Ok(OverrideChange::Set(seconds.to_string()))
            }
            (None, true) => Ok(OverrideChange::Clear),
            (None, false) => Ok(OverrideChange::Unchanged),
        }
    }

    /// Resolve `--runner-resources` / `--clear-runner-resources`.
    ///
    /// A set value is a JSON object, stored as text and emitted as that object.
    /// Blank text and malformed JSON are usage errors and never reach the API.
    pub fn resolve_runner_resources(value: Option<String>, clear: bool) -> Result<Self> {
        match (value, clear) {
            (Some(_), true) => Err(crate::exit::usage(
                "--runner-resources and --clear-runner-resources contradict each other; \
                 pass one. --clear-runner-resources restores the platform default"
                    .to_string(),
            )),
            (Some(raw), false) => {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    return Err(crate::exit::usage(
                        "--runner-resources must not be blank; use --clear-runner-resources \
                         to restore the platform default"
                            .to_string(),
                    ));
                }
                let parsed: serde_json::Value = serde_json::from_str(trimmed).map_err(|_| {
                    crate::exit::usage(format!(
                        "--runner-resources must be a JSON object of requests and limits, got {raw:?}"
                    ))
                })?;
                if !parsed.is_object() {
                    return Err(crate::exit::usage(format!(
                        "--runner-resources must be a JSON object of requests and limits, got {raw:?}"
                    )));
                }
                Ok(OverrideChange::Set(parsed.to_string()))
            }
            (None, true) => Ok(OverrideChange::Clear),
            (None, false) => Ok(OverrideChange::Unchanged),
        }
    }

    /// Same as [`patch_value`](Self::patch_value), but `Set` is a JSON object
    /// rather than a JSON string. Used for `runner_resources`.
    fn patch_value_as_object(&self) -> Option<Option<serde_json::Map<String, serde_json::Value>>> {
        match self {
            OverrideChange::Unchanged => None,
            OverrideChange::Clear => Some(None),
            OverrideChange::Set(v) => Some(Some(
                serde_json::from_str(v)
                    .expect("runner resources Set must already be a JSON object"),
            )),
        }
    }
}

/// The `PATCH /agents/{id}` body for a `<tier> overrides` write, or `None` when
/// nothing was asked for (the inspect path).
///
/// Pure so the three-way semantics are assertable with no server: the property
/// that matters is that an Unchanged field is ABSENT from the body rather than
/// present-and-null, since the API tells those apart with `model_fields_set`
/// and present-and-null is the clear.
///
/// Args:
///   model: the intent for the model override.
///   thinking: the intent for the thinking override.
///
/// Returns:
///   The body to PATCH, or `None` when both intents are `Unchanged`.
pub fn overrides_patch_body(
    model: &OverrideChange,
    thinking: &OverrideChange,
    execution_deadline: &OverrideChange,
    runner_resources: &OverrideChange,
) -> Option<AgentUpdate> {
    let all_unchanged = [model, thinking, execution_deadline, runner_resources]
        .iter()
        .all(|change| **change == OverrideChange::Unchanged);
    if all_unchanged {
        return None;
    }
    Some(AgentUpdate {
        model: model.patch_value(),
        thinking: thinking.patch_value(),
        execution_deadline_seconds: execution_deadline.patch_value_as_seconds(),
        runner_resources: runner_resources.patch_value_as_object(),
        ..AgentUpdate::default()
    })
}

/// The `PATCH /agents/{id}` body exactly as it goes on the wire, for a
/// `--dry-run` plan.
fn patch_body_json(body: &AgentUpdate) -> Result<serde_json::Value> {
    serde_json::to_value(body).context("encoding the PATCH body")
}

/// The one-line human summary of an `overrides` result.
///
/// Pure so the wording is assertable. The spacing defect this replaced (#1394)
/// existed precisely because the line was built inline in `render` and could
/// only be checked by running the command and looking at it: an inspect
/// interpolated an empty verb before the colon and printed
/// `overrides for x : model ...`.
///
/// "platform default" rather than "none": a null here is not an absence, it is
/// a deferral to the platform-level setting, and an operator reading "none"
/// would reasonably expect no model at all.
///
/// Args:
///   agent: the agent's name.
///   model: the stored model override, `None` when the platform default applies.
///   thinking: the stored thinking override, same convention.
///   execution_deadline_seconds: the stored deadline override, same convention.
///   runner_resources: the stored runner resources override, same convention.
///   memory_writes: whether the agent's memory tools are on (#1461).
///   changed: whether this invocation wrote, as opposed to inspecting.
///
/// Returns:
///   The summary line, with no trailing newline.
pub fn overrides_summary(
    agent: &str,
    model: &Option<String>,
    thinking: &Option<String>,
    execution_deadline_seconds: &Option<u32>,
    runner_resources: &Option<serde_json::Value>,
    memory_writes: bool,
    changed: bool,
) -> String {
    let show = |v: &Option<String>| v.clone().unwrap_or_else(|| "platform default".to_string());
    let deadline = execution_deadline_seconds
        .map(|s| format!("{s} s"))
        .unwrap_or_else(|| "platform default".to_string());
    let resources = runner_resources
        .as_ref()
        .map(|value| value.to_string())
        .unwrap_or_else(|| "platform default".to_string());
    // The verb carries its own leading space, so an inspect closes straight
    // onto the colon instead of leaving a gap where a word used to be.
    let verb = if changed { " now" } else { "" };
    let writes = if memory_writes { "on" } else { "off" };
    format!(
        "overrides for {agent}{verb}: model {}, thinking {}, execution deadline {deadline}, runner resources {resources}, memory writes {writes}",
        show(model),
        show(thinking)
    )
}

/// Output of `<tier> overrides <agent>`: the dry-run plan, or the agent's two
/// nullable overrides as the API stored them. Owns its data so it outlives the
/// `ApiClient`.
///
/// `model`/`thinking` are `None` when no override is pinned, which is the same
/// fact the API returns as JSON null: the platform default applies. `changed`
/// distinguishes an inspect from a write, so an agent consumer can tell "this
/// is what it is" from "this is what it now is" without diffing.
/// `memory_writes` is the agent's NOT NULL memory-tools switch (#1461), so it
/// is always a boolean, never null.
#[derive(Debug)]
pub enum OverridesOutput {
    DryRun(crate::ui::DryRunPlan),
    Done {
        agent: String,
        model: Option<String>,
        thinking: Option<String>,
        execution_deadline_seconds: Option<u32>,
        runner_resources: Option<serde_json::Value>,
        memory_writes: bool,
        changed: bool,
    },
}

impl crate::ui::CliOutput for OverridesOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            OverridesOutput::DryRun(plan) => plan.to_json(),
            OverridesOutput::Done {
                agent,
                model,
                thinking,
                execution_deadline_seconds,
                runner_resources,
                memory_writes,
                changed,
            } => serde_json::json!({
                "agent": agent,
                "model": model,
                "thinking": thinking,
                "execution_deadline_seconds": execution_deadline_seconds,
                "runner_resources": runner_resources,
                "memory_writes": memory_writes,
                "changed": changed,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            OverridesOutput::DryRun(plan) => plan.render(ui),
            OverridesOutput::Done {
                agent,
                model,
                thinking,
                execution_deadline_seconds,
                runner_resources,
                memory_writes,
                changed,
            } => {
                ui.payload(&overrides_summary(
                    agent,
                    model,
                    thinking,
                    execution_deadline_seconds,
                    runner_resources,
                    *memory_writes,
                    *changed,
                ));
            }
        }
    }
}

/// `curie <tier> overrides <agent> [--model V|--clear-model] [--thinking V|--clear-thinking]`.
///
/// With no change flags this INSPECTS: one `GET`-resolved agent, no write. With
/// any change flag it PATCHes only the fields named, then reports the row as the
/// API stored it, so the operator sees what took rather than what was intended.
///
/// Covers both nullable overrides in one verb on purpose. The issue (#1311)
/// names `thinking` only, but `model` has the identical gap -- also settable
/// only by raw PATCH, also omitted from the CLI DTO -- and the two share the
/// API's `_nullable_override_validator` and its three-way semantics. Splitting
/// them would mean two verbs, two committed schemas and two manifest
/// regenerations for one concept, and would leave the sibling defect open for
/// exactly as long as it took someone to file it again.
///
/// Args:
///   opts: api url/key, the agent name or id, and the dry-run flag.
///   model: the intent for the model override.
///   thinking: the intent for the thinking override.
///
/// Returns:
///   The stored overrides, or the dry-run plan.
pub async fn overrides(
    opts: AgentActionOpts,
    model: OverrideChange,
    thinking: OverrideChange,
    execution_deadline: OverrideChange,
    runner_resources: OverrideChange,
) -> Result<OverridesOutput> {
    overrides_with_memory_writes(
        opts,
        model,
        thinking,
        execution_deadline,
        runner_resources,
        None,
    )
    .await
}

/// The `--memory-writes on|off` value as the boolean the API stores, or `None`
/// when the flag was not passed. Clap has already refused any other value.
pub fn memory_writes_flag(value: Option<&str>) -> Option<bool> {
    value.map(|v| v == "on")
}

/// [`overrides`] plus the `memory_writes` switch (#1461).
///
/// `memory_writes` is a NOT NULL boolean rather than a nullable override, so it
/// has no clear: `Some(b)` sends a JSON boolean under its own key, `None`
/// leaves the key out of the body. The result reports the switch as the API
/// stored it, like every other field.
///
/// Args:
///   opts: api url/key, the agent name or id, and the dry-run flag.
///   model: the intent for the model override.
///   thinking: the intent for the thinking override.
///   execution_deadline: the intent for the execution deadline.
///   runner_resources: the intent for the runner resources override.
///   memory_writes: the new memory-writes switch, if one was asked for.
///
/// Returns:
///   The stored overrides, or the dry-run plan.
pub async fn overrides_with_memory_writes(
    opts: AgentActionOpts,
    model: OverrideChange,
    thinking: OverrideChange,
    execution_deadline: OverrideChange,
    runner_resources: OverrideChange,
    memory_writes: Option<bool>,
) -> Result<OverridesOutput> {
    let ui = crate::ui::ui();
    let mut body = overrides_patch_body(&model, &thinking, &execution_deadline, &runner_resources);
    if let Some(on) = memory_writes {
        body.get_or_insert_with(AgentUpdate::default).memory_writes = Some(on);
    }
    if opts.dry_run {
        let plan = match &body {
            Some(b) => format!(
                "PATCH {}/agents/<id>  {}  (would resolve agent {:?} first)",
                opts.api_url,
                patch_body_json(b)?,
                opts.agent
            ),
            None => format!(
                "GET {}/agents  (read-only: would resolve agent {:?} and print its overrides)",
                opts.api_url, opts.agent
            ),
        };
        return Ok(OverridesOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![plan],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let Some(body) = body else {
        // Inspect: find_agent already carries both fields, so there is nothing
        // further to fetch and nothing to write.
        return Ok(OverridesOutput::Done {
            agent: agent.name,
            model: agent.model,
            thinking: agent.thinking,
            execution_deadline_seconds: agent.execution_deadline_seconds,
            runner_resources: agent.runner_resources,
            memory_writes: agent.memory_writes,
            changed: false,
        });
    };
    let cl = ui.checklist();
    let step = cl.step(&format!("updating overrides for {}", agent.name));
    let saved = match client.update_agent(&agent.id, &body).await {
        Ok(saved) => {
            step.done("updated");
            saved
        }
        Err(err) => {
            step.fail("failed");
            return Err(err);
        }
    };
    Ok(OverridesOutput::Done {
        agent: saved.name,
        model: saved.model,
        thinking: saved.thinking,
        execution_deadline_seconds: saved.execution_deadline_seconds,
        runner_resources: saved.runner_resources,
        memory_writes: saved.memory_writes,
        changed: true,
    })
}

/// Output of `<tier> publication-policy`.
#[derive(Debug)]
pub enum PublicationPolicyOutput {
    DryRun(crate::ui::DryRunPlan),
    Done {
        agent: String,
        publication_policy: String,
        publication_policy_version: i64,
        publication_draft: bool,
        publication_branch_prefix: Option<String>,
        changed: bool,
    },
}

impl crate::ui::CliOutput for PublicationPolicyOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            PublicationPolicyOutput::DryRun(plan) => plan.to_json(),
            PublicationPolicyOutput::Done {
                agent,
                publication_policy,
                publication_policy_version,
                publication_draft,
                publication_branch_prefix,
                changed,
            } => serde_json::json!({
                "agent": agent,
                "publication_policy": publication_policy,
                "publication_policy_version": publication_policy_version,
                "publication_draft": publication_draft,
                "publication_branch_prefix": publication_branch_prefix,
                "changed": changed,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            PublicationPolicyOutput::DryRun(plan) => plan.render(ui),
            PublicationPolicyOutput::Done {
                agent,
                publication_policy,
                publication_policy_version,
                publication_draft,
                publication_branch_prefix,
                changed,
            } => {
                let prefix = publication_branch_prefix
                    .clone()
                    .unwrap_or_else(|| "none".to_string());
                let verb = if *changed { " now" } else { "" };
                ui.payload(&format!(
                    "publication policy for {agent}{verb}: {publication_policy} version {publication_policy_version}, draft {publication_draft}, branch prefix {prefix}"
                ));
            }
        }
    }
}

/// The PATCH body for publication policy, or None when this invocation only inspects.
pub fn publication_policy_patch_body(
    policy: &Option<String>,
    draft: bool,
    no_draft: bool,
    branch_prefix: &Option<String>,
    clear_branch_prefix: bool,
) -> Option<AgentUpdate> {
    let publication_draft = if draft {
        Some(true)
    } else if no_draft {
        Some(false)
    } else {
        None
    };
    let publication_branch_prefix = if clear_branch_prefix {
        Some(None)
    } else {
        branch_prefix.clone().map(Some)
    };
    if policy.is_none() && publication_draft.is_none() && publication_branch_prefix.is_none() {
        return None;
    }
    Some(AgentUpdate {
        publication_policy: policy.clone(),
        publication_draft,
        publication_branch_prefix,
        ..AgentUpdate::default()
    })
}

pub async fn publication_policy(
    opts: AgentActionOpts,
    policy: Option<String>,
    draft: bool,
    no_draft: bool,
    branch_prefix: Option<String>,
    clear_branch_prefix: bool,
) -> Result<PublicationPolicyOutput> {
    let ui = crate::ui::ui();
    let body = publication_policy_patch_body(
        &policy,
        draft,
        no_draft,
        &branch_prefix,
        clear_branch_prefix,
    );
    if opts.dry_run {
        let plan = match &body {
            Some(b) => format!(
                "PATCH {}/agents/<id>  {}  (would resolve agent {:?} first)",
                opts.api_url,
                patch_body_json(b)?,
                opts.agent
            ),
            None => format!(
                "GET {}/agents  (read-only: would resolve agent {:?} and print its publication policy)",
                opts.api_url, opts.agent
            ),
        };
        return Ok(PublicationPolicyOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![plan],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let Some(body) = body else {
        return Ok(done_policy(&agent, false));
    };
    let cl = ui.checklist();
    let step = cl.step(&format!("updating publication policy for {}", agent.name));
    let saved = match client.update_agent(&agent.id, &body).await {
        Ok(saved) => {
            step.done("updated");
            saved
        }
        Err(err) => {
            step.fail("failed");
            return Err(err);
        }
    };
    Ok(done_policy(&saved, true))
}

#[cfg(test)]
mod publication_policy_tests {
    use super::{patch_body_json, publication_policy_patch_body};

    #[test]
    fn an_inspect_sends_no_patch_body() {
        assert!(publication_policy_patch_body(&None, false, false, &None, false).is_none());
    }

    #[test]
    fn auto_draft_and_a_cleared_prefix_are_one_patch() {
        let body = patch_body_json(
            &publication_policy_patch_body(&Some("auto".to_string()), true, false, &None, true)
                .expect("a write"),
        )
        .expect("encodes");
        assert_eq!(body["publication_policy"], "auto");
        assert_eq!(body["publication_draft"], true);
        assert!(body["publication_branch_prefix"].is_null());
        assert!(body.get("model").is_none());
    }
}

pub(super) fn done_policy(agent: &crate::api::Agent, changed: bool) -> PublicationPolicyOutput {
    PublicationPolicyOutput::Done {
        agent: agent.name.clone(),
        publication_policy: agent.publication_policy.clone(),
        publication_policy_version: agent.publication_policy_version,
        publication_draft: agent.publication_draft,
        publication_branch_prefix: agent.publication_branch_prefix.clone(),
        changed,
    }
}

#[cfg(test)]
mod overrides_tests {
    use super::{overrides_patch_body, OverrideChange};

    /// The PATCH body as it goes on the wire, or `None` for the inspect path.
    fn wire(
        model: &OverrideChange,
        thinking: &OverrideChange,
        execution_deadline: &OverrideChange,
        runner_resources: &OverrideChange,
    ) -> Option<serde_json::Value> {
        overrides_patch_body(model, thinking, execution_deadline, runner_resources)
            .map(|body| super::patch_body_json(&body).expect("encodes"))
    }

    // The property the whole verb rests on (#1310, #1311): an UNCHANGED field is
    // absent from the body, a CLEARED field is present and null. The API tells
    // those apart with `model_fields_set`, so collapsing them means "leave this
    // alone" silently becomes "reset this to the platform default".
    #[test]
    fn an_unchanged_field_is_absent_and_a_cleared_field_is_present_and_null() {
        let body = wire(
            &OverrideChange::Unchanged,
            &OverrideChange::Clear,
            &OverrideChange::Unchanged,
            &OverrideChange::Unchanged,
        )
        .expect("a clear is a write");
        let obj = body.as_object().expect("an object");
        assert!(
            !obj.contains_key("model"),
            "unchanged must be absent: {body}"
        );
        assert_eq!(obj.get("thinking"), Some(&serde_json::Value::Null));
    }

    #[test]
    fn both_unchanged_is_no_body_at_all_which_is_the_inspect_path() {
        assert!(wire(
            &OverrideChange::Unchanged,
            &OverrideChange::Unchanged,
            &OverrideChange::Unchanged,
            &OverrideChange::Unchanged,
        )
        .is_none());
    }

    #[test]
    fn a_set_field_carries_its_value() {
        let body = wire(
            &OverrideChange::Set("kimi-k2".into()),
            &OverrideChange::Set("adaptive".into()),
            &OverrideChange::Unchanged,
            &OverrideChange::Unchanged,
        )
        .expect("a set is a write");
        assert_eq!(body["model"], "kimi-k2");
        assert_eq!(body["thinking"], "adaptive");
    }

    // --value and --clear-value are contradictory, not a precedence puzzle:
    // picking a winner would make one of them silently do nothing.
    #[test]
    fn setting_and_clearing_the_same_field_is_a_usage_error() {
        let err = OverrideChange::resolve("model", Some("m".into()), true)
            .expect_err("contradictory flags must be refused");
        let msg = err.to_string();
        assert!(msg.contains("--model"), "{msg}");
        assert!(msg.contains("--clear-model"), "{msg}");
    }

    // Refused at the CLI rather than forwarded to earn a 422: the operator gets
    // the fix from the tool they typed into, and the message names the flag that
    // actually does what they meant.
    #[test]
    fn a_blank_value_is_refused_and_points_at_the_clear_flag() {
        for blank in ["", "   ", "\t"] {
            let err = OverrideChange::resolve("thinking", Some(blank.into()), false)
                .expect_err("a blank value must be refused");
            assert!(
                err.to_string().contains("--clear-thinking"),
                "the refusal must name the flag that clears: {err}"
            );
        }
    }

    // #1394: an inspect used to print "overrides for a : model ..." because the
    // verb was interpolated as an empty string before the colon.
    #[test]
    fn the_inspect_summary_has_no_gap_where_the_verb_would_be() {
        let line = super::overrides_summary(
            "a",
            &Some("kimi-k2".into()),
            &None,
            &None,
            &None,
            false,
            false,
        );
        assert_eq!(
            line,
            "overrides for a: model kimi-k2, thinking platform default, execution deadline platform default, runner resources platform default, memory writes off"
        );
        assert!(!line.contains("  "), "no double space anywhere: {line}");
    }

    #[test]
    fn a_write_summary_says_now_and_names_a_cleared_field_as_the_default() {
        assert_eq!(
            super::overrides_summary(
                "a",
                &None,
                &Some("adaptive".into()),
                &Some(90),
                &None,
                true,
                true
            ),
            "overrides for a now: model platform default, thinking adaptive, execution deadline 90 s, runner resources platform default, memory writes on"
        );
    }

    // #1392: the CLI stored `" kimi-k2 "` verbatim while the console trimmed, so
    // the same paste produced two different stored values depending on the
    // surface. A padded id is then forwarded as CURIE_MODEL and rejected by the
    // provider at the agent's NEXT turn, far from the command that exited 0.
    #[test]
    fn a_padded_value_is_trimmed_before_it_becomes_an_intent() {
        assert_eq!(
            OverrideChange::resolve("model", Some("  kimi-k2  ".into()), false).unwrap(),
            OverrideChange::Set("kimi-k2".into())
        );
        assert_eq!(
            OverrideChange::resolve("thinking", Some("\tadaptive\n".into()), false).unwrap(),
            OverrideChange::Set("adaptive".into())
        );
    }

    // The dry-run plan is the operator's only preview of the write, so it has to
    // show the body that will actually be sent, not the argv they typed.
    #[test]
    fn the_dry_run_body_carries_the_trimmed_value() {
        let body = wire(
            &OverrideChange::resolve("model", Some(" kimi-k2 ".into()), false).unwrap(),
            &OverrideChange::Unchanged,
            &OverrideChange::Unchanged,
            &OverrideChange::Unchanged,
        )
        .expect("a set is a write");
        assert_eq!(body["model"], "kimi-k2");
    }

    #[test]
    fn the_three_intents_round_trip_from_their_flag_pairs() {
        assert_eq!(
            OverrideChange::resolve("model", None, false).unwrap(),
            OverrideChange::Unchanged
        );
        assert_eq!(
            OverrideChange::resolve("model", None, true).unwrap(),
            OverrideChange::Clear
        );
        assert_eq!(
            OverrideChange::resolve("model", Some("m".into()), false).unwrap(),
            OverrideChange::Set("m".into())
        );
    }

    // --- `--execution-deadline`/`--clear-execution-deadline` (issue #3071) --
    //
    // Same three-way PATCH semantics as `model`/`thinking`, plus one thing
    // neither of those has: the wire field, `execution_deadline_seconds`, is a
    // JSON NUMBER, not a string, so a `Set` value has to be parsed and range
    // checked (60..10800) client-side rather than forwarded as typed text.

    #[test]
    fn a_set_execution_deadline_carries_a_json_number_not_a_string() {
        let body = wire(
            &OverrideChange::Unchanged,
            &OverrideChange::Unchanged,
            &OverrideChange::resolve_execution_deadline(Some("120".into()), false).unwrap(),
            &OverrideChange::Unchanged,
        )
        .expect("a set is a write");
        assert_eq!(body["execution_deadline_seconds"], 120);
        assert!(body.get("model").is_none());
        assert!(body.get("thinking").is_none());
    }

    #[test]
    fn a_cleared_execution_deadline_is_present_and_null() {
        let body = wire(
            &OverrideChange::Unchanged,
            &OverrideChange::Unchanged,
            &OverrideChange::Clear,
            &OverrideChange::Unchanged,
        )
        .expect("a clear is a write");
        assert!(body["execution_deadline_seconds"].is_null());
    }

    #[test]
    fn an_unchanged_execution_deadline_is_absent_when_model_and_thinking_are_set() {
        let body = wire(
            &OverrideChange::Set("kimi-k2".into()),
            &OverrideChange::Set("adaptive".into()),
            &OverrideChange::Unchanged,
            &OverrideChange::Unchanged,
        )
        .expect("a set is a write");
        assert!(
            !body
                .as_object()
                .unwrap()
                .contains_key("execution_deadline_seconds"),
            "unchanged must be absent: {body}"
        );
    }

    #[test]
    fn an_execution_deadline_below_the_minimum_is_refused_client_side() {
        let err = OverrideChange::resolve_execution_deadline(Some("59".into()), false)
            .expect_err("below the 60s floor must be refused");
        let msg = err.to_string();
        assert!(msg.contains("60"), "{msg}");
        assert!(msg.contains("10800"), "{msg}");
    }

    #[test]
    fn an_execution_deadline_above_the_maximum_is_refused_client_side() {
        let err = OverrideChange::resolve_execution_deadline(Some("10801".into()), false)
            .expect_err("above the 10800s ceiling must be refused");
        let msg = err.to_string();
        assert!(msg.contains("60"), "{msg}");
        assert!(msg.contains("10800"), "{msg}");
    }

    #[test]
    fn execution_deadline_boundaries_are_accepted() {
        assert_eq!(
            OverrideChange::resolve_execution_deadline(Some("60".into()), false).unwrap(),
            OverrideChange::Set("60".into())
        );
        assert_eq!(
            OverrideChange::resolve_execution_deadline(Some("10800".into()), false).unwrap(),
            OverrideChange::Set("10800".into())
        );
    }

    #[test]
    fn a_non_integer_execution_deadline_is_refused_client_side() {
        let err = OverrideChange::resolve_execution_deadline(Some("soon".into()), false)
            .expect_err("a non-integer value must be refused");
        assert!(err.to_string().contains("--execution-deadline"), "{err}");
    }

    #[test]
    fn setting_and_clearing_execution_deadline_together_is_a_usage_error() {
        let err = OverrideChange::resolve_execution_deadline(Some("120".into()), true)
            .expect_err("contradictory flags must be refused");
        let msg = err.to_string();
        assert!(msg.contains("--execution-deadline"), "{msg}");
        assert!(msg.contains("--clear-execution-deadline"), "{msg}");
    }
}

// ---------------------------------------------------------------------------
// Connector source builds (ADR 0113): `curie build --plugin-dir`
// ---------------------------------------------------------------------------
