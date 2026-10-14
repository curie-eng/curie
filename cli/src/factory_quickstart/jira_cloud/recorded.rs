//! The add agent path's check of a release's recorded `jira` tracker and
//! binding against the ones this run would write (#4358).
//!
//! The two are compared as the api reads them, not as JSON: keys in the
//! snake_case the chart renders into `CURIE_FORGES`
//! (`curie.factory.snake` in charts/curie/templates/_helpers.tpl), the config
//! model's defaults applied (apps/api/src/curie_api/forges/config.py), and
//! its sets (allowlist accounts, review feedback accounts) unordered.

use anyhow::Result;
use serde_json::{Map, Value};

use super::{JiraPlan, BINDING_NAME};
use crate::exit::CliError;

/// `apiUrl` as the chart renders it: `api_url`. A `*Seconds` interval
/// becomes `*_s`, as the chart's rename does.
fn snake(key: &str) -> String {
    let mut out = String::new();
    for (i, c) in key.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out.replace("interval_seconds", "interval_s")
}

/// `api_url` as a values key: `apiUrl`, for the refusal.
fn camel(key: &str) -> String {
    let mut parts = key.split('_');
    let mut out = parts.next().unwrap_or_default().to_string();
    for part in parts {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            out.push(first.to_ascii_uppercase());
            out.extend(chars);
        }
    }
    out
}

/// A sorted, deduplicated array: a config set.
fn set(value: &Value) -> Value {
    let Some(items) = value.as_array() else {
        return value.clone();
    };
    let mut items = items.clone();
    items.sort_by_key(Value::to_string);
    items.dedup();
    Value::Array(items)
}

/// A number as the float the model holds, so `45` and `45.0` are one value.
fn number(value: &Value) -> Value {
    value
        .as_f64()
        .and_then(serde_json::Number::from_f64)
        .map_or_else(|| value.clone(), Value::Number)
}

/// `value` in snake_case with nulls dropped (an omitted optional), numbers as
/// floats, and an `*Id` as a string, as the chart renders it. The maps whose
/// keys are names (`components`, `reviewFeedbackAllowlist`) keep their keys.
fn snake_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = Map::new();
            for (key, child) in map {
                if child.is_null() {
                    continue;
                }
                let child = if key == "components" || key == "reviewFeedbackAllowlist" {
                    child.clone()
                } else if key.ends_with("Id") && child.is_number() {
                    // The chart's `curie.factory.str`: a whole float as an int.
                    let text = match child.as_f64() {
                        Some(f) if child.is_f64() && f.fract() == 0.0 => (f as i64).to_string(),
                        _ => child.to_string(),
                    };
                    Value::String(text)
                } else {
                    snake_value(child)
                };
                out.insert(snake(key), child);
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(snake_value).collect()),
        Value::Number(_) => number(value),
        _ => value.clone(),
    }
}

/// Removes `key` from `map` when it holds the model's default.
fn drop_default(map: &mut Map<String, Value>, key: &str, default: Value) {
    if map.get(key) == Some(&default) {
        map.remove(key);
    }
}

fn empty_object() -> Value {
    Value::Object(Map::new())
}

/// The tracker the api would build from `value`. Its egress is left out: it
/// is resolved from the site at each preflight, and the recorded one is kept.
fn effective_tracker(value: &Value) -> Map<String, Value> {
    let Value::Object(mut map) = snake_value(value) else {
        return Map::new();
    };
    map.remove("egress");
    drop_default(&mut map, "poll_interval_s", number(&Value::from(45)));
    if let Some(Value::Object(credential)) = map.get_mut("credential") {
        drop_default(credential, "never_expires", Value::Bool(false));
    }
    map
}

/// The binding the api would build from `value`.
fn effective_binding(value: &Value) -> Map<String, Value> {
    let Value::Object(mut map) = snake_value(value) else {
        return Map::new();
    };
    if let Some(Value::Array(repos)) = map.get_mut("repos") {
        for repo in repos.iter_mut().filter_map(Value::as_object_mut) {
            if let Some(Value::Object(ci)) = repo.get_mut("ci") {
                drop_default(ci, "metadata_rerun_keys", Value::Array(Vec::new()));
            }
            drop_default(repo, "ci", empty_object());
            drop_default(repo, "bases", Value::Array(Vec::new()));
        }
    }
    if let Some(Value::Object(authority)) = map.get_mut("start_authority") {
        if let Some(accounts) = authority.get_mut("accounts") {
            *accounts = set(accounts);
        }
    }
    drop_default(
        &mut map,
        "start_authority",
        serde_json::json!({"type": "write_check"}),
    );
    if let Some(Value::Object(allowlist)) = map.get_mut("review_feedback_allowlist") {
        for accounts in allowlist.values_mut() {
            *accounts = set(accounts);
        }
    }
    drop_default(&mut map, "review_feedback_allowlist", empty_object());
    drop_default(&mut map, "components", empty_object());
    map
}

/// Builds the effective tracker or binding from its values entry.
type Effective = fn(&Value) -> Map<String, Value>;

/// True when `planned` matches `recorded`. A `<...>` string in the plan is a
/// `--dry-run` placeholder for a value only the preflight reads, and matches
/// any recorded value.
fn matches(planned: &Value, recorded: &Value) -> bool {
    match (planned, recorded) {
        (Value::String(text), _) if text.starts_with('<') && text.ends_with('>') => true,
        (Value::Array(planned), Value::Array(recorded)) => {
            planned.len() == recorded.len()
                && planned.iter().zip(recorded).all(|(p, r)| matches(p, r))
        }
        (Value::Object(planned), Value::Object(recorded)) => {
            planned.len() == recorded.len()
                && planned
                    .iter()
                    .all(|(key, p)| recorded.get(key).is_some_and(|r| matches(p, r)))
        }
        _ => planned == recorded,
    }
}

/// The flags that set each compared field, for the refusal's fix.
fn field_flags(field: &str) -> &'static str {
    match field {
        "host" | "api_url" | "tracker_scope_id" => "--forge-url",
        "credential" => "--forge-username",
        "mention" => "--forge-username and --token-file (the factory account)",
        "repos" | "default_repo" => "--repo",
        "start_authority" => "--start-account or --start-group",
        _ => "the Jira flags",
    }
}

/// The release's `jira` tracker and binding serve every Jira agent on it, so
/// one that differs from what this run would write is refused rather than
/// rewritten. Returns whether to write them: false when both are recorded
/// and match, true when either is missing.
pub(in crate::factory_quickstart) fn check_recorded(
    plan: &JiraPlan,
    tracker: Option<&Value>,
    binding: Option<&Value>,
    release: &str,
) -> Result<bool> {
    let compared: [(&str, &Value, Option<&Value>, Effective); 2] = [
        ("trackers", &plan.tracker, tracker, effective_tracker),
        ("bindings", &plan.binding, binding, effective_binding),
    ];
    for (list, planned, recorded, effective) in compared {
        let Some(recorded) = recorded else {
            continue;
        };
        let (planned, recorded) = (effective(planned), effective(recorded));
        let mut fields: Vec<&String> = planned.keys().chain(recorded.keys()).collect();
        fields.sort();
        fields.dedup();
        let differing =
            fields
                .into_iter()
                .find(|field| !match (planned.get(*field), recorded.get(*field)) {
                    (Some(p), Some(r)) => matches(p, r),
                    (None, None) => true,
                    _ => false,
                });
        if let Some(field) = differing {
            let shown = |value: Option<&Value>| {
                value.map_or_else(|| "the default".to_string(), Value::to_string)
            };
            let key = camel(field);
            return Err(CliError::usage(format!(
                "release {release} already has a Jira {} `{BINDING_NAME}` that other agents serve, and its {key} is {} where this run would write {}",
                list.trim_end_matches('s'),
                shown(recorded.get(field)),
                shown(planned.get(field)),
            ))
            .with_fix(format!(
                "rerun with {} matching factory.{list}[{BINDING_NAME}].{key}, or change that entry on the release with helm for every Jira agent as docs/guides/dark-factory-quickstart.md shows, then rerun",
                field_flags(field)
            ))
            .into());
        }
    }
    Ok(tracker.is_none() || binding.is_none())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_follow_the_chart_rename_both_ways() {
        assert_eq!(snake("apiUrl"), "api_url");
        assert_eq!(snake("pollIntervalS"), "poll_interval_s");
        assert_eq!(snake("pollIntervalSeconds"), "poll_interval_s");
        assert_eq!(camel("tracker_scope_id"), "trackerScopeId");
        assert_eq!(camel("poll_interval_s"), "pollIntervalS");
    }
}
