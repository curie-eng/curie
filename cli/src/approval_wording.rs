//! Display only. Exact summaries and grant identities remain machine data.
use crate::api::ApprovalRecord;
use crate::render::{action_label, action_text};
use serde_json::Value;

fn caption(key: &str) -> String {
    let label = action_label(key);
    let mut chars = label.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => "Action".into(),
    }
}
fn display_value(value: &Value) -> String {
    match value {
        Value::Null => "none".into(),
        Value::Bool(v) => {
            if *v {
                "yes".into()
            } else {
                "no".into()
            }
        }
        Value::String(s) => {
            if s.is_empty() {
                "(empty)".into()
            } else {
                s.clone()
            }
        }
        // Nested keys and types are requested data, not schema captions.
        Value::Array(_) | Value::Object(_) => value.to_string(),
        _ => value.to_string(),
    }
}
pub fn approval_display(record: &ApprovalRecord) -> String {
    if let Some(display) = record
        .display_summary
        .as_deref()
        .filter(|s| !s.trim().is_empty())
    {
        return display.into();
    }
    if let Some(machine) = record.summary.strip_prefix("Tool call awaiting approval: ") {
        let (tool, payload) = machine.split_once(' ').unwrap_or((machine, ""));
        let mut text = format!("Approve {}.", action_label(tool));
        if let Ok(Value::Object(args)) = serde_json::from_str::<Value>(payload) {
            if !args.is_empty() {
                text.push(' ');
                text.push_str(
                    &args
                        .iter()
                        .map(|(key, value)| format!("{}: {}", caption(key), display_value(value)))
                        .collect::<Vec<_>>()
                        .join("; "),
                );
            }
        } else {
            text.push_str(" Details are incomplete; review the original request before approving.");
        }
        text
    } else {
        action_text(&record.summary)
    }
}
