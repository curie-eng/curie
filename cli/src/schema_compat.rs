//! Target-chart schema compatibility planner for `curie cluster upgrade` (#2588).
//!
//! Ports `curie_api.schema_compat.plan_upgrade` / `render_decision`. The live
//! path reads target metadata from `helm template --show-only
//! templates/schema-compat.yaml`, not from a checkout `include_str!`.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

use crate::schema_window::redact_probe_text;

pub const KIND_EXPAND: &str = "expand";
pub const KIND_CONTRACT: &str = "contract";
pub const KIND_IRREVERSIBLE: &str = "irreversible";

#[derive(Debug, Clone, Deserialize)]
pub struct RevisionNode {
    pub revision: String,
    #[serde(default)]
    pub parents: Vec<String>,
    pub kind: String,
    #[serde(default)]
    #[allow(dead_code)]
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TargetMetadata {
    pub schema_min: String,
    pub schema_head: String,
    #[serde(default)]
    pub revisions: Vec<RevisionNode>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingStep {
    pub revision: String,
    pub kind: String,
}

#[derive(Debug, Clone)]
pub struct CompatDecision {
    pub action: String,
    pub current_revision: Option<String>,
    pub target_head: String,
    pub target_min: String,
    pub pending: Vec<PendingStep>,
    pub rollback_compatible: bool,
    pub reason: String,
    pub forward_only: bool,
    pub outcome: Option<String>,
    pub source_head: Option<String>,
}

/// Parse a helm-template ConfigMap (YAML or JSON) into target metadata.
pub fn parse_target_metadata(rendered: &str) -> Result<TargetMetadata, String> {
    let trimmed = rendered.trim();
    if trimmed.is_empty() {
        return Err(
            "target chart did not render schema compatibility metadata (BYO api.deploy=false or missing template)"
                .into(),
        );
    }
    let mut last_err = None;
    for doc in trimmed.split("\n---") {
        let doc = doc.trim().trim_start_matches("---").trim();
        if doc.is_empty() {
            continue;
        }
        match serde_norway::from_str::<serde_json::Value>(doc) {
            Ok(value) => {
                if let Some(result) = metadata_from_value(&value) {
                    return result;
                }
            }
            Err(error) => last_err = Some(error.to_string()),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        "target chart did not render schema compatibility metadata (BYO api.deploy=false or missing template)"
            .into()
    }))
}

fn metadata_from_value(value: &serde_json::Value) -> Option<Result<TargetMetadata, String>> {
    if let Some(payload) = value
        .get("data")
        .and_then(|data| data.get("compatibility.json"))
    {
        return Some(parse_compatibility_payload(payload));
    }
    if value.get("schema_min").is_some() && value.get("schema_head").is_some() {
        return Some(parse_metadata_object(value));
    }
    None
}

fn parse_compatibility_payload(payload: &serde_json::Value) -> Result<TargetMetadata, String> {
    match payload {
        serde_json::Value::String(raw) => {
            let raw = raw.trim();
            let parsed: serde_json::Value = match serde_json::from_str(raw) {
                Ok(value) => value,
                Err(_) => serde_norway::from_str(raw)
                    .map_err(|e| format!("compatibility.json is not valid JSON: {e}"))?,
            };
            parse_metadata_object(&parsed)
        }
        serde_json::Value::Object(_) => parse_metadata_object(payload),
        _ => Err("compatibility.json is not an object".into()),
    }
}

fn parse_metadata_object(value: &serde_json::Value) -> Result<TargetMetadata, String> {
    serde_json::from_value(value.clone())
        .map_err(|e| format!("schema compatibility metadata is invalid: {e}"))
}

/// Walk pending revisions from `live` (or empty-DB base) to `schema_head`.
///
/// Live walk follows the unique child (parent reverse). Live not in the graph
/// is an incompatible refuse whose reason names the revision.
pub fn pending_revisions(
    live: Option<&str>,
    target: &TargetMetadata,
) -> Result<Vec<PendingStep>, String> {
    match live {
        Some(current) => pending_from_live(current, target),
        None => Ok(pending_from_empty(target)),
    }
}

fn pending_from_empty(target: &TargetMetadata) -> Vec<PendingStep> {
    let by_id: BTreeMap<&str, &RevisionNode> = target
        .revisions
        .iter()
        .map(|node| (node.revision.as_str(), node))
        .collect();
    let mut chain = Vec::new();
    let mut current = Some(target.schema_head.as_str());
    let mut seen = BTreeSet::new();
    while let Some(rev) = current {
        if !seen.insert(rev) {
            break;
        }
        let Some(node) = by_id.get(rev) else {
            break;
        };
        chain.push(PendingStep {
            revision: node.revision.clone(),
            kind: node.kind.clone(),
        });
        current = node
            .parents
            .first()
            .map(String::as_str)
            .filter(|parent| by_id.contains_key(parent));
    }
    chain.reverse();
    chain
}

fn pending_from_live(live: &str, target: &TargetMetadata) -> Result<Vec<PendingStep>, String> {
    let by_id: BTreeMap<&str, &RevisionNode> = target
        .revisions
        .iter()
        .map(|node| (node.revision.as_str(), node))
        .collect();
    if !by_id.contains_key(live) {
        return Err(format!(
            "live database revision {live} is not in the target application's schema graph"
        ));
    }
    if live == target.schema_head {
        return Ok(Vec::new());
    }
    let mut children: BTreeMap<&str, Vec<&RevisionNode>> = BTreeMap::new();
    for node in &target.revisions {
        for parent in &node.parents {
            children.entry(parent.as_str()).or_default().push(node);
        }
    }
    let mut pending = Vec::new();
    let mut current = live;
    let mut seen = BTreeSet::new();
    while current != target.schema_head {
        if !seen.insert(current) {
            return Err(format!("schema graph cycle at revision {current}"));
        }
        let Some(nexts) = children.get(current) else {
            return Err(format!(
                "live database revision {live} cannot reach target head {}",
                target.schema_head
            ));
        };
        if nexts.len() != 1 {
            return Err(format!(
                "schema graph has {} children of revision {current}; expected a unique child",
                nexts.len()
            ));
        }
        let next = nexts[0];
        pending.push(PendingStep {
            revision: next.revision.clone(),
            kind: next.kind.clone(),
        });
        current = next.revision.as_str();
    }
    Ok(pending)
}

/// Pure planner: no database mutation. Matches Python `plan_upgrade`.
pub fn plan_upgrade(
    current_revision: Option<&str>,
    target: &TargetMetadata,
    pending: &[PendingStep],
    forward_only: bool,
    source_head: Option<&str>,
) -> CompatDecision {
    let source = source_head
        .map(ToOwned::to_owned)
        .or_else(|| current_revision.map(ToOwned::to_owned));
    if current_revision.is_none() {
        return CompatDecision {
            action: "apply".into(),
            current_revision: None,
            target_head: target.schema_head.clone(),
            target_min: target.schema_min.clone(),
            pending: pending.to_vec(),
            rollback_compatible: false,
            reason: "empty database; apply migrations to target head".into(),
            forward_only,
            outcome: None,
            source_head: source,
        };
    }
    if current_revision == Some(target.schema_head.as_str()) || pending.is_empty() {
        return CompatDecision {
            action: "noop".into(),
            current_revision: current_revision.map(ToOwned::to_owned),
            target_head: target.schema_head.clone(),
            target_min: target.schema_min.clone(),
            pending: Vec::new(),
            rollback_compatible: true,
            reason: "database is already at the target head".into(),
            forward_only,
            outcome: Some("already_at_head".into()),
            source_head: source,
        };
    }
    let blocking: Vec<&PendingStep> = pending
        .iter()
        .filter(|step| step.kind == KIND_CONTRACT || step.kind == KIND_IRREVERSIBLE)
        .collect();
    if !blocking.is_empty() && !forward_only {
        let names = blocking
            .iter()
            .map(|step| step.revision.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return CompatDecision {
            action: "refuse".into(),
            current_revision: current_revision.map(ToOwned::to_owned),
            target_head: target.schema_head.clone(),
            target_min: target.schema_min.clone(),
            pending: pending.to_vec(),
            rollback_compatible: false,
            reason: format!(
                "pending contract/irreversible migration {names} would close the \
                 patch rollback window; pass --forward-only / api.migrate.forwardOnly \
                 to apply the documented forward-only procedure"
            ),
            forward_only: false,
            outcome: Some("refused".into()),
            source_head: source,
        };
    }
    let mut rollback_ok = pending.iter().all(|step| step.kind == KIND_EXPAND);
    if forward_only && !blocking.is_empty() {
        rollback_ok = false;
    }
    CompatDecision {
        action: "apply".into(),
        current_revision: current_revision.map(ToOwned::to_owned),
        target_head: target.schema_head.clone(),
        target_min: target.schema_min.clone(),
        pending: pending.to_vec(),
        rollback_compatible: rollback_ok,
        reason: if rollback_ok {
            "pending migrations are expand-only; source application can keep serving".into()
        } else {
            "forward-only apply of a contract/irreversible migration".into()
        },
        forward_only,
        outcome: None,
        source_head: source,
    }
}

pub fn refuse_with_reason(
    current_revision: Option<&str>,
    target: Option<&TargetMetadata>,
    pending: Vec<PendingStep>,
    reason: impl Into<String>,
    forward_only: bool,
    source_head: Option<&str>,
) -> CompatDecision {
    CompatDecision {
        action: "refuse".into(),
        current_revision: current_revision.map(ToOwned::to_owned),
        target_head: target.map(|t| t.schema_head.clone()).unwrap_or_default(),
        target_min: target.map(|t| t.schema_min.clone()).unwrap_or_default(),
        pending,
        rollback_compatible: false,
        reason: reason.into(),
        forward_only,
        outcome: Some("refused".into()),
        source_head: source_head.map(ToOwned::to_owned),
    }
}

/// Structured, redacted planner record. No URLs, passwords, or rows.
pub fn render_decision(decision: &CompatDecision) -> serde_json::Value {
    serde_json::json!({
        "decision": decision.action,
        "current_revision": decision.current_revision,
        "target_min": decision.target_min,
        "target_head": decision.target_head,
        "source_head": decision.source_head,
        "pending": decision.pending.iter().map(|step| {
            serde_json::json!({
                "revision": step.revision,
                "kind": step.kind,
            })
        }).collect::<Vec<_>>(),
        "rollback_compatible": decision.rollback_compatible,
        "forward_only": decision.forward_only,
        "reason": redact_probe_text(&decision.reason),
        "outcome": decision.outcome,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::Path;

    fn kinds() -> BTreeMap<String, String> {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("cli is not the repo root")
            .join("apps/api/src/curie_api/revision_kinds.json");
        serde_json::from_str(&fs::read_to_string(&path).expect("revision_kinds.json"))
            .expect("revision_kinds.json is valid")
    }

    fn kind(rev: &str) -> String {
        kinds()
            .get(rev)
            .cloned()
            .unwrap_or_else(|| KIND_EXPAND.to_string())
    }

    fn node(revision: &str, parent: Option<&str>) -> RevisionNode {
        RevisionNode {
            revision: revision.into(),
            parents: parent.map(|p| vec![p.into()]).unwrap_or_default(),
            kind: kind(revision),
            sha256: None,
        }
    }

    fn contract_target() -> TargetMetadata {
        TargetMetadata {
            schema_min: "0043".into(),
            schema_head: "0043".into(),
            revisions: vec![
                node("0039", Some("0038")),
                node("0040", Some("0039")),
                node("0041", Some("0040")),
                node("0042", Some("0041")),
                node("0043", Some("0042")),
            ],
        }
    }

    fn irreversible_target() -> TargetMetadata {
        TargetMetadata {
            schema_min: "0017".into(),
            schema_head: "0017".into(),
            revisions: vec![
                node("0015", None),
                node("0016", Some("0015")),
                node("0017", Some("0016")),
            ],
        }
    }

    #[test]
    fn kinds_file_grounds_contract_and_irreversible() {
        let kinds = kinds();
        assert_eq!(kinds.get("0041").map(String::as_str), Some(KIND_CONTRACT));
        assert_eq!(
            kinds.get("0016").map(String::as_str),
            Some(KIND_IRREVERSIBLE)
        );
    }

    #[test]
    fn contract_refuses_without_forward_only() {
        let target = contract_target();
        let pending = pending_revisions(Some("0039"), &target).expect("0039 is in graph");
        assert!(
            pending
                .iter()
                .any(|s| s.revision == "0041" && s.kind == KIND_CONTRACT),
            "{pending:?}"
        );
        let decision = plan_upgrade(Some("0039"), &target, &pending, false, Some("0039"));
        assert_eq!(decision.action, "refuse");
        assert!(
            decision.reason.contains("--forward-only"),
            "{}",
            decision.reason
        );
        assert!(decision.reason.contains("0041"), "{}", decision.reason);
        let rendered = render_decision(&decision);
        assert_eq!(rendered["decision"], "refuse");
        assert!(rendered["reason"]
            .as_str()
            .unwrap()
            .contains("--forward-only"));
    }

    #[test]
    fn forward_only_applies_pending_contract() {
        let target = contract_target();
        let pending = pending_revisions(Some("0039"), &target).expect("0039 is in graph");
        let decision = plan_upgrade(Some("0039"), &target, &pending, true, Some("0039"));
        assert_eq!(decision.action, "apply");
        assert!(decision.forward_only);
        assert!(!decision.rollback_compatible);
    }

    #[test]
    fn irreversible_refuses_without_forward_only() {
        let target = irreversible_target();
        let pending = pending_revisions(Some("0015"), &target).expect("0015 is in graph");
        assert!(
            pending
                .iter()
                .any(|s| s.revision == "0016" && s.kind == KIND_IRREVERSIBLE),
            "{pending:?}"
        );
        let decision = plan_upgrade(Some("0015"), &target, &pending, false, None);
        assert_eq!(decision.action, "refuse");
        assert!(decision.reason.contains("--forward-only"));
        assert!(decision.reason.contains("0016"));
    }

    #[test]
    fn empty_database_applies_including_historical_irreversible() {
        let target = irreversible_target();
        let pending = pending_revisions(None, &target).expect("empty walk");
        assert!(pending.iter().any(|s| s.revision == "0016"));
        let decision = plan_upgrade(None, &target, &pending, false, None);
        assert_eq!(decision.action, "apply");
        assert!(!decision.rollback_compatible);
    }

    #[test]
    fn already_at_head_is_noop() {
        let target = contract_target();
        let pending = pending_revisions(Some("0043"), &target).expect("head is in graph");
        assert!(pending.is_empty());
        let decision = plan_upgrade(Some("0043"), &target, &pending, false, Some("0043"));
        assert_eq!(decision.action, "noop");
        assert_eq!(decision.outcome.as_deref(), Some("already_at_head"));
    }

    #[test]
    fn unknown_live_revision_refuses_and_names_it() {
        let target = contract_target();
        let err = pending_revisions(Some("0099"), &target).expect_err("unknown live");
        assert!(err.contains("0099"), "{err}");
        let decision = refuse_with_reason(
            Some("0099"),
            Some(&target),
            Vec::new(),
            err,
            false,
            Some("0099"),
        );
        assert_eq!(decision.action, "refuse");
        assert!(decision.reason.contains("0099"));
    }

    #[test]
    fn parse_configmap_string_or_object_payload() {
        let object = serde_json::json!({
            "kind": "ConfigMap",
            "data": {
                "compatibility.json": {
                    "schema_min": "0043",
                    "schema_head": "0043",
                    "revisions": [{"revision": "0043", "parents": ["0042"], "kind": "expand"}]
                }
            }
        });
        let parsed = parse_target_metadata(&object.to_string()).expect("object payload");
        assert_eq!(parsed.schema_head, "0043");

        let as_string = serde_json::json!({
            "kind": "ConfigMap",
            "data": {
                "compatibility.json": serde_json::json!({
                    "schema_min": "0043",
                    "schema_head": "0043",
                    "revisions": []
                }).to_string()
            }
        });
        let parsed = parse_target_metadata(&as_string.to_string()).expect("string payload");
        assert_eq!(parsed.schema_min, "0043");
    }

    #[test]
    fn render_decision_redacts_probe_text() {
        let decision = refuse_with_reason(
            Some("0039"),
            None,
            Vec::new(),
            "could not connect to postgresql://curie:secret-password@postgres:5432/curie",
            false,
            None,
        );
        let rendered = render_decision(&decision);
        let reason = rendered["reason"].as_str().unwrap();
        assert!(!reason.contains("secret-password"));
        assert!(!reason.contains("postgresql://"));
    }
}
