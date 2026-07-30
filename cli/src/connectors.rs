//! Apply the connector objects the API rendered, and prune what is no longer
//! declared (ADR-0086, #1063).
//!
//! The API computes these manifests; this applies them. That split is
//! deliberate: rendering is a pure function, so the API needs no cluster access
//! and keeps its deliberately read-only RBAC even though it is the service that
//! receives internet webhooks. Applying happens here, under the operator's own
//! kubectl credentials -- where cluster-write authority already lived.
//!
//! Pruning is the reason this is not a bare `kubectl apply`. Every object
//! carries a label naming the agent that declared it, so removing a connector
//! from `connectors.yaml` removes its Deployment, Service, and NetworkPolicy on
//! the next deploy. Without that, a deleted connector leaves a pod running with
//! a credential mounted and nothing referencing it -- the kind of leak nobody
//! notices because nothing breaks.

use anyhow::{Context, Result};
use serde_json::Value;

/// Marks every object this command owns, so pruning can find them again.
pub const OWNER_LABEL: &str = "curie.dev/connector-owner";

/// The label value for an agent's connector objects.
pub fn owner_value(agent_name: &str) -> String {
    agent_name.to_string()
}

/// Stamp the owner label onto each rendered object.
///
/// Applied here rather than in the renderer because ownership is a property of
/// *this deploy*, not of the declaration -- the same bundle deployed as two
/// agents must not have one prune the other's objects.
pub fn label_objects(manifests: &[Value], agent_name: &str) -> Vec<Value> {
    manifests
        .iter()
        .cloned()
        .map(|mut obj| {
            if let Some(meta) = obj.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                let labels = meta
                    .entry("labels")
                    .or_insert_with(|| Value::Object(Default::default()));
                if let Some(map) = labels.as_object_mut() {
                    map.insert(
                        OWNER_LABEL.to_string(),
                        Value::String(owner_value(agent_name)),
                    );
                }
            }
            obj
        })
        .collect()
}

/// `kubectl apply` argv for a JSON document supplied on stdin.
///
/// kubectl accepts JSON wherever it accepts YAML, so the manifests travel from
/// the API as JSON and never need a YAML serializer on this side.
pub fn apply_args(namespace: &str) -> Vec<String> {
    vec![
        "kubectl".into(),
        "-n".into(),
        namespace.into(),
        "apply".into(),
        "-f".into(),
        "-".into(),
    ]
}

/// `kubectl delete` argv for owned objects of `kind` that are no longer declared.
///
/// Scoped by the owner label AND by name, so it can only ever remove objects
/// this command created for this agent. A prune that selected on the label
/// alone would delete a concurrently-deploying agent's objects.
pub fn prune_args(namespace: &str, agent_name: &str, keep: &[String]) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "kubectl".into(),
        "-n".into(),
        namespace.into(),
        "delete".into(),
        "deployment,service,networkpolicy".into(),
        "-l".into(),
        format!("{}={}", OWNER_LABEL, owner_value(agent_name)),
        "--ignore-not-found".into(),
    ];
    // `kubectl delete -l` has no "except these" flag, so exclusion rides as a
    // field selector on name. It MUST be one comma-separated flag: kubectl
    // takes the last --field-selector and silently discards earlier ones, so
    // repeating the flag would exclude only the final object and delete the
    // rest -- pruning away the connectors just applied.
    if !keep.is_empty() {
        let excluded: Vec<String> = keep.iter().map(|n| format!("metadata.name!={n}")).collect();
        args.push(format!("--field-selector={}", excluded.join(",")));
    }
    args
}

/// Object names in a rendered set, used to build the prune exclusion.
pub fn object_names(manifests: &[Value]) -> Vec<String> {
    manifests
        .iter()
        .filter_map(|o| {
            o.get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .map(str::to_string)
        })
        .collect()
}

/// Serialize a manifest list as a single JSON `List` for one apply invocation.
pub fn as_list_document(manifests: &[Value]) -> Result<String> {
    let doc = serde_json::json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": manifests,
    });
    serde_json::to_string(&doc).context("serializing connector manifests")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn objs() -> Vec<Value> {
        vec![
            json!({"apiVersion":"v1","kind":"Service","metadata":{"name":"r-mcp-g"}}),
            json!({"apiVersion":"apps/v1","kind":"Deployment",
                   "metadata":{"name":"r-mcp-g","labels":{"existing":"kept"}}}),
        ]
    }

    #[test]
    fn owner_label_is_stamped_without_dropping_existing_labels() {
        let out = label_objects(&objs(), "sre-bot");
        let dep = &out[1]["metadata"]["labels"];
        assert_eq!(dep[OWNER_LABEL], "sre-bot");
        assert_eq!(dep["existing"], "kept", "renderer labels must survive");
    }

    #[test]
    fn objects_with_no_labels_map_still_get_the_owner() {
        let out = label_objects(&objs(), "sre-bot");
        assert_eq!(out[0]["metadata"]["labels"][OWNER_LABEL], "sre-bot");
    }

    #[test]
    fn prune_is_scoped_to_this_agent_not_all_connectors() {
        // Selecting on the label alone would delete a concurrently-deploying
        // agent's objects -- both are "connector objects".
        let args = prune_args("ns", "sre-bot", &[]);
        assert!(args.contains(&format!("{OWNER_LABEL}=sre-bot")));
        assert!(!args.iter().any(|a| a == OWNER_LABEL));
    }

    #[test]
    fn prune_excludes_every_applied_object_in_one_flag() {
        // kubectl keeps only the LAST --field-selector and silently drops the
        // rest, so repeating the flag would exclude one object and prune the
        // others -- deleting the connectors this deploy just applied.
        let keep = vec!["a".to_string(), "b".to_string()];
        let args = prune_args("ns", "sre-bot", &keep);
        let selectors: Vec<&String> = args
            .iter()
            .filter(|a| a.starts_with("--field-selector"))
            .collect();
        assert_eq!(selectors.len(), 1, "must be a single flag: {args:?}");
        assert_eq!(
            selectors[0],
            "--field-selector=metadata.name!=a,metadata.name!=b"
        );
    }

    #[test]
    fn prune_with_nothing_declared_removes_everything_owned() {
        // The connector was deleted from connectors.yaml: its Deployment,
        // Service, and NetworkPolicy must go, or a pod keeps running with a
        // credential mounted and nothing referencing it.
        let args = prune_args("ns", "sre-bot", &[]);
        assert!(!args.iter().any(|a| a.starts_with("--field-selector")));
        assert!(args.contains(&"deployment,service,networkpolicy".to_string()));
    }

    #[test]
    fn manifests_serialize_as_one_list_document() {
        let doc = as_list_document(&objs()).unwrap();
        let parsed: Value = serde_json::from_str(&doc).unwrap();
        assert_eq!(parsed["kind"], "List");
        assert_eq!(parsed["items"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn apply_reads_from_stdin_so_nothing_touches_disk() {
        let args = apply_args("ns");
        assert_eq!(args.last().unwrap(), "-");
    }
}
