//! Deploy-time hosted vs mounted-declaration check (#2352).
//!
//! Hosted readiness is a Deployment in the rendered manifests. Mounted
//! declaration is an mcp_entries URL that names that Deployment. Capability
//! discovery, including empty Bearer expansion, is #2519 and must not look
//! like unmounted. Hosted rollout wait is #2350.

use std::collections::BTreeMap;

use curie::connectors::{
    connector_report_lines, hosted_unmounted_connectors, hosted_unmounted_warning, prepare,
    ConnectorSync,
};
use curie::secrets::SecretScope;
use serde_json::{json, Value};

fn hosted_deployment(name: &str) -> Value {
    json!({"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name": name}})
}

fn hosted_service(name: &str) -> Value {
    json!({"apiVersion":"v1","kind":"Service","metadata":{"name": name}})
}

fn github_mcp_entry() -> BTreeMap<String, Value> {
    let mut entries = BTreeMap::new();
    entries.insert(
        "github".into(),
        json!({
            "type": "http",
            "url": "http://curie-acme-bot-mcp-github.curie.svc.cluster.local:8000/mcp",
            "headers": {"Authorization": "Bearer ${GITHUB_TOKEN}"}
        }),
    );
    entries
}

#[test]
fn hosted_and_declared_is_not_unmounted() {
    let name = "curie-acme-bot-mcp-github";
    let manifests = vec![hosted_deployment(name), hosted_service(name)];
    let unmounted =
        hosted_unmounted_connectors(&manifests, &github_mcp_entry(), "curie", "acme-bot");
    assert!(unmounted.is_empty(), "{unmounted:?}");
    assert_eq!(hosted_unmounted_warning(&unmounted), None);
    let (warnings, notes) = connector_report_lines(&ConnectorSync {
        urls: BTreeMap::from([(
            "github".into(),
            "http://curie-acme-bot-mcp-github.curie.svc.cluster.local:8000/mcp".into(),
        )]),
        hosted_unmounted: unmounted,
        ..Default::default()
    });
    assert!(warnings.is_empty(), "{warnings:?}");
    assert_eq!(
        notes,
        vec![
            "connector github: http://curie-acme-bot-mcp-github.curie.svc.cluster.local:8000/mcp"
                .to_string()
        ]
    );
}

#[test]
fn hosted_without_mcp_entry_is_unmounted() {
    let name = "curie-acme-bot-mcp-github";
    let manifests = vec![hosted_deployment(name), hosted_service(name)];
    let unmounted = hosted_unmounted_connectors(&manifests, &BTreeMap::new(), "curie", "acme-bot");
    assert_eq!(unmounted, vec!["github".to_string()]);
    let warning = hosted_unmounted_warning(&unmounted).expect("warning");
    assert!(warning.contains("hosted but not mounted"), "{warning}");
    assert!(warning.contains("github"), "{warning}");
    let (warnings, notes) = connector_report_lines(&ConnectorSync {
        hosted_unmounted: unmounted,
        ..Default::default()
    });
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("hosted but not mounted"));
    assert!(
        notes.is_empty(),
        "unmounted connectors must not print a URL: {notes:?}"
    );
}

#[test]
fn remote_url_without_deployment_is_not_hosted_unmounted() {
    let mut entries = BTreeMap::new();
    entries.insert(
        "internal".into(),
        json!({"type": "http", "url": "https://mcp.example.com/mcp"}),
    );
    let manifests = vec![hosted_service("curie-acme-bot-mcp-internal")];
    let unmounted = hosted_unmounted_connectors(&manifests, &entries, "curie", "acme-bot");
    assert!(unmounted.is_empty(), "{unmounted:?}");
    assert_eq!(hosted_unmounted_warning(&unmounted), None);
}

#[test]
fn empty_bearer_header_is_still_mounted_declaration() {
    // The Authorization placeholder expanding empty is #2519, not an
    // unmounted connector. Deploy must not emit the #2352 warning for it.
    let name = "curie-acme-bot-mcp-github";
    let manifests = vec![hosted_deployment(name)];
    let unmounted =
        hosted_unmounted_connectors(&manifests, &github_mcp_entry(), "curie", "acme-bot");
    assert!(unmounted.is_empty(), "{unmounted:?}");
}

#[test]
fn two_hosted_connectors_name_only_the_unmounted_one() {
    let github = "curie-acme-bot-mcp-github";
    let slack = "curie-acme-bot-mcp-slack";
    let manifests = vec![hosted_deployment(github), hosted_deployment(slack)];
    let unmounted =
        hosted_unmounted_connectors(&manifests, &github_mcp_entry(), "curie", "acme-bot");
    assert_eq!(unmounted, vec!["slack".to_string()]);
    let warning = hosted_unmounted_warning(&unmounted).unwrap();
    assert!(warning.contains("slack"), "{warning}");
    assert!(!warning.contains("github"), "{warning}");
}

#[test]
fn prepare_records_hosted_unmounted_from_manifests() {
    let name = "curie-acme-bot-mcp-github";
    let scope = SecretScope {
        cluster_identity: "ca:test".into(),
        release: "curie".into(),
        namespace: "curie".into(),
    };
    let prepared = prepare(
        &[hosted_deployment(name), hosted_service(name)],
        &BTreeMap::new(),
        "",
        &[],
        &scope,
        "acme-bot",
        &BTreeMap::new(),
    )
    .unwrap();
    assert_eq!(
        prepared.hosted_unmounted(),
        &["github".to_string()] as &[String]
    );
}
