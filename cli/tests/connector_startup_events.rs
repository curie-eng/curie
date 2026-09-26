//! Issue #3156: connector startup failures project one pod's events and omit
//! every other field and every planted secret.

use serde_json::json;
use std::collections::BTreeMap;

const MISSING_SECRET: &str = "secret \"curie-grafana-connector\" not found";
const PLANTED_TOKEN: &str = "super-secret-token";
const PLANTED_BEARER: &str = "gho_this_is_not_a_real_token";
const POD: &str = "mcp-grafana-abc";

fn event_list(message: &str) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "EventList",
        "items": [
            {
                "type": "Warning",
                "reason": "Failed",
                "message": message,
                "involvedObject": {"name": POD},
                "managedFields": [{"manager": "kubelet"}],
                "token": PLANTED_TOKEN
            },
            {
                "type": "Normal",
                "reason": "Pulled",
                "message": "ignore me",
                "involvedObject": {"name": "other-pod"}
            }
        ]
    })
}

fn planted_secrets() -> BTreeMap<String, String> {
    let mut secrets = BTreeMap::new();
    secrets.insert("GRAFANA_SERVICE_ACCOUNT_TOKEN".into(), PLANTED_TOKEN.into());
    secrets
}

#[test]
fn project_pod_events_keeps_only_the_matching_pod_message() {
    let projected = curie::connectors::project_pod_events(&event_list(MISSING_SECRET), POD);
    assert!(projected.contains(MISSING_SECRET), "{projected}");
    assert!(!projected.contains("ignore me"), "{projected}");
    assert!(!projected.contains("managedFields"), "{projected}");
    assert!(!projected.contains(PLANTED_TOKEN), "{projected}");
}

#[test]
fn sanitized_pod_events_keep_the_missing_secret_and_drop_planted_secrets() {
    let message = format!("{MISSING_SECRET}\nAuthorization: Bearer {PLANTED_BEARER}");
    let secrets = planted_secrets();
    let excerpt = curie::connectors::sanitized_pod_events(&event_list(&message), POD, &secrets)
        .expect("the missing secret sentence must survive redaction");
    assert!(excerpt.contains(MISSING_SECRET), "{excerpt}");
    assert!(!excerpt.contains(PLANTED_TOKEN), "{excerpt}");
    assert!(!excerpt.contains(PLANTED_BEARER), "{excerpt}");
}

#[test]
fn rollout_failure_appends_pod_events_and_the_logs_command() {
    let events = format!("Warning Failed: {MISSING_SECRET}");
    let err = curie::connectors::rollout_failure(
        "grafana",
        "curie",
        "curie-sre-bot-mcp-grafana",
        "CreateContainerConfigError",
        None,
        Some(&events),
    );
    let text = format!("{err:#}");
    assert!(text.contains("pod events:"), "{text}");
    assert!(text.contains(MISSING_SECRET), "{text}");
    let (class, fix) = curie::exit::classify(&err);
    assert_eq!(class, curie::exit::ExitClass::Failure);
    let fix = fix.expect("recovery command");
    assert!(
        fix.contains("kubectl -n curie logs deploy/curie-sre-bot-mcp-grafana --tail=50"),
        "{fix}"
    );
}

#[test]
fn rollout_failure_without_events_omits_the_events_section() {
    let err = curie::connectors::rollout_failure(
        "grafana",
        "curie",
        "curie-sre-bot-mcp-grafana",
        "CreateContainerConfigError",
        None,
        None,
    );
    let text = format!("{err:#}");
    assert!(!text.contains("pod events:"), "{text}");
}

#[test]
fn sanitized_pod_events_do_not_return_a_known_secret_value() {
    let secrets = planted_secrets();
    let excerpt =
        curie::connectors::sanitized_pod_events(&event_list(PLANTED_TOKEN), POD, &secrets);
    if let Some(text) = excerpt {
        assert!(
            !text.contains(PLANTED_TOKEN),
            "a known secret value must not remain in pod events: {text}"
        );
    }
}
