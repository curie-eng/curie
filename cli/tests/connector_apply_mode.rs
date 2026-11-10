//! Fix pin for #5131: the connector List is written with one server-side apply
//! under the CLI's own field manager, so a concurrent create by the in-cluster
//! connector reconciler becomes an update instead of an `AlreadyExists` error.
//! The owning-agent sandbox pool clone stays a client-side apply.
//!
//! Uses only `prepare`/`sync`/`bind_current_cluster` (already on main) plus a
//! fake kubectl that records every invocation, so reversing the fix in
//! `cli/src/connectors.rs` leaves this file compiling and makes it fail.

use crate::support::executable as test_executable;

use curie::connectors::{bind_current_cluster, prepare, sync};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::Path;

const NAMESPACE: &str = "acme-ns";
const RELEASE: &str = "acme";
const AGENT: &str = "acme-bot";
const DEPLOYMENT: &str = "acme-acme-bot-mcp-rotate";
const OWNED_SECRET: &str = "acme-acme-bot-connector-secrets";
const SECRET_KEY: &str = "ROTATE_TOKEN";
const SECRET_VALUE: &str = "sentinel-rotate-value-example-5131";
const SERVER_SIDE_FLAGS: [&str; 3] = [
    "--server-side",
    "--force-conflicts",
    "--field-manager=curie-cli-connectors",
];

fn prepend_path(dir: &Path) -> Option<OsString> {
    let previous = std::env::var_os("PATH");
    let mut paths = vec![dir.to_path_buf()];
    paths.extend(std::env::split_paths(&previous.clone().unwrap_or_default()));
    std::env::set_var("PATH", std::env::join_paths(paths).expect("PATH"));
    previous
}

fn restore_path(previous: Option<OsString>) {
    match previous {
        Some(value) => std::env::set_var("PATH", value),
        None => std::env::remove_var("PATH"),
    }
}

/// A fake kubectl that logs each argv to `argv.log` and, for every apply,
/// writes the argv and the stdin body to `apply-<n>.argv` / `apply-<n>.body`.
fn fake_kubectl(log: &Path) -> String {
    let log = log.display();
    format!(
        r#"#!/bin/sh
LOG='{log}'
printf '%s\n' "$*" >> "$LOG/argv.log"
case " $* " in
  *" config view "*)
    printf '%s\n' '{{"clusters":[{{"cluster":{{"server":"https://cluster-a.example.com","certificate-authority-data":"Y2EtYQ=="}}}}]}}'
    exit 0
    ;;
  *" apply -f - "*)
    n=$(cat "$LOG/apply.count" 2>/dev/null || echo 0)
    printf '%s\n' $(( n + 1 )) > "$LOG/apply.count"
    printf '%s\n' "$*" > "$LOG/apply-$n.argv"
    cat > "$LOG/apply-$n.body"
    exit 0
    ;;
  *" delete "*)
    exit 0
    ;;
  *" get secret "*)
    printf '%s\n' 'Error from server (NotFound): secrets "x" not found' >&2
    exit 1
    ;;
  *" get sandboxtemplates."*)
    printf '%s\n' '{{"items":[{{"apiVersion":"extensions.agents.x-k8s.io/v1beta1","kind":"SandboxTemplate","metadata":{{"name":"acme-curie-runner","labels":{{"app.kubernetes.io/instance":"acme","app.kubernetes.io/managed-by":"Helm"}},"annotations":{{"meta.helm.sh/release-name":"acme"}}}},"spec":{{"podTemplate":{{"spec":{{"containers":[{{"name":"runner","image":"runner.example.com/runner:dev"}}]}}}}}}}}]}}'
    exit 0
    ;;
  *" get sandboxwarmpools."*)
    printf '%s\n' '{{"apiVersion":"extensions.agents.x-k8s.io/v1beta1","kind":"SandboxWarmPool","metadata":{{"name":"acme-curie-runner-pool","labels":{{"app.kubernetes.io/instance":"acme"}}}},"spec":{{"replicas":1,"sandboxTemplateRef":{{"name":"acme-curie-runner"}}}}}}'
    exit 0
    ;;
  *" get deployment "*)
    printf '%s\n' '{{"metadata":{{"generation":1}},"spec":{{"replicas":1}},"status":{{"observedGeneration":1,"replicas":1,"updatedReplicas":1,"readyReplicas":1,"availableReplicas":1}}}}'
    exit 0
    ;;
  *" get replicasets "*)
    printf '%s\n' '{{"items":[{{"metadata":{{"annotations":{{"deployment.kubernetes.io/revision":"1"}},"labels":{{"pod-template-hash":"abc"}}}}}}]}}'
    exit 0
    ;;
  *" get pods "*)
    printf '%s\n' '{{"items":[{{"metadata":{{"name":"acme-acme-bot-mcp-rotate-abc-xyz","labels":{{"pod-template-hash":"abc"}}}},"status":{{"phase":"Running","containerStatuses":[{{"name":"server","ready":true,"state":{{"running":{{}}}}}}]}}}}]}}'
    exit 0
    ;;
esac
printf 'unexpected kubectl invocation: %s\n' "$*" >&2
exit 64
"#
    )
}

/// One recorded `kubectl ... apply -f -` call.
struct ApplyCall {
    argv: Vec<String>,
    body: Value,
}

impl ApplyCall {
    fn kinds(&self) -> Vec<String> {
        self.body
            .get("items")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|item| item.get("kind").and_then(Value::as_str))
            .map(str::to_string)
            .collect()
    }

    fn is_pool_clone(&self) -> bool {
        self.kinds().iter().any(|kind| kind == "SandboxTemplate")
    }

    fn reads_stdin(&self) -> bool {
        self.argv.windows(2).any(|pair| pair == ["-f", "-"])
    }

    fn has_flag(&self, flag: &str) -> bool {
        self.argv.iter().any(|arg| arg == flag)
    }
}

struct Recorded {
    argv_lines: Vec<String>,
    applies: Vec<ApplyCall>,
}

impl Recorded {
    fn connector_applies(&self) -> Vec<&ApplyCall> {
        self.applies
            .iter()
            .filter(|call| !call.is_pool_clone())
            .collect()
    }

    fn pool_applies(&self) -> Vec<&ApplyCall> {
        self.applies
            .iter()
            .filter(|call| call.is_pool_clone())
            .collect()
    }
}

fn read_recorded(log: &Path) -> Recorded {
    let argv_lines = std::fs::read_to_string(log.join("argv.log"))
        .expect("fake kubectl argv log")
        .lines()
        .map(str::to_string)
        .collect();
    let count: usize = std::fs::read_to_string(log.join("apply.count"))
        .map(|text| text.trim().parse().expect("apply count"))
        .unwrap_or(0);
    let applies = (0..count)
        .map(|n| {
            let argv = std::fs::read_to_string(log.join(format!("apply-{n}.argv")))
                .expect("apply argv")
                .split_whitespace()
                .map(str::to_string)
                .collect();
            let body =
                std::fs::read_to_string(log.join(format!("apply-{n}.body"))).expect("apply body");
            let body = serde_json::from_str(&body).expect("apply stdin is a JSON document");
            ApplyCall { argv, body }
        })
        .collect();
    Recorded {
        argv_lines,
        applies,
    }
}

fn connector_manifests(with_secret: bool) -> Vec<Value> {
    let mut container = json!({
        "name": "server",
        "image": "connector.example.com/rotate:dev",
        "ports": [{"containerPort": 8080}],
    });
    if with_secret {
        container["env"] = json!([{
            "name": SECRET_KEY,
            "valueFrom": {"secretKeyRef": {"name": OWNED_SECRET, "key": SECRET_KEY}},
        }]);
    }
    vec![
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": DEPLOYMENT},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app.kubernetes.io/name": DEPLOYMENT}},
                "template": {
                    "metadata": {"labels": {"app.kubernetes.io/name": DEPLOYMENT}},
                    "spec": {"containers": [container]},
                },
            },
        }),
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": DEPLOYMENT},
            "spec": {
                "selector": {"app.kubernetes.io/name": DEPLOYMENT},
                "ports": [{"port": 8080, "targetPort": 8080}],
            },
        }),
    ]
}

/// Drive `prepare` + `sync` for one hosted connector against the fake kubectl.
async fn record_sync(with_secret: bool) -> Recorded {
    let _lock = crate::support::ENV_LOCK.lock().await;
    let bin = tempfile::tempdir().expect("bin");
    let log = tempfile::tempdir().expect("log");
    test_executable::install_in(bin.path(), "kubectl", &fake_kubectl(log.path()));
    let previous = prepend_path(bin.path());
    let result = async {
        let target = bind_current_cluster(NAMESPACE, RELEASE)
            .await
            .expect("bind cluster");
        let mut mcp = BTreeMap::new();
        mcp.insert(
            "rotate".into(),
            json!({"url": format!("http://{DEPLOYMENT}.{NAMESPACE}.svc.cluster.local:8080/mcp")}),
        );
        let (secret_name, secret_keys, overrides) = if with_secret {
            (
                OWNED_SECRET,
                vec![SECRET_KEY.to_string()],
                BTreeMap::from([(SECRET_KEY.to_string(), SECRET_VALUE.to_string())]),
            )
        } else {
            ("", Vec::new(), BTreeMap::new())
        };
        let prepared = prepare(
            &connector_manifests(with_secret),
            &mcp,
            secret_name,
            &secret_keys,
            &target.scope,
            AGENT,
            &overrides,
        )
        .expect("prepare")
        .bind_target(target)
        .expect("bind");
        sync(prepared).await
    }
    .await;
    restore_path(previous);
    result.expect("sync against a Ready fake cluster succeeds");
    read_recorded(log.path())
}

fn assert_server_side(call: &ApplyCall) {
    for flag in SERVER_SIDE_FLAGS {
        assert!(
            call.has_flag(flag),
            "connector apply must carry {flag}; argv was: {}",
            call.argv.join(" ")
        );
    }
    assert!(
        call.reads_stdin(),
        "connector apply must read the manifest from stdin; argv was: {}",
        call.argv.join(" ")
    );
}

/// THE REGRESSION TEST for #5131. The owned Secret and the rendered connector
/// objects go out in one server-side apply under `curie-cli-connectors`, and
/// the credential stays on stdin.
#[tokio::test]
async fn hosted_connector_list_applies_server_side_with_cli_field_manager() {
    let recorded = record_sync(true).await;
    let connector = recorded.connector_applies();
    assert_eq!(
        connector.len(),
        1,
        "expected exactly one connector List apply, saw {}",
        connector.len()
    );
    let call = connector[0];
    let kinds = call.kinds();
    for kind in ["Secret", "Deployment", "Service"] {
        assert!(kinds.iter().any(|k| k == kind), "{kind} missing: {kinds:?}");
    }
    let secret = call.body["items"]
        .as_array()
        .expect("List items")
        .iter()
        .find(|item| item["kind"] == "Secret")
        .expect("owned Secret");
    assert_eq!(secret["metadata"]["name"], OWNED_SECRET);
    assert_eq!(secret["stringData"][SECRET_KEY], SECRET_VALUE);
    assert_server_side(call);
    for line in &recorded.argv_lines {
        assert!(
            !line.contains(SECRET_VALUE),
            "a connector secret value reached kubectl argv"
        );
    }
}

/// The owning-agent sandbox pool clone is not a connector object and nothing
/// else writes it, so it keeps the client-side apply (#5131 decision 4).
#[tokio::test]
async fn sandbox_pool_clone_apply_stays_client_side() {
    let recorded = record_sync(true).await;
    let pool = recorded.pool_applies();
    assert_eq!(
        pool.len(),
        1,
        "expected one sandbox pool clone apply, saw {}",
        pool.len()
    );
    let call = pool[0];
    assert!(
        call.kinds().iter().any(|kind| kind == "SandboxWarmPool"),
        "{:?}",
        call.kinds()
    );
    assert!(call.reads_stdin(), "{}", call.argv.join(" "));
    for flag in SERVER_SIDE_FLAGS {
        assert!(
            !call.has_flag(flag),
            "sandbox pool clone must stay client-side, found {flag}: {}",
            call.argv.join(" ")
        );
    }
}

/// Secondary path: a connector with no owned Secret still writes its objects
/// with the server-side apply, since the reconciler can create them too.
#[tokio::test]
async fn connector_without_owned_secret_applies_server_side() {
    let recorded = record_sync(false).await;
    let connector = recorded.connector_applies();
    assert_eq!(
        connector.len(),
        1,
        "expected exactly one connector List apply, saw {}",
        connector.len()
    );
    let call = connector[0];
    let kinds = call.kinds();
    assert!(!kinds.iter().any(|k| k == "Secret"), "{kinds:?}");
    assert!(kinds.iter().any(|k| k == "Deployment"), "{kinds:?}");
    assert_server_side(call);
}
