//! Binary contract for the self contained SRE bot observability installer.
//!
//! Every case drives the released command surface with recording `kubectl` and
//! `helm` peers. The cluster capacity cases stop at the Helm boundary on
//! purpose: they prove the preflight permits or refuses mutation without
//! replacing Helm, Kubernetes, or the platform API with internal mocks.

mod support;

use std::fs;
use std::io::{Cursor, Read};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use flate2::read::GzDecoder;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use support::{serve, MockServer, Response};

const OBSERVABILITY_NAMESPACE: &str = "observability";
const FIRST_RELEASE: &str = "grafana";
const REQUIRED_MIB: u64 = 1312;
const TEMPO_TAGGED_IMAGE: &str = "ghcr.io/curie-eng/curie-sre-bot-tempo:0.8.0";
const REGISTRY_INDEX: &str =
    r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[]}"#;
const REGISTRY_INDEX_WITHOUT_REQUIRED_FIELDS: &str =
    r#"{"mediaType":"application/vnd.oci.image.index.v1+json"}"#;
const AGENT_ID: &str = "00000000-0000-0000-0000-000000000001";
const VERSION_ID: &str = "00000000-0000-0000-0000-000000000002";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cli has a repository parent")
        .to_path_buf()
}

fn write_exec(dir: &Path, name: &str, body: &str) {
    let path = dir.join(name);
    fs::write(&path, body).unwrap_or_else(|error| panic!("write {name}: {error}"));
    let mut permissions = fs::metadata(&path)
        .unwrap_or_else(|error| panic!("read {name} metadata: {error}"))
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions)
        .unwrap_or_else(|error| panic!("make {name} executable: {error}"));
}

struct Fixture {
    _temp: tempfile::TempDir,
    bin_dir: PathBuf,
    helm_log: PathBuf,
    kubectl_log: PathBuf,
    action_log: PathBuf,
    grafana_stdin: PathBuf,
    connector_stdin: PathBuf,
    nodes: String,
    pods: String,
    nodes_mode: &'static str,
    pods_mode: &'static str,
    helm_mode: &'static str,
    grafana_secret_mode: &'static str,
    reader_token_mode: &'static str,
    helm_values: String,
    api: MockServer,
    registry: MockServer,
    registry_endpoint: String,
}

impl Fixture {
    fn new(nodes: Value, pods: Value) -> Self {
        Self::with_modes(nodes, pods, "success", "success", "stop")
    }

    fn with_modes(
        nodes: Value,
        pods: Value,
        nodes_mode: &'static str,
        pods_mode: &'static str,
        helm_mode: &'static str,
    ) -> Self {
        let temp = tempfile::tempdir().expect("temporary directory");
        let bin_dir = temp.path().join("bin");
        fs::create_dir(&bin_dir).expect("create fake binary directory");
        let helm_log = temp.path().join("helm.log");
        let kubectl_log = temp.path().join("kubectl.log");
        let action_log = temp.path().join("actions.log");
        let grafana_stdin = temp.path().join("grafana.stdin");
        let connector_stdin = temp.path().join("connector.stdin");

        write_exec(
            &bin_dir,
            "kubectl",
            r#"#!/bin/sh
printf '%s\n' "$*" >> "$CURIE_TEST_KUBECTL_LOG"
printf 'KUBECTL %s\n' "$*" >> "$CURIE_TEST_ACTION_LOG"

case " $* " in
    *" get nodes "*|*" get node "*)
        case "$CURIE_TEST_NODES_MODE" in
            success) printf '%s\n' "$CURIE_TEST_NODES_JSON" ;;
            malformed) printf '%s\n' '{"apiVersion":"v1","items":[' ;;
            failure)
                printf '%s\n' 'Error from server (Forbidden): nodes is forbidden' >&2
                exit 1
                ;;
        esac
        exit 0
        ;;
    *" get pods "*|*" get pod "*)
        case "$CURIE_TEST_PODS_MODE" in
            success) printf '%s\n' "$CURIE_TEST_PODS_JSON" ;;
            malformed) printf '%s\n' '{"apiVersion":"v1","items":[{"spec":' ;;
            failure)
                printf '%s\n' 'Error from server (Forbidden): pods is forbidden' >&2
                exit 1
                ;;
        esac
        exit 0
        ;;
    *" get statefulset "*|*" get statefulsets "*)
        printf '%s\n' '{"apiVersion":"v1","items":[],"kind":"List","metadata":{"resourceVersion":""}}'
        exit 0
        ;;
    *" get namespace "*)
        exit 0
        ;;
    *" get priorityclass "*|*" get priorityclasses "*)
        exit 0
        ;;
    *" get secret grafana-admin "*)
        case "$CURIE_TEST_GRAFANA_SECRET_MODE" in
            existing) printf '%s\n' '{"apiVersion":"v1","kind":"Secret","metadata":{"name":"grafana-admin"}}' ;;
            absent|apply-failure|migrate|migration-source-failure)
                printf '%s\n' 'Error from server (NotFound): secrets "grafana-admin" not found' >&2
                exit 1
                ;;
            read-failure)
                printf '%s\n' 'Error from server (Forbidden): secrets "grafana-admin" is forbidden' >&2
                exit 1
                ;;
        esac
        exit 0
        ;;
    *" get deployment,statefulset "*" app.kubernetes.io/instance=grafana "*)
        printf '%s\n' '{"apiVersion":"v1","kind":"List","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"grafana"},"spec":{"template":{"spec":{"containers":[{"name":"grafana","env":[{"name":"GF_SECURITY_ADMIN_USER","valueFrom":{"secretKeyRef":{"name":"grafana","key":"admin-user"}}},{"name":"GF_SECURITY_ADMIN_PASSWORD","valueFrom":{"secretKeyRef":{"name":"grafana","key":"admin-password"}}}] }]}}}}]}'
        exit 0
        ;;
    *" get secret grafana "*)
        if [ "$CURIE_TEST_GRAFANA_SECRET_MODE" = "migration-source-failure" ]; then
            printf '%s\n' 'Error from server (Forbidden): secrets "grafana" is forbidden' >&2
            exit 1
        fi
        printf '%s\n' '{"apiVersion":"v1","kind":"Secret","metadata":{"name":"grafana"},"data":{"admin-user":"bWlncmF0ZWQtYWRtaW4=","admin-password":"cHc="}}'
        exit 0
        ;;
    *" wait "*" secret/sre-bot-reader-token "*)
        case "$CURIE_TEST_READER_TOKEN_MODE" in
            success|read-failure) exit 0 ;;
            timeout)
                printf '%s\n' 'error: timed out waiting for the condition' >&2
                exit 1
                ;;
        esac
        ;;
    *" get secret sre-bot-reader-token "*)
        case "$CURIE_TEST_READER_TOKEN_MODE" in
            success)
                printf '%s\n' '{"apiVersion":"v1","kind":"Secret","data":{"ca.crt":"Zml4dHVyZS1jYQ==","token":"YWJj"}}'
                exit 0
                ;;
            read-failure)
                printf '%s\n' 'Error from server (Forbidden): secrets "sre-bot-reader-token" is forbidden' >&2
                exit 1
                ;;
        esac
        ;;
    *" -n curie apply -f - "*)
        cat > "$CURIE_TEST_CONNECTOR_STDIN"
        printf '%s\n' 'connector objects configured'
        exit 0
        ;;
    *" apply -f - "*)
        cat > "$CURIE_TEST_GRAFANA_STDIN"
        if [ "$CURIE_TEST_GRAFANA_SECRET_MODE" = "apply-failure" ]; then
            printf '%s\n' 'Error from server (Forbidden): cannot apply grafana-admin' >&2
            exit 1
        fi
        printf '%s\n' 'secret/grafana-admin configured'
        exit 0
        ;;
    *" -n curie get deployment "*" app.kubernetes.io/instance=curie "*)
        printf '%s\n' 'curie'
        exit 0
        ;;
    *" -n curie delete deployment,service,networkpolicy,secret "*)
        exit 0
        ;;
    *" apply "*|*" rollout status "*)
        exit 0
        ;;
    *" get secret "*" app.kubernetes.io/instance=curie "*)
        printf '%s\n' 'curie-secrets'
        exit 0
        ;;
    *" get secret curie-secrets "*)
        printf '%s\n' 'test-api-key'
        exit 0
        ;;
    *" get secret "*)
        printf '%s\n' '{"data":{"API_KEY":"aw=="}}'
        exit 0
        ;;
esac

printf 'unexpected kubectl invocation: %s\n' "$*" >&2
exit 64
"#,
        );

        write_exec(
            &bin_dir,
            "helm",
            r#"#!/bin/sh
printf '%s\n' "$*" >> "$CURIE_TEST_HELM_LOG"
printf 'HELM %s\n' "$*" >> "$CURIE_TEST_ACTION_LOG"

if [ "$1" = "get" ] && [ "$2" = "values" ]; then
    if [ "${CURIE_TEST_HELM_VALUES:-absent}" = "absent" ]; then
        printf '%s\n' 'Error: release: not found' >&2
        exit 1
    fi
    printf '%s\n' "$CURIE_TEST_HELM_VALUES"
    exit 0
fi

if [ "$1" = "status" ] && [ "$2" = "grafana" ]; then
    case "$CURIE_TEST_GRAFANA_SECRET_MODE" in
        migrate|migration-source-failure)
            printf '%s\n' '{"name":"grafana","namespace":"observability","info":{"status":"deployed"}}'
            exit 0
            ;;
        *)
            printf '%s\n' 'Error: release: not found' >&2
            exit 1
            ;;
    esac
fi

if [ "$1" = "repo" ]; then
    exit 0
fi

if [ "$1" = "template" ]; then
    exit 0
fi

if [ "$1" = "upgrade" ] && [ "$2" = "--install" ]; then
    case "$CURIE_TEST_HELM_MODE" in
        timeout)
            printf '%s\n' 'Error: UPGRADE FAILED: context deadline exceeded' >&2
            exit 1
            ;;
        stop)
            printf '%s\n' 'intentional fixture stop after Helm mutation boundary' >&2
            exit 42
            ;;
        success)
            exit 0
            ;;
    esac
fi

if [ "$1" = "upgrade" ] && [ "$2" = "curie" ]; then
    exit 0
fi

printf 'unexpected helm invocation: %s\n' "$*" >&2
exit 64
"#,
        );

        let api = serve(
            |request| match (request.method.as_str(), request.path.as_str()) {
                ("GET", "/agents") => Response::json(200, "[]"),
                ("POST", "/agents") => Response::json(
                    201,
                    &format!(
                        r##"{{"id":"{AGENT_ID}","name":"sre-bot","channels":[{{"kind":"slack","address":"#local-dev"}}],"created_at":"2026-08-21T00:00:00Z"}}"##
                    ),
                ),
                ("POST", path) if path == format!("/agents/{AGENT_ID}/versions") => Response::json(
                    201,
                    &format!(
                        r#"{{"id":"{VERSION_ID}","agent_id":"{AGENT_ID}","version_label":"0.1.0-test","bundle_ref":null,"bundle_sha256":null,"created_by":"tester","created_at":"2026-08-21T00:00:00Z"}}"#
                    ),
                ),
                ("PUT", path)
                    if path == format!("/agents/{AGENT_ID}/versions/{VERSION_ID}/bundle") =>
                {
                    Response::json(
                        201,
                        &format!(
                            r#"{{"version_id":"{VERSION_ID}","bundle_ref":"bundles/sre-bot.tar.gz","bundle_sha256":"fixture-digest","size_bytes":512}}"#
                        ),
                    )
                }
                ("POST", "/deployments") => Response::json(
                    201,
                    &format!(
                        r#"{{"id":"00000000-0000-0000-0000-000000000003","agent_id":"{AGENT_ID}","version_id":"{VERSION_ID}","environment":"dev","status":"active","deployed_at":"2026-08-21T00:00:00Z"}}"#
                    ),
                ),
                ("GET", path)
                    if path.starts_with(&format!(
                        "/agents/{AGENT_ID}/versions/{VERSION_ID}/connectors?"
                    )) =>
                {
                    Response::json(
                        200,
                        r#"{"manifests":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"curie-sre-bot-kubernetes"}}],"owned_secret_name":"curie-sre-bot-connector-secrets","owned_secret_keys":["K8S_READONLY_KUBECONFIG"],"mcp_entries":{"kubernetes":{"url":"http://curie-sre-bot-kubernetes.curie.svc.cluster.local:8000/mcp"}}}"#,
                    )
                }
                _ => Response::json(500, r#"{"error":"unexpected API request"}"#),
            },
        );
        let registry = serve(|request| {
            if request.path.starts_with("/failure/") {
                return Response::json(503, r#"{"error":"registry unavailable"}"#);
            }
            if request.path.starts_with("/token?")
                || request.path.starts_with("/wrong-shape/token?")
            {
                return Response::json(200, r#"{"token":"anonymous-pull-token"}"#);
            }
            if request.path == "/wrong-shape/v2/curie-eng/curie-sre-bot-tempo/manifests/0.8.0" {
                return Response {
                    status: 200,
                    content_type: "application/vnd.oci.image.index.v1+json".into(),
                    body: REGISTRY_INDEX_WITHOUT_REQUIRED_FIELDS.as_bytes().to_vec(),
                };
            }
            if request.path == "/v2/curie-eng/curie-sre-bot-tempo/manifests/0.8.0" {
                return Response {
                    status: 200,
                    content_type: "application/vnd.oci.image.index.v1+json".into(),
                    body: REGISTRY_INDEX.as_bytes().to_vec(),
                };
            }
            Response::json(404, r#"{"error":"not found"}"#)
        });
        let registry_endpoint = registry.base_url.clone();

        Self {
            _temp: temp,
            bin_dir,
            helm_log,
            kubectl_log,
            action_log,
            grafana_stdin,
            connector_stdin,
            nodes: nodes.to_string(),
            pods: pods.to_string(),
            nodes_mode,
            pods_mode,
            helm_mode,
            grafana_secret_mode: "existing",
            reader_token_mode: "success",
            helm_values: "absent".to_string(),
            api,
            registry,
            registry_endpoint,
        }
    }

    fn with_grafana_secret_mode(mut self, mode: &'static str) -> Self {
        self.grafana_secret_mode = mode;
        self
    }

    fn with_registry_failure(mut self) -> Self {
        self.registry_endpoint = format!("{}/failure", self.registry.base_url);
        self
    }

    fn with_registry_wrong_shape(mut self) -> Self {
        self.registry_endpoint = format!("{}/wrong-shape", self.registry.base_url);
        self
    }

    fn with_reader_token_mode(mut self, mode: &'static str) -> Self {
        self.reader_token_mode = mode;
        self
    }

    fn with_helm_values(mut self, values: Value) -> Self {
        self.helm_values = values.to_string();
        self
    }

    fn run(&self, extra: &[&str]) -> Output {
        self.run_from(extra, &repo_root(), None)
    }

    fn run_from(&self, extra: &[&str], current_dir: &Path, release_cache: Option<&Path>) -> Output {
        let mut paths = vec![self.bin_dir.clone()];
        if let Some(current) = std::env::var_os("PATH") {
            paths.extend(std::env::split_paths(&current));
        }
        let path = std::env::join_paths(paths).expect("join PATH");

        let mut args = vec![
            "--color",
            "never",
            "example",
            "sre-bot",
            "install",
            "--observability",
        ];
        args.extend_from_slice(extra);

        let mut command = Command::new(bin());
        command
            .current_dir(current_dir)
            .args(args)
            .env("PATH", path)
            .env("CI", "1")
            .env("TERM", "dumb")
            .env("NO_COLOR", "1")
            .env("CURIE_API_URL", &self.api.base_url)
            .env("CURIE_TEST_HELM_LOG", &self.helm_log)
            .env("CURIE_TEST_KUBECTL_LOG", &self.kubectl_log)
            .env("CURIE_TEST_ACTION_LOG", &self.action_log)
            .env("CURIE_TEST_GRAFANA_STDIN", &self.grafana_stdin)
            .env("CURIE_TEST_CONNECTOR_STDIN", &self.connector_stdin)
            .env("CURIE_TEST_NODES_JSON", &self.nodes)
            .env("CURIE_TEST_PODS_JSON", &self.pods)
            .env("CURIE_TEST_NODES_MODE", self.nodes_mode)
            .env("CURIE_TEST_PODS_MODE", self.pods_mode)
            .env("CURIE_TEST_HELM_MODE", self.helm_mode)
            .env("CURIE_TEST_HELM_VALUES", &self.helm_values)
            .env("CURIE_TEST_GRAFANA_SECRET_MODE", self.grafana_secret_mode)
            .env("CURIE_TEST_READER_TOKEN_MODE", self.reader_token_mode)
            .env(
                "CURIE_TEST_SRE_BOT_REGISTRY_ENDPOINT",
                &self.registry_endpoint,
            )
            .env_remove("CURIE_API_KEY")
            .env_remove("CURIE_CREDENTIALS")
            .env_remove("CURIE_MODEL_CREDENTIALS")
            .env_remove("CURIE_GITHUB_TOKEN")
            .env_remove("CURIE_MODEL")
            .env_remove("GRAFANA_ADMIN_PASSWORD")
            .env_remove("GRAFANA_SERVICE_ACCOUNT_TOKEN");
        if let Some(cache) = release_cache {
            command
                .env("CURIE_TEST_ARTIFACT_CHANNEL", "release")
                .env("XDG_CACHE_HOME", cache);
        } else {
            command.env_remove("CURIE_TEST_ARTIFACT_CHANNEL");
        }
        command.output().expect("run SRE bot example installer")
    }

    fn helm_calls(&self) -> Vec<String> {
        lines(&self.helm_log)
    }

    fn kubectl_calls(&self) -> Vec<String> {
        lines(&self.kubectl_log)
    }

    fn kubectl_stdin(&self) -> Vec<u8> {
        fs::read(&self.grafana_stdin).unwrap_or_default()
    }

    fn connector_stdin(&self) -> Vec<u8> {
        fs::read(&self.connector_stdin).unwrap_or_default()
    }

    fn actions(&self) -> Vec<String> {
        lines(&self.action_log)
    }
}

fn lines(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

fn shown(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn expected_tempo_digest() -> String {
    let hex = Sha256::digest(REGISTRY_INDEX.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("sha256:{hex}")
}

fn uploaded_bundle_file(fixture: &Fixture, wanted: &str) -> Vec<u8> {
    let upload = fixture
        .api
        .recorded()
        .into_iter()
        .find(|request| request.method == "PUT" && request.path.ends_with("/bundle"))
        .unwrap_or_else(|| panic!("deploy must upload the embedded bundle"));
    let gzip_start = upload
        .body
        .windows(2)
        .position(|window| window == [0x1f, 0x8b])
        .expect("multipart upload must contain a gzip archive");
    let decoder = GzDecoder::new(Cursor::new(&upload.body[gzip_start..]));
    let mut archive = tar::Archive::new(decoder);
    for entry in archive.entries().expect("read uploaded bundle archive") {
        let mut entry = entry.expect("read uploaded bundle entry");
        if entry.path().expect("bundle path").as_ref() == Path::new(wanted) {
            let mut contents = Vec::new();
            entry
                .read_to_end(&mut contents)
                .expect("read uploaded bundle file");
            return contents;
        }
    }
    panic!("uploaded bundle must contain {wanted}");
}

fn node(name: &str, memory: &str, ready: bool) -> Value {
    json!({
        "metadata": {"name": name},
        "status": {
            "allocatable": {"memory": memory},
            "conditions": [{
                "type": "Ready",
                "status": if ready { "True" } else { "False" }
            }]
        }
    })
}

fn nodes(items: Vec<Value>) -> Value {
    json!({"apiVersion": "v1", "kind": "List", "items": items})
}

fn pods(items: Vec<Value>) -> Value {
    json!({"apiVersion": "v1", "kind": "List", "items": items})
}

fn pod(name: &str, node_name: Option<&str>, phase: &str, containers: Value) -> Value {
    let mut spec = json!({"containers": containers});
    if let Some(node_name) = node_name {
        spec["nodeName"] = json!(node_name);
    }
    json!({
        "metadata": {"name": name, "namespace": "fixture"},
        "spec": spec,
        "status": {"phase": phase}
    })
}

fn labeled_pod(name: &str, node_name: &str, namespace: &str, labels: Value, memory: &str) -> Value {
    let mut value = pod(
        name,
        Some(node_name),
        "Running",
        json!([memory_container("app", memory)]),
    );
    value["metadata"]["namespace"] = json!(namespace);
    value["metadata"]["labels"] = labels;
    value
}

fn managed_stack_pods(node_name: &str) -> Vec<Value> {
    vec![
        labeled_pod(
            "grafana",
            node_name,
            OBSERVABILITY_NAMESPACE,
            json!({"app.kubernetes.io/instance": "grafana"}),
            "128Mi",
        ),
        labeled_pod(
            "loki",
            node_name,
            OBSERVABILITY_NAMESPACE,
            json!({"app.kubernetes.io/instance": "loki"}),
            "256Mi",
        ),
        labeled_pod(
            "alloy",
            node_name,
            OBSERVABILITY_NAMESPACE,
            json!({"app.kubernetes.io/instance": "alloy"}),
            "128Mi",
        ),
        labeled_pod(
            "tempo",
            node_name,
            OBSERVABILITY_NAMESPACE,
            json!({"app.kubernetes.io/name": "tempo"}),
            "192Mi",
        ),
        labeled_pod(
            "prometheus-server",
            node_name,
            OBSERVABILITY_NAMESPACE,
            json!({"app.kubernetes.io/instance": "prometheus"}),
            "512Mi",
        ),
        labeled_pod(
            "kube-state-metrics",
            node_name,
            OBSERVABILITY_NAMESPACE,
            json!({"app.kubernetes.io/instance": "prometheus"}),
            "64Mi",
        ),
        labeled_pod(
            "node-exporter",
            node_name,
            OBSERVABILITY_NAMESPACE,
            json!({"app.kubernetes.io/instance": "prometheus"}),
            "32Mi",
        ),
    ]
}

fn memory_container(name: &str, memory: &str) -> Value {
    json!({"name": name, "resources": {"requests": {"memory": memory}}})
}

fn assert_reached_helm_upgrade(fixture: &Fixture, output: &Output) -> String {
    let calls = fixture.helm_calls();
    assert!(
        calls
            .iter()
            .any(|call| call.starts_with("upgrade --install ")),
        "capacity should pass through to the Helm mutation boundary\nstdout:\n{}\nstderr:\n{}\nhelm calls: {calls:?}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let text = shown(output);
    assert!(
        !text.contains("required 1312Mi") && !text.contains("available"),
        "a cluster with enough capacity must not be refused: {text}"
    );
    text
}

fn assert_refused_before_helm(fixture: &Fixture, output: &Output) -> String {
    let text = shown(output);
    assert_eq!(
        output.status.code(),
        Some(1),
        "a failed capacity read is a normal runtime refusal: {text}"
    );
    assert!(
        fixture.helm_calls().is_empty(),
        "capacity must be known before any Helm command: {:?}",
        fixture.helm_calls()
    );
    assert!(
        fixture.api.recorded().is_empty(),
        "capacity refusal must make no platform API mutation"
    );
    text
}

#[test]
fn clap_routes_the_one_command_and_exposes_no_operator_configuration_or_credential_flags() {
    let output = Command::new(bin())
        .args(["example", "sre-bot", "install", "--help"])
        .output()
        .expect("run example installer help");
    let text = shown(&output);
    assert!(output.status.success(), "new command must parse: {text}");
    assert!(
        text.contains("--observability"),
        "the install surface must expose the one observability flag: {text}"
    );
    assert!(
        text.contains("--slack-channel"),
        "the install surface must permit the optional Slack binding: {text}"
    );
    for forbidden in [
        "--values",
        "--file",
        "--namespace",
        "--release",
        "--api-key",
        "--grafana-token",
        "--service-account-token",
        "--model",
        "--credentials",
    ] {
        assert!(
            !text.contains(forbidden),
            "the zero configuration command must not ask for {forbidden}: {text}"
        );
    }
}

#[test]
fn json_dry_run_is_one_object_and_orders_apply_before_bundle_deploy() {
    let fixture = Fixture::new(nodes(vec![node("node-a", "4Gi", true)]), pods(vec![]));
    let output = fixture.run(&["--dry-run", "--json"]);
    let text = shown(&output);
    assert!(output.status.success(), "dry run must succeed: {text}");

    let documents: Vec<Value> = serde_json::Deserializer::from_slice(&output.stdout)
        .into_iter::<Value>()
        .collect::<Result<_, _>>()
        .unwrap_or_else(|error| panic!("dry run stdout must contain only JSON: {error}; {text}"));
    assert_eq!(
        documents.len(),
        1,
        "dry run must emit exactly one JSON object to stdout"
    );
    let object = documents[0]
        .as_object()
        .expect("dry run JSON must be an object");
    let plan = object
        .get("plan")
        .and_then(Value::as_array)
        .expect("dry run object must carry its ordered plan");
    let lines: Vec<&str> = plan
        .iter()
        .map(|line| line.as_str().expect("plan entries are strings"))
        .collect();
    let expected_repositories = [
        (
            "grafana-community",
            "https://grafana-community.github.io/helm-charts",
        ),
        ("grafana", "https://grafana.github.io/helm-charts"),
        (
            "prometheus-community",
            "https://prometheus-community.github.io/helm-charts",
        ),
    ];
    for (alias, url) in expected_repositories {
        assert!(
            lines
                .iter()
                .any(|line| line == &format!("helm repo add {alias} {url} --force-update")),
            "plan must configure the exact {alias} repository: {lines:?}"
        );
    }
    assert!(
        lines.iter().any(|line| {
            *line == "helm repo update grafana-community grafana prometheus-community"
        }),
        "plan must update every configured repository: {lines:?}"
    );
    let expected_upgrades = [
        ("grafana", "grafana-community/grafana", "12.11.1"),
        ("loki", "grafana-community/loki", "18.10.1"),
        ("alloy", "grafana/alloy", "1.11.1"),
        ("prometheus", "prometheus-community/prometheus", "29.27.0"),
    ];
    let mut previous = None;
    for (release, chart, version) in expected_upgrades {
        let index = lines
            .iter()
            .position(|line| {
                line.contains(&format!("helm upgrade --install {release} {chart}"))
                    && line.contains(&format!("--version {version}"))
                    && line.contains("--namespace observability")
            })
            .unwrap_or_else(|| panic!("plan must contain pinned {release} upgrade: {lines:?}"));
        if let Some(previous) = previous {
            assert!(
                previous < index,
                "upstream release order drifted: {lines:?}"
            );
        }
        previous = Some(index);
    }
    let tempo = lines
        .iter()
        .position(|line| {
            line.contains("kubectl apply")
                && line.contains("--namespace observability")
                && line.contains("tempo.yaml")
        })
        .unwrap_or_else(|| panic!("plan must apply embedded Tempo: {lines:?}"));
    let tempo_ready = lines
        .iter()
        .position(|line| {
            line.contains("kubectl rollout status statefulset/tempo")
                && line.contains("--namespace observability")
                && line.contains("--timeout=")
        })
        .unwrap_or_else(|| panic!("plan must wait for Tempo readiness: {lines:?}"));
    let curie_apply = lines
        .iter()
        .position(|line| {
            line.contains("helm upgrade --install curie")
                && line.contains(" -n curie ")
                && !line.contains("curie-values.yaml")
        })
        .unwrap_or_else(|| {
            panic!("plan must run the guarded Curie installation planner: {lines:?}")
        });
    let curie_integration = lines
        .iter()
        .position(|line| {
            line.contains("helm upgrade curie")
                && !line.contains("--install")
                && line.contains("--reuse-values")
                && line.contains("curie-values.yaml")
                && line.contains("--wait")
                && line.contains("--timeout 10m")
        })
        .unwrap_or_else(|| {
            panic!("plan must add Curie integration values after guarded apply: {lines:?}")
        });
    let read_access = lines
        .iter()
        .position(|line| {
            line.contains("kubectl apply") && line.contains("manifests/read-access.yaml")
        })
        .unwrap_or_else(|| panic!("plan must apply the read only connector RBAC: {lines:?}"));
    let token_wait = lines
        .iter()
        .position(|line| {
            line.contains("kubectl wait")
                && line.contains("secret/sre-bot-reader-token")
                && line.contains("--timeout=2m")
        })
        .unwrap_or_else(|| panic!("plan must wait boundedly for the reader token: {lines:?}"));
    let deploy = lines
        .iter()
        .position(|line| line.contains("deploy") && line.contains("sre-bot"))
        .unwrap_or_else(|| panic!("plan must contain SRE bot bundle deployment: {lines:?}"));
    let connector_sync = lines
        .iter()
        .position(|line| line.contains("render and reconcile") && line.contains("connectors"))
        .unwrap_or_else(|| panic!("plan must reconcile connectors after deploy: {lines:?}"));
    assert!(
        previous.is_some_and(|previous| previous < tempo)
            && tempo < tempo_ready
            && tempo_ready < curie_apply
            && curie_apply < curie_integration
            && curie_integration < read_access
            && read_access < token_wait
            && token_wait < deploy
            && deploy < connector_sync,
        "the stack, guarded platform apply, integration, RBAC, deploy, and connector sync order drifted: {lines:?}"
    );
    let plan_text = lines.join("\n").to_ascii_lowercase();
    for forbidden in [
        "grafana_service_account_token=",
        "grafana-service-account-token=",
        "--api-key",
        "--credentials",
    ] {
        assert!(
            !plan_text.contains(forbidden),
            "dry run must not carry operator credential input or token material: {lines:?}"
        );
    }
    assert!(
        fixture.api.recorded().is_empty(),
        "dry run must not mutate the platform API"
    );
    assert!(
        lines.iter().any(|line| {
            line.contains(TEMPO_TAGGED_IMAGE)
                && line.to_ascii_lowercase().contains("immutable")
                && line.to_ascii_lowercase().contains("digest")
        }),
        "dry run must disclose immutable Tempo image resolution: {lines:?}"
    );
    assert!(
        fixture.registry.recorded().is_empty(),
        "dry run must not contact the image registry"
    );
    assert!(
        !fixture
            .helm_calls()
            .iter()
            .any(|call| call.starts_with("upgrade --install ")),
        "dry run must not execute Helm upgrade"
    );
}

#[test]
fn small_node_names_required_and_available_memory_before_any_mutation() {
    let existing = pod(
        "existing",
        Some("node-a"),
        "Running",
        json!([memory_container("app", "1Gi")]),
    );
    let fixture = Fixture::new(
        nodes(vec![node("node-a", "2Gi", true)]),
        pods(vec![existing]),
    );
    let output = fixture.run(&[]);
    let text = assert_refused_before_helm(&fixture, &output);
    assert!(
        text.contains(&format!("required {REQUIRED_MIB}Mi")),
        "refusal must name the pinned stack footprint: {text}"
    );
    assert!(
        text.contains("available 1024Mi"),
        "refusal must name the measured remaining capacity: {text}"
    );
    assert!(
        text.contains("curie example sre-bot install --observability"),
        "refusal must name the command whose prerequisite failed: {text}"
    );
    assert!(
        fixture.registry.recorded().is_empty() && fixture.kubectl_stdin().is_empty(),
        "capacity refusal must precede registry access and Secret mutation"
    );
}

#[test]
fn absent_grafana_admin_secret_is_generated_via_stdin_without_credential_exposure() {
    let fixture = Fixture::new(nodes(vec![node("node-a", "4Gi", true)]), pods(vec![]))
        .with_grafana_secret_mode("absent");
    let output = fixture.run(&[]);
    assert_reached_helm_upgrade(&fixture, &output);

    let apply_calls = fixture
        .kubectl_calls()
        .into_iter()
        .filter(|call| call.contains("apply -f -"))
        .collect::<Vec<_>>();
    assert_eq!(
        apply_calls,
        ["apply -f -"],
        "a missing admin Secret must be created through stdin exactly once"
    );
    let manifest: Value = serde_json::from_slice(&fixture.kubectl_stdin())
        .expect("Grafana admin Secret stdin must be JSON");
    assert_eq!(manifest["kind"], "Secret");
    assert_eq!(manifest["metadata"]["name"], "grafana-admin");
    assert_eq!(manifest["metadata"]["namespace"], OBSERVABILITY_NAMESPACE);
    assert_eq!(manifest["stringData"]["admin-user"], "admin");
    let password = manifest["stringData"]["admin-password"]
        .as_str()
        .expect("generated admin password is a string");
    assert_eq!(password.len(), 64, "password must contain 32 random bytes");
    assert!(
        password
            .chars()
            .all(|character| character.is_ascii_hexdigit())
            && password == password.to_ascii_lowercase(),
        "generated admin password must be lowercase hex"
    );
    let observable = format!(
        "{}\n{:?}\n{:?}",
        shown(&output),
        fixture.kubectl_calls(),
        fixture.helm_calls()
    );
    assert!(
        !observable.contains(password),
        "generated credentials must never reach argv, logs, stdout, or stderr"
    );
}

#[test]
fn existing_grafana_admin_secret_is_preserved_without_apply() {
    let fixture = Fixture::new(nodes(vec![node("node-a", "4Gi", true)]), pods(vec![]));
    let output = fixture.run(&[]);
    assert_reached_helm_upgrade(&fixture, &output);
    let calls = fixture.kubectl_calls();
    assert!(
        calls
            .iter()
            .any(|call| call.contains("get secret grafana-admin")),
        "installer must inspect the migration Secret: {calls:?}"
    );
    assert!(
        !calls.iter().any(|call| call.contains("apply -f -")) && fixture.kubectl_stdin().is_empty(),
        "an existing Grafana admin Secret must not be replaced: {calls:?}"
    );
}

#[test]
fn existing_grafana_release_migrates_its_live_admin_credential_without_exposure() {
    let fixture = Fixture::new(nodes(vec![node("node-a", "4Gi", true)]), pods(vec![]))
        .with_grafana_secret_mode("migrate");
    let output = fixture.run(&[]);
    assert_reached_helm_upgrade(&fixture, &output);

    let manifest: Value = serde_json::from_slice(&fixture.kubectl_stdin())
        .expect("migrated Grafana admin Secret stdin must be JSON");
    assert_eq!(manifest["metadata"]["name"], "grafana-admin");
    assert_eq!(manifest["metadata"]["namespace"], OBSERVABILITY_NAMESPACE);
    assert_eq!(manifest["data"]["admin-user"], "bWlncmF0ZWQtYWRtaW4=");
    assert_eq!(manifest["data"]["admin-password"], "cHc=");
    assert!(manifest.get("stringData").is_none());

    let calls = fixture.kubectl_calls();
    let target_read = calls
        .iter()
        .position(|call| call.contains("get secret grafana-admin"))
        .expect("installer must inspect the target Secret");
    let workload_read = calls
        .iter()
        .position(|call| {
            call.contains("get deployment,statefulset")
                && call.contains("app.kubernetes.io/instance=grafana")
        })
        .expect("migration must discover the credential source from the live workload");
    let source_read = calls
        .iter()
        .position(|call| call.contains("get secret grafana --namespace observability"))
        .expect("migration must read the currently mounted admin Secret");
    let apply = calls
        .iter()
        .position(|call| call.contains("apply -f -"))
        .expect("migration must create grafana-admin through private stdin");
    assert!(target_read < workload_read && workload_read < source_read && source_read < apply);

    let observable = format!(
        "{}\n{:?}\n{:?}",
        shown(&output),
        fixture.kubectl_calls(),
        fixture.helm_calls()
    );
    for secret in ["migrated-admin", "pw"] {
        assert!(
            !observable.contains(secret),
            "migrated credentials must not reach argv, logs, stdout, or stderr"
        );
    }
}

#[test]
fn existing_grafana_release_with_unreadable_live_credential_fails_closed() {
    let fixture = Fixture::new(nodes(vec![node("node-a", "4Gi", true)]), pods(vec![]))
        .with_grafana_secret_mode("migration-source-failure");
    let output = fixture.run(&[]);
    let text = shown(&output);

    assert_eq!(output.status.code(), Some(1), "migration must fail: {text}");
    assert!(
        text.contains("could not read the existing Grafana admin credential"),
        "failure must name the blocked migration without exposing values: {text}"
    );
    assert!(fixture.kubectl_stdin().is_empty());
    assert!(
        fixture
            .helm_calls()
            .iter()
            .all(|call| !call.starts_with("upgrade")),
        "an unreadable migration credential must stop before Helm mutation"
    );
}

#[test]
fn grafana_admin_secret_read_failure_stops_before_mutation() {
    let fixture = Fixture::new(nodes(vec![node("node-a", "4Gi", true)]), pods(vec![]))
        .with_grafana_secret_mode("read-failure");
    let output = fixture.run(&[]);
    let text = shown(&output);
    assert_eq!(
        output.status.code(),
        Some(1),
        "Secret read must fail: {text}"
    );
    assert!(
        text.contains("could not inspect Secret grafana-admin"),
        "failure must name the unreadable prerequisite: {text}"
    );
    assert!(fixture.helm_calls().is_empty());
    assert!(fixture.api.recorded().is_empty());
    assert!(fixture.kubectl_stdin().is_empty());
    assert!(
        fixture
            .kubectl_calls()
            .iter()
            .all(|call| !call.contains("apply") && !call.contains("create")),
        "an ambiguous Secret read must fail closed before mutation"
    );
}

#[test]
fn grafana_admin_secret_creation_failure_stops_before_helm_without_leaking_password() {
    let fixture = Fixture::new(nodes(vec![node("node-a", "4Gi", true)]), pods(vec![]))
        .with_grafana_secret_mode("apply-failure");
    let output = fixture.run(&[]);
    let manifest: Value = serde_json::from_slice(&fixture.kubectl_stdin())
        .expect("failed apply still receives the Secret manifest");
    let password = manifest["stringData"]["admin-password"]
        .as_str()
        .expect("generated password");
    let text = shown(&output);
    assert_eq!(
        output.status.code(),
        Some(1),
        "Secret failure must fail: {text}"
    );
    assert!(
        fixture
            .helm_calls()
            .iter()
            .all(|call| !call.starts_with("upgrade")),
        "Secret creation must complete before Helm mutation"
    );
    assert!(
        text.contains("grafana-admin") && !text.contains(password),
        "failure must name the Secret without exposing its credential: {text}"
    );
}

#[test]
fn tempo_index_resolution_failure_precedes_every_cluster_mutation() {
    let fixture = Fixture::new(nodes(vec![node("node-a", "4Gi", true)]), pods(vec![]))
        .with_registry_failure();
    let output = fixture.run(&[]);
    let text = shown(&output);
    assert_eq!(
        output.status.code(),
        Some(1),
        "registry failure must fail: {text}"
    );
    assert!(
        text.contains(TEMPO_TAGGED_IMAGE),
        "registry error must name the image it could not pin: {text}"
    );
    assert!(fixture.helm_calls().is_empty());
    assert!(fixture.api.recorded().is_empty());
    assert!(fixture.kubectl_stdin().is_empty());
    assert!(
        fixture
            .kubectl_calls()
            .iter()
            .all(|call| call.starts_with("get nodes") || call.starts_with("get pods")),
        "registry failure may perform capacity reads but no cluster mutation: {:?}",
        fixture.kubectl_calls()
    );
}

#[test]
fn tempo_resolution_rejects_an_incomplete_index_before_cluster_mutation() {
    let fixture = Fixture::new(nodes(vec![node("node-a", "4Gi", true)]), pods(vec![]))
        .with_registry_wrong_shape();
    let output = fixture.run(&[]);
    let text = shown(&output);
    assert_eq!(
        output.status.code(),
        Some(1),
        "invalid index must fail: {text}"
    );
    assert!(
        text.contains("expected an OCI image index"),
        "failure must identify the invalid registry representation: {text}"
    );
    assert!(fixture.helm_calls().is_empty());
    assert!(fixture.api.recorded().is_empty());
    assert!(fixture.kubectl_stdin().is_empty());
    assert!(
        fixture
            .kubectl_calls()
            .iter()
            .all(|call| call.starts_with("get nodes") || call.starts_with("get pods")),
        "an invalid index must fail before cluster mutation: {:?}",
        fixture.kubectl_calls()
    );
}

#[test]
fn successful_install_uploads_only_the_resolved_tempo_index_digest() {
    let fixture = Fixture::with_modes(
        nodes(vec![node("node-a", "4Gi", true)]),
        pods(vec![]),
        "success",
        "success",
        "success",
    );
    let output = fixture.run(&[]);
    let text = shown(&output);
    assert!(
        output.status.success(),
        "full install fixture must deploy: {text}"
    );

    let connectors = String::from_utf8(uploaded_bundle_file(&fixture, "connectors.yaml"))
        .expect("uploaded connectors are UTF-8");
    let pinned = format!(
        "ghcr.io/curie-eng/curie-sre-bot-tempo@{}",
        expected_tempo_digest()
    );
    assert!(
        connectors.contains(&pinned),
        "resolved digest must reach deploy"
    );
    assert!(
        !connectors.contains(TEMPO_TAGGED_IMAGE),
        "the declared mutable tag must never reach deploy"
    );
    let declaration: Value =
        serde_norway::from_str(&connectors).expect("uploaded connectors must remain valid YAML");
    assert_eq!(
        declaration["connectors"]["tempo"]["image"], pinned,
        "the runtime bundle must replace the local build declaration with the release image"
    );
    assert!(
        declaration["connectors"]["tempo"].get("build").is_none(),
        "the uploaded runtime declaration must not ask the cluster deploy path to build Tempo"
    );
    assert!(
        declaration["connectors"].get("k8s-write").is_none(),
        "the credential free installer must omit the gated write connector from its runtime bundle"
    );

    let plugin: Value = serde_json::from_slice(&uploaded_bundle_file(
        &fixture,
        ".claude-plugin/plugin.json",
    ))
    .expect("uploaded plugin manifest must remain valid JSON");
    assert!(
        plugin.get("approvalPolicy").is_none(),
        "the runtime bundle must remove the write gate with its omitted connector"
    );
    assert_eq!(
        plugin["description"],
        "SRE triage assistant for plain English production health and Kubernetes questions in Slack. This installer deploys read only Kubernetes, Grafana, and Tempo connectors. It omits the source bundle's gated write connector and approval policy; enable that path only through the documented explicit build and deploy flow.",
        "the runtime manifest must describe the installer bundle's read only surface"
    );

    // GitHub documents anonymous public GHCR pulls, and the Distribution token
    // specification defines the service and repository pull scope exchange:
    // https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry
    // https://distribution.github.io/distribution/spec/auth/token/
    let registry = fixture.registry.recorded();
    assert_eq!(
        registry.len(),
        2,
        "resolution must use token then index requests"
    );
    assert!(
        registry[0].path.starts_with("/token?")
            && registry[0]
                .path
                .contains("repository%3Acurie-eng%2Fcurie-sre-bot-tempo%3Apull")
    );
    assert_eq!(
        registry[1].header("authorization"),
        Some("Bearer anonymous-pull-token")
    );
    assert!(
        registry[1]
            .header("accept")
            .is_some_and(|accept| accept.contains("application/vnd.oci.image.index.v1+json")),
        "manifest request must require a multi-platform image index"
    );
}

#[test]
fn released_binary_path_uses_cached_chart_and_embedded_assets_outside_checkout() {
    let fixture = Fixture::with_modes(
        nodes(vec![node("node-a", "4Gi", true)]),
        pods(vec![]),
        "success",
        "success",
        "success",
    );
    let outside = fixture._temp.path().join("outside-source-checkout");
    fs::create_dir(&outside).expect("create working directory outside the source checkout");
    let cache = fixture._temp.path().join("release-cache");
    let chart = cache
        .join("curie")
        .join(format!("v{}", env!("CARGO_PKG_VERSION")))
        .join(format!("curie-{}.tgz", env!("CARGO_PKG_VERSION")));
    fs::create_dir_all(chart.parent().expect("release chart has a cache parent"))
        .expect("create release chart cache");
    fs::write(&chart, b"cached release chart fixture")
        .expect("seed the released chart artifact cache");

    let output = fixture.run_from(&[], &outside, Some(&cache));
    let text = shown(&output);
    assert!(
        output.status.success(),
        "released binary path must install outside a source checkout: {text}"
    );
    assert!(
        !outside.join("charts/curie").exists(),
        "the release path must not gain a source chart"
    );
    let chart_arg = chart.display().to_string();
    assert!(
        fixture.helm_calls().iter().any(|call| {
            call.starts_with("upgrade --install curie ") && call.contains(&chart_arg)
        }),
        "the guarded platform apply must use the cached released chart {chart_arg}: {:?}",
        fixture.helm_calls()
    );
    assert!(
        !fixture
            .actions()
            .iter()
            .any(|action| action.contains("charts/curie")),
        "the released command must not fall back to a source chart: {:?}",
        fixture.actions()
    );
    assert!(
        !uploaded_bundle_file(&fixture, "connectors.yaml").is_empty(),
        "the binary must deploy its embedded SRE bot bundle outside the checkout"
    );
}

#[test]
fn guarded_platform_apply_precedes_additive_integration_and_reader_access() {
    let fixture = Fixture::with_modes(
        nodes(vec![node("node-a", "4Gi", true)]),
        pods(vec![]),
        "success",
        "success",
        "success",
    )
    .with_helm_values(json!({"api": {"apiKey": "fixture-existing-api-key"}}));
    let output = fixture.run(&[]);
    let text = shown(&output);
    assert!(output.status.success(), "full install must succeed: {text}");

    let actions = fixture.actions();
    let planner = actions
        .iter()
        .position(|call| call.starts_with("HELM upgrade --install curie "))
        .unwrap_or_else(|| {
            panic!("the installer must use the installation apply planner: {actions:?}")
        });
    let integration = actions
        .iter()
        .position(|call| call.starts_with("HELM upgrade curie "))
        .unwrap_or_else(|| {
            panic!("the installer must run the additive integration upgrade: {actions:?}")
        });
    let read_access = actions
        .iter()
        .position(|call| {
            call.starts_with("KUBECTL apply -f ") && call.ends_with("manifests/read-access.yaml")
        })
        .unwrap_or_else(|| panic!("the installer must apply the reader RBAC: {actions:?}"));
    let token_wait = actions
        .iter()
        .position(|call| {
            call.contains("KUBECTL wait")
                && call.contains("secret/sre-bot-reader-token")
                && call.contains("--timeout=2m")
        })
        .unwrap_or_else(|| {
            panic!("the installer must wait boundedly for the reader token: {actions:?}")
        });
    assert!(
        planner < integration && integration < read_access && read_access < token_wait,
        "platform apply, additive integration, RBAC, and token order drifted: {actions:?}"
    );

    let planner_call = &actions[planner];
    assert!(
        !planner_call.contains("curie-values.yaml")
            && !planner_call.contains("fixture-existing-api-key"),
        "the guarded full apply must preserve existing values through its private planner path: {planner_call}"
    );
    let integration_call = &actions[integration];
    assert!(
        !integration_call.contains("--install")
            && integration_call.contains("--reuse-values")
            && integration_call.contains("curie-values.yaml")
            && integration_call.contains("--wait")
            && integration_call.contains("--timeout 10m"),
        "the integration step must be additive, bounded, and never install: {integration_call}"
    );
}

#[test]
fn reader_kubeconfig_is_owned_secret_stdin_and_connectors_reconcile_after_deploy() {
    let fixture = Fixture::with_modes(
        nodes(vec![node("node-a", "4Gi", true)]),
        pods(vec![]),
        "success",
        "success",
        "success",
    );
    let output = fixture.run(&[]);
    let text = shown(&output);
    assert!(output.status.success(), "full install must succeed: {text}");

    let requests = fixture.api.recorded();
    let deployment = requests
        .iter()
        .position(|request| request.method == "POST" && request.path == "/deployments")
        .expect("the bundle must be deployed");
    let render = requests
        .iter()
        .position(|request| {
            request.method == "GET"
                && request.path.starts_with(&format!(
                    "/agents/{AGENT_ID}/versions/{VERSION_ID}/connectors?"
                ))
        })
        .expect("the deployed version connectors must be rendered through the API");
    assert!(
        deployment < render,
        "connector rendering must happen only after deploy: {requests:?}"
    );

    let document: Value = serde_json::from_slice(&fixture.connector_stdin())
        .expect("connector reconciliation must send one JSON List on stdin");
    let items = document["items"]
        .as_array()
        .expect("connector reconciliation stdin must contain items");
    let secret = items
        .iter()
        .find(|item| item["kind"] == "Secret")
        .expect("connector reconciliation must include its owned Secret");
    assert_eq!(
        secret["metadata"]["name"],
        "curie-sre-bot-connector-secrets"
    );
    let kubeconfig = secret["stringData"]["K8S_READONLY_KUBECONFIG"]
        .as_str()
        .expect("the owned Secret must carry the generated kubeconfig");
    let config: Value = serde_json::from_str(kubeconfig).expect("kubeconfig is structured JSON");
    assert_eq!(
        config["clusters"][0]["cluster"]["server"],
        "https://kubernetes.default.svc"
    );
    assert_eq!(config["users"][0]["user"]["token"], "abc");
    assert_eq!(
        config["clusters"][0]["cluster"]["certificate-authority-data"],
        "Zml4dHVyZS1jYQ=="
    );

    let kubectl = fixture.kubectl_calls();
    assert!(
        kubectl.iter().any(|call| call == "-n curie apply -f -"),
        "rendered connector objects must be applied from stdin: {kubectl:?}"
    );
    assert!(
        kubectl.iter().any(|call| {
            call.starts_with("-n curie delete deployment,service,networkpolicy,secret ")
        }),
        "stale connector objects must be reconciled after apply: {kubectl:?}"
    );
    let observable = format!("{text}\n{kubectl:?}\n{:?}", fixture.helm_calls());
    assert!(
        !observable.contains("abc") && !observable.contains("YWJj"),
        "the generated kubeconfig credential must not reach output or argv: {observable}"
    );
}

#[test]
fn reader_token_timeout_and_read_failure_are_bounded_and_stop_before_deploy() {
    for (mode, expected) in [
        ("timeout", "was not populated within 2m"),
        ("read-failure", "could not read Secret sre-bot-reader-token"),
    ] {
        let fixture = Fixture::with_modes(
            nodes(vec![node("node-a", "4Gi", true)]),
            pods(vec![]),
            "success",
            "success",
            "success",
        )
        .with_reader_token_mode(mode);
        let output = fixture.run(&[]);
        let text = shown(&output);
        assert_eq!(
            output.status.code(),
            Some(1),
            "reader token failure must fail: {text}"
        );
        assert!(
            text.contains(expected)
                && text.contains("kubectl get secret sre-bot-reader-token -n curie"),
            "reader token failure must be actionable: {text}"
        );
        let wait = fixture
            .kubectl_calls()
            .into_iter()
            .find(|call| call.starts_with("wait "))
            .expect("reader token must use kubectl wait");
        assert!(
            wait.contains("--timeout=2m"),
            "token wait must be bounded: {wait}"
        );
        assert!(
            fixture.api.recorded().is_empty() && fixture.connector_stdin().is_empty(),
            "a reader token failure must stop before bundle deploy and connector apply"
        );
    }
}

#[test]
fn optional_slack_channel_reaches_the_same_embedded_deploy_path() {
    let fixture = Fixture::with_modes(
        nodes(vec![node("node-a", "4Gi", true)]),
        pods(vec![]),
        "success",
        "success",
        "success",
    );
    let output = fixture.run(&["--slack-channel", "C0EXAMPLE1"]);
    let text = shown(&output);
    assert!(
        output.status.success(),
        "Slack install must succeed: {text}"
    );
    let create = fixture
        .api
        .recorded()
        .into_iter()
        .find(|request| request.method == "POST" && request.path == "/agents")
        .expect("embedded deploy must create or resolve the agent");
    let body = String::from_utf8(create.body).expect("agent create request is UTF-8");
    assert!(
        body.contains("C0EXAMPLE1"),
        "the example Slack channel must reach the normal deploy request: {body}"
    );
}

#[test]
fn exactly_required_memory_and_more_both_pass_the_capacity_gate() {
    for memory in ["1343488Ki", "2Gi"] {
        let fixture = Fixture::new(nodes(vec![node("node-a", memory, true)]), pods(vec![]));
        let output = fixture.run(&[]);
        assert_reached_helm_upgrade(&fixture, &output);
    }
}

#[test]
fn rerun_replaces_managed_stack_requests_but_keeps_unrelated_observability_load() {
    let mut items = managed_stack_pods("node-a");
    items.push(labeled_pod(
        "unrelated",
        "node-a",
        OBSERVABILITY_NAMESPACE,
        json!({"app.kubernetes.io/instance": "other"}),
        "64Mi",
    ));

    let fixture = Fixture::new(
        nodes(vec![node("node-a", "1376Mi", true)]),
        pods(items.clone()),
    );
    let output = fixture.run(&[]);
    assert_reached_helm_upgrade(&fixture, &output);

    let insufficient = Fixture::new(nodes(vec![node("node-a", "1375Mi", true)]), pods(items));
    let output = insufficient.run(&[]);
    let text = assert_refused_before_helm(&insufficient, &output);
    assert!(
        text.contains("required 1312Mi") && text.contains("available 1311Mi"),
        "only managed stack requests may be replaced during a rerun: {text}"
    );
}

#[test]
fn daemonset_requests_scale_the_required_memory_per_ready_node() {
    let enough = Fixture::new(
        nodes(vec![
            node("node-a", "736Mi", true),
            node("node-b", "736Mi", true),
        ]),
        pods(vec![]),
    );
    let output = enough.run(&[]);
    assert_reached_helm_upgrade(&enough, &output);

    let insufficient = Fixture::new(
        nodes(vec![
            node("node-a", "736Mi", true),
            node("node-b", "735Mi", true),
        ]),
        pods(vec![]),
    );
    let output = insufficient.run(&[]);
    let text = assert_refused_before_helm(&insufficient, &output);
    assert!(
        text.contains("required 1472Mi") && text.contains("available 1471Mi"),
        "two Ready nodes must reserve two Alloy and node exporter requests: {text}"
    );
}

#[test]
fn capacity_aggregates_ready_nodes_and_only_their_nonterminal_scheduled_pods() {
    let items = vec![
        pod(
            "ready-a-load",
            Some("ready-a"),
            "Running",
            json!([memory_container("app", "256Mi")]),
        ),
        pod(
            "ready-b-load",
            Some("ready-b"),
            "Pending",
            json!([memory_container("app", "544Mi")]),
        ),
        pod(
            "not-ready-load",
            Some("not-ready"),
            "Running",
            json!([memory_container("app", "8Gi")]),
        ),
        pod(
            "completed",
            Some("ready-a"),
            "Succeeded",
            json!([memory_container("app", "8Gi")]),
        ),
        pod(
            "unscheduled",
            None,
            "Pending",
            json!([memory_container("app", "8Gi")]),
        ),
    ];
    let fixture = Fixture::new(
        nodes(vec![
            node("ready-a", "1Gi", true),
            node("ready-b", "1248Mi", true),
            node("not-ready", "16Gi", false),
        ]),
        pods(items),
    );
    let output = fixture.run(&[]);
    assert_reached_helm_upgrade(&fixture, &output);
}

fn restartable_init_pod() -> Value {
    let mut value = pod(
        "restartable-init-accounting",
        Some("node-a"),
        "Running",
        json!([
            memory_container("app-a", "110Mi"),
            memory_container("app-b", "90Mi")
        ]),
    );
    value["spec"]["initContainers"] = json!([
        {
            "name": "sidecar-a",
            "restartPolicy": "Always",
            "resources": {"requests": {"memory": "120Mi"}}
        },
        memory_container("setup", "380Mi"),
        {
            "name": "sidecar-b",
            "restartPolicy": "Always",
            "resources": {"requests": {"memory": "80Mi"}}
        },
        memory_container("migrate", "250Mi")
    ]);
    value
}

#[test]
fn restartable_init_scheduling_semantics_pin_the_exact_five_hundred_mib_request() {
    // Effective request is 500Mi: sidecar-a plus setup is the largest init
    // stage. The steady state is 400Mi: both app containers plus both
    // restartable sidecars. Summing every init container would overcount;
    // taking only the largest single init container would undercount.
    let exact = Fixture::new(
        nodes(vec![node("node-a", "1812Mi", true)]),
        pods(vec![restartable_init_pod()]),
    );
    let exact_output = exact.run(&[]);
    assert_reached_helm_upgrade(&exact, &exact_output);

    let one_short = Fixture::new(
        nodes(vec![node("node-a", "1811Mi", true)]),
        pods(vec![restartable_init_pod()]),
    );
    let one_short_output = one_short.run(&[]);
    let text = assert_refused_before_helm(&one_short, &one_short_output);
    assert!(
        text.contains("required 1312Mi") && text.contains("available 1311Mi"),
        "the capacity boundary must use the effective 500Mi pod request: {text}"
    );
}

#[test]
fn zero_ready_nodes_fails_closed_with_the_cluster_prerequisite() {
    let fixture = Fixture::new(nodes(vec![node("node-a", "8Gi", false)]), pods(vec![]));
    let output = fixture.run(&[]);
    let text = assert_refused_before_helm(&fixture, &output);
    assert!(
        text.to_ascii_lowercase().contains("no ready")
            && text.contains("kubectl get nodes")
            && text.contains("allocatable"),
        "zero Ready nodes must name the failed prerequisite and inspection command: {text}"
    );
}

#[test]
fn kubectl_failure_and_malformed_pod_json_both_fail_closed() {
    let cases = [
        ("failure", "success", "nodes is forbidden"),
        ("success", "malformed", "kubectl get pods"),
    ];
    for (nodes_mode, pods_mode, expected) in cases {
        let fixture = Fixture::with_modes(
            nodes(vec![node("node-a", "8Gi", true)]),
            pods(vec![]),
            nodes_mode,
            pods_mode,
            "stop",
        );
        let output = fixture.run(&[]);
        let text = assert_refused_before_helm(&fixture, &output);
        assert!(
            text.contains(expected),
            "capacity read error must preserve the cause or inspection command `{expected}`: {text}"
        );
        assert!(
            text.contains("kubectl get nodes") || text.contains("kubectl get pods"),
            "capacity read error must give the exact kubectl inspection surface: {text}"
        );
    }
}

#[test]
fn helm_wait_is_bounded_and_timeout_names_safe_pending_upgrade_recovery() {
    let fixture = Fixture::with_modes(
        nodes(vec![node("node-a", "8Gi", true)]),
        pods(vec![]),
        "success",
        "success",
        "timeout",
    );
    let output = fixture.run(&[]);
    let text = shown(&output);
    assert_eq!(
        output.status.code(),
        Some(1),
        "Helm timeout must fail: {text}"
    );
    let upgrade = fixture
        .helm_calls()
        .into_iter()
        .find(|call| call.starts_with("upgrade --install "))
        .unwrap_or_else(|| panic!("installer must reach helm upgrade: {text}"));
    assert!(
        upgrade.split_whitespace().any(|token| token == "--wait"),
        "installer must wait for the Grafana token updater hook: {upgrade}"
    );
    assert!(
        upgrade
            .split_whitespace()
            .any(|token| token == "--timeout" || token.starts_with("--timeout=")),
        "Helm wait must carry an explicit timeout: {upgrade}"
    );

    let normalized = text.replace(['\'', '"'], "");
    let recovery = format!(
        "kubectl delete secret -n {OBSERVABILITY_NAMESPACE} -l owner=helm,name={FIRST_RELEASE},status=pending-upgrade"
    );
    assert!(
        normalized.contains(FIRST_RELEASE)
            && normalized.contains("context deadline exceeded")
            && normalized.contains(&recovery),
        "timeout must name the release and exact pending upgrade recovery command `{recovery}`: {text}"
    );
}

#[test]
fn all_capacity_reads_are_cluster_wide_and_json_shaped() {
    let fixture = Fixture::new(nodes(vec![]), pods(vec![]));
    let _ = fixture.run(&[]);
    let calls = fixture.kubectl_calls();
    let node_read = calls
        .iter()
        .find(|call| call.contains("get nodes") || call.contains("get node"))
        .unwrap_or_else(|| panic!("capacity preflight must read nodes: {calls:?}"));
    assert!(
        node_read.contains("-o json") || node_read.contains("--output json"),
        "node accounting must consume structured JSON: {node_read}"
    );
    // Zero Ready nodes may fail before reading pods. A cluster with a Ready
    // node proves the second read is cluster wide rather than namespace local.
    let ready = Fixture::new(nodes(vec![node("node-a", "8Gi", true)]), pods(vec![]));
    let _ = ready.run(&[]);
    let pod_read = ready
        .kubectl_calls()
        .into_iter()
        .find(|call| call.contains("get pods") || call.contains("get pod"))
        .unwrap_or_else(|| panic!("capacity preflight must read pods"));
    assert!(
        (pod_read.contains("--all-namespaces") || pod_read.contains(" -A"))
            && (pod_read.contains("-o json") || pod_read.contains("--output json")),
        "pod request accounting must be cluster wide structured JSON: {pod_read}"
    );
}
