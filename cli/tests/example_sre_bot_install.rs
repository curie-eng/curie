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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use flate2::read::GzDecoder;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use support::{serve, MockServer, Response};

const OBSERVABILITY_NAMESPACE: &str = "observability";
const FIRST_RELEASE: &str = "grafana";
const REQUIRED_MIB: u64 = 1376;
const TEMPO_TAGGED_IMAGE: &str = "ghcr.io/curie-eng/curie-sre-bot-tempo:0.8.0";
const REGISTRY_INDEX: &str =
    r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[]}"#;
/// Mirrors `examples::RUNTIME_PLUGIN_WRITE_DESCRIPTION`. Duplicated deliberately:
/// the constant is private to the CLI crate, and an integration test asserting the
/// uploaded bundle should read the wire, not the producer's own constant.
const RUNTIME_PLUGIN_WRITE_DESCRIPTION_TEXT: &str = "SRE triage assistant for plain English production health and Kubernetes questions in Slack. This installer deploys read only Kubernetes, Grafana, and Tempo connectors, plus one approval-gated Kubernetes restart tool scoped to the Deployments named at install time. Every restart requires a human approval; no other write verb is available.";

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
    let reads = include_str!("data/converged-installation-read.sh");
    let body = if name == "helm" {
        // Preserve this installer's existing Grafana status/migration replies.
        let marker = "if [ \"$1\" = \"repo\" ]; then";
        body.replacen(marker, &format!("{reads}\n{marker}"), 1)
    } else if name == "kubectl" {
        format!(
            "#!/bin/sh\n{reads}\n{}",
            body.strip_prefix("#!/bin/sh\n").unwrap_or(body)
        )
    } else {
        body.to_string()
    };
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
    applied_dir: PathBuf,
    helm_values_dir: PathBuf,
    nodes: String,
    pods: String,
    nodes_mode: &'static str,
    pods_mode: &'static str,
    helm_mode: &'static str,
    grafana_secret_mode: &'static str,
    reader_token_mode: &'static str,
    /// Whether the `sre-bot-writer` ServiceAccount already exists, i.e. fresh
    /// install (`absent`, the default) versus re-install (`existing`).
    writer_identity_mode: &'static str,
    helm_values: String,
    api: MockServer,
    registry: MockServer,
    registry_endpoint: String,
    /// Flips `POST /deployments` to a 500. Shared with the mock API's handler
    /// closure rather than baked in at construction, because the server thread
    /// is already running by the time a builder method is called -- the handler
    /// reads it per request, and the CLI is not launched until `run()`.
    deploy_fails: Arc<AtomicBool>,
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
        let applied_dir = temp.path().join("applied");
        let helm_values_dir = temp.path().join("helm-values");
        fs::create_dir(&applied_dir).expect("create applied-file capture directory");
        fs::create_dir(&helm_values_dir).expect("create helm-values capture directory");

        write_exec(
            &bin_dir,
            "kubectl",
            r#"#!/bin/sh
printf '%s\n' "$*" >> "$CURIE_TEST_KUBECTL_LOG"
printf 'KUBECTL %s\n' "$*" >> "$CURIE_TEST_ACTION_LOG"

prev=""
for arg in "$@"; do
    if [ "$prev" = "-f" ] && [ "$arg" != "-" ] && [ -f "$arg" ]; then
        cp "$arg" "$CURIE_TEST_APPLIED_DIR/$(basename "$arg")"
    fi
    prev=$arg
done

case " $* " in
    *" config view --minify --raw -o json "*)
        printf '%s\n' '{"clusters":[{"cluster":{"server":"https://cluster.example.com","certificate-authority-data":"Y2E="}}]}'
        exit 0
        ;;
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
        case " $* " in
            *" get namespace curie "*)
                printf '%s\n' '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"curie","labels":{"curietech.ai/created-by":"curie","curietech.ai/created-in":"curie"},"uid":"uid-curie","resourceVersion":"17"}}'
                ;;
            *" get namespace soak "*)
                printf '%s\n' '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"soak","labels":{"curietech.ai/created-by":"soak-rel","curietech.ai/created-in":"soak"},"uid":"uid-soak","resourceVersion":"17"}}'
                ;;
        esac
        exit 0
        ;;
    *" get priorityclass "*|*" get priorityclasses "*)
        exit 0
        ;;
    # Existence probe for the writer identity, which is what decides the ORDER of
    # the write-RBAC apply relative to the deploy. Absent (the default) means a
    # fresh install and the deferred, post-deploy ordering every other test in
    # this file asserts; `existing` means a re-install, where the ceiling must be
    # tightened BEFORE the new version activates.
    #
    # Mirrors `--ignore-not-found`: absent is empty stdout and exit 0, never a
    # NotFound failure. Deliberately placed ABOVE every `get secret` arm, and
    # keyed on a token none of those arms contain, so neither group can shadow
    # the other in either direction.
    *" get serviceaccount "*)
        case "$CURIE_TEST_WRITER_IDENTITY_MODE" in
            existing) printf '%s\n' 'serviceaccount/sre-bot-writer' ;;
            absent) ;;
        esac
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
                printf '%s\n' '{"apiVersion":"v1","kind":"Secret","data":{"ca.crt":"Zml4dHVyZS1jYQ==","token":"enp6enp6enp6enp6"}}'
                exit 0
                ;;
            read-failure)
                printf '%s\n' 'Error from server (Forbidden): secrets "sre-bot-reader-token" is forbidden' >&2
                exit 1
                ;;
        esac
        ;;
    # The write identity is part of the DEFAULT install now, so the stub answers
    # for it the way it does for the reader. The writer is reached only after the
    # DEPLOY succeeds -- the identity is minted last on purpose, so a failed
    # install cannot strand a live write credential (#1946).
    #
    # Keyed on the same mode variable regardless: a reader-token timeout or read
    # failure aborts before the deploy is ever attempted, so there is no reachable
    # run in which the reader path fails and the writer path is still consulted.
    # One switch still describes the whole token path, and a second knob would
    # only be able to express states the installer cannot produce.
    *" wait "*" secret/sre-bot-writer-token "*)
        case "$CURIE_TEST_READER_TOKEN_MODE" in
            success|read-failure) exit 0 ;;
            timeout)
                printf '%s\n' 'error: timed out waiting for the condition' >&2
                exit 1
                ;;
        esac
        ;;
    *" get secret sre-bot-writer-token "*)
        case "$CURIE_TEST_READER_TOKEN_MODE" in
            success)
                printf '%s\n' '{"apiVersion":"v1","kind":"Secret","data":{"ca.crt":"Zml4dHVyZS1jYQ==","token":"eXl5eXl5eXl5eXl5"}}'
                exit 0
                ;;
            read-failure)
                printf '%s\n' 'Error from server (Forbidden): secrets "sre-bot-writer-token" is forbidden' >&2
                exit 1
                ;;
        esac
        ;;
    *" apply -f - "*)
        if printf ' %s ' "$*" | grep -q ' -n '; then
            cat > "$CURIE_TEST_CONNECTOR_STDIN"
            printf '%s\n' 'connector objects configured'
            exit 0
        fi
        cat > "$CURIE_TEST_GRAFANA_STDIN"
        if [ "$CURIE_TEST_GRAFANA_SECRET_MODE" = "apply-failure" ]; then
            printf '%s\n' 'Error from server (Forbidden): cannot apply grafana-admin' >&2
            exit 1
        fi
        printf '%s\n' 'secret/grafana-admin configured'
        exit 0
        ;;
    *" get deployment "*" app.kubernetes.io/instance="*)
        printf '%s\n' 'curie'
        exit 0
        ;;
    *" get replicasets "*)
        printf '%s\n' '{"apiVersion":"v1","kind":"List","items":[]}'
        exit 0
        ;;
    *" get pods "*)
        printf '%s\n' '{"apiVersion":"v1","kind":"List","items":[]}'
        exit 0
        ;;
    *" get deployment "*" -o json"*)
        printf '%s\n' '{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"generation":1},"spec":{"replicas":1},"status":{"observedGeneration":1,"replicas":1,"updatedReplicas":1,"readyReplicas":1,"availableReplicas":1}}'
        exit 0
        ;;
    *" delete deployment,service,networkpolicy,secret "*)
        exit 0
        ;;
    *" apply "*|*" rollout status "*)
        exit 0
        ;;
    *" get secret "*" app.kubernetes.io/instance="*)
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

prev=""
for arg in "$@"; do
    if [ "$prev" = "-f" ] && [ -f "$arg" ]; then
        cp "$arg" "$CURIE_TEST_HELM_VALUES_DIR/$(basename "$arg")"
    fi
    prev=$arg
done

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

if [ "$1" = "upgrade" ] && [ "$2" != "--install" ]; then
    exit 0
fi

printf 'unexpected helm invocation: %s\n' "$*" >&2
exit 64
"#,
        );

        let deploy_fails = Arc::new(AtomicBool::new(false));
        let handler_deploy_fails = Arc::clone(&deploy_fails);
        let api = serve(
            move |request| match (request.method.as_str(), request.path.as_str()) {
                ("GET", "/agents") => Response::json(200, "[]"),
                ("POST", "/agents") => Response::json(
                    201,
                    &format!(
                        r##"{{"id":"{AGENT_ID}","name":"sre-bot","channels":[{{"kind":"slack","address":"#local-dev"}}],"created_at":"2026-08-21T00:00:00Z","memory":false}}"##
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
                // The deploy-failure knob fails HERE and nowhere earlier. This
                // leg runs after agent creation, version creation, and the
                // bundle upload, so the fixture reproduces the case the ticket
                // describes: a genuine deploy failure with the bundle already
                // on the platform. Failing the upload instead would also make
                // the test pass while proving far less -- an abort that early
                // never reaches the window where a write credential could be
                // stranded.
                ("POST", "/deployments") if handler_deploy_fails.load(Ordering::SeqCst) => {
                    Response::json(500, r#"{"error":"fixture deploy failure"}"#)
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
            // The gated write connector resolves its own digest the same way, so
            // the mock answers for any published sre-bot connector rather than
            // one hard-coded repository -- otherwise adding a connector silently
            // turns these into "registry unreachable" tests.
            if request.path.starts_with("/v2/curie-eng/curie-sre-bot-")
                && request.path.contains("/manifests/")
                && !request.path.contains("curie-sre-bot-tempo/manifests/0.8.0")
            {
                return Response {
                    status: 200,
                    content_type: "application/vnd.oci.image.index.v1+json".into(),
                    body: REGISTRY_INDEX.as_bytes().to_vec(),
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
            applied_dir,
            helm_values_dir,
            nodes: nodes.to_string(),
            pods: pods.to_string(),
            nodes_mode,
            pods_mode,
            helm_mode,
            grafana_secret_mode: "existing",
            reader_token_mode: "success",
            // Fresh install by default, so every existing test keeps asserting
            // the deferred post-deploy ordering it was written against.
            writer_identity_mode: "absent",
            helm_values: "absent".to_string(),
            api,
            registry,
            registry_endpoint,
            deploy_fails,
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

    /// Present the cluster as one where the writer identity already exists, i.e.
    /// a RE-INSTALL rather than a first install.
    fn with_writer_identity_mode(mut self, mode: &'static str) -> Self {
        self.writer_identity_mode = mode;
        self
    }

    /// Make the platform API refuse the deployment after it has accepted the
    /// agent, the version, and the uploaded bundle.
    fn with_deploy_failure(self) -> Self {
        self.deploy_fails.store(true, Ordering::SeqCst);
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
            .env("CURIE_TEST_APPLIED_DIR", &self.applied_dir)
            .env("CURIE_TEST_HELM_VALUES_DIR", &self.helm_values_dir)
            .env("CURIE_TEST_NODES_JSON", &self.nodes)
            .env("CURIE_TEST_PODS_JSON", &self.pods)
            .env("CURIE_TEST_NODES_MODE", self.nodes_mode)
            .env("CURIE_TEST_PODS_MODE", self.pods_mode)
            .env("CURIE_TEST_HELM_MODE", self.helm_mode)
            .env("CURIE_TEST_HELM_VALUES", &self.helm_values)
            .env("CURIE_TEST_GRAFANA_SECRET_MODE", self.grafana_secret_mode)
            .env("CURIE_TEST_READER_TOKEN_MODE", self.reader_token_mode)
            .env("CURIE_TEST_WRITER_IDENTITY_MODE", self.writer_identity_mode)
            .env(
                "CURIE_TEST_SRE_BOT_REGISTRY_ENDPOINT",
                &self.registry_endpoint,
            )
            .env_remove("CURIE_API_KEY")
            .env_remove("CURIE_NAMESPACE")
            .env_remove("CURIE_RELEASE")
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

    fn applied_file(&self, name: &str) -> String {
        fs::read_to_string(self.applied_dir.join(name))
            .unwrap_or_else(|error| panic!("read applied {name}: {error}"))
    }

    fn helm_values_file(&self, name: &str) -> String {
        fs::read_to_string(self.helm_values_dir.join(name))
            .unwrap_or_else(|error| panic!("read helm values {name}: {error}"))
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

/// The gzip stream out of the recorded multipart bundle upload.
///
/// Extracted so the whole-archive readers below locate the upload exactly the
/// way `uploaded_bundle_file` always has -- one place that knows the upload is
/// a multipart body with the archive somewhere inside it, so a change to the
/// wire shape breaks every reader at once instead of one of them.
fn uploaded_bundle_archive(fixture: &Fixture) -> Vec<u8> {
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
    upload.body[gzip_start..].to_vec()
}

/// Every path the uploaded archive carries, in archive order.
///
/// The absence of a file is as much a bundle property as its contents -- a
/// `connectors.lock.yaml` that should not exist cannot be asserted by reading
/// one named entry.
fn uploaded_bundle_paths(fixture: &Fixture) -> Vec<String> {
    let decoder = GzDecoder::new(Cursor::new(uploaded_bundle_archive(fixture)));
    let mut archive = tar::Archive::new(decoder);
    archive
        .entries()
        .expect("read uploaded bundle archive")
        .map(|entry| {
            let entry = entry.expect("read uploaded bundle entry");
            entry.path().expect("bundle path").display().to_string()
        })
        .collect()
}

/// Unpack the WHOLE uploaded archive into a temporary directory.
///
/// The bundle validator takes a bundle DIRECTORY, not a file: it cross-checks
/// the manifest against the skills, the connectors, and the deploy targets
/// beside it, so no single-file read can stand in for it. The returned
/// `TempDir` owns the directory, so callers must hold it for as long as they
/// read the path.
fn unpacked_uploaded_bundle(fixture: &Fixture) -> tempfile::TempDir {
    let directory = tempfile::tempdir().expect("temporary directory for the uploaded bundle");
    let decoder = GzDecoder::new(Cursor::new(uploaded_bundle_archive(fixture)));
    let mut archive = tar::Archive::new(decoder);
    archive
        .unpack(directory.path())
        .expect("unpack the uploaded bundle archive");
    directory
}

/// Parse a rendered multi-document manifest into its documents.
fn manifest_documents(rendered: &str) -> Vec<Value> {
    serde_norway::Deserializer::from_str(rendered)
        .map(|document| -> Value {
            serde::Deserialize::deserialize(document)
                .unwrap_or_else(|error| panic!("rendered manifest must be valid YAML: {error}"))
        })
        .collect()
}

fn uploaded_bundle_file(fixture: &Fixture, wanted: &str) -> Vec<u8> {
    let decoder = GzDecoder::new(Cursor::new(uploaded_bundle_archive(fixture)));
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
            "256Mi",
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
        !text.contains(&format!("required {REQUIRED_MIB}Mi")) && !text.contains("available"),
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
    for required in ["--namespace", "--release", "--observability-namespace"] {
        assert!(
            text.contains(required),
            "the install surface must expose targeting flag {required}: {text}"
        );
    }
    for forbidden in [
        "--values",
        "--file",
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
    // The default install now KEEPS the gated write connector (it is what the bot
    // can be asked to do), and closes its ceiling instead of removing the tool.
    // Same digest treatment as Tempo, for the same reason: a `build:` declaration
    // records a local image id the cluster tier refuses.
    let write = &declaration["connectors"]["k8s-write"];
    assert!(
        !write.is_null(),
        "the default install must ship the gated write connector"
    );
    assert!(
        write.get("build").is_none(),
        "the uploaded declaration must not ask the cluster deploy path to build the write connector"
    );
    assert!(
        write["image"]
            .as_str()
            .is_some_and(|image| image.contains("@sha256:")),
        "the write connector must be pinned by digest, got {:?}",
        write["image"]
    );
    assert_eq!(
        write["env"]["K8S_WRITE_ALLOWLIST"], "",
        "with no targets named the ceiling must be empty, so every call is refused"
    );

    let plugin: Value = serde_json::from_slice(&uploaded_bundle_file(
        &fixture,
        ".claude-plugin/plugin.json",
    ))
    .expect("uploaded plugin manifest must remain valid JSON");
    // The gate ships WITH the connector: keeping the tool and dropping the gate is
    // the pairing that produced an ungated write path downstream, so the two are
    // decided together and neither can arrive alone.
    let gates = plugin["approvalPolicy"]["gates"]
        .as_array()
        .expect("the default install must declare its write gate");
    assert_eq!(
        gates.len(),
        1,
        "exactly the restart gate, and nothing that names an absent connector"
    );
    assert_eq!(gates[0]["gate"], "mcp__k8s-write__restart_deployment");
    assert_eq!(
        plugin["description"], RUNTIME_PLUGIN_WRITE_DESCRIPTION_TEXT,
        "the runtime manifest must describe the surface it actually ships"
    );

    // GitHub documents anonymous public GHCR pulls, and the Distribution token
    // specification defines the service and repository pull scope exchange:
    // https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry
    // https://distribution.github.io/distribution/spec/auth/token/
    let registry = fixture.registry.recorded();
    // Two connectors are resolved now -- Tempo and the gated write connector --
    // and each costs a scoped token plus an index request.
    assert_eq!(
        registry.len(),
        4,
        "each resolved connector must use token then index requests"
    );
    // Asserted by repository rather than by position: the count alone would pass
    // if one connector were resolved twice.
    for repository in ["curie-sre-bot-tempo", "curie-sre-bot-k8s-write"] {
        let scope = format!("repository%3Acurie-eng%2F{repository}%3Apull");
        assert!(
            registry
                .iter()
                .any(|request| request.path.starts_with("/token?")
                    && request.path.contains(&scope)),
            "{repository} must be resolved through its own scoped pull token"
        );
        assert!(
            registry.iter().any(|request| request
                .path
                .starts_with(&format!("/v2/curie-eng/{repository}/manifests/"))),
            "{repository} must be resolved to an index digest before any cluster mutation"
        );
    }
    for request in registry
        .iter()
        .filter(|request| request.path.contains("/manifests/"))
    {
        assert_eq!(
            request.header("authorization"),
            Some("Bearer anonymous-pull-token")
        );
        assert!(
            request
                .header("accept")
                .is_some_and(|accept| accept.contains("application/vnd.oci.image.index.v1+json")),
            "manifest request must require a multi-platform image index"
        );
    }
}

#[test]
fn install_records_the_current_commit_sha_on_the_created_version() {
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

    let version_request = fixture
        .api
        .recorded()
        .into_iter()
        .find(|request| {
            request.method == "POST" && request.path == format!("/agents/{AGENT_ID}/versions")
        })
        .expect("the installer must create a version");
    let body: Value = serde_json::from_slice(&version_request.body)
        .expect("the version-create request body must be valid JSON");
    match option_env!("CURIE_BUILD_COMMIT") {
        Some(expected_commit_sha) => {
            let commit_sha = body["commit_sha"]
                .as_str()
                .expect("the version-create request must record the installer commit SHA");
            assert_eq!(
                commit_sha, expected_commit_sha,
                "the installer must forward this binary's build commit"
            );
            assert!(
                commit_sha.len() == 40 && commit_sha.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "the recorded commit SHA must be a 40-character ASCII hex value: {commit_sha:?}"
            );
        }
        None => assert_eq!(
            body.get("commit_sha"),
            Some(&Value::Null),
            "a binary built without Git provenance must send a JSON null commit SHA"
        ),
    }
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
    assert_eq!(config["users"][0]["user"]["token"], "zzzzzzzzzzzz");
    assert_eq!(
        config["clusters"][0]["cluster"]["certificate-authority-data"],
        "Zml4dHVyZS1jYQ=="
    );

    let kubectl = fixture.kubectl_calls();
    assert!(
        kubectl
            .iter()
            .any(|call| call.contains("-n curie apply -f -")),
        "rendered connector objects must be applied from stdin: {kubectl:?}"
    );
    assert!(
        kubectl.iter().any(|call| {
            call.contains("-n curie delete deployment,service,networkpolicy,secret ")
        }),
        "stale connector objects must be reconciled after apply: {kubectl:?}"
    );
    // The needle must be a string that CANNOT occur by chance. It was "abc",
    // and `observable` carries temp paths built from random UUIDs -- which are
    // hex, so a, b and c are all in the alphabet. A run whose helm values file
    // landed on `/tmp/curie-helm-values-0e7b51b5-ab30-4abc-a064-...` failed this
    // assertion with no credential anywhere near the output.
    //
    // That is worse than a flake. A leak check that fires on a filename gets
    // re-run until green, and then it is not checking anything.
    //
    // So the fixture token is `z` repeated: `z` is outside hex, so no UUID or
    // digest can produce it, and the base64 form is one block repeated, so its
    // entropy stays where "abc" had it. A distinctive high-entropy string fixes
    // the collision and becomes a gitleaks `generic-api-key` finding sitting
    // next to the word "token" -- which is the secret-scanner's job working
    // correctly, and the reason this is a placeholder rather than a plausible
    // credential.
    let observable = format!("{text}\n{kubectl:?}\n{:?}", fixture.helm_calls());
    assert!(
        !observable.contains("zzzzzzzzzzzz") && !observable.contains("enp6enp6enp6enp6"),
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
    for memory in ["1409024Ki", "2Gi"] {
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
        nodes(vec![node("node-a", "1440Mi", true)]),
        pods(items.clone()),
    );
    let output = fixture.run(&[]);
    assert_reached_helm_upgrade(&fixture, &output);

    let insufficient = Fixture::new(nodes(vec![node("node-a", "1439Mi", true)]), pods(items));
    let output = insufficient.run(&[]);
    let text = assert_refused_before_helm(&insufficient, &output);
    assert!(
        text.contains("required 1376Mi") && text.contains("available 1375Mi"),
        "only managed stack requests may be replaced during a rerun: {text}"
    );
}

#[test]
fn daemonset_requests_scale_the_required_memory_per_ready_node() {
    let enough = Fixture::new(
        nodes(vec![
            node("node-a", "768Mi", true),
            node("node-b", "768Mi", true),
        ]),
        pods(vec![]),
    );
    let output = enough.run(&[]);
    assert_reached_helm_upgrade(&enough, &output);

    let insufficient = Fixture::new(
        nodes(vec![
            node("node-a", "768Mi", true),
            node("node-b", "767Mi", true),
        ]),
        pods(vec![]),
    );
    let output = insufficient.run(&[]);
    let text = assert_refused_before_helm(&insufficient, &output);
    assert!(
        text.contains("required 1536Mi") && text.contains("available 1535Mi"),
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
            node("ready-b", "1312Mi", true),
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
        nodes(vec![node("node-a", "1876Mi", true)]),
        pods(vec![restartable_init_pod()]),
    );
    let exact_output = exact.run(&[]);
    assert_reached_helm_upgrade(&exact, &exact_output);

    let one_short = Fixture::new(
        nodes(vec![node("node-a", "1875Mi", true)]),
        pods(vec![restartable_init_pod()]),
    );
    let one_short_output = one_short.run(&[]);
    let text = assert_refused_before_helm(&one_short, &one_short_output);
    assert!(
        text.contains("required 1376Mi") && text.contains("available 1375Mi"),
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

const CUSTOM_NAMESPACE: &str = "soak";
const CUSTOM_RELEASE: &str = "soak-rel";
const CUSTOM_OBS_NAMESPACE: &str = "soak-obs";

fn custom_target_args() -> [&'static str; 6] {
    [
        "--namespace",
        CUSTOM_NAMESPACE,
        "--release",
        CUSTOM_RELEASE,
        "--observability-namespace",
        CUSTOM_OBS_NAMESPACE,
    ]
}

fn dry_run_plan_lines(output: &Output) -> Vec<String> {
    let documents: Vec<Value> = serde_json::Deserializer::from_slice(&output.stdout)
        .into_iter::<Value>()
        .collect::<Result<_, _>>()
        .unwrap_or_else(|error| panic!("dry run stdout must contain only JSON: {error}"));
    assert_eq!(
        documents.len(),
        1,
        "dry run must emit exactly one JSON object"
    );
    documents[0]["plan"]
        .as_array()
        .expect("dry run object must carry its ordered plan")
        .iter()
        .map(|line| line.as_str().expect("plan entries are strings").to_string())
        .collect()
}

fn assert_no_default_curie_or_observability_targets(lines: &[String]) {
    let plan = lines.join("\n");
    for forbidden in [
        "--namespace observability",
        "--namespace curie",
        " -n curie ",
        "helm upgrade --install curie ",
        "helm upgrade curie ",
        "curie cluster deploy --plugin-dir embedded:examples/sre-bot --namespace curie --release curie",
        "in namespace observability",
        "kubectl wait --namespace curie ",
    ] {
        assert!(
            !plan.contains(forbidden),
            "custom targeting must not retain default identity `{forbidden}`: {lines:?}"
        );
    }
}

#[test]
fn no_flag_dry_run_preserves_curie_and_observability_defaults() {
    let fixture = Fixture::new(nodes(vec![node("node-a", "4Gi", true)]), pods(vec![]));
    let output = fixture.run(&["--dry-run", "--json"]);
    let text = shown(&output);
    assert!(
        output.status.success(),
        "default dry run must succeed: {text}"
    );
    let lines = dry_run_plan_lines(&output);
    assert!(
        lines.iter().any(|line| {
            line.contains("helm upgrade --install grafana ")
                && line.contains("--namespace observability")
        }),
        "no-flag plan must keep the observability namespace default: {lines:?}"
    );
    assert!(
        lines.iter().any(|line| {
            line.contains("helm upgrade --install curie") && line.contains(" -n curie ")
        }),
        "no-flag plan must keep the Curie namespace and release defaults: {lines:?}"
    );
    assert!(
        lines.iter().any(|line| line.contains(
            "curie cluster deploy --plugin-dir embedded:examples/sre-bot --namespace curie --release curie"
        )),
        "no-flag deploy must keep the Curie identity defaults: {lines:?}"
    );
    assert!(
        lines.iter().any(|line| {
            line.contains("preserve or create Secret grafana-admin in namespace observability")
        }),
        "no-flag plan must keep the Grafana Secret in observability: {lines:?}"
    );
    assert!(
        lines.iter().any(|line| {
            line.contains("kubectl wait --namespace curie ")
                && line.contains("secret/sre-bot-reader-token")
        }),
        "no-flag token wait must keep the Curie namespace: {lines:?}"
    );
}

#[test]
fn custom_target_dry_run_reports_and_uses_only_the_selected_identities() {
    let fixture = Fixture::new(nodes(vec![node("node-a", "4Gi", true)]), pods(vec![]));
    let mut args = custom_target_args().to_vec();
    args.extend(["--dry-run", "--json"]);
    let output = fixture.run(&args);
    let text = shown(&output);
    assert!(
        output.status.success(),
        "custom-target dry run must succeed: {text}"
    );
    let lines = dry_run_plan_lines(&output);
    assert_no_default_curie_or_observability_targets(&lines);
    assert!(
        lines.iter().any(|line| {
            line.contains("preserve or create Secret grafana-admin in namespace soak-obs")
        }),
        "Grafana Secret plan must name the selected observability namespace: {lines:?}"
    );
    for release in ["grafana", "loki", "alloy", "prometheus"] {
        assert!(
            lines.iter().any(|line| {
                line.contains(&format!("helm upgrade --install {release} "))
                    && line.contains("--namespace soak-obs")
            }),
            "upstream {release} must target soak-obs: {lines:?}"
        );
    }
    assert!(
        lines.iter().any(|line| {
            line.contains("kubectl apply")
                && line.contains("--namespace soak-obs")
                && line.contains("tempo.yaml")
        }),
        "Tempo apply must target soak-obs: {lines:?}"
    );
    assert!(
        lines.iter().any(|line| {
            line.contains("helm upgrade --install soak-rel") && line.contains(" -n soak ")
        }),
        "guarded Curie apply must use the selected release and namespace: {lines:?}"
    );
    assert!(
        lines.iter().any(|line| {
            line.contains("helm upgrade soak-rel")
                && !line.contains("--install")
                && line.contains("--namespace soak")
                && line.contains("--reuse-values")
                && line.contains("curie-values.yaml")
        }),
        "integration upgrade must use the selected Curie identity: {lines:?}"
    );
    assert!(
        lines.iter().any(|line| {
            line.contains("kubectl wait --namespace soak ")
                && line.contains("secret/sre-bot-reader-token")
        }),
        "reader token wait must use the selected Curie namespace: {lines:?}"
    );
    assert!(
        lines.iter().any(|line| line.contains(
            "curie cluster deploy --plugin-dir embedded:examples/sre-bot --namespace soak --release soak-rel"
        )),
        "deploy plan must report the selected Curie identity: {lines:?}"
    );
}

#[test]
fn custom_targets_thread_through_helm_kubectl_manifests_secret_discovery_and_connectors() {
    let fixture = Fixture::with_modes(
        nodes(vec![node("node-a", "4Gi", true)]),
        pods(vec![]),
        "success",
        "success",
        "success",
    )
    .with_grafana_secret_mode("absent");
    let output = fixture.run(&custom_target_args());
    let text = shown(&output);
    assert!(
        output.status.success(),
        "custom-target install must succeed: {text}"
    );

    let helm = fixture.helm_calls();
    assert!(
        helm.iter().any(|call| {
            call.starts_with("upgrade --install grafana ") && call.contains("--namespace soak-obs")
        }),
        "Grafana Helm must target soak-obs: {helm:?}"
    );
    assert!(
        helm.iter().any(
            |call| call.starts_with("status grafana ") && call.contains("--namespace soak-obs")
        ),
        "Grafana status must inspect soak-obs: {helm:?}"
    );
    assert!(
        helm.iter().any(|call| {
            call.starts_with("upgrade --install soak-rel ") && call.contains(" -n soak ")
        }),
        "guarded Curie apply must target soak/soak-rel: {helm:?}"
    );
    assert!(
        helm.iter().any(|call| {
            call.starts_with("upgrade soak-rel ")
                && call.contains("--namespace soak")
                && call.contains("--reuse-values")
        }),
        "integration upgrade must target soak/soak-rel: {helm:?}"
    );
    assert!(
        helm.iter().all(|call| {
            !call.contains("--namespace observability")
                && !call.contains(" -n curie ")
                && !call.contains("--namespace curie")
                && !call.starts_with("upgrade --install curie ")
                && !call.starts_with("upgrade curie ")
        }),
        "Helm must not retain default Curie or observability identities: {helm:?}"
    );

    let kubectl = fixture.kubectl_calls();
    assert!(
        kubectl.iter().any(|call| {
            call.contains("get secret grafana-admin") && call.contains("--namespace soak-obs")
        }),
        "Grafana admin inspect must use soak-obs: {kubectl:?}"
    );
    assert!(
        kubectl.iter().any(|call| {
            call.contains("apply")
                && call.contains("--namespace soak-obs")
                && call.contains("tempo.yaml")
        }),
        "Tempo apply must use soak-obs: {kubectl:?}"
    );
    assert!(
        kubectl.iter().any(|call| {
            call.contains("wait")
                && call.contains("--namespace soak")
                && call.contains("secret/sre-bot-reader-token")
        }),
        "reader token wait must use soak: {kubectl:?}"
    );
    assert!(
        kubectl.iter().any(|call| {
            call.contains("get secret sre-bot-reader-token") && call.contains("--namespace soak")
        }),
        "reader token read must use soak: {kubectl:?}"
    );
    assert!(
        kubectl.iter().any(|call| call.contains("get secret")
            && call.contains("app.kubernetes.io/instance=soak-rel")
            && (call.contains("-n soak") || call.contains("--namespace soak"))),
        "API key discovery must use the selected Curie identity: {kubectl:?}"
    );
    assert!(
        kubectl
            .iter()
            .any(|call| call.contains("-n soak apply -f -")),
        "connector reconciliation must apply in soak: {kubectl:?}"
    );
    assert!(
        kubectl.iter().any(|call| {
            call.contains("-n soak delete deployment,service,networkpolicy,secret ")
        }),
        "stale connector objects must be deleted from soak: {kubectl:?}"
    );
    assert!(
        kubectl.iter().all(|call| {
            !call.contains("--namespace observability")
                && !call.contains("-n curie")
                && !call.contains("--namespace curie")
                && !call.contains("instance=curie")
        }),
        "kubectl must not retain default Curie or observability identities: {kubectl:?}"
    );

    let grafana: Value = serde_json::from_slice(&fixture.kubectl_stdin())
        .expect("Grafana admin Secret stdin must be JSON");
    assert_eq!(grafana["kind"], "Secret");
    assert_eq!(grafana["metadata"]["namespace"], CUSTOM_OBS_NAMESPACE);

    let read_access = fixture.applied_file("read-access.yaml");
    assert!(
        read_access.contains("namespace: soak"),
        "read-access must render the selected Curie namespace: {read_access}"
    );
    assert!(
        !read_access.contains("namespace: curie"),
        "read-access must not retain the default Curie namespace: {read_access}"
    );

    let tempo = fixture.applied_file("tempo.yaml");
    assert!(
        tempo.contains("namespace: soak-obs"),
        "Tempo must render the selected observability namespace: {tempo}"
    );
    assert!(
        !tempo.contains("namespace: observability"),
        "Tempo must not retain the default observability namespace: {tempo}"
    );

    let grafana_values = fixture.helm_values_file("grafana-values.yaml");
    assert!(
        grafana_values.contains("loki.soak-obs.svc.cluster.local"),
        "Grafana values must point at soak-obs: {grafana_values}"
    );
    assert!(
        !grafana_values.contains(".observability.svc.cluster.local"),
        "Grafana values must not retain observability DNS: {grafana_values}"
    );

    let alloy_values = fixture.helm_values_file("alloy-values.yaml");
    assert!(
        alloy_values.contains("loki.soak-obs.svc.cluster.local"),
        "Alloy values must point at soak-obs: {alloy_values}"
    );
    assert!(
        !alloy_values.contains(".observability.svc.cluster.local"),
        "Alloy values must not retain observability DNS: {alloy_values}"
    );

    let curie_values = fixture.helm_values_file("curie-values.yaml");
    assert!(
        curie_values.contains("tempo.soak-obs.svc.cluster.local")
            && curie_values.contains("grafana.soak-obs.svc.cluster.local")
            && curie_values.contains("prometheus-server.soak-obs.svc.cluster.local")
            && curie_values.contains("namespace: soak-obs")
            && curie_values.contains("kubernetes.io/metadata.name: soak-obs"),
        "Curie integration values must point at soak-obs: {curie_values}"
    );
    assert!(
        !curie_values.contains(".observability.svc.cluster.local")
            && !curie_values.contains("namespace: observability")
            && !curie_values.contains("kubernetes.io/metadata.name: observability"),
        "Curie integration values must not retain observability: {curie_values}"
    );

    let connectors = String::from_utf8(uploaded_bundle_file(&fixture, "connectors.yaml"))
        .expect("uploaded connectors are UTF-8");
    assert!(
        connectors.contains("grafana.soak-obs.svc.cluster.local"),
        "runtime connectors must point Grafana at soak-obs: {connectors}"
    );
    assert!(
        !connectors.contains("grafana.observability.svc.cluster.local"),
        "runtime connectors must not retain observability DNS: {connectors}"
    );
}

/// #2059: the memory envelope and query bounds must survive the INSTALLER path.
///
/// The OOM in #2059 was in the SHIPPED manifest -- the bytes the CLI actually
/// hands `kubectl apply`, not the bytes sitting in
/// `examples/sre-bot/observability/tempo.yaml`. The CLI embeds that file with
/// `include_bytes!` (`cli/src/examples.rs`) and rewrites it before applying, so
/// asserting the source file alone proves nothing about what an operator gets.
/// This test reads the captured applied document and asserts the envelope there.
///
/// It parses the applied YAML rather than substring-matching, so a bound
/// restored to Tempo's distributed default fails on the VALUE rather than
/// passing a presence check. Each bound has its own assertion and its own
/// message, so a failure names the key that went missing.
#[test]
fn applied_tempo_manifest_carries_the_bounded_memory_envelope() {
    let fixture = Fixture::with_modes(
        nodes(vec![node("node-a", "4Gi", true)]),
        pods(vec![]),
        "success",
        "success",
        "success",
    );
    let output = fixture.run(&[]);
    let shown_output = shown(&output);
    assert!(
        output.status.success(),
        "the default install must complete: {shown_output}"
    );

    let tempo = fixture.applied_file("tempo.yaml");
    let documents = manifest_documents(&tempo);

    let stateful_set = documents
        .iter()
        .find(|document| document["kind"] == "StatefulSet")
        .unwrap_or_else(|| panic!("applied Tempo must carry a StatefulSet: {tempo}"));
    let container = &stateful_set["spec"]["template"]["spec"]["containers"][0];

    let resources = &container["resources"];
    assert_eq!(
        resources["requests"]["memory"], "256Mi",
        "the applied Tempo container must request the measured 256Mi that the installer \
         capacity preflight sums (REQUIRED_MIB {REQUIRED_MIB}Mi): {tempo}"
    );
    assert_eq!(
        resources["limits"]["memory"], "1Gi",
        "the applied Tempo container must carry the measured 1Gi hard ceiling: {tempo}"
    );

    let gomemlimit = container["env"]
        .as_array()
        .map(|env| env.to_vec())
        .unwrap_or_default()
        .into_iter()
        .find(|entry| entry["name"] == "GOMEMLIMIT");
    assert_eq!(
        gomemlimit.as_ref().map(|entry| entry["value"].clone()),
        Some(Value::from("600MiB")),
        "the applied Tempo container must carry the soft Go heap ceiling below its cgroup \
         limit; without it the Go GC never learns the cgroup ceiling: {tempo}"
    );

    let annotations = &stateful_set["spec"]["template"]["metadata"]["annotations"];
    assert_ne!(
        annotations["checksum/config"], "v1",
        "checksum/config must move off v1, or `kubectl apply` updates the ConfigMap and the \
         running pod keeps the unbounded config: {tempo}"
    );

    let config_map = documents
        .iter()
        .find(|document| document["kind"] == "ConfigMap")
        .unwrap_or_else(|| panic!("applied Tempo must carry a ConfigMap: {tempo}"));
    let config_text = config_map["data"]["tempo.yaml"]
        .as_str()
        .unwrap_or_else(|| panic!("applied Tempo ConfigMap must carry tempo.yaml: {tempo}"));
    let config: Value = serde_norway::from_str(config_text)
        .unwrap_or_else(|error| panic!("applied Tempo config must be valid YAML: {error}"));

    // The read-buffer product `max_workers * read_buffer_count *
    // read_buffer_size_bytes` is the structural defect: at Tempo's distributed
    // defaults it is 12.8 GiB against a small-node limit. These two bounds are
    // what turn it into a ceiling rather than a threshold.
    assert_eq!(
        config["storage"]["trace"]["pool"]["max_workers"], 20,
        "storage.trace.pool.max_workers must be bounded (Tempo's distributed default is 400): \
         {config_text}"
    );
    assert_eq!(
        config["storage"]["trace"]["search"]["read_buffer_count"], 8,
        "storage.trace.search.read_buffer_count must be bounded (default 32): {config_text}"
    );
    assert_eq!(
        config["ingester"]["max_block_bytes"], 52428800,
        "ingester.max_block_bytes must be bounded; the 500 MiB default lets one in-memory head \
         block reach the whole pod limit: {config_text}"
    );
    assert_eq!(
        config["query_frontend"]["search"]["concurrent_jobs"], 40,
        "query_frontend.search.concurrent_jobs must be bounded (default 1000) so search fan-out \
         does not scale with block count: {config_text}"
    );
    assert_eq!(
        config["query_frontend"]["search"]["max_result_limit"], 50,
        "query_frontend.search.max_result_limit must mirror MAX_LIMIT in \
         examples/sre-bot/connectors/tempo/server.py; the two move together: {config_text}"
    );
}

#[test]
fn selected_observability_namespace_is_the_only_managed_capacity_namespace() {
    let mut items = managed_stack_pods("node-a");
    items.push(labeled_pod(
        "unrelated",
        "node-a",
        OBSERVABILITY_NAMESPACE,
        json!({"app.kubernetes.io/instance": "other"}),
        "64Mi",
    ));
    let fixture = Fixture::new(
        nodes(vec![node("node-a", "1440Mi", true)]),
        pods(items.clone()),
    );
    let default_output = fixture.run(&[]);
    assert_reached_helm_upgrade(&fixture, &default_output);

    let custom = Fixture::new(nodes(vec![node("node-a", "1440Mi", true)]), pods(items));
    let output = custom.run(&custom_target_args());
    let text = assert_refused_before_helm(&custom, &output);
    assert!(
        text.contains("required 1376Mi"),
        "load in the default observability namespace must count when targeting soak-obs: {text}"
    );
}

#[test]
fn helm_timeout_recovery_names_the_selected_observability_namespace() {
    let fixture = Fixture::with_modes(
        nodes(vec![node("node-a", "8Gi", true)]),
        pods(vec![]),
        "success",
        "success",
        "timeout",
    );
    let output = fixture.run(&custom_target_args());
    let text = shown(&output);
    assert_eq!(
        output.status.code(),
        Some(1),
        "Helm timeout must fail: {text}"
    );
    let recovery =
        "kubectl delete secret -n soak-obs -l owner=helm,name=grafana,status=pending-upgrade";
    let normalized = text.replace(['\'', '"'], "");
    assert!(
        normalized.contains(recovery),
        "timeout recovery must name soak-obs, not observability: {text}"
    );
    assert!(
        !normalized.contains(
            "kubectl delete secret -n observability -l owner=helm,name=grafana,status=pending-upgrade"
        ),
        "timeout recovery must not name the default observability namespace: {text}"
    );
}

/// AC3: the `--write-allowlist` path must upload a bundle the CLUSTER tier will
/// accept, and it must mint its write identity only after the deploy landed.
///
/// Issue #1946's evidence block is this bundle being refused: a connector that
/// declares only `build:` records a LOCAL image id, and `BUNDLE_FILES` carries
/// no `connectors/` build context, so the cluster preflight had nothing to pull
/// and nothing to build. Nothing pinned that, which is why it regressed once and
/// could regress again the next time a connector is added.
///
/// This is the assertion that NEVER SKIPS. It is a pure-Rust mirror of what
/// `lock_preflight` enforces at `DeployTier::Cluster`, so it holds the same
/// property as the bundle validator below without needing `uv` on PATH -- so the
/// property stays gated on a local run where that validator test skips.
///
/// The connector properties are asserted by ITERATING the uploaded declaration
/// rather than by naming `k8s-write`. A per-connector assertion is what makes a
/// future connector fail this test instead of slipping past it; naming the one
/// we already know about would pin the incident and not the class.
#[test]
fn write_allowlist_install_uploads_a_bundle_the_cluster_tier_accepts() {
    let fixture = Fixture::with_modes(
        nodes(vec![node("node-a", "4Gi", true)]),
        pods(vec![]),
        "success",
        "success",
        "success",
    );
    let output = fixture.run(&["--write-allowlist", "prod/api"]);
    let text = shown(&output);
    assert!(
        output.status.success(),
        "the named-target write install must complete: {text}"
    );

    let connectors_yaml = String::from_utf8(uploaded_bundle_file(&fixture, "connectors.yaml"))
        .expect("uploaded connectors are UTF-8");
    let declaration: Value = serde_norway::from_str(&connectors_yaml)
        .expect("uploaded connectors must remain valid YAML");
    let connectors = declaration["connectors"].as_object().unwrap_or_else(|| {
        panic!("the uploaded bundle must declare connectors: {connectors_yaml}")
    });
    assert!(
        !connectors.is_empty(),
        "an empty connector map would satisfy every loop below vacuously: {connectors_yaml}"
    );
    for (name, connector) in connectors {
        assert!(
            connector.get("build").is_none(),
            "connector {name} still asks the cluster deploy path to build it; a `build:` \
             declaration records a local image id the cluster tier refuses, and the bundle \
             carries no build context to fall back on: {connector:?}"
        );
        if let Some(image) = connector.get("image") {
            let image = image
                .as_str()
                .unwrap_or_else(|| panic!("connector {name} image must be a string: {image:?}"));
            assert!(
                image.contains("@sha256:"),
                "connector {name} must be pinned by digest rather than by a mutable tag, got \
                 {image}"
            );
        }
    }
    let write = &declaration["connectors"]["k8s-write"];
    let write_image = write["image"]
        .as_str()
        .unwrap_or_else(|| panic!("the write connector must carry a resolved image: {write:?}"));
    assert!(
        write_image.starts_with("ghcr.io/curie-eng/curie-sre-bot-k8s-write@sha256:"),
        "the write connector must run the PUBLISHED image, pinned by digest, got {write_image}"
    );
    assert_eq!(
        write["env"]["K8S_WRITE_ALLOWLIST"], "prod/api",
        "the connector-side ceiling must state exactly the targets named at install time"
    );
    // Correct precisely BECAUSE no connector declares `build:`: a lock file
    // exists to record the image id of something that had to be built, so a
    // fully digest-pinned declaration needs none. If one ever appears here it
    // means a build declaration came back with it.
    let paths = uploaded_bundle_paths(&fixture);
    assert!(
        !paths.iter().any(|path| path == "connectors.lock.yaml"),
        "a fully digest-pinned declaration needs no connector lock: {paths:?}"
    );

    // The API-server-side half of the ceiling. #1886's incident was these two
    // allowlists drifting apart, which is a 403 AFTER a human approved the call.
    let write_role = fixture.applied_file("write-role.yaml");
    let documents = manifest_documents(&write_role);
    let role = documents
        .iter()
        .find(|document| document["kind"] == "Role")
        .unwrap_or_else(|| panic!("named targets must render a Role: {write_role}"));
    assert_eq!(
        role["metadata"]["namespace"], "prod",
        "the Role must live in the target's namespace: {write_role}"
    );
    assert_eq!(
        role["rules"][0]["resourceNames"],
        json!(["api"]),
        "the Role must be scoped to exactly the named Deployment: {write_role}"
    );
    let binding = documents
        .iter()
        .find(|document| document["kind"] == "RoleBinding")
        .unwrap_or_else(|| panic!("named targets must render a RoleBinding: {write_role}"));
    assert_eq!(binding["metadata"]["namespace"], "prod");
    assert_eq!(binding["roleRef"]["name"], "sre-bot-writer");
    assert_eq!(binding["subjects"][0]["name"], "sre-bot-writer");
    // The occurrence the shipped manifest's own comment warns is easy to miss:
    // a RoleBinding whose subject namespace is wrong still applies cleanly and
    // grants nothing.
    assert_eq!(
        binding["subjects"][0]["namespace"], "curie",
        "the RoleBinding must bind the ServiceAccount in the Curie namespace: {write_role}"
    );

    // AC2, pinned in one log. The writer is a NON-EXPIRING ServiceAccount token
    // carrying `get,patch` on `apps/deployments`, so it must not exist until the
    // deploy it belongs to has landed. Before #1946 the apply ran before the
    // release API key was even discovered, which put the whole deploy inside the
    // window where a failure stranded a live write credential.
    let kubectl = fixture.kubectl_calls();
    let write_role_apply = kubectl
        .iter()
        .position(|call| {
            call.starts_with("apply -f ") && call.ends_with("manifests/write-role.yaml")
        })
        .unwrap_or_else(|| {
            panic!("the installer must apply the rendered write identity: {kubectl:?}")
        });
    let api_key_discovery = kubectl
        .iter()
        .rposition(|call| {
            call.contains("get secret") && call.contains("app.kubernetes.io/instance=curie")
        })
        .unwrap_or_else(|| panic!("the deploy must discover the release API key: {kubectl:?}"));
    let reader_token_read = kubectl
        .iter()
        .position(|call| call.contains("get secret sre-bot-reader-token"))
        .unwrap_or_else(|| panic!("the installer must read the reader token: {kubectl:?}"));
    assert!(
        write_role_apply > api_key_discovery && write_role_apply > reader_token_read,
        "the write identity must be minted AFTER the deploy: write-role apply at \
         {write_role_apply}, API key discovery at {api_key_discovery}, reader token read at \
         {reader_token_read}: {kubectl:?}"
    );
}

/// The MIRROR IMAGE of the ordering above, for the re-install path.
///
/// The incident this pins: an operator NARROWS the allowlist on a re-install
/// (`prod/admin` -> `prod/api`). `sync_deployed_version` reconciles the connector
/// Deployment and its kubeconfig Secret only after the deploy has ACTIVATED the
/// new version, so if the write RBAC were also deferred past the deploy there
/// would be a window in which the new version is live while BOTH the old, wider
/// Role and the old connector's wider env are still in force. A `patch` on
/// `prod/admin` succeeds in that window -- the install fails OPEN on precisely
/// the change an operator made to close something down.
///
/// So the ordering is keyed on whether the writer identity already exists, and
/// the invariant has two halves: never create a NEW privileged identity before
/// the deploy, and always tighten an EXISTING one before the deploy. This test
/// and `a_failed_deploy_leaves_no_write_rbac_or_writer_token_behind` are THE SAME
/// SCENARIO -- `--write-allowlist prod/api` against a deploy the platform
/// refuses -- differing in exactly one input, whether the writer
/// ServiceAccount already existed. The opposite outcomes ARE the invariant:
/// there the apply must be absent, here it must be present.
///
/// Why the oracle is a failed deploy rather than a position in the kubectl log.
/// `deploy_embedded_sre_bot` is API-driven and emits no kubectl calls at all, so
/// the log has no marker for "the deploy happened". On BOTH orderings the
/// write-role apply lands after the connection-setup lookups (API key discovery
/// belongs to `resolve_embedded_cluster_connection`, which is setup, not
/// activation) and before the post-deploy connector sync. Any index comparison
/// therefore either passes on both paths or fails on both, and proves nothing.
/// Failing the deploy makes the causal relationship itself observable: the apply
/// can only appear in the log of a run whose deploy failed if it ran BEFORE that
/// deploy. Nothing is stranded by applying early on this path, because every
/// object the apply touches already exists.
#[test]
fn an_existing_writer_identity_is_tightened_before_the_deploy_that_can_fail() {
    let fixture = Fixture::with_modes(
        nodes(vec![node("node-a", "4Gi", true)]),
        pods(vec![]),
        "success",
        "success",
        "success",
    )
    .with_writer_identity_mode("existing")
    .with_deploy_failure();
    let output = fixture.run(&["--write-allowlist", "prod/api"]);
    let text = shown(&output);
    assert!(
        !output.status.success(),
        "a refused deployment must fail the install: {text}"
    );

    let kubectl = fixture.kubectl_calls();
    assert!(
        kubectl.iter().any(|call| {
            call.contains("get serviceaccount sre-bot-writer")
                && call.contains("--ignore-not-found")
        }),
        "the ordering can only be chosen if the installer probes for the existing writer \
         identity: {kubectl:?}"
    );
    // THE LOAD-BEARING ASSERTION. The deploy failed, so this call exists only if
    // the narrowed ceiling was applied before it -- the exact call the
    // fresh-install twin proves is absent under the same failure.
    assert!(
        kubectl.iter().any(|call| {
            call.starts_with("apply -f ") && call.ends_with("manifests/write-role.yaml")
        }),
        "on a re-install the narrowed ceiling must land BEFORE the deploy, so a FAILED deploy \
         must still have applied it: {kubectl:?}"
    );
    // `apply_write_access` mints the kubeconfig in the same step, so the token
    // half of the identity moves with the RBAC half and is asserted with it.
    assert!(
        kubectl
            .iter()
            .any(|call| call.contains("wait") && call.contains("secret/sre-bot-writer-token")),
        "the tightening step must have waited for the writer token: {kubectl:?}"
    );
    assert!(
        kubectl
            .iter()
            .any(|call| call.contains("get secret sre-bot-writer-token")),
        "the tightening step must have read the writer token: {kubectl:?}"
    );

    // ANTI-VACUITY. "The apply is present in a failed run" is only evidence of
    // ordering if the run actually reached and attempted the deploy. Without
    // this the test would pass on any earlier abort -- a capacity refusal, a
    // registry failure -- which is how a reordering bug would hide behind green.
    let requests = fixture.api.recorded();
    let version = requests
        .iter()
        .position(|request| {
            request.method == "POST" && request.path == format!("/agents/{AGENT_ID}/versions")
        })
        .unwrap_or_else(|| panic!("the run must have created a version: {requests:?}"));
    let upload = requests
        .iter()
        .position(|request| request.method == "PUT" && request.path.ends_with("/bundle"))
        .unwrap_or_else(|| panic!("the run must have uploaded the bundle: {requests:?}"));
    let deployment = requests
        .iter()
        .position(|request| request.method == "POST" && request.path == "/deployments")
        .unwrap_or_else(|| panic!("the run must have attempted the deployment: {requests:?}"));
    assert!(
        version < upload && upload < deployment,
        "the failure must be the DEPLOY, reached after the bundle was already uploaded: \
         version at {version}, upload at {upload}, deployment at {deployment}"
    );
}

/// AC3, the bundle-validator leg: hand the uploaded bundle to the authoritative
/// `plugin_format.validate_bundle` and take its verdict rather than a
/// re-derivation of it.
///
/// SKIPS WHEN `uv` IS ABSENT, which now follows the same posture as the
/// `chart_check.rs` precedent it is borrowed from rather than diverging from it:
/// `chart_check.rs` skips locally but its assertions run in `helm-ci`, which
/// installs `uv`. The `rust:` job in `.github/workflows/ci.yaml` now installs
/// `uv` too, so this test RUNS in CI and its verdict gates there; the skip is
/// left in place only for a local developer who has no `uv` on PATH.
///
/// The skip branch is still a real hole in local runs, so it is not the only
/// thing standing behind this property.
/// `write_allowlist_install_uploads_a_bundle_the_cluster_tier_accepts` above is
/// the backstop: its properties are written to stand alone without Python for
/// exactly that reason, so they never skip anywhere.
///
/// Negative control, recorded so the skip is not mistaken for a test that
/// cannot bite: if `build:` survived into the uploaded bundle, this validator
/// reports `connectors.build_context_missing`, because the bundle allowlist
/// carries no `connectors/` directory. That is issue #1946's own evidence
/// block.
#[test]
fn write_allowlist_bundle_passes_the_real_bundle_validator() {
    if Command::new("uv").arg("--version").output().is_err() {
        eprintln!(
            "skipping write_allowlist_bundle_passes_the_real_bundle_validator: uv is not on PATH"
        );
        return;
    }

    let fixture = Fixture::with_modes(
        nodes(vec![node("node-a", "4Gi", true)]),
        pods(vec![]),
        "success",
        "success",
        "success",
    );
    let output = fixture.run(&["--write-allowlist", "prod/api"]);
    let text = shown(&output);
    assert!(
        output.status.success(),
        "the named-target write install must complete: {text}"
    );

    let bundle = unpacked_uploaded_bundle(&fixture);
    let validation = Command::new("uv")
        .current_dir(repo_root())
        .args([
            "run",
            "--frozen",
            "python",
            "-c",
            "import sys\n\
             from plugin_format import validate_bundle\n\
             print(validate_bundle(sys.argv[1]).model_dump_json())\n",
        ])
        .arg(bundle.path())
        .output()
        .expect("run the bundle validator");
    let stdout = String::from_utf8_lossy(&validation.stdout).to_string();
    let stderr = String::from_utf8_lossy(&validation.stderr).to_string();
    assert!(
        validation.status.success(),
        "the bundle validator must run: stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    let reported = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .unwrap_or_else(|| panic!("the validator must report a result: stderr:\n{stderr}"));
    let result: Value = serde_json::from_str(reported)
        .unwrap_or_else(|error| panic!("validator output must be JSON ({error}): {reported}"));
    assert_eq!(
        result["errors"],
        json!([]),
        "the uploaded bundle must carry no validation errors: {reported}"
    );
    assert_eq!(
        result["valid"],
        json!(true),
        "the uploaded bundle must be valid: {reported}"
    );
}

/// AC2: a failed deploy must leave NO write RBAC and NO writer token behind.
///
/// The writer identity is a non-expiring `kubernetes.io/service-account-token`
/// plus, with named targets, `get,patch` on `apps/deployments` -- and `patch` on
/// a Deployment is `set image` and `set env`, not just a restart. Creating it
/// before the deploy meant any failure in between (unreachable API server,
/// rate-limited RBAC, a dead port-forward) left that credential standing with no
/// bot attached and no teardown path.
///
/// The remedy is DEFERRAL, not rollback, so this test looks for the apply never
/// having happened rather than for a compensating delete: a rollback needs the
/// same cluster API call that just failed.
#[test]
fn a_failed_deploy_leaves_no_write_rbac_or_writer_token_behind() {
    let fixture = Fixture::with_modes(
        nodes(vec![node("node-a", "4Gi", true)]),
        pods(vec![]),
        "success",
        "success",
        "success",
    )
    .with_deploy_failure();
    let output = fixture.run(&["--write-allowlist", "prod/api"]);
    let text = shown(&output);
    assert!(
        !output.status.success(),
        "a refused deployment must fail the install: {text}"
    );

    let kubectl = fixture.kubectl_calls();
    assert!(
        !kubectl
            .iter()
            .any(|call| call.contains("manifests/write-role.yaml")),
        "a failed deploy must not have applied the write RBAC: {kubectl:?}"
    );
    assert!(
        !kubectl
            .iter()
            .any(|call| call.contains("sre-bot-writer-token")),
        "a failed deploy must neither wait for nor read the writer token: {kubectl:?}"
    );
    // Asserted as an absent file rather than through `applied_file`, which
    // panics on a miss and would report the wrong thing.
    assert!(
        !fixture.applied_dir.join("write-role.yaml").exists(),
        "a failed deploy must leave no rendered write identity behind: {:?}",
        fs::read_dir(&fixture.applied_dir).map(|entries| entries
            .flatten()
            .map(|entry| entry.file_name())
            .collect::<Vec<_>>())
    );

    // ANTI-VACUITY. Without this the test passes trivially on any early abort --
    // a capacity refusal, a registry failure, a reader-token timeout -- which is
    // exactly how a reordering bug would hide behind a green run. The assertions
    // above only mean something if this run really did get as far as the deploy.
    assert!(
        fixture.applied_dir.join("read-access.yaml").exists(),
        "the run must have reached the reader RBAC apply"
    );
    assert!(
        kubectl
            .iter()
            .any(|call| call.contains("get secret sre-bot-reader-token")),
        "the run must have read the reader token: {kubectl:?}"
    );
    let requests = fixture.api.recorded();
    let version = requests
        .iter()
        .position(|request| {
            request.method == "POST" && request.path == format!("/agents/{AGENT_ID}/versions")
        })
        .unwrap_or_else(|| panic!("the run must have created a version: {requests:?}"));
    let upload = requests
        .iter()
        .position(|request| request.method == "PUT" && request.path.ends_with("/bundle"))
        .unwrap_or_else(|| panic!("the run must have uploaded the bundle: {requests:?}"));
    let deployment = requests
        .iter()
        .position(|request| request.method == "POST" && request.path == "/deployments")
        .unwrap_or_else(|| panic!("the run must have attempted the deployment: {requests:?}"));
    assert!(
        version < upload && upload < deployment,
        "the failure must be the DEPLOY, reached after the bundle was already uploaded: \
         version at {version}, upload at {upload}, deployment at {deployment}"
    );
}

/// AC4a: `--no-write` stays behaviourally read only, asserted on the wire.
///
/// `read_only_install_is_unchanged_by_the_new_flag` and
/// `opting_out_leaves_the_write_connector_absent` prove this at helper level;
/// both stay, because they are cheaper and they localise a failure. This lifts
/// the same property to what the platform actually receives, which is where a
/// regression would be visible to an operator.
#[test]
fn no_write_install_stays_behaviourally_read_only() {
    let fixture = Fixture::with_modes(
        nodes(vec![node("node-a", "4Gi", true)]),
        pods(vec![]),
        "success",
        "success",
        "success",
    );
    let output = fixture.run(&["--no-write"]);
    let text = shown(&output);
    assert!(
        output.status.success(),
        "the read only install must complete: {text}"
    );

    let connectors_yaml = String::from_utf8(uploaded_bundle_file(&fixture, "connectors.yaml"))
        .expect("uploaded connectors are UTF-8");
    let declaration: Value = serde_norway::from_str(&connectors_yaml)
        .expect("uploaded connectors must remain valid YAML");
    let connectors = declaration["connectors"].as_object().unwrap_or_else(|| {
        panic!("the uploaded bundle must declare connectors: {connectors_yaml}")
    });
    for absent in ["k8s-write", "k8s-scale"] {
        assert!(
            !connectors.contains_key(absent),
            "the read only install must not ship connector {absent}: {connectors_yaml}"
        );
    }

    let plugin: Value = serde_json::from_slice(&uploaded_bundle_file(
        &fixture,
        ".claude-plugin/plugin.json",
    ))
    .expect("uploaded plugin manifest must remain valid JSON");
    // A gate naming a connector this install removed fails bundle validation for
    // everyone, so the policy leaves WITH the connectors it guards.
    assert!(
        plugin.get("approvalPolicy").is_none(),
        "a bundle with no write verbs must declare no approval policy: {plugin}"
    );

    assert!(
        !fixture.applied_dir.join("write-role.yaml").exists(),
        "the read only install must render and apply no write identity"
    );
    let kubectl = fixture.kubectl_calls();
    assert!(
        !kubectl
            .iter()
            .any(|call| call.contains("sre-bot-writer-token")),
        "the read only install must never touch the writer token: {kubectl:?}"
    );

    // The registry count is the observable proof that the connector was dropped
    // BEFORE its image was resolved -- a `--no-write` install must make no
    // registry call for an image it will never run. Counted rather than merely
    // absent, and asserted here rather than by relaxing the count in
    // `successful_install_uploads_only_the_resolved_tempo_index_digest`, which
    // pins the four-request default.
    let registry = fixture.registry.recorded();
    assert_eq!(
        registry.len(),
        2,
        "only Tempo may be resolved, at one scoped token plus one index request: {:?}",
        registry
            .iter()
            .map(|request| request.path.clone())
            .collect::<Vec<_>>()
    );
    assert!(
        !registry
            .iter()
            .any(|request| request.path.contains("curie-sre-bot-k8s-write")),
        "the read only install must make no registry request for the write image: {:?}",
        registry
            .iter()
            .map(|request| request.path.clone())
            .collect::<Vec<_>>()
    );
}

/// AC4b: the no-flag default keeps the connector with an EMPTY ceiling and
/// renders NO Role.
///
/// This is #1886's load-bearing invariant and no integration test pinned it. An
/// RBAC rule whose `resourceNames` is empty or absent does not grant nothing --
/// it grants the verb on EVERY resource of that type. So the empty case must
/// OMIT the Role rather than render one with no names, and a nameless Role would
/// hand the bot patch on every Deployment in the namespace: the opposite of what
/// an empty allowlist reads like, and it would look correct in review.
///
/// The identity is still minted, because bring-up refuses without
/// `K8S_WRITE_KUBECONFIG`; an identity that can do nothing is what makes
/// "installed, ceiling empty" a state the platform can boot.
#[test]
fn default_install_keeps_the_empty_ceiling_and_renders_no_role() {
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
        "the default install must complete: {text}"
    );

    let connectors_yaml = String::from_utf8(uploaded_bundle_file(&fixture, "connectors.yaml"))
        .expect("uploaded connectors are UTF-8");
    let declaration: Value = serde_norway::from_str(&connectors_yaml)
        .expect("uploaded connectors must remain valid YAML");
    assert_eq!(
        declaration["connectors"]["k8s-write"]["env"]["K8S_WRITE_ALLOWLIST"], "",
        "with no targets named the connector-side ceiling must be empty, so every call is \
         refused: {connectors_yaml}"
    );

    let write_role = fixture.applied_file("write-role.yaml");
    let kinds: Vec<String> = manifest_documents(&write_role)
        .iter()
        .filter_map(|document| document["kind"].as_str().map(str::to_string))
        .collect();
    assert!(
        kinds.iter().any(|kind| kind == "ServiceAccount"),
        "the identity is minted either way: {write_role}"
    );
    assert!(
        kinds.iter().any(|kind| kind == "Secret"),
        "the non-expiring token Secret rides with the ServiceAccount: {write_role}"
    );
    assert!(
        !kinds
            .iter()
            .any(|kind| kind == "Role" || kind == "RoleBinding"),
        "an empty ceiling must render NO Role -- an empty resourceNames grants every \
         Deployment, not none: {kinds:?} in {write_role}"
    );
}
