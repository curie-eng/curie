//! Binary integration contract for the gVisor preflight diagnosis in issue
//! #1653. Every case drives the real `curie cluster up` entrypoint with fake
//! Helm and kubectl executables on PATH.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant};

const TARGET_RELEASE: &str = "target-release";
const TARGET_NAMESPACE: &str = "target-namespace";
const RENDERED_JOB: &str = "acme-runtime-preflight-gvisor";
const RUNTIME_CLASS_REJECTION: &str = "Error creating: pods \"acme-runtime-preflight-gvisor-example\" is forbidden: pod rejected: RuntimeClass \"gvisor\" not found";
const OPENROUTER_CREDENTIAL: &str = "sk-or-v1-PLACEHOLDER";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn chart() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/../charts/curie")
}

fn write_exec(dir: &Path, name: &str, body: &str) {
    let body = if matches!(name, "helm" | "kubectl") {
        format!(
            "#!/bin/sh\n{}\n{}",
            include_str!("data/converged-installation-read.sh"),
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
    plan_file: PathBuf,
    upgrade_log: PathBuf,
    uninstall_log: PathBuf,
    helm_sequence: PathBuf,
    event_log: PathBuf,
    helm_pid: PathBuf,
    helm_termination_log: PathBuf,
    helm_pending: PathBuf,
    namespace_ready: PathBuf,
    fresh_event: PathBuf,
    watch_pid: PathBuf,
    event_emitted: PathBuf,
    event_mode: String,
    singleton_mode: String,
    credential: String,
    history_status: String,
}

impl Fixture {
    fn new(event_mode: &str, singleton_mode: &str, credential: &str) -> Self {
        let temp = tempfile::tempdir().expect("temporary directory");
        let bin_dir = temp.path().join("bin");
        fs::create_dir(&bin_dir).expect("create fake binary directory");
        let plan_file = temp.path().join("curie.yaml");
        fs::write(
            &plan_file,
            "version: 1\ninstall:\n  namespace: target-namespace\n  release: target-release\ncredentials:\n  model: CURIE_CREDENTIALS\nset:\n  fullnameOverride: acme-runtime\n  security.gvisor.mode: require\n",
        )
        .expect("write prepared apply plan");
        let upgrade_log = temp.path().join("upgrades.log");
        let uninstall_log = temp.path().join("uninstalls.log");
        let helm_sequence = temp.path().join("helm-sequence.log");
        let event_log = temp.path().join("event-queries.log");
        let helm_pid = temp.path().join("helm.pid");
        let helm_termination_log = temp.path().join("helm-termination.log");
        let helm_pending = temp.path().join("helm-pending");
        let namespace_ready = temp.path().join("namespace-ready");
        let fresh_event = temp.path().join("fresh-event");
        let watch_pid = temp.path().join("watch.pid");
        let event_emitted = temp.path().join("event-emitted");

        write_exec(
            &bin_dir,
            "helm",
            r#"#!/bin/sh
if [ "$1" = "get" ] && [ "$2" = "values" ]; then
    printf '%s\n' 'Error: release: not found' >&2
    exit 1
fi

if [ "$1" = "template" ]; then
    fullname="target-release"
    platform_create="true"
    sandbox_create="true"
    gvisor_mode="auto"
    fake_model="true"
    install_runtimeclass="false"
    show_only=""
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --set|--set-string)
                shift
                case "$1" in
                    fullnameOverride=*) fullname=${1#*=} ;;
                    priorityClasses.platform.create=*) platform_create=${1#*=} ;;
                    priorityClasses.sandbox.create=*) sandbox_create=${1#*=} ;;
                    security.gvisor.mode=*) gvisor_mode=${1#*=} ;;
                    security.gvisor.installRuntimeClass=*) install_runtimeclass=${1#*=} ;;
                    agentSandbox.runner.fakeModel=*) fake_model=${1#*=} ;;
                esac
                ;;
            --show-only)
                shift
                show_only="$1"
                ;;
            --show-only=*)
                show_only=${1#*=}
                ;;
        esac
        shift
    done

    if [ "$show_only" = "templates/preflight-gvisor.yaml" ]; then
        if [ "$gvisor_mode" = "off" ] || { [ "$gvisor_mode" = "auto" ] && [ "$fake_model" = "true" ]; }; then
            printf '%s\n' 'Error: could not find template "templates/preflight-gvisor.yaml" in chart' >&2
            exit 1
        fi
        if [ "$install_runtimeclass" = "true" ]; then
            printf '%s\n' \
                'apiVersion: node.k8s.io/v1' \
                'kind: RuntimeClass' \
                'metadata:' \
                '  name: gvisor' \
                'handler: runsc' \
                '---'
        fi
        printf '%s\n' \
            'apiVersion: batch/v1' \
            'kind: Job' \
            'metadata:' \
            "  name: $fullname-preflight-gvisor" \
            'spec:' \
            '  backoffLimit: 0'
        exit 0
    fi

    if [ "$show_only" = "templates/priorityclass.yaml" ]; then
        first="true"
        if [ "$platform_create" = "true" ]; then
            printf '%s\n' \
                'apiVersion: scheduling.k8s.io/v1' \
                'kind: PriorityClass' \
                'metadata:' \
                '  name: curie-platform' \
                'value: 1000000' \
                'globalDefault: false'
            first="false"
        fi
        if [ "$sandbox_create" = "true" ]; then
            if [ "$first" = "false" ]; then
                printf '%s\n' '---'
            fi
            printf '%s\n' \
                'apiVersion: scheduling.k8s.io/v1' \
                'kind: PriorityClass' \
                'metadata:' \
                '  name: curie-sandbox' \
                'value: 100000' \
                'globalDefault: false'
        fi
        exit 0
    fi

    printf 'unexpected helm template invocation: %s\n' "$*" >&2
    exit 64
fi

if [ "$1" = "upgrade" ] && [ "$2" = "--install" ]; then
    printf '%s\n' "$*" >> "$CURIE_TEST_UPGRADE_LOG"
    printf '%s\n' "$*" >> "$CURIE_TEST_HELM_SEQUENCE"
    printf '%s\n' "$$" > "$CURIE_TEST_HELM_PID"
    gvisor_mode="auto"
    fake_model="true"
    for argument in "$@"; do
        case "$argument" in
            security.gvisor.mode=*) gvisor_mode=${argument#*=} ;;
            agentSandbox.runner.fakeModel=*) fake_model=${argument#*=} ;;
        esac
    done
    if [ -e "$CURIE_TEST_HELM_PENDING" ]; then
        printf '%s\n' 'Error: UPGRADE FAILED: another operation (install/upgrade/rollback) is in progress' >&2
        exit 1
    fi
    if { [ "$CURIE_TEST_EVENT_MODE" = "matching" ] || [ "$CURIE_TEST_EVENT_MODE" = "fresh-namespace" ]; } && { [ "$gvisor_mode" = "require" ] || { [ "$gvisor_mode" = "auto" ] && [ "$fake_model" = "false" ]; }; }; then
        : > "$CURIE_TEST_HELM_PENDING"
        graceful_exit() {
            signal="$1"
            if [ -n "${sleep_pid:-}" ]; then
                kill "$sleep_pid" 2>/dev/null || true
                wait "$sleep_pid" 2>/dev/null || true
            fi
            rm -f "$CURIE_TEST_HELM_PENDING"
            printf '%s\n' "$signal" >> "$CURIE_TEST_HELM_TERMINATION_LOG"
            exit 1
        }
        trap 'graceful_exit HUP' HUP
        trap 'graceful_exit INT' INT
        trap 'graceful_exit TERM' TERM
        if [ "$CURIE_TEST_EVENT_MODE" = "fresh-namespace" ]; then
            : > "$CURIE_TEST_NAMESPACE_READY"
            : > "$CURIE_TEST_FRESH_EVENT"
        fi
        sleep 5 &
        sleep_pid=$!
        wait "$sleep_pid"
        trap - HUP INT TERM
        rm -f "$CURIE_TEST_HELM_PENDING"
        printf '%s\n' 'Error: UPGRADE FAILED: pre-upgrade hooks failed: job target-release-preflight-gvisor failed: DeadlineExceeded' >&2
        exit 1
    fi
    if [ "$CURIE_TEST_HISTORY_STATUS" = "failed" ] && [ ! -s "$CURIE_TEST_UNINSTALL_LOG" ]; then
        printf '%s\n' 'Error: UPGRADE FAILED: pre-upgrade hooks failed: job target-release-upgrade-drain failed: secret not found' >&2
        exit 1
    fi
    sleep 1
    printf '%s\n' 'Release installed'
    exit 0
fi

if [ "$1" = "history" ]; then
    printf '%s\n' "$*" >> "$CURIE_TEST_HELM_SEQUENCE"
    if [ "$2" != "target-release" ] || [ "$3" != "-n" ] || [ "$4" != "target-namespace" ]; then
        printf 'unexpected helm history: %s\n' "$*" >&2
        exit 64
    fi
    case "$CURIE_TEST_HISTORY_STATUS" in
        absent)
            printf '%s\n' 'Error: release: not found' >&2
            exit 1
            ;;
        deployed)
            printf '%s\n' '[{"revision":1,"status":"deployed","chart":"curie-0.0.0","description":"Install complete"}]'
            exit 0
            ;;
        superseded)
            printf '%s\n' '[{"revision":1,"status":"superseded","chart":"curie-0.0.0","description":"Upgrade complete"},{"revision":2,"status":"failed","chart":"curie-0.0.0","description":"Upgrade \"target-release\" failed: context canceled"}]'
            exit 0
            ;;
        failed)
            printf '%s\n' '[{"revision":1,"status":"failed","chart":"curie-0.0.0","description":"Release \"target-release\" failed: context canceled"}]'
            exit 0
            ;;
        pending-install)
            printf '%s\n' '[{"revision":1,"status":"pending-install","chart":"curie-0.0.0","description":"Initial install underway"}]'
            exit 0
            ;;
        *)
            if [ -s "$CURIE_TEST_UPGRADE_LOG" ]; then
                printf '%s\n' '[{"revision":1,"status":"failed","chart":"curie-0.0.0","description":"Release \"target-release\" failed: context canceled"}]'
                exit 0
            fi
            printf '%s\n' 'Error: release: not found' >&2
            exit 1
            ;;
    esac
fi

if [ "$1" = "uninstall" ]; then
    printf '%s\n' "$*" >> "$CURIE_TEST_UNINSTALL_LOG"
    printf '%s\n' "$*" >> "$CURIE_TEST_HELM_SEQUENCE"
    if [ "$2" != "target-release" ] || [ "$3" != "-n" ] || [ "$4" != "target-namespace" ]; then
        printf 'unexpected helm uninstall: %s\n' "$*" >&2
        exit 64
    fi
    printf '%s\n' 'release "target-release" uninstalled'
    exit 0
fi

printf 'unexpected helm invocation: %s\n' "$*" >&2
exit 64
"#,
        );

        write_exec(
            &bin_dir,
            "kubectl",
            r#"#!/bin/sh
if [ "$1" = "get" ] && [ "$2" = "namespace" ]; then
    if [ "$3" = "target-namespace" ] && [ "$CURIE_TEST_EVENT_MODE" = "fresh-namespace" ] && [ ! -e "$CURIE_TEST_NAMESPACE_READY" ]; then
        # `--ignore-not-found -o json`: absence is exit 0 plus empty stdout.
        exit 0
    fi
    if [ "$3" = "target-namespace" ]; then
        printf '%s\n' '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"target-namespace","labels":{"curietech.ai/created-by":"target-release","curietech.ai/created-in":"target-namespace"},"uid":"uid-target-namespace","resourceVersion":"17"}}'
    fi
    exit 0
fi

if [ "$1" = "create" ] && [ "$2" = "-f" ] && [ "$3" = "-" ]; then
    manifest=$(cat)
    if MANIFEST="$manifest" python3 -c 'import json, os, sys
document = json.loads(os.environ["MANIFEST"])
metadata = document.get("metadata", {})
labels = metadata.get("labels", {})
sys.exit(0 if document.get("kind") == "Namespace"
         and metadata.get("name") == "target-namespace"
         and labels.get("curietech.ai/created-by") == "target-release"
         and labels.get("curietech.ai/created-in") == "target-namespace"
         else 1)'
    then
        : > "$CURIE_TEST_NAMESPACE_READY"
        printf '%s\n' 'namespace/target-namespace created'
        exit 0
    fi
    printf 'namespace was not atomically created with its ownership pair: %s\n' "$manifest" >&2
    exit 64
fi

if [ "$1" = "get" ] && [ "$2" = "statefulset" ]; then
    if [ "$3" = "-n" ] && [ "$4" = "target-namespace" ] && [ "$5" = "-o" ] && [ "$6" = "json" ]; then
        printf '%s\n' '{"apiVersion":"v1","kind":"List","items":[]}'
        exit 0
    fi
    printf 'unexpected StatefulSet query: %s\n' "$*" >&2
    exit 64
fi

if [ "$1" = "label" ] && [ "$2" = "namespace" ] && [ "$3" = "target-namespace" ]; then
    case " $* " in
        *" --overwrite "*) exit 0 ;;
    esac
fi

if [ "$1" = "get" ] && [ "$2" = "priorityclass" ]; then
    if [ "$CURIE_TEST_SINGLETON_MODE" = "foreign" ]; then
        printf '{"apiVersion":"scheduling.k8s.io/v1","kind":"PriorityClass","metadata":{"name":"%s","labels":{"app.kubernetes.io/managed-by":"Helm"},"annotations":{"meta.helm.sh/release-name":"shared-owner","meta.helm.sh/release-namespace":"shared-owner-namespace"}}}\n' "$3"
    fi
    exit 0
fi

case " $* " in
    *" get deployment agent-sandbox-controller "*)
        case " $* " in
            *" -n agent-sandbox-system "*) ;;
            *)
                printf 'controller query was not scoped to agent-sandbox-system: %s\n' "$*" >&2
                exit 64
                ;;
        esac
        if [ "$CURIE_TEST_SINGLETON_MODE" = "foreign" ]; then
            printf '%s\n' '{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"agent-sandbox-controller","labels":{"app.kubernetes.io/managed-by":"Helm"},"annotations":{"meta.helm.sh/release-name":"shared-controller","meta.helm.sh/release-namespace":"shared-owner-namespace"}}}'
        fi
        exit 0
        ;;
esac

if [ "$1" = "get" ] && { [ "$2" = "event" ] || [ "$2" = "events" ]; }; then
    printf '%s\n' "$*" >> "$CURIE_TEST_EVENT_LOG"
    case " $* " in
        *" --all-namespaces "*)
            printf '%s\n' 'Error from server (Forbidden): events is forbidden at the cluster scope' >&2
            exit 1
            ;;
        *" -n target-namespace "*) ;;
        *)
            printf 'event query was not scoped to target-namespace: %s\n' "$*" >&2
            exit 64
            ;;
    esac
    watch="false"
    watch_only="false"
    output_watch_events="false"
    combined_jsonpath="false"
    unit_separator_jsonpath="false"
    uid_snapshot="false"
    for arg in "$@"; do
        case "$arg" in
            --watch|--watch=true|-w) watch="true" ;;
            --watch-only) watch_only="true" ;;
            --output-watch-events) output_watch_events="true" ;;
            jsonpath=*items*metadata.uid*) uid_snapshot="true" ;;
            jsonpath=*.object.metadata.uid*object.involvedObject.kind*) combined_jsonpath="true" ;;
        esac
        case "$arg" in
            *\\u001f*) unit_separator_jsonpath="true" ;;
        esac
    done

    if [ "$CURIE_TEST_EVENT_MODE" = "watch-unavailable" ]; then
        printf '%s\n' 'Error from server (Forbidden): events is forbidden in target-namespace' >&2
        exit 1
    fi
    if [ "$watch" != "true" ]; then
        if [ "$uid_snapshot" != "true" ] || [ "$watch_only" = "true" ] || [ "$output_watch_events" = "true" ]; then
            printf 'unexpected Event UID snapshot invocation: %s\n' "$*" >&2
            exit 64
        fi
        if [ "$CURIE_TEST_EVENT_MODE" = "matching" ]; then
            printf '%s\n' 'existing-event-uid'
        fi
        exit 0
    fi
    if [ "$watch" != "true" ] || [ "$watch_only" = "true" ] || [ "$output_watch_events" != "true" ] || [ "$combined_jsonpath" != "true" ] || [ "$unit_separator_jsonpath" != "true" ]; then
        printf 'unexpected list and watch invocation: %s\n' "$*" >&2
        exit 64
    fi
    if [ "$CURIE_TEST_EVENT_MODE" = "fresh-namespace" ] && [ ! -e "$CURIE_TEST_NAMESPACE_READY" ]; then
        printf '%s\n' 'Error from server (NotFound): namespaces "target-namespace" not found' >&2
        exit 1
    fi
    printf '%s\n' "$$" > "$CURIE_TEST_WATCH_PID"
    trap 'exit 0' HUP INT TERM
    if [ "$CURIE_TEST_EVENT_MODE" = "fresh-namespace" ]; then
        attempts=0
        while [ ! -e "$CURIE_TEST_FRESH_EVENT" ] && [ "$attempts" -lt 100 ]; do
            sleep 0.01
            attempts=$((attempts + 1))
        done
        if [ ! -e "$CURIE_TEST_FRESH_EVENT" ]; then
            printf '%s\n' 'Helm did not emit the fresh namespace event' >&2
            exit 64
        fi
        : > "$CURIE_TEST_EVENT_EMITTED"
        printf '%s\037%s\037%s\037%s\037%s\037%s\037%s\n' \
            'ADDED' 'fresh-event-uid' 'Job' 'target-namespace' 'acme-runtime-preflight-gvisor' 'FailedCreate' \
            'Error creating: pods "acme-runtime-preflight-gvisor-example" is forbidden: pod rejected: RuntimeClass "gvisor" not found'
        sleep 30
        exit 0
    fi
    if [ "$CURIE_TEST_EVENT_MODE" = "matching" ]; then
        printf '%s\037%s\037%s\037%s\037%s\037%s\037%s\n' \
            'ADDED' 'existing-event-uid' 'Job' 'target-namespace' 'acme-runtime-preflight-gvisor' 'FailedCreate' \
            'Error creating: pods "stale-added-preflight-example" is forbidden: pod rejected: RuntimeClass "stale-added" not found'
    fi
    attempts=0
    while [ ! -e "$CURIE_TEST_HELM_PID" ] && [ "$attempts" -lt 100 ]; do
        sleep 0.01
        attempts=$((attempts + 1))
    done
    if [ ! -e "$CURIE_TEST_HELM_PID" ]; then
        printf '%s\n' 'Helm did not start while the Event stream had no initial row' >&2
        exit 64
    fi
    sleep 0.15
    if [ "$CURIE_TEST_EVENT_MODE" = "matching" ]; then
        printf '%s\037%s\037%s\037%s\037%s\037%s\037%s\n' \
            'MODIFIED' 'existing-event-uid' 'Job' 'target-namespace' 'acme-runtime-preflight-gvisor' 'FailedCreate' \
            'Error creating: pods "stale-modified-preflight-example" is forbidden: pod rejected: RuntimeClass "stale-modified" not found'
        sleep 0.05
        event_type='ADDED'
        event_uid='new-event-uid'
        message='Error creating: pods "acme-runtime-preflight-gvisor-example" is forbidden: pod rejected: RuntimeClass "gvisor" not found'
    else
        event_type='ADDED'
        event_uid='new-event-uid'
        message='admission webhook "images.example.com" denied the request'
    fi
    : > "$CURIE_TEST_EVENT_EMITTED"
    printf '%s\037%s\037%s\037%s\037%s\037%s\037%s\n' \
        "$event_type" "$event_uid" 'Job' 'target-namespace' 'acme-runtime-preflight-gvisor' 'FailedCreate' "$message"
    sleep 30
    exit 0
fi

printf 'unexpected kubectl invocation: %s\n' "$*" >&2
exit 64
"#,
        );

        Self {
            _temp: temp,
            bin_dir,
            plan_file,
            upgrade_log,
            uninstall_log,
            helm_sequence,
            event_log,
            helm_pid,
            helm_termination_log,
            helm_pending,
            namespace_ready,
            fresh_event,
            watch_pid,
            event_emitted,
            event_mode: event_mode.to_string(),
            singleton_mode: singleton_mode.to_string(),
            credential: credential.to_string(),
            history_status: "auto".to_string(),
        }
    }

    fn with_history_status(mut self, status: &str) -> Self {
        self.history_status = status.to_string();
        self
    }

    fn run(&self, extra: &[&str]) -> (Output, Duration) {
        let mut args = vec![
            "--color",
            "never",
            "cluster",
            "up",
            "--chart",
            chart(),
            "--namespace",
            TARGET_NAMESPACE,
            "--release",
            TARGET_RELEASE,
            "--dev",
            "--no-expose",
            "--set",
            "fullnameOverride=acme-runtime",
        ];
        args.extend_from_slice(extra);
        self.run_args(&args)
    }

    fn run_apply(&self) -> (Output, Duration) {
        self.run_args(&[
            "--color",
            "never",
            "apply",
            "--file",
            self.plan_file.to_str().expect("UTF 8 plan path"),
            "--chart",
            chart(),
        ])
    }

    fn run_args(&self, args: &[&str]) -> (Output, Duration) {
        let mut paths = vec![self.bin_dir.clone()];
        if let Some(current) = std::env::var_os("PATH") {
            paths.extend(std::env::split_paths(&current));
        }
        let path = std::env::join_paths(paths).expect("join PATH");

        let started = Instant::now();
        let mut command = Command::new(bin());
        command
            .args(args)
            .env("PATH", path)
            .env("CI", "1")
            .env("TERM", "dumb")
            .env("NO_COLOR", "1")
            .env("CURIE_TEST_UPGRADE_LOG", &self.upgrade_log)
            .env("CURIE_TEST_UNINSTALL_LOG", &self.uninstall_log)
            .env("CURIE_TEST_HELM_SEQUENCE", &self.helm_sequence)
            .env("CURIE_TEST_HISTORY_STATUS", &self.history_status)
            .env("CURIE_TEST_EVENT_LOG", &self.event_log)
            .env("CURIE_TEST_HELM_PID", &self.helm_pid)
            .env(
                "CURIE_TEST_HELM_TERMINATION_LOG",
                &self.helm_termination_log,
            )
            .env("CURIE_TEST_HELM_PENDING", &self.helm_pending)
            .env("CURIE_TEST_NAMESPACE_READY", &self.namespace_ready)
            .env("CURIE_TEST_FRESH_EVENT", &self.fresh_event)
            .env("CURIE_TEST_WATCH_PID", &self.watch_pid)
            .env("CURIE_TEST_EVENT_EMITTED", &self.event_emitted)
            .env("CURIE_TEST_EVENT_MODE", &self.event_mode)
            .env("CURIE_TEST_SINGLETON_MODE", &self.singleton_mode)
            .env(
                "CURIE_TEST_PROVIDER_EGRESS_JSON",
                r#"{"openrouter.ai":["1.1.1.1"],"api.anthropic.com":["8.8.8.8"]}"#,
            )
            .env_remove("CURIE_MODEL_CREDENTIALS")
            .env_remove("CURIE_GITHUB_TOKEN")
            .env_remove("CURIE_MODEL");
        if self.credential.is_empty() {
            command.env_remove("CURIE_CREDENTIALS");
        } else {
            command.env("CURIE_CREDENTIALS", &self.credential);
        }
        let output = command.output().expect("run curie command");
        (output, started.elapsed())
    }

    fn upgrade_count(&self) -> usize {
        fs::read_to_string(&self.upgrade_log)
            .unwrap_or_default()
            .lines()
            .count()
    }

    fn uninstall_count(&self) -> usize {
        fs::read_to_string(&self.uninstall_log)
            .unwrap_or_default()
            .lines()
            .count()
    }

    fn helm_mutations(&self) -> Vec<String> {
        fs::read_to_string(&self.helm_sequence)
            .unwrap_or_default()
            .lines()
            .filter_map(|line| {
                let verb = line.split_whitespace().next()?;
                matches!(verb, "upgrade" | "uninstall").then(|| line.to_string())
            })
            .collect()
    }

    /// Issue #2347: aborting the first Helm attempt leaves a failed revision.
    /// Recovery must uninstall that never-deployed release before the gVisor-off
    /// retry so Helm treats the retry as a clean install, not a pre-upgrade.
    fn assert_failed_revision_discarded_before_retry(&self) {
        let uninstalls = fs::read_to_string(&self.uninstall_log).unwrap_or_default();
        assert_eq!(
            uninstalls.lines().count(),
            1,
            "automatic gVisor recovery must uninstall the failed first revision once:\n{uninstalls}"
        );
        assert_eq!(
            uninstalls.trim(),
            "uninstall target-release -n target-namespace --wait --timeout 60s",
            "the discard must helm uninstall --wait so owned namespaces are gone before reinstall:\n{uninstalls}"
        );
        let mutations = self.helm_mutations();
        assert_eq!(
            mutations.len(),
            3,
            "recovery must run upgrade, uninstall, upgrade — not a second upgrade against the failed revision:\n{mutations:?}"
        );
        assert!(
            mutations[0].starts_with("upgrade --install"),
            "the interrupted first attempt must be helm upgrade --install:\n{mutations:?}"
        );
        assert_eq!(
            mutations[1], "uninstall target-release -n target-namespace --wait --timeout 60s",
            "the failed revision must be uninstalled before the retry:\n{mutations:?}"
        );
        assert!(
            mutations[2].starts_with("upgrade --install")
                && mutations[2].contains("security.gvisor.mode=off"),
            "the retry must be a clean helm upgrade --install with gVisor off:\n{mutations:?}"
        );
        let sequence = fs::read_to_string(&self.helm_sequence).unwrap_or_default();
        assert!(
            sequence.lines().any(|line| line.starts_with("history ")
                && line.contains("target-release")
                && line.contains("-n target-namespace")
                && line.contains("-o json")
                && line.contains("--max")
                && line.contains("256")),
            "recovery must read helm history before deciding to discard:\n{sequence}"
        );
    }

    fn assert_no_failed_revision_discard(&self) {
        let uninstalls = fs::read_to_string(&self.uninstall_log).unwrap_or_default();
        assert!(
            uninstalls.is_empty(),
            "this path must not helm uninstall the release:\n{uninstalls}"
        );
    }

    fn write_apply_plan(&self, gvisor_mode: &str) {
        fs::write(
            &self.plan_file,
            format!(
                "version: 1\ninstall:\n  namespace: {TARGET_NAMESPACE}\n  release: {TARGET_RELEASE}\ncredentials:\n  model: CURIE_CREDENTIALS\nset:\n  fullnameOverride: acme-runtime\n  security.gvisor.mode: \"{gvisor_mode}\"\n"
            ),
        )
        .expect("write apply plan");
    }

    /// Issue #2856: a later apply/up against a failed-only history must
    /// uninstall before `helm upgrade --install`, not fire the pre-upgrade drain.
    fn assert_failed_only_release_discarded_before_install(&self) {
        let uninstalls = fs::read_to_string(&self.uninstall_log).unwrap_or_default();
        assert_eq!(
            uninstalls.lines().count(),
            1,
            "a failed-only history must be uninstalled once before install:\n{uninstalls}"
        );
        assert_eq!(
            uninstalls.trim(),
            "uninstall target-release -n target-namespace --wait --timeout 60s",
            "the discard must helm uninstall --wait so owned namespaces are gone before reinstall:\n{uninstalls}"
        );
        let mutations = self.helm_mutations();
        assert_eq!(
            mutations.len(),
            2,
            "must uninstall then upgrade --install, not upgrade a failed revision:\n{mutations:?}"
        );
        assert_eq!(
            mutations[0], "uninstall target-release -n target-namespace --wait --timeout 60s",
            "the failed revision must be uninstalled before helm upgrade --install:\n{mutations:?}"
        );
        assert!(
            mutations[1].starts_with("upgrade --install"),
            "install after discard must be helm upgrade --install:\n{mutations:?}"
        );
        let sequence = fs::read_to_string(&self.helm_sequence).unwrap_or_default();
        let history = sequence
            .lines()
            .position(|line| {
                line.starts_with("history ")
                    && line.contains("target-release")
                    && line.contains("-n target-namespace")
                    && line.contains("-o json")
                    && line.contains("--max")
                    && line.contains("256")
            })
            .expect("must read helm history before deciding to discard");
        let uninstall = sequence
            .lines()
            .position(|line| {
                line == "uninstall target-release -n target-namespace --wait --timeout 60s"
            })
            .expect("must uninstall the failed revision");
        let upgrade = sequence
            .lines()
            .position(|line| line.starts_with("upgrade --install"))
            .expect("must helm upgrade --install after discard");
        assert!(
            history < uninstall && uninstall < upgrade,
            "history, uninstall, then upgrade --install; got:\n{sequence}"
        );
    }

    fn assert_event_was_observed_for_rendered_job(&self) {
        assert!(
            self.event_emitted.is_file(),
            "the fake kubectl watch must emit an event before the command finishes"
        );
        let invocations = fs::read_to_string(&self.event_log).unwrap_or_default();
        assert!(
            invocations.contains(&format!("involvedObject.name={RENDERED_JOB}")),
            "the event selector must use the rendered fullname override:\n{invocations}"
        );
        let watch_invocations: Vec<&str> = invocations
            .lines()
            .filter(|line| line.contains("--watch"))
            .collect();
        assert_eq!(
            watch_invocations.len(),
            1,
            "exactly one list and watch stream must observe the install:\n{invocations}"
        );
        assert!(
            watch_invocations[0].contains("{.object.metadata.uid}")
                && watch_invocations[0].contains("{.object.involvedObject.kind}")
                && watch_invocations[0].contains(r#"{"\u001f"}"#)
                && !watch_invocations[0].contains("--watch-only")
                && !invocations.contains("--resource-version"),
            "one list and watch stream must cover current and future Events:\n{invocations}"
        );
        if self.event_mode != "fresh-namespace" {
            let snapshots: Vec<&str> = invocations
                .lines()
                .filter(|line| !line.contains("--watch"))
                .collect();
            assert_eq!(
                snapshots.len(),
                1,
                "an existing namespace must snapshot matching Event UIDs once:\n{invocations}"
            );
            assert!(
                snapshots[0].contains(r#"{range .items[*]}{.metadata.uid}"#),
                "the stale Event boundary must use object UIDs, not list resource versions:\n{invocations}"
            );
        }
        assert!(
            invocations
                .lines()
                .all(|line| line.contains("-n target-namespace")
                    && !line.contains("--all-namespaces")),
            "every event query must use namespaced permissions:\n{invocations}"
        );
    }

    fn assert_graceful_helm_interruption(&self) {
        let signals = fs::read_to_string(&self.helm_termination_log).unwrap_or_default();
        assert_eq!(
            signals, "INT\n",
            "Helm must receive one graceful interrupt before any forced cleanup"
        );
        assert!(
            !self.helm_pending.exists(),
            "the interrupted Helm operation must not leave a pending release"
        );
    }

    fn assert_no_event_watch(&self) {
        assert!(
            fs::read_to_string(&self.event_log)
                .unwrap_or_default()
                .is_empty(),
            "a nonrendering gVisor preflight must not query Events"
        );
        assert!(
            !self.watch_pid.exists(),
            "a nonrendering gVisor preflight must not spawn an Event watch"
        );
    }

    fn assert_children_stopped(&self) {
        assert_process_stopped(&self.helm_pid, "helm upgrade");
        assert_process_stopped(&self.watch_pid, "kubectl event watch");
    }
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn assert_process_stopped(pid_file: &Path, label: &str) {
    let pid = fs::read_to_string(pid_file)
        .unwrap_or_else(|error| panic!("read {label} pid: {error}"))
        .trim()
        .parse::<u32>()
        .unwrap_or_else(|error| panic!("parse {label} pid: {error}"));
    let process = PathBuf::from(format!("/proc/{pid}"));
    for _ in 0..50 {
        if !process.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("{label} process {pid} survived curie cluster up");
}

fn assert_inference_once(shown: &str, applied_override: &str) {
    assert_eq!(
        shown.matches(applied_override).count(),
        1,
        "the applied override must be disclosed exactly once: {applied_override}\n{shown}"
    );
}

fn failed_install_line(shown: &str) -> bool {
    shown
        .lines()
        .any(|line| line.contains(&format!("installing release {TARGET_RELEASE}: failed")))
}

fn retrying_install_line(shown: &str) -> bool {
    shown
        .lines()
        .any(|line| line.contains(&format!("installing release {TARGET_RELEASE}: retrying")))
}

fn ok_install_line(shown: &str) -> bool {
    shown
        .lines()
        .any(|line| line.contains(&format!("installing release {TARGET_RELEASE}: ok")))
}

/// Issue #1906: automatic gVisor recovery must narrate the first attempt as
/// retrying, not as a terminal red failure, and must leave one successful
/// install result after the retry.
fn assert_automatic_gvisor_recovery_narration(shown: &str) {
    assert!(
        !failed_install_line(shown),
        "automatic gVisor recovery must not freeze the first install attempt as a terminal failure:\n{shown}"
    );
    assert!(
        retrying_install_line(shown),
        "the first install attempt must render as retrying after admission rejects gVisor:\n{shown}"
    );
    assert_eq!(
        shown
            .lines()
            .filter(|line| line.contains(&format!("installing release {TARGET_RELEASE}: retrying")))
            .count(),
        1,
        "exactly one retrying install attempt:\n{shown}"
    );
    assert!(
        ok_install_line(shown),
        "the retry must leave one unambiguous installed result:\n{shown}"
    );
    assert!(
        shown.contains("inferred that the cluster has no `gvisor` RuntimeClass from admission")
            && shown.contains("--set security.gvisor.mode=off"),
        "recovery must announce the gVisor admission retry with the inferred override:\n{shown}"
    );
}

#[test]
fn bare_cluster_up_infers_all_detected_facts_and_retries_once() {
    let fixture = Fixture::new("matching", "foreign", OPENROUTER_CREDENTIAL);
    let (output, elapsed) = fixture.run(&[]);
    let shown = stderr(&output);

    assert!(
        output.status.success(),
        "the bare command must converge on the detected cluster facts\nstdout:\n{}\nstderr:\n{shown}",
        String::from_utf8_lossy(&output.stdout),
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "the event must win well before the five second fake Helm deadline, elapsed {elapsed:?}\nstderr:\n{shown}"
    );
    assert!(
        !shown.contains("stale-added") && !shown.contains("stale-modified"),
        "Events that existed before this install must be ignored even when modified:\n{shown}"
    );
    for applied in [
        "--allow-egress-host openrouter",
        "--set priorityClasses.platform.create=false",
        "--set priorityClasses.sandbox.create=false",
        "--set agentSandbox.controller.deploy=false",
        "--set security.gvisor.mode=off",
    ] {
        assert_inference_once(&shown, applied);
    }
    assert_automatic_gvisor_recovery_narration(&shown);
    assert!(
        !shown.contains("DeadlineExceeded"),
        "the later Helm timeout must not become the diagnosis:\n{shown}"
    );
    assert_eq!(
        fixture.upgrade_count(),
        2,
        "the matching admission result permits exactly one retry"
    );
    let upgrades = fs::read_to_string(&fixture.upgrade_log).expect("read Helm upgrade log");
    assert!(
        upgrades.contains("security.networkPolicy.allowedEgress[0].cidr=1.1.1.1/32"),
        "the inferred OpenRouter route must reach Helm:\n{upgrades}"
    );
    assert!(
        upgrades
            .lines()
            .last()
            .is_some_and(|line| line.contains("security.gvisor.mode=off")),
        "the retry must carry the inferred mode off value:\n{upgrades}"
    );
    assert!(
        !shown.contains(OPENROUTER_CREDENTIAL),
        "credential leaked: {shown}"
    );
    fixture.assert_failed_revision_discarded_before_retry();
    fixture.assert_event_was_observed_for_rendered_job();
    fixture.assert_graceful_helm_interruption();
    fixture.assert_children_stopped();
}

#[test]
fn runtimeclass_document_before_job_still_observes_the_rendered_job() {
    let fixture = Fixture::new("nonmatching", "absent", "");
    let (output, _) = fixture.run(&[
        "--fake-model",
        "--set",
        "security.gvisor.mode=require",
        "--set",
        "security.gvisor.installRuntimeClass=true",
    ]);
    assert!(
        output.status.success(),
        "a RuntimeClass document before the Job must not block the install\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        stderr(&output)
    );
    assert_eq!(fixture.upgrade_count(), 1);
    fixture.assert_no_failed_revision_discard();
    fixture.assert_event_was_observed_for_rendered_job();
    fixture.assert_children_stopped();
}

#[test]
fn fresh_namespace_rejection_is_observed_after_cli_creates_the_namespace() {
    let fixture = Fixture::new("fresh-namespace", "absent", OPENROUTER_CREDENTIAL);
    let (output, elapsed) = fixture.run(&[]);
    let shown = stderr(&output);

    assert!(
        output.status.success(),
        "a fresh namespace must converge after the observed RuntimeClass rejection\nstdout:\n{}\nstderr:\n{shown}",
        String::from_utf8_lossy(&output.stdout),
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "the pre-Helm Event watch must still beat the fake deadline, elapsed {elapsed:?}\nstderr:\n{shown}"
    );
    assert_inference_once(&shown, "--set security.gvisor.mode=off");
    assert_automatic_gvisor_recovery_narration(&shown);
    assert!(
        !shown.contains("DeadlineExceeded"),
        "the Helm deadline must not replace the fresh namespace diagnosis:\n{shown}"
    );
    assert_eq!(fixture.upgrade_count(), 2);
    assert!(
        fixture.namespace_ready.is_file() && fixture.event_emitted.is_file(),
        "the CLI must create the namespace before Helm emits the event observed by the retry"
    );
    let invocations = fs::read_to_string(&fixture.event_log).unwrap_or_default();
    assert_eq!(
        invocations.lines().count(),
        2,
        "fresh namespace recovery must snapshot once and use one list-and-watch stream:\n{invocations}"
    );
    assert!(
        invocations
            .lines()
            .any(|line| !line.contains("--watch") && line.contains("{range .items[*]}{.metadata.uid}")),
        "the atomically created namespace must establish a stale Event UID boundary:\n{invocations}"
    );
    assert!(
        invocations.contains("{.object.metadata.uid}")
            && invocations.contains("{.object.involvedObject.kind}")
            && invocations.contains(r#"{"\u001f"}"#)
            && !invocations.contains("--resource-version"),
        "the stream must inspect current Events without an EventList resource version:\n{invocations}"
    );
    assert!(
        invocations
            .lines()
            .all(|line| line.contains("-n target-namespace") && !line.contains("--all-namespaces")),
        "fresh namespace retries must retain namespaced permissions:\n{invocations}"
    );
    fixture.assert_failed_revision_discarded_before_retry();
    fixture.assert_graceful_helm_interruption();
    fixture.assert_children_stopped();
}

#[test]
fn nonmatching_failedcreate_does_not_abort_successful_install() {
    let fixture = Fixture::new("nonmatching", "absent", "");
    let (output, _) = fixture.run(&["--fake-model", "--set", "security.gvisor.mode=require"]);
    let shown = stderr(&output);

    assert!(
        output.status.success(),
        "an unrelated FailedCreate event must not abort cluster up\nstdout:\n{}\nstderr:\n{shown}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert_eq!(
        fixture.upgrade_count(),
        1,
        "helm upgrade --install must run exactly once"
    );
    assert!(
        !shown.contains("--set security.gvisor.mode=off"),
        "an unrelated event must not produce the gVisor remediation:\n{shown}"
    );
    fixture.assert_no_failed_revision_discard();
    fixture.assert_event_was_observed_for_rendered_job();
    fixture.assert_children_stopped();
}

#[test]
fn bare_cluster_up_json_keeps_inferences_on_stderr_and_one_success_on_stdout() {
    let fixture = Fixture::new("matching", "foreign", OPENROUTER_CREDENTIAL);
    let (output, elapsed) = fixture.run(&["--json"]);
    let shown = stderr(&output);

    assert!(
        output.status.success(),
        "the JSON path must converge on detected facts\nstderr:\n{shown}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "the JSON path must also beat the fake Helm deadline, elapsed {elapsed:?}"
    );
    let payload: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("--json must emit exactly one success object: {error}"));
    assert!(
        payload.get("error").is_none(),
        "successful output must not retain the recovered rejection: {payload}"
    );
    for applied in [
        "--allow-egress-host openrouter",
        "--set priorityClasses.platform.create=false",
        "--set priorityClasses.sandbox.create=false",
        "--set agentSandbox.controller.deploy=false",
        "--set security.gvisor.mode=off",
    ] {
        assert_inference_once(&shown, applied);
    }
    assert_automatic_gvisor_recovery_narration(&shown);
    assert_eq!(fixture.upgrade_count(), 2);
    fixture.assert_failed_revision_discarded_before_retry();
    fixture.assert_event_was_observed_for_rendered_job();
    fixture.assert_graceful_helm_interruption();
    fixture.assert_children_stopped();
}

#[test]
fn explicit_gvisor_mode_contradicting_admission_is_rejected_before_retry() {
    for setting in ["security.gvisor.mode=auto", "security.gvisor.mode=require"] {
        let fixture = Fixture::new("matching", "absent", OPENROUTER_CREDENTIAL);
        let (output, elapsed) = fixture.run(&["--set", setting]);
        let shown = stderr(&output);

        assert_eq!(
            output.status.code(),
            Some(2),
            "{setting} contradicts the observed missing RuntimeClass\nstdout:\n{}\nstderr:\n{shown}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(elapsed < Duration::from_secs(3), "{setting}: {elapsed:?}");
        assert_eq!(
            fixture.upgrade_count(),
            1,
            "the explicit contradiction must stop before retry"
        );
        assert!(
            shown.contains(RUNTIME_CLASS_REJECTION),
            "{setting}: {shown}"
        );
        assert!(shown.contains(setting), "{setting}: {shown}");
        assert!(
            !shown.contains("--set security.gvisor.mode=off"),
            "the explicit value must not be silently overridden: {shown}"
        );
        assert!(
            failed_install_line(&shown),
            "an explicit contradiction is a terminal failure, not recovery:\n{shown}"
        );
        assert!(
            !retrying_install_line(&shown),
            "an explicit contradiction must not narrate a retry:\n{shown}"
        );
        fixture.assert_no_failed_revision_discard();
        fixture.assert_graceful_helm_interruption();
        fixture.assert_children_stopped();
    }
}

#[test]
fn failed_helm_revision_does_not_replace_explicit_gvisor_usage_recovery() {
    let fixture = Fixture::new("matching", "absent", OPENROUTER_CREDENTIAL);
    fs::write(fixture.bin_dir.join("convergence-release-failed"), "").unwrap();
    let (output, _) = fixture.run(&["--json", "--set", "security.gvisor.mode=require"]);
    assert_eq!(
        output.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(json["fix"]
        .as_str()
        .unwrap()
        .contains("remove the explicit"));
    assert_eq!(fixture.upgrade_count(), 1);
    fixture.assert_no_failed_revision_discard();
    fixture.assert_children_stopped();
}

#[test]
fn prepared_apply_keeps_the_exact_gvisor_rejection_fail_closed() {
    let fixture = Fixture::new("matching", "absent", OPENROUTER_CREDENTIAL);
    let (output, elapsed) = fixture.run_apply();
    let shown = stderr(&output);

    assert_eq!(
        output.status.code(),
        Some(1),
        "prepared apply must fail closed\nstdout:\n{}\nstderr:\n{shown}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(elapsed < Duration::from_secs(3), "{elapsed:?}: {shown}");
    assert!(shown.contains(RUNTIME_CLASS_REJECTION), "{shown}");
    assert!(
        shown.contains("curie apply") && shown.contains("platform.gvisor: off"),
        "prepared apply must point to the file based recovery: {shown}"
    );
    assert!(
        !shown.contains("curie cluster up --set security.gvisor.mode=off")
            && !shown.contains("security.gvisor.mode=off"),
        "prepared apply must not direct the operator to a cluster up override: {shown}"
    );
    assert!(
        !shown.contains("inferred that the cluster has no"),
        "prepared apply must not infer a posture: {shown}"
    );
    assert!(
        failed_install_line(&shown),
        "prepared apply must keep the first attempt as a terminal failure:\n{shown}"
    );
    assert!(
        !retrying_install_line(&shown),
        "prepared apply must not narrate automatic recovery:\n{shown}"
    );
    assert_eq!(fixture.upgrade_count(), 1, "prepared apply must not retry");
    fixture.assert_no_failed_revision_discard();
    fixture.assert_graceful_helm_interruption();
    fixture.assert_children_stopped();
}

#[test]
fn prepared_apply_progress_uses_the_file_recovery_path() {
    let fixture = Fixture::new("matching", "absent", OPENROUTER_CREDENTIAL);
    let (output, _) = fixture.run_apply();
    let shown = stderr(&output);

    assert!(
        shown.contains("generated strong per-release secrets")
            && shown.contains("re-running `curie apply` reuses them"),
        "the credential progress line must tell file based installs to rerun apply:\n{shown}"
    );
    assert!(
        shown.contains("resolved provider IPs can rotate; re-run `curie apply`")
            && !shown.contains("re-run `curie cluster up` if model calls start failing"),
        "the provider progress line must preserve the apply invocation:\n{shown}"
    );
    fixture.assert_graceful_helm_interruption();
    fixture.assert_children_stopped();
}

#[test]
fn fake_model_auto_and_mode_off_skip_the_event_observer() {
    for setting in ["security.gvisor.mode=auto", "security.gvisor.mode=off"] {
        let fixture = Fixture::new("matching", "absent", "");
        let (output, _) = fixture.run(&["--fake-model", "--set", setting]);
        assert!(
            output.status.success(),
            "{setting} must proceed when Helm reports that the conditional template rendered nothing\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            stderr(&output)
        );
        assert_eq!(fixture.upgrade_count(), 1);
        fixture.assert_no_failed_revision_discard();
        fixture.assert_no_event_watch();
    }
}

#[test]
fn unavailable_event_watch_preserves_the_helm_result() {
    let fixture = Fixture::new("watch-unavailable", "absent", "");
    let (output, _) = fixture.run(&["--fake-model", "--set", "security.gvisor.mode=require"]);
    assert!(
        output.status.success(),
        "an unavailable Event read must fall back to Helm's result\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        stderr(&output)
    );
    assert_eq!(fixture.upgrade_count(), 1);
    fixture.assert_no_failed_revision_discard();
    assert!(
        !fixture.watch_pid.exists(),
        "a failed Event snapshot must not spawn the watch process"
    );
}

/// Issue #2347: the gVisor observer aborts attempt 1, Helm records a failed
/// revision, and a naive `helm upgrade --install` retry is an upgrade whose
/// pre-upgrade drain hook waits on a Secret that revision never rendered.
#[test]
fn gvisor_retry_uninstalls_a_failed_first_revision_before_reinstall() {
    let fixture = Fixture::new("matching", "foreign", OPENROUTER_CREDENTIAL);
    let (output, elapsed) = fixture.run(&[]);
    let shown = stderr(&output);

    assert!(
        output.status.success(),
        "recovery must still converge after discarding the failed revision\nstdout:\n{}\nstderr:\n{shown}",
        String::from_utf8_lossy(&output.stdout),
    );
    assert!(elapsed < Duration::from_secs(3), "{elapsed:?}\n{shown}");
    assert_automatic_gvisor_recovery_narration(&shown);
    assert_eq!(fixture.upgrade_count(), 2);
    assert_eq!(fixture.uninstall_count(), 1);
    fixture.assert_failed_revision_discarded_before_retry();
    fixture.assert_graceful_helm_interruption();
    fixture.assert_children_stopped();
}

/// Negative for #2347: a history that already has a known-good revision is an
/// in-place upgrade. Discarding it would delete a working release.
#[test]
fn gvisor_retry_preserves_a_known_good_release() {
    for status in ["deployed", "superseded"] {
        let fixture =
            Fixture::new("matching", "foreign", OPENROUTER_CREDENTIAL).with_history_status(status);
        let (output, elapsed) = fixture.run(&[]);
        let shown = stderr(&output);

        assert!(
            output.status.success(),
            "{status} history must still recover with gVisor off\nstdout:\n{}\nstderr:\n{shown}",
            String::from_utf8_lossy(&output.stdout),
        );
        assert!(elapsed < Duration::from_secs(3), "{status}: {elapsed:?}");
        assert_automatic_gvisor_recovery_narration(&shown);
        assert_eq!(fixture.upgrade_count(), 2, "{status}");
        fixture.assert_no_failed_revision_discard();
        let mutations = fixture.helm_mutations();
        assert_eq!(
            mutations.len(),
            2,
            "{status}: must retry as a second upgrade, not uninstall a known-good release:\n{mutations:?}"
        );
        assert!(
            mutations
                .iter()
                .all(|line| line.starts_with("upgrade --install")),
            "{status}: {mutations:?}"
        );
        fixture.assert_graceful_helm_interruption();
        fixture.assert_children_stopped();
    }
}

/// An interrupted attempt that never recorded a Helm release is already a
/// clean install target; uninstall must tolerate "not found".
#[test]
fn gvisor_retry_tolerates_an_absent_release_history() {
    let fixture =
        Fixture::new("matching", "foreign", OPENROUTER_CREDENTIAL).with_history_status("absent");
    let (output, elapsed) = fixture.run(&[]);
    let shown = stderr(&output);

    assert!(
        output.status.success(),
        "an absent history must still recover\nstdout:\n{}\nstderr:\n{shown}",
        String::from_utf8_lossy(&output.stdout),
    );
    assert!(elapsed < Duration::from_secs(3), "{elapsed:?}\n{shown}");
    assert_automatic_gvisor_recovery_narration(&shown);
    fixture.assert_failed_revision_discarded_before_retry();
    fixture.assert_children_stopped();
}

/// Issue #2856: `curie apply` against a failed-only revision 1 must discard
/// that record and install, not take Helm's upgrade path.
#[test]
fn apply_discards_a_failed_only_revision_before_install() {
    let fixture =
        Fixture::new("matching", "absent", OPENROUTER_CREDENTIAL).with_history_status("failed");
    fixture.write_apply_plan("off");
    let (output, elapsed) = fixture.run_apply();
    let shown = stderr(&output);

    assert!(
        output.status.success(),
        "apply must converge after discarding the failed revision\nstdout:\n{}\nstderr:\n{shown}",
        String::from_utf8_lossy(&output.stdout),
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "must not wait on the pre-upgrade drain hook, elapsed {elapsed:?}\n{shown}"
    );
    assert_eq!(
        fixture.upgrade_count(),
        1,
        "apply must install once:\n{shown}"
    );
    fixture.assert_failed_only_release_discarded_before_install();
    fixture.assert_no_event_watch();
    assert_process_stopped(&fixture.helm_pid, "helm upgrade");
}

/// Negative for #2856: a known-good history is an in-place upgrade.
#[test]
fn apply_preserves_a_known_good_release() {
    for status in ["deployed", "superseded"] {
        let fixture =
            Fixture::new("matching", "absent", OPENROUTER_CREDENTIAL).with_history_status(status);
        fixture.write_apply_plan("off");
        let (output, elapsed) = fixture.run_apply();
        let shown = stderr(&output);

        assert!(
            output.status.success(),
            "{status} history must still apply\nstdout:\n{}\nstderr:\n{shown}",
            String::from_utf8_lossy(&output.stdout),
        );
        assert!(elapsed < Duration::from_secs(3), "{status}: {elapsed:?}");
        assert_eq!(fixture.upgrade_count(), 1, "{status}");
        fixture.assert_no_failed_revision_discard();
        let mutations = fixture.helm_mutations();
        assert_eq!(
            mutations.len(),
            1,
            "{status}: must upgrade the known-good release, not uninstall it:\n{mutations:?}"
        );
        assert!(
            mutations[0].starts_with("upgrade --install"),
            "{status}: {mutations:?}"
        );
        fixture.assert_no_event_watch();
        assert_process_stopped(&fixture.helm_pid, "helm upgrade");
    }
}

/// Sibling of apply: `cluster up` against a failed-only history must discard
/// before `helm upgrade --install` even when gVisor is already off.
#[test]
fn cluster_up_discards_a_failed_only_revision_before_install() {
    let fixture = Fixture::new("matching", "absent", "").with_history_status("failed");
    let (output, elapsed) = fixture.run(&["--fake-model", "--set", "security.gvisor.mode=off"]);
    let shown = stderr(&output);

    assert!(
        output.status.success(),
        "cluster up must converge after discarding the failed revision\nstdout:\n{}\nstderr:\n{shown}",
        String::from_utf8_lossy(&output.stdout),
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "must not wait on the pre-upgrade drain hook, elapsed {elapsed:?}\n{shown}"
    );
    assert_eq!(fixture.upgrade_count(), 1);
    fixture.assert_failed_only_release_discarded_before_install();
    fixture.assert_no_event_watch();
    assert_process_stopped(&fixture.helm_pid, "helm upgrade");
}

/// In-flight Helm status is not an upgrade target and is not silently discarded.
#[test]
fn apply_refuses_a_pending_only_revision() {
    let fixture = Fixture::new("matching", "absent", OPENROUTER_CREDENTIAL)
        .with_history_status("pending-install");
    fixture.write_apply_plan("off");
    let (output, elapsed) = fixture.run_apply();
    let shown = stderr(&output);

    assert!(
        !output.status.success(),
        "pending-install must refuse, not helm upgrade --install\n{shown}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "must refuse at once, elapsed {elapsed:?}\n{shown}"
    );
    assert!(
        shown.contains("no deployed revision") && shown.contains("pending-install"),
        "must name the in-flight status:\n{shown}"
    );
    assert!(
        shown.contains(
            "curie cluster down --yes --namespace target-namespace --release target-release"
        ),
        "must name the teardown command:\n{shown}"
    );
    assert_eq!(
        fixture.upgrade_count(),
        0,
        "must not helm upgrade:\n{shown}"
    );
    fixture.assert_no_failed_revision_discard();
    fixture.assert_no_event_watch();
}
