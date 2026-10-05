//! Binary regression contract for fast, ownership-checked admission failures
//! during `curie cluster up` (#3354).

#[path = "support/executable.rs"]
mod test_executable;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

const TARGET_RELEASE: &str = "target-release";
const TARGET_NAMESPACE: &str = "target-namespace";
const RELEASE_WORKLOAD_FAILURE: &str = "Error creating: pods \"acme-worker-controller-7f68c9\" is forbidden: serviceaccount \"target-namespace:acme-worker-controller\" not found";
const SYSTEM_CONTROLLER_FAILURE: &str = "Error creating: pods \"agent-sandbox-controller-7f68c9\" is forbidden: serviceaccount \"agent-sandbox-system:agent-sandbox-controller\" not found";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn chart() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/../charts/curie")
}

fn install_converged_stub(dir: &Path, name: &str, body: &str) {
    let body = if matches!(name, "helm" | "kubectl") {
        format!(
            "#!/bin/sh\n{}\n{}",
            include_str!("data/converged-installation-read.sh"),
            body.strip_prefix("#!/bin/sh\n").unwrap_or(body)
        )
    } else {
        body.to_string()
    };
    test_executable::install_in(dir, name, &body);
}

struct Fixture {
    _temp: tempfile::TempDir,
    bin_dir: PathBuf,
    upgrade_log: PathBuf,
    helm_pid: PathBuf,
    helm_pending: PathBuf,
    helm_termination_log: PathBuf,
    event_log: PathBuf,
    event_snapshot: PathBuf,
    event_mode: String,
    event_namespace: String,
}

impl Fixture {
    fn new(event_mode: &str, event_namespace: &str) -> Self {
        let temp = tempfile::tempdir().expect("temporary directory");
        let bin_dir = temp.path().join("bin");
        fs::create_dir(&bin_dir).expect("create fake binary directory");
        let upgrade_log = temp.path().join("upgrades.log");
        let helm_pid = temp.path().join("helm.pid");
        let helm_pending = temp.path().join("helm-pending");
        let helm_termination_log = temp.path().join("helm-termination.log");
        let event_log = temp.path().join("event-queries.log");
        let event_snapshot = temp.path().join("event-snapshot-seen");

        install_converged_stub(
            &bin_dir,
            "helm",
            r#"#!/bin/sh
if [ "$1" = "get" ] && [ "$2" = "values" ]; then
    printf '%s\n' 'Error: release: not found' >&2
    exit 1
fi

if [ "$1" = "template" ]; then
    case " $* " in
        *" --show-only templates/priorityclass.yaml "*|*" --show-only=templates/priorityclass.yaml "*)
            printf '%s\n' 'Error: could not find template templates/priorityclass.yaml in chart' >&2
            exit 1
            ;;
        *" --show-only templates/preflight-gvisor.yaml "*|*" --show-only=templates/preflight-gvisor.yaml "*)
            printf '%s\n' 'Error: could not find template templates/preflight-gvisor.yaml in chart' >&2
            exit 1
            ;;
    esac
    printf 'unexpected helm template invocation: %s\n' "$*" >&2
    exit 64
fi

if [ "$1" = "upgrade" ] && [ "$2" = "--install" ]; then
    printf '%s\n' "$*" >> "$CURIE_TEST_UPGRADE_LOG"
    printf '%s\n' "$$" > "$CURIE_TEST_HELM_PID"
    : > "$CURIE_TEST_HELM_PENDING"
    sleep 4 &
    sleep_pid=$!
    graceful_exit() {
        signal="$1"
        kill "$sleep_pid" 2>/dev/null || true
        wait "$sleep_pid" 2>/dev/null || true
        rm -f "$CURIE_TEST_HELM_PENDING"
        printf '%s\n' "$signal" >> "$CURIE_TEST_HELM_TERMINATION_LOG"
        exit 1
    }
    trap 'graceful_exit INT' INT
    trap 'graceful_exit TERM' TERM
    wait "$sleep_pid"
    trap - INT TERM
    rm -f "$CURIE_TEST_HELM_PENDING"
    printf '%s\n' 'Error: UPGRADE FAILED: timed out waiting for the condition' >&2
    exit 1
fi

if [ "$1" = "history" ]; then
    printf '%s\n' 'Error: release: not found' >&2
    exit 1
fi

printf 'unexpected helm invocation: %s\n' "$*" >&2
exit 64
"#,
        );

        install_converged_stub(
            &bin_dir,
            "kubectl",
            r#"#!/bin/sh
if [ "$1" = "get" ] && [ "$2" = "namespace" ]; then
    if [ "$3" = "target-namespace" ]; then
        printf '%s\n' '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"target-namespace","labels":{"curietech.ai/created-by":"target-release","curietech.ai/created-in":"target-namespace"},"uid":"uid-target-namespace","resourceVersion":"17"}}'
    elif [ "$3" = "agent-sandbox-system" ] && [ "$CURIE_TEST_EVENT_MODE" = "system" ] && [ -e "$CURIE_TEST_HELM_PID" ]; then
        printf '%s\n' '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"agent-sandbox-system","uid":"uid-agent-sandbox-system","resourceVersion":"18"}}'
    fi
    exit 0
fi

case " $* " in
    *" get deployment agent-sandbox-controller "*)
        case " $* " in
            *" -n agent-sandbox-system "*) exit 0 ;;
            *)
                printf 'controller ownership query was not scoped to agent-sandbox-system: %s\n' "$*" >&2
                exit 64
                ;;
        esac
        ;;
esac

if [ "$1" = "get" ] && [ "$2" = "priorityclass" ]; then
    exit 0
fi

if [ "$1" = "get" ] && [ "$2" = "events" ]; then
    printf '%s\n' "$*" >> "$CURIE_TEST_EVENT_LOG"
    query_namespace=""
    previous_arg=""
    for argument in "$@"; do
        if [ "$previous_arg" = "-n" ]; then query_namespace="$argument"; fi
        previous_arg="$argument"
    done
    case "$query_namespace" in
        target-namespace|agent-sandbox-system) ;;
        *)
            printf 'event query used an unexpected namespace: %s\n' "$*" >&2
            exit 64
            ;;
    esac
    owned_event='{"apiVersion":"v1","kind":"Event","metadata":{"name":"acme-worker-controller-7f68c9.18a2","namespace":"target-namespace","uid":"fresh-failed-create-uid","creationTimestamp":"2026-09-27T10:00:00Z"},"involvedObject":{"apiVersion":"apps/v1","kind":"ReplicaSet","namespace":"target-namespace","name":"acme-worker-controller-7f68c9","uid":"controller-replicaset-uid"},"reason":"FailedCreate","message":"Error creating: pods \"acme-worker-controller-7f68c9\" is forbidden: serviceaccount \"target-namespace:acme-worker-controller\" not found","count":1,"lastTimestamp":"2026-09-27T10:00:00Z"}'
    unrelated_event='{"apiVersion":"v1","kind":"Event","metadata":{"name":"curie-unrelated-controller-4c2df5.18a3","namespace":"target-namespace","uid":"fresh-unrelated-event-uid","creationTimestamp":"2026-09-27T10:00:00Z"},"involvedObject":{"apiVersion":"apps/v1","kind":"ReplicaSet","namespace":"target-namespace","name":"curie-unrelated-controller-4c2df5","uid":"unrelated-replicaset-uid"},"reason":"FailedCreate","message":"Error creating: pods \"curie-unrelated-controller-4c2df5\" is forbidden: serviceaccount \"target-namespace:other-controller\" not found","count":1,"lastTimestamp":"2026-09-27T10:00:00Z"}'
    stale_event='{"apiVersion":"v1","kind":"Event","metadata":{"name":"acme-worker-controller-7f68c9.18a2","namespace":"target-namespace","uid":"stale-failed-create-uid","creationTimestamp":"2026-09-01T10:00:00Z"},"involvedObject":{"apiVersion":"apps/v1","kind":"ReplicaSet","namespace":"target-namespace","name":"acme-worker-controller-7f68c9","uid":"controller-replicaset-uid"},"reason":"FailedCreate","message":"Error creating: pods \"acme-worker-controller-7f68c9\" is forbidden: serviceaccount \"target-namespace:acme-worker-controller\" not found","count":1,"lastTimestamp":"2026-09-01T10:00:00Z"}'
    system_event='{"apiVersion":"v1","kind":"Event","metadata":{"name":"agent-sandbox-controller-7f68c9.18a4","namespace":"agent-sandbox-system","uid":"fresh-system-controller-event-uid","creationTimestamp":"2026-09-27T10:00:00Z"},"involvedObject":{"apiVersion":"apps/v1","kind":"ReplicaSet","namespace":"agent-sandbox-system","name":"agent-sandbox-controller-7f68c9","uid":"system-controller-replicaset-uid"},"reason":"FailedCreate","message":"Error creating: pods \"agent-sandbox-controller-7f68c9\" is forbidden: serviceaccount \"agent-sandbox-system:agent-sandbox-controller\" not found","count":1,"lastTimestamp":"2026-09-27T10:00:00Z"}'
    if [ ! -e "$CURIE_TEST_HELM_PID" ]; then
        : > "$CURIE_TEST_EVENT_SNAPSHOT"
        if [ "$CURIE_TEST_EVENT_MODE" = "stale" ] && [ "$query_namespace" = "$CURIE_TEST_EVENT_NAMESPACE" ]; then
            printf '{"apiVersion":"v1","kind":"EventList","items":[%s]}\n' "$stale_event"
        else
            printf '%s\n' '{"apiVersion":"v1","kind":"EventList","items":[]}'
        fi
    elif [ "$query_namespace" != "$CURIE_TEST_EVENT_NAMESPACE" ]; then
        printf '%s\n' '{"apiVersion":"v1","kind":"EventList","items":[]}'
    else
        case "$CURIE_TEST_EVENT_MODE" in
            fresh)
                printf '{"apiVersion":"v1","kind":"EventList","items":[%s]}\n' "$owned_event"
                ;;
            unrelated)
                printf '{"apiVersion":"v1","kind":"EventList","items":[%s]}\n' "$unrelated_event"
                ;;
            stale)
                printf '{"apiVersion":"v1","kind":"EventList","items":[%s]}\n' "$stale_event"
                ;;
            system)
                printf '{"apiVersion":"v1","kind":"EventList","items":[%s]}\n' "$system_event"
                ;;
            *)
                printf 'unexpected event mode: %s\n' "$CURIE_TEST_EVENT_MODE" >&2
                exit 64
                ;;
        esac
    fi
    exit 0
fi

case "$2" in
    deployments,statefulsets,daemonsets,replicasets,jobs)
        printf '%s\n' "$*" >> "$CURIE_TEST_EVENT_LOG"
        query_namespace=""
        previous_arg=""
        for argument in "$@"; do
            if [ "$previous_arg" = "-n" ]; then query_namespace="$argument"; fi
            previous_arg="$argument"
        done
        case "$query_namespace" in
            target-namespace)
                cat <<'JSON'
{"apiVersion":"v1","kind":"List","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"acme-worker-controller","namespace":"target-namespace","uid":"controller-deployment-uid","labels":{"app":"acme-worker-controller","app.kubernetes.io/managed-by":"Helm"},"annotations":{"meta.helm.sh/release-name":"target-release","meta.helm.sh/release-namespace":"target-namespace"}}},{"apiVersion":"apps/v1","kind":"ReplicaSet","metadata":{"name":"acme-worker-controller-7f68c9","namespace":"target-namespace","uid":"controller-replicaset-uid","ownerReferences":[{"apiVersion":"apps/v1","kind":"Deployment","name":"acme-worker-controller","uid":"controller-deployment-uid","controller":true}]}},{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"curie-unrelated-controller","namespace":"target-namespace","uid":"unrelated-deployment-uid","labels":{"app":"curie-unrelated-controller","app.kubernetes.io/managed-by":"Helm"},"annotations":{"meta.helm.sh/release-name":"other-release","meta.helm.sh/release-namespace":"target-namespace"}}},{"apiVersion":"apps/v1","kind":"ReplicaSet","metadata":{"name":"curie-unrelated-controller-4c2df5","namespace":"target-namespace","uid":"unrelated-replicaset-uid","ownerReferences":[{"apiVersion":"apps/v1","kind":"Deployment","name":"curie-unrelated-controller","uid":"unrelated-deployment-uid","controller":true}]}}]}
JSON
                ;;
            agent-sandbox-system)
                cat <<'JSON'
{"apiVersion":"v1","kind":"List","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"agent-sandbox-controller","namespace":"agent-sandbox-system","uid":"system-controller-deployment-uid","labels":{"app":"agent-sandbox-controller","app.kubernetes.io/managed-by":"Helm"},"annotations":{"meta.helm.sh/release-name":"target-release","meta.helm.sh/release-namespace":"target-namespace"}}},{"apiVersion":"apps/v1","kind":"ReplicaSet","metadata":{"name":"agent-sandbox-controller-7f68c9","namespace":"agent-sandbox-system","uid":"system-controller-replicaset-uid","ownerReferences":[{"apiVersion":"apps/v1","kind":"Deployment","name":"agent-sandbox-controller","uid":"system-controller-deployment-uid","controller":true}]}}]}
JSON
                ;;
            *)
                printf 'live workload query used an unexpected namespace: %s\n' "$*" >&2
                exit 64
                ;;
        esac
        exit 0
        ;;
esac

if [ "$1" = "get" ] && [ "$2" = "runtimeclass" ]; then
    printf '%s\n' 'Error from server (Forbidden): runtimeclasses.node.k8s.io "gvisor" is forbidden: User "system:serviceaccount:example:example" cannot get resource "runtimeclasses" in API group "node.k8s.io" at the cluster scope' >&2
    exit 1
fi

printf 'unexpected kubectl invocation: %s\n' "$*" >&2
exit 64
"#,
        );

        Self {
            _temp: temp,
            bin_dir,
            upgrade_log,
            helm_pid,
            helm_pending,
            helm_termination_log,
            event_log,
            event_snapshot,
            event_mode: event_mode.to_string(),
            event_namespace: event_namespace.to_string(),
        }
    }

    fn run(&self) -> (Output, Duration) {
        let mut paths = vec![self.bin_dir.clone()];
        if let Some(current) = std::env::var_os("PATH") {
            paths.extend(std::env::split_paths(&current));
        }
        let path = std::env::join_paths(paths).expect("join PATH");

        let started = Instant::now();
        let controller_deploy_setting = if self.event_mode == "system" {
            "agentSandbox.controller.deploy=true"
        } else {
            "agentSandbox.controller.deploy=false"
        };
        let output = Command::new(bin())
            .args([
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
            ])
            .args(["--set", controller_deploy_setting])
            .env("PATH", path)
            .env("CI", "1")
            .env("TERM", "dumb")
            .env("NO_COLOR", "1")
            .env("CURIE_TEST_UPGRADE_LOG", &self.upgrade_log)
            .env("CURIE_TEST_HELM_PID", &self.helm_pid)
            .env("CURIE_TEST_HELM_PENDING", &self.helm_pending)
            .env(
                "CURIE_TEST_HELM_TERMINATION_LOG",
                &self.helm_termination_log,
            )
            .env("CURIE_TEST_EVENT_LOG", &self.event_log)
            .env("CURIE_TEST_EVENT_SNAPSHOT", &self.event_snapshot)
            .env("CURIE_TEST_EVENT_MODE", &self.event_mode)
            .env("CURIE_TEST_EVENT_NAMESPACE", &self.event_namespace)
            .env_remove("CURIE_CREDENTIALS")
            .env_remove("CURIE_MODEL_CREDENTIALS")
            .env_remove("CURIE_GITHUB_TOKEN")
            .env_remove("CURIE_MODEL")
            .output()
            .expect("run curie cluster up");
        (output, started.elapsed())
    }
}

fn shown(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn fresh_owned_release_workload_failedcreate_aborts_cluster_up_with_message() {
    let fixture = Fixture::new("fresh", TARGET_NAMESPACE);
    let (output, elapsed) = fixture.run();
    let stderr = shown(&output);

    assert!(
        !output.status.success(),
        "a forbidden release workload Pod must fail cluster up rather than report success\nstdout:\n{}\nstderr:\n{stderr}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "the fresh FailedCreate event must beat Helm's four second timeout, elapsed {elapsed:?}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains(RELEASE_WORKLOAD_FAILURE),
        "cluster up must preserve the Kubernetes admission message verbatim\nstderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("timed out waiting for the condition"),
        "the event diagnosis must replace Helm's later generic timeout\nstderr:\n{stderr}"
    );
    assert_eq!(
        fs::read_to_string(&fixture.upgrade_log)
            .unwrap_or_default()
            .lines()
            .count(),
        1,
        "admission diagnosis must not retry the install"
    );
    assert!(
        fixture.event_snapshot.is_file(),
        "the observer must establish a pre-install Event baseline"
    );
    assert_eq!(
        fs::read_to_string(&fixture.helm_termination_log).unwrap_or_default(),
        "INT\n",
        "the observed event must interrupt the pending Helm command gracefully"
    );
    assert!(
        !fixture.helm_pending.exists(),
        "the interrupted Helm command must not leave the fake release pending"
    );
}

#[test]
fn fresh_agent_sandbox_controller_failedcreate_aborts_from_its_namespace() {
    let fixture = Fixture::new("system", "agent-sandbox-system");
    let (output, elapsed) = fixture.run();
    let stderr = shown(&output);

    assert!(
        !output.status.success(),
        "a forbidden agent-sandbox-controller Pod must fail cluster up\nstdout:\n{}\nstderr:\n{stderr}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "the controller namespace event must beat Helm's four second timeout, elapsed {elapsed:?}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains(SYSTEM_CONTROLLER_FAILURE),
        "cluster up must preserve the controller namespace admission message verbatim\nstderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("timed out waiting for the condition"),
        "the fresh controller event must replace Helm's generic timeout\nstderr:\n{stderr}"
    );
    assert_eq!(
        fs::read_to_string(&fixture.upgrade_log)
            .unwrap_or_default()
            .lines()
            .count(),
        1,
        "controller admission diagnosis must not retry the install"
    );
    let event_queries = fs::read_to_string(&fixture.event_log).unwrap_or_default();
    assert!(
        event_queries.lines().any(|line| {
            line.contains("get events -n agent-sandbox-system")
                && line.contains("--field-selector reason=FailedCreate")
        }),
        "cluster up must observe namespaced FailedCreate Events in agent-sandbox-system:\n{event_queries}"
    );
    assert!(
        event_queries
            .lines()
            .any(|line| line.contains("get deployments,statefulsets,daemonsets,replicasets,jobs -n agent-sandbox-system")),
        "the event must be joined against live controller objects in their namespace:\n{event_queries}"
    );
    assert_eq!(
        fs::read_to_string(&fixture.helm_termination_log).unwrap_or_default(),
        "INT\n",
        "the observed controller event must interrupt Helm gracefully"
    );
}

#[test]
fn fresh_failedcreate_for_another_release_does_not_abort_cluster_up() {
    let fixture = Fixture::new("unrelated", TARGET_NAMESPACE);
    let (output, elapsed) = fixture.run();
    let stderr = shown(&output);

    assert!(
        !output.status.success(),
        "the fake Helm timeout should fail cluster up"
    );
    assert!(
        elapsed >= Duration::from_secs(3),
        "a fresh but unrelated FailedCreate event must not abort Helm, elapsed {elapsed:?}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("timed out waiting for the condition"),
        "the unrelated event must leave Helm's own timeout as the result\nstderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("curie-unrelated-controller-4c2df5"),
        "the unrelated object must not become the reported admission failure\nstderr:\n{stderr}"
    );
    assert_eq!(
        fs::read_to_string(&fixture.upgrade_log)
            .unwrap_or_default()
            .lines()
            .count(),
        1,
        "an unrelated event must not trigger an install retry"
    );
    assert!(
        fixture.event_snapshot.is_file(),
        "the observer must baseline Events first"
    );
    assert!(
        fs::read_to_string(&fixture.helm_termination_log)
            .unwrap_or_default()
            .is_empty(),
        "the unrelated event must not interrupt Helm"
    );
}

#[test]
fn unchanged_preinstall_failedcreate_event_is_stale() {
    let fixture = Fixture::new("stale", TARGET_NAMESPACE);
    let (output, elapsed) = fixture.run();
    let stderr = shown(&output);

    assert!(
        !output.status.success(),
        "the fake Helm timeout should fail cluster up"
    );
    assert!(
        elapsed >= Duration::from_secs(3),
        "an unchanged pre-install Event must not abort Helm, elapsed {elapsed:?}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("timed out waiting for the condition"),
        "the stale Event must leave Helm's own timeout as the result\nstderr:\n{stderr}"
    );
    assert!(
        !stderr.contains(RELEASE_WORKLOAD_FAILURE),
        "an Event already present in the baseline must not be reported as a new failure\nstderr:\n{stderr}"
    );
    assert_eq!(
        fs::read_to_string(&fixture.upgrade_log)
            .unwrap_or_default()
            .lines()
            .count(),
        1,
        "a stale event must not trigger an install retry"
    );
    assert!(
        fixture.event_snapshot.is_file(),
        "the observer must baseline Events first"
    );
    assert!(
        fs::read_to_string(&fixture.helm_termination_log)
            .unwrap_or_default()
            .is_empty(),
        "the stale event must not interrupt Helm"
    );
}
