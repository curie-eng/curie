//! Issue #4413: drive the advisory check through the real up/apply binary.
//! Only the external Helm and Kubernetes processes are replaced.
//! NodeList/allocatable/Ready and PodList/nodeName/resources shapes follow:
//! https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/node-v1/
//! https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/
//! Deployment replicas and pod templates follow:
//! https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/deployment-v1/
//! The pod field selector is documented at:
//! https://kubernetes.io/docs/concepts/overview/working-with-objects/field-selectors/

use crate::support::executable as test_executable;

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{json, Value};

const RELEASE: &str = "acme-capacity";
const NAMESPACE: &str = "acme-capacity-namespace";
const CONTEXT: &str = "acme-capacity-context";
const OPENROUTER_CREDENTIAL: &str = "sk-or-v1-PLACEHOLDER";
const GENERIC_RECOMMENDATION: &str =
    "Add nodes or use larger nodes until allocatable grows by the shortfall.";
const RENDER: &str = r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: acme-capacity-api
spec:
  replicas: 1
  selector:
    matchLabels:
      component: api
  template:
    metadata:
      labels:
        component: api
    spec:
      containers:
        - name: api
          image: example.com/acme-api:example
          resources:
            requests:
              cpu: 1500m
              memory: 128Mi
"#;

// Record the real argv plus values-file contents before Curie removes private
// temporary files. SQLite keeps this process helper's state in one owned file.
// Helm's value precedence/flags are documented at:
// https://helm.sh/docs/helm/helm_template/
// https://helm.sh/docs/helm/helm_upgrade/
const RECORD_VALUES: &str = r#"python3 - "$@" <<'PY'
import json
import os
import sqlite3
import sys
from pathlib import Path

argv = sys.argv[1:]
release = os.environ["CURIE_CAPACITY_RELEASE"]
full_template = (argv[:2] == ["template", release]
                 and "--show-only" not in argv
                 and not any(arg.startswith("--show-only=") for arg in argv))
install = argv[:3] == ["upgrade", "--install", release]
if full_template or install:
    files = {}
    for index, arg in enumerate(argv[:-1]):
        if arg in ("-f", "--values"):
            path = argv[index + 1]
            files[path] = Path(path).read_text()
    with sqlite3.connect(os.environ["CURIE_CAPACITY_VALUE_CAPTURE"]) as db:
        db.execute("CREATE TABLE IF NOT EXISTS calls (id INTEGER PRIMARY KEY, kind TEXT, argv TEXT, files TEXT)")
        db.execute("INSERT INTO calls (kind, argv, files) VALUES (?, ?, ?)",
                   ("template" if full_template else "install", json.dumps(argv), json.dumps(files)))
PY
"#;

#[derive(Debug, Deserialize)]
struct HelmInvocation {
    kind: String,
    argv: Vec<String>,
    files: BTreeMap<String, String>,
}

impl HelmInvocation {
    fn value_arguments(&self) -> Vec<(String, Value)> {
        let mut values = Vec::new();
        let mut index = 0;
        while index < self.argv.len() {
            let flag = &self.argv[index];
            if matches!(
                flag.as_str(),
                "--set" | "--set-string" | "--set-json" | "-f" | "--values"
            ) {
                let argument = self
                    .argv
                    .get(index + 1)
                    .expect("value flag has an argument");
                let value = if matches!(flag.as_str(), "-f" | "--values") {
                    let payload = self
                        .files
                        .get(argument)
                        .expect("values captured while file existed");
                    serde_norway::from_str(payload).expect("captured values file is YAML or JSON")
                } else {
                    Value::String(argument.clone())
                };
                values.push((flag.clone(), value));
                index += 2;
            } else {
                index += 1;
            }
        }
        values
    }
}

struct Fixture {
    temp: tempfile::TempDir,
    bin_dir: PathBuf,
    calls: PathBuf,
    upgrades: PathBuf,
    stderr_file: PathBuf,
    stderr_at_install: PathBuf,
    render_file: PathBuf,
    nodes_file: PathBuf,
    pods_file: PathBuf,
    kubeconfig: PathBuf,
    plan_file: PathBuf,
    value_capture: PathBuf,
    node_read_pid: PathBuf,
    node_mode: &'static str,
    pod_mode: &'static str,
    render_mode: &'static str,
    existing_values: Option<Value>,
    infer_gvisor: bool,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("temporary capacity fixture");
        let bin_dir = temp.path().join("bin");
        fs::create_dir(&bin_dir).expect("create fake executable directory");
        let fixture = Self {
            bin_dir,
            calls: temp.path().join("calls.log"),
            upgrades: temp.path().join("upgrades.log"),
            stderr_file: temp.path().join("stderr.log"),
            stderr_at_install: temp.path().join("stderr-at-install.log"),
            render_file: temp.path().join("render.yaml"),
            nodes_file: temp.path().join("nodes.json"),
            pods_file: temp.path().join("pods.json"),
            kubeconfig: temp.path().join("kubeconfig"),
            plan_file: temp.path().join("curie.yaml"),
            value_capture: temp.path().join("helm-values.sqlite"),
            node_read_pid: temp.path().join("node-read.pid"),
            temp,
            node_mode: "ready",
            pod_mode: "ready",
            render_mode: "ready",
            existing_values: None,
            infer_gvisor: false,
        };
        fs::write(&fixture.render_file, RENDER).expect("write rendered chart fixture");
        fixture.write_nodes("1000m", "1Gi");
        fs::write(
            &fixture.pods_file,
            json!({
                "apiVersion": "v1", "kind": "PodList", "items": [{
                    "metadata": {"name": "acme-system", "namespace": "kube-system"},
                    "spec": {
                        "nodeName": "acme-node",
                        "containers": [{
                            "name": "system", "image": "example.com/acme-system:example",
                            "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}}
                        }]
                    },
                    "status": {"phase": "Running"}
                }]
            })
            .to_string(),
        )
        .expect("write existing system pod");
        fs::write(
            &fixture.kubeconfig,
            format!(
                "apiVersion: v1\nkind: Config\ncurrent-context: acme-other-context\ncontexts:\n- name: acme-other-context\n  context:\n    cluster: acme-other-cluster\n- name: {CONTEXT}\n  context:\n    cluster: acme-capacity-cluster\nclusters: []\nusers: []\n"
            ),
        )
        .expect("write two-context kubeconfig");
        fs::write(
            &fixture.plan_file,
            format!(
                "version: 1\ninstall:\n  namespace: {NAMESPACE}\n  release: {RELEASE}\n  context: {CONTEXT}\ncredentials:\n  model: CURIE_CAPACITY_TEST_CREDENTIAL\nplatform:\n  ui: false\n  sandbox_controller: false\n  priority_classes:\n    platform: false\n    sandbox: false\n  gvisor: off\n"
            ),
        )
        .expect("write prepared installation plan");

        let helm = format!(
            r#"#!/bin/sh
{}
printf 'helm\t%s\n' "$*" >> "$CURIE_CAPACITY_CALLS"
if [ "$1" = template ]; then
    case " $* " in
        *" --show-only templates/priorityclass.yaml "*|*" --show-only=templates/priorityclass.yaml "*)
            printf '%s\n' 'Error: could not find template templates/priorityclass.yaml in chart' >&2
            exit 1
            ;;
        *" --show-only templates/preflight-gvisor.yaml "*|*" --show-only=templates/preflight-gvisor.yaml "*)
            if [ "$CURIE_CAPACITY_INFER_GVISOR" = 1 ]; then
                case " $* " in
                    *" security.gvisor.mode=off "*) ;;
                    *)
                        printf '%s\n' 'apiVersion: batch/v1' 'kind: Job' 'metadata:' '  name: acme-capacity-preflight-gvisor' 'spec:' '  template:' '    spec:' '      runtimeClassName: gvisor'
                        exit 0
                        ;;
                esac
            fi
            printf '%s\n' 'Error: could not find template templates/preflight-gvisor.yaml in chart' >&2
            exit 1
            ;;
        *" --show-only "*|*" --show-only="*)
            printf 'Error: could not find template %s in chart\n' "$*" >&2
            exit 1
            ;;
    esac
    if [ "$CURIE_CAPACITY_RENDER_MODE" = failed ]; then
        printf '%s\n' 'Error: chart render failed' >&2
        exit 1
    fi
    cat "$CURIE_CAPACITY_RENDER"
    exit 0
fi
if [ "$1" = get ] && [ "$2" = values ]; then
    if [ -n "$CURIE_CAPACITY_EXISTING_VALUES" ]; then
        printf '%s\n' "$CURIE_CAPACITY_EXISTING_VALUES"
        exit 0
    fi
    printf '%s\n' 'Error: release: not found' >&2
    exit 1
fi
if [ "$1" = history ]; then
    if [ -n "$CURIE_CAPACITY_EXISTING_VALUES" ]; then
        printf '%s\n' '[{{"revision":1,"status":"deployed","chart":"curie-0.0.0","description":"Install complete"}}]'
        exit 0
    fi
    printf '%s\n' 'Error: release: not found' >&2
    exit 1
fi
if [ "$1" = upgrade ] && [ "$2" = --install ]; then
    # Snapshot the CLI's actual stderr at invocation, before Helm can return.
    cat "$CURIE_CAPACITY_STDERR" > "$CURIE_CAPACITY_STDERR_AT_INSTALL"
    printf '%s\n' "$*" >> "$CURIE_CAPACITY_UPGRADES"
    exit 0
fi
{}
printf 'unexpected helm invocation: %s\n' "$*" >&2
exit 64
"#,
            RECORD_VALUES,
            include_str!("data/converged-installation-read.sh")
        );
        test_executable::install_in(&fixture.bin_dir, "helm", &helm);
        let kubectl = format!(
            r#"#!/bin/sh
printf 'kubectl\t%s\n' "$*" >> "$CURIE_CAPACITY_CALLS"
if [ "$1" = get ] && [ "$2" = nodes ]; then
    if [ "$CURIE_CAPACITY_NODE_MODE" = timeout ]; then
        printf '%s\n' "$$" > "$CURIE_CAPACITY_NODE_READ_PID"
        # exec leaves one owned process, so the deadline must stop the read.
        exec sleep 60
    fi
    if [ "$CURIE_CAPACITY_NODE_MODE" = forbidden ]; then
        printf '%s\n' 'Error from server (Forbidden): nodes is forbidden: cannot list resource nodes at the cluster scope' >&2
        exit 1
    fi
    cat "$CURIE_CAPACITY_NODES"
    exit 0
fi
if [ "$1" = get ] && [ "$2" = pods ]; then
    case " $* " in
        *" --all-namespaces "*) ;;
        *) printf 'capacity pod read must cover all namespaces: %s\n' "$*" >&2; exit 64 ;;
    esac
    if [ "$CURIE_CAPACITY_POD_MODE" = forbidden ]; then
        printf '%s\n' 'Error from server (Forbidden): pods is forbidden: cannot list resource pods at the cluster scope' >&2
        exit 1
    fi
    cat "$CURIE_CAPACITY_PODS"
    exit 0
fi
if [ "$1" = get ] && [ "$2" = runtimeclass ] && [ "$CURIE_CAPACITY_INFER_GVISOR" = 1 ]; then
    # A normal GET of an absent object reports HTTP 404 through kubectl.
    # https://kubernetes.io/docs/reference/using-api/api-concepts/#http-status-codes
    printf '%s\n' 'Error from server (NotFound): runtimeclasses.node.k8s.io "gvisor" not found' >&2
    exit 1
fi
{}
if [ "$1" = get ] && [ "$2" = namespace ]; then
    if [ "$3" = acme-capacity-namespace ]; then
        printf '%s\n' '{{"apiVersion":"v1","kind":"Namespace","metadata":{{"name":"acme-capacity-namespace","labels":{{"curietech.ai/created-by":"acme-capacity","curietech.ai/created-in":"acme-capacity-namespace"}},"uid":"acme-namespace-uid","resourceVersion":"17"}}}}'
    fi
    exit 0
fi
if [ "$1" = get ] && [ "$2" = statefulset ]; then
    printf '%s\n' '{{"apiVersion":"v1","kind":"List","items":[]}}'
    exit 0
fi
if [ "$1" = get ] && {{ [ "$2" = events ] || [ "$2" = event ]; }}; then
    printf '%s\n' '{{"apiVersion":"v1","kind":"EventList","items":[]}}'
    exit 0
fi
printf 'unexpected kubectl invocation: %s\n' "$*" >&2
exit 64
"#,
            include_str!("data/converged-installation-read.sh")
        );
        test_executable::install_in(&fixture.bin_dir, "kubectl", &kubectl);
        fixture
    }

    fn write_nodes(&self, cpu: &str, memory: &str) {
        fs::write(
            &self.nodes_file,
            json!({
                "apiVersion": "v1", "kind": "NodeList", "items": [{
                    "metadata": {"name": "acme-node", "labels": {}},
                    "spec": {},
                    "status": {
                        "allocatable": {"cpu": cpu, "memory": memory},
                        "conditions": [{"type": "Ready", "status": "True"}]
                    }
                }]
            })
            .to_string(),
        )
        .expect("write eligible node capacity");
    }

    fn run_up(&self, extra: &[&str]) -> Output {
        let mut args = vec![
            "cluster",
            "up",
            "--context",
            CONTEXT,
            "--chart",
            concat!(env!("CARGO_MANIFEST_DIR"), "/../charts/curie"),
            "--namespace",
            NAMESPACE,
            "--release",
            RELEASE,
            "--dev",
            "--no-expose",
            "--set",
            "agentSandbox.controller.deploy=false",
            "--set",
            "priorityClasses.platform.create=false",
            "--set",
            "priorityClasses.sandbox.create=false",
        ];
        if !self.infer_gvisor {
            args.extend_from_slice(&["--fake-model", "--set", "security.gvisor.mode=off"]);
        }
        args.extend_from_slice(extra);
        self.run(&args)
    }

    fn run_apply(&self) -> Output {
        self.run(&[
            "apply",
            "--file",
            self.plan_file.to_str().expect("UTF-8 plan path"),
            "--chart",
            concat!(env!("CARGO_MANIFEST_DIR"), "/../charts/curie"),
        ])
    }

    fn run(&self, args: &[&str]) -> Output {
        let mut paths = vec![self.bin_dir.clone()];
        paths.extend(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        ));
        let stderr = fs::File::create(&self.stderr_file).expect("capture stderr before spawn");
        let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
        command
            .args(["--color", "never"])
            .args(args)
            .current_dir(self.temp.path())
            .stdin(Stdio::null())
            .stderr(Stdio::from(stderr))
            .env(
                "PATH",
                std::env::join_paths(paths).expect("join fixture PATH"),
            )
            .env("CI", "1")
            .env("TERM", "dumb")
            .env("NO_COLOR", "1")
            .env("TMPDIR", self.temp.path())
            .env("KUBECONFIG", &self.kubeconfig)
            .env("HELM_KUBECONTEXT", "acme-other-context")
            .env("CURIE_CONFIG_DIR", self.temp.path().join("config"))
            .env("CURIE_CAPACITY_CALLS", &self.calls)
            .env("CURIE_CAPACITY_UPGRADES", &self.upgrades)
            .env("CURIE_CAPACITY_STDERR", &self.stderr_file)
            .env("CURIE_CAPACITY_STDERR_AT_INSTALL", &self.stderr_at_install)
            .env("CURIE_CAPACITY_RENDER", &self.render_file)
            .env("CURIE_CAPACITY_NODES", &self.nodes_file)
            .env("CURIE_CAPACITY_PODS", &self.pods_file)
            .env("CURIE_CAPACITY_NODE_MODE", self.node_mode)
            .env("CURIE_CAPACITY_POD_MODE", self.pod_mode)
            .env("CURIE_CAPACITY_RENDER_MODE", self.render_mode)
            .env("CURIE_CAPACITY_RELEASE", RELEASE)
            .env("CURIE_CAPACITY_VALUE_CAPTURE", &self.value_capture)
            .env("CURIE_CAPACITY_NODE_READ_PID", &self.node_read_pid)
            .env(
                "CURIE_CAPACITY_INFER_GVISOR",
                if self.infer_gvisor { "1" } else { "0" },
            )
            .env(
                "CURIE_CAPACITY_EXISTING_VALUES",
                self.existing_values
                    .as_ref()
                    .map(Value::to_string)
                    .unwrap_or_default(),
            )
            .env("CURIE_CAPACITY_TEST_CREDENTIAL", OPENROUTER_CREDENTIAL)
            .env(
                "CURIE_TEST_PROVIDER_EGRESS_JSON",
                r#"{"openrouter.ai":["1.1.1.1"]}"#,
            )
            .env_remove("CURIE_CREDENTIALS")
            .env_remove("CURIE_MODEL_CREDENTIALS")
            .env_remove("CURIE_GITHUB_TOKEN")
            .env_remove("CURIE_MODEL");
        if self.infer_gvisor {
            command.env("CURIE_CREDENTIALS", OPENROUTER_CREDENTIAL);
        }
        let mut output = command.output().expect("execute real curie binary");
        output.stderr = fs::read(&self.stderr_file).expect("read captured CLI stderr");
        output
    }

    fn calls(&self) -> String {
        fs::read_to_string(&self.calls).unwrap_or_default()
    }

    fn assert_one_install(&self) {
        let upgrades = fs::read_to_string(&self.upgrades).expect("read actual install calls");
        assert_eq!(upgrades.lines().count(), 1, "{upgrades}");
        assert!(upgrades.starts_with("upgrade --install "), "{upgrades}");
    }

    fn assert_warning_before_install(&self) {
        let before = fs::read_to_string(&self.stderr_at_install)
            .expect("Helm must snapshot stderr when invoked");
        assert_cpu_warning(&before);
    }

    fn assert_render_matches_install_values(&self) -> Vec<(String, Value)> {
        let captured = Command::new("python3")
            .args([
                "-c",
                "import json, sqlite3, sys; db=sqlite3.connect(sys.argv[1]); print(json.dumps([{'kind':kind,'argv':json.loads(argv),'files':json.loads(files)} for kind,argv,files in db.execute('SELECT kind,argv,files FROM calls ORDER BY id')]))",
            ])
            .arg(&self.value_capture)
            .output()
            .expect("read owned Helm capture");
        assert!(captured.status.success(), "Helm capture must be readable");
        let calls: Vec<HelmInvocation> =
            serde_json::from_slice(&captured.stdout).expect("capture reader returns invocations");
        let templates: Vec<_> = calls
            .iter()
            .filter(|call| call.kind == "template")
            .collect();
        let installs: Vec<_> = calls.iter().filter(|call| call.kind == "install").collect();
        assert_eq!(templates.len(), 1, "exactly one full capacity render");
        assert_eq!(installs.len(), 1, "exactly one installation");
        let rendered_values = templates[0].value_arguments();
        let installed_values = installs[0].value_arguments();
        // Do not print private values on a failed comparison.
        assert!(
            rendered_values == installed_values,
            "capacity render must use exactly the ordered final installation value arguments and file payloads"
        );
        assert!(
            !rendered_values.is_empty(),
            "comparison must include real values"
        );
        rendered_values
    }

    fn write_phase_hooks(&self) {
        // Helm only fires the current lifecycle phase's Job hooks:
        // https://helm.sh/docs/topics/charts_hooks/
        let mut rendered = RENDER.to_string();
        for (phase, cpu, memory) in [
            ("pre-install", "100m", "64Mi"),
            ("pre-upgrade", "900m", "256Mi"),
        ] {
            rendered.push_str("\n---\n");
            rendered.push_str(
                &json!({
                    "apiVersion": "batch/v1", "kind": "Job",
                    "metadata": {
                        "name": format!("acme-{phase}"),
                        "annotations": {"helm.sh/hook": phase}
                    },
                    "spec": {"template": {"spec": {
                        "restartPolicy": "Never",
                        "containers": [{
                            "name": "hook", "image": "example.com/acme-hook:example",
                            "resources": {"requests": {"cpu": cpu, "memory": memory}}
                        }]
                    }}}
                })
                .to_string(),
            );
        }
        fs::write(&self.render_file, rendered).expect("write hooks with different phase requests");
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // A failed timeout assertion must not strand this fixture's sleeper.
        // Verify its command before using the recorded process identity.
        if let Ok(pid) = fs::read_to_string(&self.node_read_pid) {
            let pid = pid.trim();
            let owned = Command::new("ps")
                .args(["-p", pid, "-o", "args="])
                .output()
                .is_ok_and(|output| String::from_utf8_lossy(&output.stdout).trim() == "sleep 60");
            if owned {
                let _ = Command::new("kill")
                    .args(["-KILL", pid])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
        }
    }
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn assert_success(output: &Output) {
    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        stderr(output)
    );
}

fn assert_cpu_warning(shown: &str) {
    let warnings: Vec<_> = shown
        .lines()
        .filter(|line| line.contains("warn") && line.contains("capacity:"))
        .collect();
    assert_eq!(
        warnings.len(),
        1,
        "one CPU warning, no memory warning:\n{shown}"
    );
    assert!(warnings[0].to_lowercase().contains("cpu"), "{shown}");
    assert!(warnings[0].contains("160%"), "{shown}");
    assert!(warnings[0].contains("shortfall"), "{shown}");
    assert!(shown.contains(GENERIC_RECOMMENDATION), "{shown}");
}

#[test]
fn over_capacity_warns_before_helm_install_and_exits_zero() {
    let fixture = Fixture::new();
    let output = fixture.run_up(&[]);
    assert_success(&output);
    assert_cpu_warning(&stderr(&output));
    fixture.assert_warning_before_install();
    fixture.assert_one_install();
    fixture.assert_render_matches_install_values();
    assert_eq!(
        stderr(&output)
            .lines()
            .filter(|line| line.starts_with("capacity:"))
            .count(),
        1,
        "exactly one capacity note:\n{}",
        stderr(&output)
    );
}

#[test]
fn forbidden_nodes_skips_advisory_and_preserves_install_exit_code() {
    let baseline = Fixture::new();
    baseline.write_nodes("8", "16Gi");
    let baseline_output = baseline.run_up(&[]);
    assert_success(&baseline_output);
    baseline.assert_one_install();
    assert!(
        !stderr(&baseline_output).contains("warn  capacity:"),
        "ample capacity baseline must have no warning"
    );

    let mut forbidden = Fixture::new();
    forbidden.node_mode = "forbidden";
    let output = forbidden.run_up(&[]);
    assert_eq!(output.status.code(), baseline_output.status.code());
    forbidden.assert_one_install();
    let shown = stderr(&output);
    assert_eq!(
        shown.matches("capacity preflight skipped:").count(),
        1,
        "{shown}"
    );
    assert!(
        shown.contains("Forbidden"),
        "the skip must explain the failed read:\n{shown}"
    );
    assert!(
        fs::read_to_string(&forbidden.stderr_at_install)
            .unwrap()
            .contains("capacity preflight skipped:"),
        "skip note must precede the install"
    );
}

#[test]
fn failed_pod_read_or_malformed_input_skips_once_and_still_installs() {
    for failure in [
        "pods-forbidden",
        "nodes-json",
        "pods-json",
        "quantity",
        "render",
    ] {
        let mut fixture = Fixture::new();
        match failure {
            "pods-forbidden" => fixture.pod_mode = "forbidden",
            "nodes-json" => fs::write(&fixture.nodes_file, "{not-json").unwrap(),
            "pods-json" => fs::write(&fixture.pods_file, "{not-json").unwrap(),
            "quantity" => {
                fs::write(&fixture.render_file, RENDER.replace("1500m", "invalid-cpu")).unwrap();
            }
            "render" => fixture.render_mode = "failed",
            _ => unreachable!(),
        }
        let output = fixture.run_up(&[]);
        assert_success(&output);
        fixture.assert_one_install();
        let shown = stderr(&output);
        assert_eq!(
            shown.matches("capacity preflight skipped:").count(),
            1,
            "{failure}: {shown}"
        );
        assert!(!shown.contains("warn  capacity:"), "{failure}: {shown}");
        if failure == "render" {
            assert!(shown.contains("could not render chart requests"), "{shown}");
            assert!(!fixture.calls().contains("get nodes"));
            assert!(!fixture.calls().contains("get pods"));
        }
    }
}

#[test]
fn json_stdout_keeps_the_committed_cluster_up_schema() {
    let fixture = Fixture::new();
    let output = fixture.run_up(&["--json"]);
    assert_success(&output);
    let value: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "stdout must contain exactly one JSON value: {error}\n{}",
            String::from_utf8_lossy(&output.stdout)
        )
    });
    let schema: Value = serde_json::from_str(include_str!("../schema/cluster-up.schema.json"))
        .expect("committed cluster up schema parses");
    let validator = jsonschema::validator_for(&schema).expect("cluster up schema compiles");
    assert!(validator.is_valid(&value), "{value}");
    assert_eq!(
        value,
        json!({"status": "up", "namespace": NAMESPACE, "release": RELEASE})
    );
    assert_cpu_warning(&stderr(&output));
    fixture.assert_warning_before_install();
    fixture.assert_one_install();
}

#[test]
fn dry_run_performs_no_capacity_reads_or_render() {
    let fixture = Fixture::new();
    let output = fixture.run_up(&["--dry-run", "--json"]);
    assert_success(&output);
    let calls = fixture.calls();
    assert!(!calls.contains("get nodes"), "{calls}");
    assert!(!calls.contains("get pods"), "{calls}");
    assert!(!calls.contains("helm\ttemplate"), "{calls}");
    assert!(!calls.contains("helm\tupgrade"), "{calls}");
    assert!(!stderr(&output).contains("capacity:"));
    assert!(!stderr(&output).contains("capacity preflight skipped:"));
    let value: Value = serde_json::from_slice(&output.stdout).expect("dry run emits one JSON plan");
    assert_eq!(value["dry_run"], true);
}

#[test]
fn apply_runs_the_same_warning_before_install() {
    let fixture = Fixture::new();
    let output = fixture.run_apply();
    assert_success(&output);
    assert_cpu_warning(&stderr(&output));
    fixture.assert_warning_before_install();
    fixture.assert_one_install();
    let values = fixture.assert_render_matches_install_values();
    assert!(
        values.iter().any(|(flag, _)| flag == "-f"),
        "apply must exercise private values payload comparison"
    );
}

#[test]
fn full_render_uses_the_install_set_string_json_and_private_file_payloads() {
    let mut fixture = Fixture::new();
    fixture.existing_values = Some(json!({
        "config": {"schemaVersion": "0.9.0"},
        "security": {"allowDevDefaults": true},
        "agentSandbox": {"runner": {"fakeModel": true}},
        "worker": {"extraEnv": [{"name": "CAPACITY_MODE", "value": "capacity string"}]}
    }));
    let output = fixture.run_up(&[
        "--set",
        "worker.replicas=3",
        "--set",
        "worker.adapterCredentials={}",
        "--github-token",
        "PLACEHOLDER-capacity-github-token",
    ]);
    assert_success(&output);
    fixture.assert_one_install();
    let values = fixture.assert_render_matches_install_values();
    for (flag, expression) in [
        ("--set", "worker.replicas=3"),
        ("--set-string", "worker.extraEnv[0].value=capacity string"),
        ("--set-json", "worker.adapterCredentials={}"),
    ] {
        assert!(
            values.contains(&(flag.to_string(), Value::String(expression.to_string()))),
            "the value argument lane {flag} must be present"
        );
    }
    assert!(
        values.iter().any(|(flag, payload)| flag == "-f"
            && payload["api"]["githubToken"] == "PLACEHOLDER-capacity-github-token"),
        "private values payload must survive both invocations"
    );
}

#[test]
fn capacity_render_uses_gvisor_off_inferred_from_absent_runtimeclass() {
    let mut fixture = Fixture::new();
    fixture.infer_gvisor = true;
    let output = fixture.run_up(&[]);
    assert_success(&output);
    fixture.assert_one_install();
    let calls = fixture.calls();
    assert!(
        calls.contains("get runtimeclass gvisor"),
        "lookup must actually execute:\n{calls}"
    );
    assert!(stderr(&output).contains("RuntimeClass lookup (NotFound)"));
    let values = fixture.assert_render_matches_install_values();
    assert!(
        values.contains(&(
            "--set".to_string(),
            Value::String("security.gvisor.mode=off".to_string())
        )),
        "the full capacity render must receive the final inferred mode"
    );
    fixture.assert_warning_before_install();
}

#[test]
fn existing_release_selects_upgrade_hook_requests_through_the_binary() {
    for (existing, cpu, memory) in [(false, "1600m", "192Mi"), (true, "2400m", "384Mi")] {
        let mut fixture = Fixture::new();
        fixture.write_phase_hooks();
        if existing {
            fixture.existing_values = Some(json!({
                "config": {"schemaVersion": "0.9.0"},
                "security": {"allowDevDefaults": true},
                "agentSandbox": {"runner": {"fakeModel": true}}
            }));
        }
        let output = fixture.run_up(&[]);
        assert_success(&output);
        fixture.assert_one_install();
        fixture.assert_render_matches_install_values();
        let shown = stderr(&output);
        let note = shown
            .lines()
            .find(|line| line.starts_with("capacity:"))
            .expect("completed capacity report");
        assert!(
            note.contains(&format!("install requests {cpu} CPU and {memory} memory")),
            "existing={existing}: {note}"
        );
        assert!(!shown.contains("capacity preflight skipped:"), "{shown}");
    }
}

#[test]
fn node_read_timeout_is_bounded_skips_once_and_stops_the_owned_read() {
    let mut fixture = Fixture::new();
    fixture.node_mode = "timeout";
    let started = Instant::now();
    let output = fixture.run_up(&[]);
    let elapsed = started.elapsed();
    assert_success(&output);
    fixture.assert_one_install();
    let shown = stderr(&output);
    assert_eq!(
        shown.matches("capacity preflight skipped:").count(),
        1,
        "{shown}"
    );
    assert!(shown.contains("nodes read exceeded 20 seconds"), "{shown}");
    assert!(
        elapsed >= Duration::from_secs(19),
        "the actual 20-second deadline must be exercised: {elapsed:?}"
    );
    // Waiting for the read means returning after it ends on its own at 60
    // seconds. A tighter bound measures the host: a CI runner stall of about
    // 13 seconds pushed a correct 20-second deadline past 30.
    assert!(
        elapsed < Duration::from_secs(60),
        "the advisory must not wait for the 60-second read: {elapsed:?}"
    );
    assert!(
        !fixture.calls().contains("get pods"),
        "a timed-out node read must skip the remaining check"
    );
    let pid = fs::read_to_string(&fixture.node_read_pid).expect("actual read recorded its process");
    let stopped_by = Instant::now() + Duration::from_secs(2);
    loop {
        let alive = Command::new("kill")
            .args(["-0", pid.trim()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("observe read process liveness")
            .success();
        if !alive {
            break;
        }
        assert!(
            Instant::now() < stopped_by,
            "the timed-out read process must be stopped before returning"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn both_capacity_reads_use_the_pinned_context_and_active_pod_selector() {
    let fixture = Fixture::new();
    let output = fixture.run_up(&[]);
    assert_success(&output);
    let calls = fixture.calls();
    let reads: Vec<_> = calls
        .lines()
        .filter(|line| {
            line.starts_with("kubectl\t")
                && (line.contains("get nodes") || line.contains("get pods"))
        })
        .collect();
    assert_eq!(reads.len(), 2, "both capacity reads must execute:\n{calls}");
    for read in &reads {
        assert!(read.contains(&format!("--context {CONTEXT}")), "{read}");
        assert!(!read.contains("--context acme-other-context"), "{read}");
        assert!(read.contains("-o json"), "{read}");
    }
    let pods = reads.iter().find(|line| line.contains("get pods")).unwrap();
    assert!(pods.contains("--all-namespaces"), "{pods}");
    assert!(
        pods.contains("status.phase!=Succeeded,status.phase!=Failed"),
        "{pods}"
    );
}
