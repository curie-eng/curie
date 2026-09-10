//! #2375 binary regression for namespace adoption and failed-install cleanup.
//!
//! The fake executables model Kubernetes at the command/API boundary while the
//! test drives the real `curie` binary. Namespace replies mirror the documented
//! Namespace metadata shape (`labels`, `uid`, and `resourceVersion`), and an
//! ignored missing object is exit 0 with empty stdout:
//! https://kubernetes.io/docs/reference/using-api/api-concepts/
//! Hook Jobs use Helm's documented `helm.sh/hook` annotation:
//! https://helm.sh/docs/topics/charts_hooks/

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::{json, Value};

const NS: &str = "agent-ns";
const RELEASE: &str = "prod-release";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn chart() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/../charts/curie")
}

fn write_exec(dir: &Path, name: &str, body: &str) {
    let path = dir.join(name);
    fs::write(&path, body).expect("write fake executable");
    let mut permissions = fs::metadata(&path).expect("stat fake").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("chmod fake executable");
}

const FAKE_CLUSTER: &str = r###"#!/usr/bin/env python3
import json, os, pathlib, sys

NS = "agent-ns"
RELEASE = "prod-release"

state_path = pathlib.Path(os.environ["CURIE_TEST_CLUSTER_STATE"])
log_path = pathlib.Path(os.environ["CURIE_TEST_CLUSTER_LOG"])
state = json.loads(state_path.read_text())
args = sys.argv[1:]
with log_path.open("a") as log:
    log.write(pathlib.Path(sys.argv[0]).name + " " + " ".join(args) + "\n")

def save():
    state_path.write_text(json.dumps(state))

def fail(message, code=1):
    print(message, file=sys.stderr)
    save()
    raise SystemExit(code)

def option(*names):
    for index, arg in enumerate(args):
        for name in names:
            if arg == name and index + 1 < len(args):
                return args[index + 1]
            if arg.startswith(name + "="):
                return arg.split("=", 1)[1]
    return None

def ns_json(name, record):
    metadata = {
        "name":name, "labels":record.get("labels", {}), "uid":record["uid"],
        "resourceVersion":record["resourceVersion"]}
    if record.get("annotations"):
        metadata["annotations"] = record["annotations"]
    if "deletionTimestamp" in record:
        metadata["deletionTimestamp"] = record["deletionTimestamp"]
    return {"apiVersion":"v1", "kind":"Namespace", "metadata":metadata}

def matches(labels, selector):
    if not selector:
        return True
    for term in selector.split(","):
        key, _, value = term.partition("=")
        if not value or labels.get(key) != value:
            return False
    return True

def guarded(payload, record):
    def find(value, key, expected):
        if isinstance(value, dict):
            if value.get(key) == expected:
                return True
            if str(value.get("path", "")).endswith("/" + key) and value.get("value") == expected:
                return True
            return any(find(child, key, expected) for child in value.values())
        if isinstance(value, list):
            return any(find(child, key, expected) for child in value)
        return False
    return find(payload, "uid", record["uid"]) and find(
        payload, "resourceVersion", record["resourceVersion"])

def desired_labels(payload):
    # An ordinary adoption writes the created-by/created-in sweep pair; the
    # #2557 operator override writes adopted-by/adopted-in INSTEAD, and writing
    # both would put an operator's namespace back into the teardown sweep.
    rendered = json.dumps(payload)
    owned = "created-by" in rendered and "created-in" in rendered
    adopted = "adopted-by" in rendered and "adopted-in" in rendered
    if owned and adopted:
        return False
    return ("curietech.ai" in rendered and RELEASE in rendered and NS in rendered
            and (owned or adopted))

program = pathlib.Path(sys.argv[0]).name
if program == "helm":
    if args[:2] == ["get", "values"]:
        fail("Error: release: not found")
    if args and args[0] == "template":
        shown = option("--show-only") or "template"
        fail(f"Error: could not find template {shown} in chart")
    if args[:2] == ["upgrade", "--install"]:
        state["helm_upgrades"] += 1
        namespaces = state["namespaces"]
        if NS not in namespaces:
            namespaces[NS] = {"labels":{}, "uid":"uid-agent-ns",
                              "resourceVersion":"17", "objects":state["defaults"]}
        state["labels_at_upgrade"] = namespaces[NS].get("labels", {}).copy()
        state["release_exists"] = True
        state["jobs"].setdefault(NS, []).append({"apiVersion":"batch/v1", "kind":"Job",
            "metadata":{"name":"failed-hook", "namespace":NS,
                "labels":{"app.kubernetes.io/instance":RELEASE,
                          "app.kubernetes.io/managed-by":"Helm"},
                "annotations":{"helm.sh/hook":"pre-install"}}})
        fail("Error: INSTALLATION FAILED: pre-install hook failed: BackoffLimitExceeded")
    if args and args[0] == "status":
        print('{"version":1,"info":{"status":"failed"},"hooks":[]}')
        raise SystemExit(0)
    if args[:2] == ["get", "manifest"]:
        print('{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"acme-probe"}}')
        raise SystemExit(0)
    if args and args[0] == "uninstall":
        if state["release_exists"]:
            state["release_exists"] = False
            save(); print(f'release "{RELEASE}" uninstalled'); raise SystemExit(0)
        fail(f'Error: uninstall: Release not loaded: {RELEASE}: release: not found')
    fail("unexpected helm invocation: " + " ".join(args), 64)

namespaces = state["namespaces"]
if args[:2] == ["get", "apiservices.apiregistration.k8s.io"]:
    if state.get("apiservices_error"):
        fail("Error from server (Forbidden): apiservices.apiregistration.k8s.io is forbidden")
    print(json.dumps(state["apiservices"])); raise SystemExit(0)

if args[:2] in (["get", "namespace"], ["get", "namespaces"]):
    name = args[2]
    record = namespaces.get(name)
    if record is None:
        if "--ignore-not-found" in args:
            raise SystemExit(0)
        fail(f'Error from server (NotFound): namespaces "{name}" not found')
    print(json.dumps(ns_json(name, record)))
    raise SystemExit(0)

if args and args[0] == "api-resources":
    if state.get("inventory_error"):
        fail("Error from server (Forbidden): inventory forbidden")
    print("\n".join(state["resources"]))
    state["discovery_calls"] += 1; save(); raise SystemExit(0)

if args[:2] in (["get", "deployment"], ["get", "priorityclass"]):
    raise SystemExit(0)

if args and args[0] == "get" and len(args) >= 2:
    resource = args[1]
    namespace = option("-n", "--namespace") or NS
    record = namespaces.get(namespace)
    if record is None:
        fail(f'Error from server (NotFound): namespaces "{namespace}" not found')
    selector = option("-l", "--selector")
    if not selector and resource in state["resources"]:
        state["inventory_seen"].append(resource)
    if resource in ("jobs", "job", "jobs.batch"):
        items = [item for item in state["jobs"].get(namespace, [])
                 if matches(item.get("metadata", {}).get("labels", {}), selector)]
    else:
        items = record.get("objects", {}).get(resource, [])
    output = option("-o", "--output")
    if resource in ("jobs", "job", "jobs.batch") and output and output.startswith("go-template="):
        names = []
        for item in items:
            metadata = item.get("metadata", {})
            annotations = metadata.get("annotations", {})
            if (annotations.get("helm.sh/hook")
                    and annotations.get("meta.helm.sh/release-name", RELEASE) == RELEASE
                    and annotations.get("meta.helm.sh/release-namespace", NS) == NS):
                names.append(metadata["name"])
        save(); print("\n".join(names)); raise SystemExit(0)
    save(); print(json.dumps({"apiVersion":"v1", "kind":"List", "items":items}))
    raise SystemExit(0)

if args[:2] == ["create", "-f"] and option("-f") == "-":
    payload = json.load(sys.stdin)
    metadata = payload.get("metadata", {})
    name = metadata.get("name")
    if payload.get("kind") != "Namespace" or name in namespaces:
        fail("Error from server (AlreadyExists): namespace already exists")
    labels = metadata.get("labels", {})
    if labels.get("curietech.ai/created-by") != RELEASE or labels.get("curietech.ai/created-in") != NS:
        fail("namespace create was not atomically pair-labelled")
    namespaces[name] = {"labels":labels, "uid":"uid-agent-ns",
                        "resourceVersion":"17", "objects":state["defaults"]}
    state["created_atomically"] = True
    save(); print(f'namespace/{name} created'); raise SystemExit(0)

if args and args[0] in ("patch", "replace") and "namespace" in args:
    name = args[args.index("namespace") + 1] if "namespace" in args else NS
    record = namespaces[name]
    raw = option("-p", "--patch")
    payload = json.loads(raw) if raw else json.load(sys.stdin)
    if not guarded(payload, record) or not desired_labels(payload):
        fail("adoption omitted the namespace uid/resourceVersion ownership precondition")
    if state.get("version_conflict"):
        fail(f'Error from server (Conflict): Operation cannot be fulfilled on namespaces "{name}": the object has been modified')
    for operation in payload:
        if operation.get("op") != "add":
            continue
        if operation.get("path") == "/metadata/labels":
            record["labels"] = operation["value"]
        elif operation.get("path") == "/metadata/annotations":
            record["annotations"] = operation["value"]
    record["resourceVersion"] = str(int(record["resourceVersion"]) + 1)
    state["adoption_guarded"] = True
    save(); print(f'namespace/{name} patched'); raise SystemExit(0)

if args[:2] == ["delete", "namespace"]:
    selector = option("-l", "--selector")
    removed = []
    for name, record in list(namespaces.items()):
        if matches(record.get("labels", {}), selector):
            removed.append(name); del namespaces[name]; state["jobs"].pop(name, None)
    save()
    for name in removed: print(f'namespace "{name}" deleted')
    raise SystemExit(0)

if args and args[0] == "delete" and len(args) > 2 and args[1] in ("job", "jobs", "jobs.batch"):
    namespace = option("-n", "--namespace") or NS
    names = [arg for arg in args[2:] if not arg.startswith("-") and arg != namespace]
    if state.get("hook_delete_error"):
        fail("Error from server (Forbidden): jobs is forbidden")
    kept = []
    for job in state["jobs"].get(namespace, []):
        if job["metadata"]["name"] in names:
            print(f'job.batch "{job["metadata"]["name"]}" deleted')
        else: kept.append(job)
    state["jobs"][namespace] = kept; save(); raise SystemExit(0)

fail("unexpected kubectl invocation: " + " ".join(args), 64)
"###;

struct Fixture {
    _temp: tempfile::TempDir,
    bin_dir: PathBuf,
    state: PathBuf,
    log: PathBuf,
}

impl Fixture {
    fn new(namespaces: Value, jobs: Value, inventory_error: bool, version_conflict: bool) -> Self {
        let temp = tempfile::tempdir().expect("tempdir");
        let bin_dir = temp.path().join("bin");
        fs::create_dir(&bin_dir).expect("create bin dir");
        write_exec(&bin_dir, "helm", FAKE_CLUSTER);
        write_exec(&bin_dir, "kubectl", FAKE_CLUSTER);
        let state = temp.path().join("state.json");
        let log = temp.path().join("commands.log");
        fs::write(&log, "").expect("write log");
        fs::write(&state, serde_json::to_vec(&json!({
            "namespaces": namespaces, "jobs": jobs, "release_exists": false,
            "helm_upgrades": 0, "labels_at_upgrade": {}, "created_atomically": false,
            "adoption_guarded": false, "inventory_error": inventory_error,
            "version_conflict": version_conflict, "discovery_calls": 0,
            "hook_delete_error": false,
            "apiservices_error": false,
            "apiservices": {"apiVersion":"v1", "kind":"List", "items":[]},
            "inventory_seen": [],
            "resources": ["serviceaccounts", "configmaps", "secrets", "events", "jobs.batch", "deployments.apps"],
            "defaults": Self::default_furniture()
        })).unwrap()).expect("write state");
        Self {
            _temp: temp,
            bin_dir,
            state,
            log,
        }
    }

    fn default_furniture() -> Value {
        json!({
            "serviceaccounts": [{"apiVersion":"v1", "kind":"ServiceAccount",
                "metadata":{"name":"default", "namespace":NS}}],
            "configmaps": [{"apiVersion":"v1", "kind":"ConfigMap",
                "metadata":{"name":"kube-root-ca.crt", "namespace":NS},
                "data":{"ca.crt":"PLACEHOLDER CERTIFICATE"}}],
            "secrets": [], "events": [], "jobs.batch": [], "deployments.apps": []
        })
    }

    fn namespace(labels: Value, objects: Value) -> Value {
        json!({"labels":labels, "uid":"uid-agent-ns", "resourceVersion":"17", "objects":objects})
    }

    fn annotated_namespace(labels: Value, annotations: Value, objects: Value) -> Value {
        json!({"labels":labels, "annotations":annotations, "uid":"uid-agent-ns",
               "resourceVersion":"17", "objects":objects})
    }

    fn command(&self) -> Command {
        let mut paths = vec![self.bin_dir.clone()];
        paths.extend(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        ));
        let mut command = Command::new(bin());
        command
            .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
            .env("PATH", std::env::join_paths(paths).unwrap())
            .env("CURIE_TEST_CLUSTER_STATE", &self.state)
            .env("CURIE_TEST_CLUSTER_LOG", &self.log)
            .env("CURIE_CONFIG_DIR", self._temp.path().join("config"))
            .env("CI", "1")
            .env("TERM", "dumb")
            .env("NO_COLOR", "1")
            .env_remove("CURIE_CREDENTIALS")
            .env_remove("CURIE_MODEL_CREDENTIALS")
            .env_remove("CURIE_GITHUB_TOKEN")
            .env_remove("CURIE_MODEL");
        command
    }

    fn up(&self) -> Output {
        self.up_in(NS, &[])
    }

    /// `cluster up` against `namespace` with `extra` operator flags appended,
    /// so one fixture can drive the plain install, the `--adopt` override, and
    /// the shared controller namespace that the override must never reach.
    fn up_in(&self, namespace: &str, extra: &[&str]) -> Output {
        let mut command = self.command();
        command.args([
            "--color",
            "never",
            "cluster",
            "up",
            "--chart",
            chart(),
            "--namespace",
            namespace,
            "--release",
            RELEASE,
            "--dev",
            "--no-expose",
            "--fake-model",
            "--set",
            "agentSandbox.controller.deploy=false",
            "--set",
            "security.gvisor.mode=off",
        ]);
        command.args(extra);
        command.output().expect("run cluster up")
    }

    fn down(&self) -> Output {
        self.command()
            .args([
                "--color",
                "never",
                "cluster",
                "down",
                "--namespace",
                NS,
                "--release",
                RELEASE,
                "--yes",
            ])
            .output()
            .expect("run cluster down")
    }

    fn down_json(&self) -> Output {
        self.command()
            .args([
                "--json",
                "--color",
                "never",
                "cluster",
                "down",
                "--namespace",
                NS,
                "--release",
                RELEASE,
                "--yes",
            ])
            .output()
            .expect("run cluster down with structured output")
    }

    fn set_hook_delete_error(&self, enabled: bool) {
        let mut state = self.state();
        state["hook_delete_error"] = json!(enabled);
        fs::write(&self.state, serde_json::to_vec(&state).unwrap()).expect("update fake state");
    }

    fn set_apiservices(&self, document: Value, read_error: bool) {
        let mut state = self.state();
        state["apiservices"] = document;
        state["apiservices_error"] = json!(read_error);
        fs::write(&self.state, serde_json::to_vec(&state).unwrap())
            .expect("update APIService state");
    }

    fn run_resume(&self, command: &str) -> Output {
        let mut paths = vec![self.bin_dir.clone()];
        paths.extend(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        ));
        Command::new("sh")
            .args(["-c", command])
            .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
            .env("PATH", std::env::join_paths(paths).unwrap())
            .env("CURIE_TEST_CLUSTER_STATE", &self.state)
            .env("CURIE_TEST_CLUSTER_LOG", &self.log)
            .output()
            .expect("execute emitted cleanup command")
    }

    fn state(&self) -> Value {
        serde_json::from_slice(&fs::read(&self.state).expect("read state")).expect("parse state")
    }
}

fn shown(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn assert_blocked_before_helm(fixture: &Fixture, output: &Output, needle: &str) {
    let state = fixture.state();
    assert!(
        !output.status.success(),
        "unsafe adoption unexpectedly succeeded: {}",
        shown(output)
    );
    assert_eq!(
        state["helm_upgrades"],
        0,
        "Helm mutated before the namespace guard: {}",
        shown(output)
    );
    assert!(
        shown(output)
            .to_lowercase()
            .contains(&needle.to_lowercase()),
        "guard error omitted {needle:?}: {}",
        shown(output)
    );
}

fn job(name: &str, labels: Value, hook: bool) -> Value {
    let mut metadata = json!({"name":name, "namespace":NS, "labels":labels});
    if hook {
        metadata["annotations"] = json!({"helm.sh/hook":"pre-install"});
    }
    json!({"apiVersion":"batch/v1", "kind":"Job", "metadata":metadata})
}

fn annotated_job(name: &str, labels: Value, annotations: Value) -> Value {
    json!({"apiVersion":"batch/v1", "kind":"Job", "metadata":{
        "name":name, "namespace":NS, "labels":labels, "annotations":annotations
    }})
}

fn remote_apiservice(available: &str) -> Value {
    json!({
        "apiVersion":"apiregistration.k8s.io/v1", "kind":"APIService",
        "metadata":{"name":"v1alpha1.metrics.example.com"},
        "spec":{
            "group":"metrics.example.com", "version":"v1alpha1",
            "groupPriorityMinimum":1000, "versionPriority":15,
            "service":{"namespace":"extension-system", "name":"metrics-adapter"}
        },
        "status":{"conditions":[{
            "type":"Available", "status":available, "reason":"FixtureCondition"
        }]}
    })
}

#[test]
fn empty_namespace_adoption_fails_closed_on_unavailable_remote_apiservices() {
    // Observed on Kubernetes v1.31: after a remote APIService is advertised,
    // `kubectl api-resources` may exit 0, omit that unavailable aggregated API
    // group from its partial output, and emit no stderr. The APIService inventory
    // is therefore an independent fail-closed prerequisite for empty adoption.
    let empty = Fixture::default_furniture();
    let fixture = |document: Value, read_error: bool| {
        let fixture = Fixture::new(
            json!({NS: Fixture::namespace(json!({}), empty.clone())}),
            json!({}),
            false,
            false,
        );
        fixture.set_apiservices(document, read_error);
        fixture
    };
    let list = |items: Value| {
        json!({
            "apiVersion":"v1",
            "kind":"List",
            "items":items
        })
    };

    let unavailable = fixture(list(json!([remote_apiservice("False")])), false);
    let output = unavailable.up();
    assert!(
        !output.status.success(),
        "unavailable remote APIService was accepted"
    );
    let state = unavailable.state();
    assert_eq!(
        state["helm_upgrades"], 0,
        "Helm ran after an incomplete discovery surface"
    );
    assert_eq!(
        state["adoption_guarded"], false,
        "ownership changed before APIService readiness"
    );
    assert_eq!(
        state["namespaces"][NS]["labels"],
        json!({}),
        "refusal changed namespace labels"
    );

    for blocked in [
        fixture(json!({"apiVersion":"v1", "kind":"List"}), false),
        fixture(list(json!([])), true),
    ] {
        let output = blocked.up();
        assert!(
            !output.status.success(),
            "unreadable or malformed APIService inventory was accepted"
        );
        let state = blocked.state();
        assert_eq!(state["helm_upgrades"], 0);
        assert_eq!(state["adoption_guarded"], false);
        assert_eq!(state["namespaces"][NS]["labels"], json!({}));
    }

    let local = json!({
        "apiVersion":"apiregistration.k8s.io/v1", "kind":"APIService",
        "metadata":{"name":"v1.local"},
        "spec":{"groupPriorityMinimum":18000, "versionPriority":1}
    });
    for allowed in [
        fixture(list(json!([remote_apiservice("True")])), false),
        fixture(list(json!([local])), false),
    ] {
        let output = allowed.up();
        assert!(
            !output.status.success(),
            "the fixture Helm hook must still fail after adoption"
        );
        let state = allowed.state();
        assert_eq!(
            state["adoption_guarded"], true,
            "safe APIService inventory blocked adoption"
        );
        assert_eq!(
            state["helm_upgrades"], 1,
            "normal install did not continue after guarded adoption"
        );
    }
}

#[test]
fn terminating_namespace_blocks_up_but_down_still_cleans_owned_hooks() {
    let mut terminating = Fixture::namespace(json!({}), Fixture::default_furniture());
    terminating["deletionTimestamp"] = json!("2026-09-08T14:30:00Z");
    let target =
        json!({"app.kubernetes.io/instance":RELEASE, "app.kubernetes.io/managed-by":"Helm"});
    let fixture = Fixture::new(
        json!({NS: terminating}),
        json!({NS: [
            job("terminating-owned-hook", target.clone(), true),
            job("terminating-non-hook", target, false),
            job("terminating-foreign-hook", json!({
                "app.kubernetes.io/instance":"other-release",
                "app.kubernetes.io/managed-by":"Helm"
            }), true)
        ]}),
        false,
        false,
    );

    let up = fixture.up();
    assert_blocked_before_helm(&fixture, &up, "terminating");

    let down = fixture.down();
    let commands = fs::read_to_string(&fixture.log).expect("read terminating cleanup log");
    assert!(
        commands
            .contains("kubectl delete job terminating-owned-hook -n agent-ns --ignore-not-found"),
        "down must attempt selective hook deletion even while the namespace terminates: {commands}"
    );
    assert!(
        down.status.success(),
        "selective cleanup on a terminating namespace must complete: {}",
        shown(&down)
    );
    let state = fixture.state();
    assert!(
        state["namespaces"].get(NS).is_some(),
        "the ownership sweep may retain the terminating namespace"
    );
    let names: Vec<_> = state["jobs"][NS]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["metadata"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        ["terminating-non-hook", "terminating-foreign-hook"],
        "down must retain non-hook and foreign-release controls"
    );
}

#[test]
fn failed_install_cleanup_and_empty_namespace_adoption() {
    let empty = Fixture::default_furniture();

    // A newly created namespace must carry the full ownership pair before Helm;
    // a failed hook install must still be removable by a later `cluster down`.
    let fresh = Fixture::new(json!({}), json!({}), false, false);
    let failed = fresh.up();
    assert!(!failed.status.success(), "the fake Helm hook must fail");
    let state = fresh.state();
    assert_eq!(
        state["created_atomically"],
        true,
        "namespace was not created atomically: {}",
        String::from_utf8_lossy(&failed.stderr)
    );
    assert_eq!(
        state["labels_at_upgrade"],
        json!({
            "curietech.ai/created-by": RELEASE, "curietech.ai/created-in": NS
        }),
        "Helm ran before ownership was established"
    );
    let down = fresh.down();
    assert!(
        down.status.success(),
        "failed install teardown failed: {}",
        shown(&down)
    );
    assert!(
        fresh.state()["namespaces"].get(NS).is_none(),
        "failed-install namespace survived down"
    );

    // An exact, empty preexisting namespace is adopted with identity/version
    // preconditions, and therefore becomes sweepable too.
    let adopt = Fixture::new(
        json!({NS: Fixture::namespace(json!({}), empty.clone())}),
        json!({}),
        false,
        false,
    );
    let failed = adopt.up();
    assert!(
        !failed.status.success(),
        "the fake Helm hook must fail after adoption"
    );
    let state = adopt.state();
    assert_eq!(
        state["adoption_guarded"], true,
        "adoption lacked uid/resourceVersion guards"
    );
    assert_eq!(
        state["discovery_calls"], 1,
        "adoption did not discover the live namespaced API inventory"
    );
    assert_eq!(
        state["inventory_seen"].as_array().unwrap().len(),
        state["resources"].as_array().unwrap().len(),
        "adoption skipped an API resource: {state}"
    );
    assert!(adopt.down().status.success());
    assert!(
        adopt.state()["namespaces"].get(NS).is_none(),
        "adopted namespace survived down"
    );

    // A retained/shared namespace is never swept, but a target-release Helm
    // hook is object-scoped cleanup. Non-hooks and insufficiently labelled hooks stay.
    let target =
        json!({"app.kubernetes.io/instance":RELEASE, "app.kubernetes.io/managed-by":"Helm"});
    let shared_jobs = json!({NS: [
        job("owned-hook", target.clone(), true), job("owned-non-hook", target.clone(), false),
        job("spoofed-hook", json!({"app.kubernetes.io/instance":RELEASE}), true),
        job("foreign-hook", json!({"app.kubernetes.io/instance":"other-release",
            "app.kubernetes.io/managed-by":"Helm"}), true)
    ]});
    let shared = Fixture::new(
        json!({NS: Fixture::namespace(json!({}), json!({
            "serviceaccounts":[], "configmaps":[], "secrets":[{"kind":"Secret","metadata":{"name":"foreign"}}],
            "events":[], "jobs.batch":[], "deployments.apps":[]
        }))}),
        shared_jobs,
        false,
        false,
    );
    let down = shared.down();
    assert!(
        down.status.success(),
        "hook cleanup failed: {}",
        shown(&down)
    );
    let state = shared.state();
    assert!(
        state["namespaces"].get(NS).is_some(),
        "shared namespace was deleted"
    );
    let names: Vec<_> = state["jobs"][NS]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["metadata"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["owned-non-hook", "spoofed-hook", "foreign-hook"]);

    // A failed exact hook deletion is fail-forward: the ownership-scoped
    // namespace sweep still runs, and structured recovery is executable once
    // the permanent API error clears. Empty hook annotations and conflicting
    // optional Helm owner annotations are intentionally outside that recovery.
    let retry_jobs = json!({NS: [
        job("retry-hook", target.clone(), true),
        job("ordinary-job", target.clone(), false),
        annotated_job("empty-hook", target.clone(), json!({"helm.sh/hook":""})),
        annotated_job("foreign-owner-name", target.clone(), json!({
            "helm.sh/hook":"pre-install", "meta.helm.sh/release-name":"other-release"
        })),
        annotated_job("foreign-owner-namespace", target, json!({
            "helm.sh/hook":"pre-install", "meta.helm.sh/release-namespace":"other-ns"
        }))
    ]});
    let retry = Fixture::new(
        json!({NS: Fixture::namespace(json!({}), json!({
            "serviceaccounts":[], "configmaps":[], "secrets":[{"kind":"Secret","metadata":{"name":"foreign"}}],
            "events":[], "jobs.batch":[], "deployments.apps":[]
        }))}),
        retry_jobs,
        false,
        false,
    );
    retry.set_hook_delete_error(true);
    let failed_down = retry.down_json();
    assert_eq!(
        failed_down.status.code(),
        Some(1),
        "a permanent hook-delete error must fail with exit 1: {}",
        shown(&failed_down)
    );
    let error: Value = serde_json::from_slice(&failed_down.stdout).unwrap_or_else(|parse| {
        panic!(
            "down error was not one JSON object ({parse}): {}",
            shown(&failed_down)
        )
    });
    assert!(
        error["error"]
            .as_str()
            .unwrap_or_default()
            .contains("Resume with:")
            && error["error"]
                .as_str()
                .unwrap_or_default()
                .contains("forbidden"),
        "down omitted the permanent reason or recovery command: {error}"
    );
    let resume = error["fix"]
        .as_str()
        .expect("hook-delete failure must carry structured recovery");
    assert!(
        resume
            .contains("app.kubernetes.io/instance=prod-release,app.kubernetes.io/managed-by=Helm")
            && resume.contains("kubectl delete job"),
        "recovery must retain the exact Helm hook selector and delete surface: {resume}"
    );
    let commands = fs::read_to_string(&retry.log).expect("read hook failure command log");
    let hook_delete = commands
        .find("kubectl delete job retry-hook")
        .expect("down must attempt the exact hook deletion");
    let sweep = commands
        .find("kubectl delete namespace -l curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns")
        .expect("down must continue to the ownership-scoped namespace sweep");
    assert!(
        hook_delete < sweep,
        "namespace sweep did not run after hook failure: {commands}"
    );
    assert!(
        retry.state()["namespaces"].get(NS).is_some(),
        "the foreign-content namespace must remain retained"
    );

    retry.set_hook_delete_error(false);
    let resumed = retry.run_resume(resume);
    assert!(
        resumed.status.success(),
        "emitted hook cleanup did not succeed after the API error cleared: {}",
        shown(&resumed)
    );
    let state = retry.state();
    assert!(
        state["namespaces"].get(NS).is_some(),
        "executing hook recovery must not delete the retained namespace"
    );
    let names: Vec<_> = state["jobs"][NS]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["metadata"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "ordinary-job",
            "empty-hook",
            "foreign-owner-name",
            "foreign-owner-namespace"
        ],
        "recovery must delete only the exact non-empty target hook"
    );

    // Foreign content and modified default furniture are both non-empty.
    let foreign = Fixture::new(
        json!({NS: Fixture::namespace(json!({}), json!({
            "serviceaccounts":[], "configmaps":[], "secrets":[{"apiVersion":"v1", "kind":"Secret",
                "metadata":{"name":"foreign-secret", "namespace":NS}}], "events":[], "jobs.batch":[], "deployments.apps":[]
        }))}),
        json!({}),
        false,
        false,
    );
    assert_blocked_before_helm(&foreign, &foreign.up(), "Secret");

    let mut custom = empty.clone();
    custom["serviceaccounts"][0]["imagePullSecrets"] = json!([{"name":"private-registry"}]);
    let custom_default = Fixture::new(
        json!({NS: Fixture::namespace(json!({}), custom)}),
        json!({}),
        false,
        false,
    );
    assert_blocked_before_helm(&custom_default, &custom_default.up(), "ServiceAccount");

    // Incomplete/foreign ownership, unreadable inventory, and a concurrent
    // namespace update all fail closed before Helm. Same release in another
    // install namespace remains out of reach.
    let partial = Fixture::new(
        json!({NS: Fixture::namespace(json!({
        "curietech.ai/created-by": RELEASE
    }), empty.clone())}),
        json!({}),
        false,
        false,
    );
    assert_blocked_before_helm(&partial, &partial.up(), "created-in");

    let inventory = Fixture::new(
        json!({NS: Fixture::namespace(json!({}), empty.clone())}),
        json!({}),
        true,
        false,
    );
    assert_blocked_before_helm(&inventory, &inventory.up(), "inventory forbidden");

    let conflict = Fixture::new(
        json!({NS: Fixture::namespace(json!({}), empty.clone())}),
        json!({}),
        false,
        true,
    );
    assert_blocked_before_helm(&conflict, &conflict.up(), "modified");
    assert_eq!(conflict.state()["namespaces"][NS]["labels"], json!({}));

    let other_ns = Fixture::new(
        json!({NS: Fixture::namespace(json!({
        "curietech.ai/created-by": RELEASE, "curietech.ai/created-in":"other-ns"
    }), empty)}),
        json!({}),
        false,
        false,
    );
    assert_blocked_before_helm(&other_ns, &other_ns.up(), "other-ns");
    let down = other_ns.down();
    assert!(
        down.status.success(),
        "foreign-owner down should be resumably complete: {}",
        shown(&down)
    );
    assert!(
        other_ns.state()["namespaces"].get(NS).is_some(),
        "same release in another namespace was swept"
    );

    // Keep the declared issue fix-pin selector comprehensive while both review
    // regressions remain independently runnable by their focused test names.
    terminating_namespace_blocks_up_but_down_still_cleans_owned_hooks();
    empty_namespace_adoption_fails_closed_on_unavailable_remote_apiservices();
    adopt_override_records_what_it_adopted_and_stays_out_of_the_teardown_sweep();
    adopt_override_never_reaches_the_controller_namespace_or_an_unreadable_one();
}

/// #2557: the explicit operator override for the three pre-existing namespace
/// refusals. It admits a namespace that already has its own labels and its own
/// objects, records what it took over on the object itself, and -- the load
/// bearing half -- leaves that namespace OUT of the `cluster down` sweep, so
/// the operator's pre-existing objects are never collateral of a teardown.
#[test]
fn adopt_override_records_what_it_adopted_and_stays_out_of_the_teardown_sweep() {
    let occupied = json!({
        "serviceaccounts":[{"apiVersion":"v1", "kind":"ServiceAccount",
            "metadata":{"name":"default", "namespace":NS}}],
        "configmaps":[{"apiVersion":"v1", "kind":"ConfigMap",
            "metadata":{"name":"kube-root-ca.crt", "namespace":NS},
            "data":{"ca.crt":"PLACEHOLDER CERTIFICATE"}}],
        "secrets":[{"apiVersion":"v1", "kind":"Secret",
            "metadata":{"name":"platform-pull-secret", "namespace":NS}}],
        "events":[], "jobs.batch":[],
        "deployments.apps":[{"apiVersion":"apps/v1", "kind":"Deployment",
            "metadata":{"name":"platform-agent", "namespace":NS}}]
    });
    let platform_labels = json!({
        "kubernetes.io/metadata.name": NS,
        "pod-security.kubernetes.io/enforce": "restricted",
        "argocd.argoproj.io/instance": "platform"
    });
    let fixture = Fixture::new(
        json!({NS: Fixture::annotated_namespace(
            platform_labels,
            json!({"openshift.io/description":"pre-provisioned by the platform team"}),
            occupied
        )}),
        json!({}),
        false,
        false,
    );

    // Without the flag the refusal stands, and now names the way through.
    let refused = fixture.up();
    assert_blocked_before_helm(&fixture, &refused, "cannot be adopted");
    assert!(
        shown(&refused).contains("--adopt"),
        "the refusal must name the override that gets past it: {}",
        shown(&refused)
    );

    // With the flag the install proceeds (the fixture's Helm hook still fails).
    let adopted = fixture.up_in(NS, &["--adopt"]);
    assert!(
        !adopted.status.success(),
        "the fixture Helm hook must still fail after an override adoption"
    );
    let state = fixture.state();
    assert_eq!(
        state["adoption_guarded"],
        true,
        "the override adoption dropped the uid/resourceVersion guard: {}",
        shown(&adopted)
    );
    assert_eq!(
        state["helm_upgrades"],
        1,
        "the install did not continue past the override: {}",
        shown(&adopted)
    );

    let labels = &state["namespaces"][NS]["labels"];
    assert_eq!(
        labels["curietech.ai/adopted-by"],
        json!(RELEASE),
        "override adoption did not stamp adopted-by: {labels}"
    );
    assert_eq!(
        labels["curietech.ai/adopted-in"],
        json!(NS),
        "override adoption did not stamp adopted-in: {labels}"
    );
    assert!(
        labels.get("curietech.ai/created-by").is_none()
            && labels.get("curietech.ai/created-in").is_none(),
        "an adopted namespace must NOT carry the sweep's ownership pair: {labels}"
    );
    assert_eq!(
        labels["pod-security.kubernetes.io/enforce"],
        json!("restricted"),
        "override adoption destroyed a pre-existing label: {labels}"
    );

    let annotations = &state["namespaces"][NS]["annotations"];
    assert_eq!(
        annotations["openshift.io/description"],
        json!("pre-provisioned by the platform team"),
        "override adoption destroyed a pre-existing annotation: {annotations}"
    );
    let recorded_labels = annotations["curietech.ai/adopted-labels"]
        .as_str()
        .expect("override adoption must record the labels it found");
    assert!(
        recorded_labels.contains("pod-security.kubernetes.io/enforce=restricted")
            && recorded_labels.contains("argocd.argoproj.io/instance=platform"),
        "recorded labels omitted what was adopted: {recorded_labels}"
    );
    let recorded_contents = annotations["curietech.ai/adopted-contents"]
        .as_str()
        .expect("override adoption must record the objects it found");
    assert!(
        recorded_contents.contains("platform-pull-secret")
            && recorded_contents.contains("platform-agent"),
        "recorded contents omitted what was adopted: {recorded_contents}"
    );
    let recorded_at = annotations["curietech.ai/adopted-at"]
        .as_str()
        .expect("override adoption must record when it happened")
        .to_string();
    assert!(
        recorded_at.starts_with("20") && recorded_at.ends_with('Z'),
        "adopted-at is not an RFC 3339 UTC instant: {recorded_at}"
    );
    assert!(
        shown(&adopted).contains("platform-pull-secret") && shown(&adopted).contains("RETAIN"),
        "the override must say on the terminal what it took and that down retains it: {}",
        shown(&adopted)
    );

    // A re-run needs no flag and must not rewrite the original decision.
    let rerun = fixture.up();
    assert!(
        !rerun.status.success(),
        "the fixture Helm hook must still fail"
    );
    let state = fixture.state();
    assert_eq!(
        state["helm_upgrades"],
        2,
        "a re-run over an already adopted namespace was refused: {}",
        shown(&rerun)
    );
    assert_eq!(
        state["namespaces"][NS]["annotations"]["curietech.ai/adopted-at"],
        json!(recorded_at),
        "a re-run rewrote the recorded adoption"
    );

    // The load bearing negative control: teardown must retain it and its
    // pre-existing objects, because it never carries the sweep selector.
    let down = fixture.down();
    assert!(
        down.status.success(),
        "teardown of an adopted namespace failed: {}",
        shown(&down)
    );
    let state = fixture.state();
    assert!(
        state["namespaces"].get(NS).is_some(),
        "cluster down deleted an adopted namespace and the operator's objects with it"
    );
    assert_eq!(
        state["namespaces"][NS]["objects"]["secrets"][0]["metadata"]["name"],
        json!("platform-pull-secret"),
        "the operator's pre-existing objects were swept"
    );
}

/// #2557 negative controls. `--adopt` overrides exactly three refusals; every
/// other refusal on this path must be byte-for-byte unchanged with the flag
/// present.
#[test]
fn adopt_override_never_reaches_the_controller_namespace_or_an_unreadable_one() {
    let empty = Fixture::default_furniture();

    // The shared, cluster-singleton controller namespace: never adoptable.
    let controller = Fixture::new(
        json!({"agent-sandbox-system": Fixture::namespace(json!({}), empty.clone())}),
        json!({}),
        false,
        false,
    );
    let output = controller.up_in("agent-sandbox-system", &["--adopt"]);
    assert_blocked_before_helm(&controller, &output, "never eligible for adoption");
    assert_eq!(
        controller.state()["namespaces"]["agent-sandbox-system"]["labels"],
        json!({}),
        "--adopt mutated the shared controller namespace"
    );

    // A terminating namespace cannot hold new objects at all.
    let mut record = Fixture::namespace(json!({}), empty.clone());
    record["deletionTimestamp"] = json!("2026-09-10T14:30:00Z");
    let terminating = Fixture::new(json!({NS: record}), json!({}), false, false);
    let output = terminating.up_in(NS, &["--adopt"]);
    assert_blocked_before_helm(&terminating, &output, "terminating");

    // An unreadable inventory stays fatal: contents nobody can read are
    // contents the adoption record cannot name.
    let unreadable = Fixture::new(
        json!({NS: Fixture::namespace(json!({"argocd.argoproj.io/instance":"platform"}), empty.clone())}),
        json!({}),
        true,
        false,
    );
    let output = unreadable.up_in(NS, &["--adopt"]);
    assert_blocked_before_helm(&unreadable, &output, "inventory forbidden");
    assert_eq!(
        unreadable.state()["namespaces"][NS]["labels"],
        json!({"argocd.argoproj.io/instance":"platform"}),
        "--adopt stamped a namespace whose contents it could not read"
    );

    // So does a concurrent modification of the namespace being adopted.
    let conflict = Fixture::new(
        json!({NS: Fixture::namespace(json!({"argocd.argoproj.io/instance":"platform"}), empty.clone())}),
        json!({}),
        false,
        true,
    );
    let output = conflict.up_in(NS, &["--adopt"]);
    assert_blocked_before_helm(&conflict, &output, "modified");

    // An unavailable aggregated APIService means the inventory is incomplete.
    let incomplete = Fixture::new(
        json!({NS: Fixture::namespace(json!({"argocd.argoproj.io/instance":"platform"}), empty.clone())}),
        json!({}),
        false,
        false,
    );
    incomplete.set_apiservices(
        json!({"apiVersion":"v1", "kind":"List", "items":[remote_apiservice("False")]}),
        false,
    );
    let output = incomplete.up_in(NS, &["--adopt"]);
    assert_blocked_before_helm(&incomplete, &output, "Available");

    // `--adopt` with nothing to override is not an override: an empty,
    // unlabelled namespace still takes the ordinary owned path and therefore
    // stays sweepable.
    let ordinary = Fixture::new(
        json!({NS: Fixture::namespace(json!({}), empty)}),
        json!({}),
        false,
        false,
    );
    let output = ordinary.up_in(NS, &["--adopt"]);
    assert!(
        !output.status.success(),
        "the fixture Helm hook must still fail"
    );
    let labels = &ordinary.state()["namespaces"][NS]["labels"];
    assert_eq!(
        labels["curietech.ai/created-by"],
        json!(RELEASE),
        "--adopt changed the ordinary empty-namespace adoption: {labels}"
    );
    assert!(
        labels.get("curietech.ai/adopted-by").is_none(),
        "--adopt recorded an override it did not take: {labels}"
    );
    assert!(ordinary.down().status.success());
    assert!(
        ordinary.state()["namespaces"].get(NS).is_none(),
        "an ordinary owned namespace must still be swept by down"
    );
}
