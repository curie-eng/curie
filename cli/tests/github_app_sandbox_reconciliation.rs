//! Real-binary regression coverage for #2270.
//!
//! `cluster github-app` mutates a Helm release. SandboxTemplate and
//! SandboxWarmPool objects are Helm-owned runtime dependencies, so success is
//! truthful only when every complete pair from the pre-mutation deployed
//! manifest still exists after the credential change. These tests put
//! deterministic `helm` and `kubectl` shims on the child process's PATH. The
//! shims model Helm revision 12, multiple sandbox pairs, live deletion, and
//! recovery while the real CLI owns parsing, ordering, bounded retries, JSON
//! errors, and the success decision.

#![cfg(unix)]

mod support;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use support::{serve, MockServer, Request, Response};

const RELEASE: &str = "acme-platform";
const NAMESPACE: &str = "acme-system";
const REVISION: &str = "12";

const PRE_MANIFEST: &str = r#"---
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxTemplate
metadata:
  name: acme-platform-runner
  labels:
    app.kubernetes.io/component: agent-sandbox
    app.kubernetes.io/instance: acme-platform
    app.kubernetes.io/managed-by: Helm
spec:
  service: true
---
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxWarmPool
metadata:
  name: acme-platform-runner-pool
  labels:
    app.kubernetes.io/component: agent-sandbox
    app.kubernetes.io/instance: acme-platform
    app.kubernetes.io/managed-by: Helm
spec:
  replicas: 0
  sandboxTemplateRef:
    name: acme-platform-runner
---
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxTemplate
metadata:
  name: acme-platform-agent-red-runner
  labels:
    app.kubernetes.io/component: agent-sandbox
    app.kubernetes.io/instance: acme-platform
    app.kubernetes.io/managed-by: Helm
spec:
  service: true
---
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxWarmPool
metadata:
  name: acme-platform-agent-red-runner-pool
  labels:
    app.kubernetes.io/component: agent-sandbox
    app.kubernetes.io/instance: acme-platform
    app.kubernetes.io/managed-by: Helm
spec:
  replicas: 0
  sandboxTemplateRef:
    name: acme-platform-agent-red-runner
"#;

const DROPPED_MANIFEST: &str = r#"---
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxTemplate
metadata:
  name: acme-platform-runner
  labels:
    app.kubernetes.io/component: agent-sandbox
    app.kubernetes.io/instance: acme-platform
    app.kubernetes.io/managed-by: Helm
spec:
  service: true
---
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxWarmPool
metadata:
  name: acme-platform-runner-pool
  labels:
    app.kubernetes.io/component: agent-sandbox
    app.kubernetes.io/instance: acme-platform
    app.kubernetes.io/managed-by: Helm
spec:
  replicas: 0
  sandboxTemplateRef:
    name: acme-platform-runner
"#;

const INCOMPLETE_MANIFEST: &str = r#"---
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxTemplate
metadata:
  name: acme-platform-runner
spec:
  service: true
---
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxWarmPool
metadata:
  name: acme-platform-runner-pool
spec:
  replicas: 0
  sandboxTemplateRef:
    name: acme-platform-does-not-exist
"#;

// One script is installed under both tool names. Python keeps the fake
// cluster's state machine readable without depending on PyYAML or a shell YAML
// parser. It receives only the generated test credential, never live material.
const TOOL_SHIM: &str = r#"#!/usr/bin/env python3
import json
import os
import re
import sys
import time
from pathlib import Path

tool = Path(sys.argv[0]).name
args = sys.argv[1:]
root = Path(os.environ["FAKE_CLUSTER_STATE"])
scenario = os.environ["FAKE_CLUSTER_SCENARIO"]
raw = root / "raw.log"
events = root / "events.log"

with raw.open("a") as stream:
    stream.write(tool + " " + " ".join(args) + "\n")

def event(value):
    with events.open("a") as stream:
        stream.write(value + "\n")

def marker(name):
    return root / ("live__" + name)

def set_live(name, present=True):
    path = marker(name)
    if present:
        path.touch()
    elif path.exists():
        path.unlink()

def metadata(name):
    return {
        "name": name,
        "labels": {
            "app.kubernetes.io/component": "agent-sandbox",
            "app.kubernetes.io/instance": "acme-platform",
            "app.kubernetes.io/managed-by": "Helm",
        },
        "annotations": {
            "meta.helm.sh/release-name": "acme-platform",
            "meta.helm.sh/release-namespace": "acme-system",
        },
    }

objects = {
    "acme-platform-runner": {
        "apiVersion": "extensions.agents.x-k8s.io/v1beta1",
        "kind": "SandboxTemplate",
        "metadata": metadata("acme-platform-runner"),
        "spec": {"service": True},
    },
    "acme-platform-runner-pool": {
        "apiVersion": "extensions.agents.x-k8s.io/v1beta1",
        "kind": "SandboxWarmPool",
        "metadata": metadata("acme-platform-runner-pool"),
        "spec": {"replicas": 0, "sandboxTemplateRef": {"name": "acme-platform-runner"}},
    },
    "acme-platform-agent-red-runner": {
        "apiVersion": "extensions.agents.x-k8s.io/v1beta1",
        "kind": "SandboxTemplate",
        "metadata": metadata("acme-platform-agent-red-runner"),
        "spec": {"service": True},
    },
    "acme-platform-agent-red-runner-pool": {
        "apiVersion": "extensions.agents.x-k8s.io/v1beta1",
        "kind": "SandboxWarmPool",
        "metadata": metadata("acme-platform-agent-red-runner-pool"),
        "spec": {"replicas": 0, "sandboxTemplateRef": {"name": "acme-platform-agent-red-runner"}},
    },
}

if scenario == "divergent-live":
    objects["acme-platform-agent-red-runner"]["metadata"]["labels"]["app.kubernetes.io/managed-by"] = "external-controller"
    objects["acme-platform-agent-red-runner"]["spec"]["service"] = False
if scenario == "wrong-live-ownership":
    del objects["acme-platform-agent-red-runner"]["metadata"]["annotations"]["meta.helm.sh/release-namespace"]
def sandbox_list():
    items = []
    for name, value in objects.items():
        if not marker(name).exists():
            continue
        persisted = root / ("created-json__" + name)
        items.append(json.loads(persisted.read_text()) if persisted.exists() else value)
    return {"apiVersion": "v1", "items": items}

if tool == "helm":
    verb = args[0] if args else ""
    if verb == "history":
        event("helm:history")
        if scenario == "history-failure":
            print("history unavailable", file=sys.stderr)
            raise SystemExit(1)
        counter = root / "history-count"
        count = int(counter.read_text()) if counter.exists() else 0
        counter.write_text(str(count + 1))
        if scenario == "history-failure-before-write" and count >= 2:
            print("history unavailable at the recovery write fence", file=sys.stderr)
            raise SystemExit(1)
        if scenario == "history-failure-after-create" and (root / "create-succeeded").exists():
            print("history unavailable after sandbox create", file=sys.stderr)
            raise SystemExit(1)
        if scenario == "history-failure-after-helm" and (root / "upgraded").exists():
            print("history unavailable after credential mutation", file=sys.stderr)
            raise SystemExit(1)
        if scenario == "history-failure-after-rollout" and (root / "rollout-finished").exists():
            print("history unavailable after API rollout", file=sys.stderr)
            raise SystemExit(1)
        if scenario == "pending-head-initial" or (scenario == "pending-head-write-fence" and count > 0):
            rows = [
                {"revision": 12, "updated": "2026-01-01T00:00:00Z", "status": "deployed", "chart": "curie-0.8.6", "app_version": "0.8.6", "description": "Upgrade complete"},
                {"revision": 13, "updated": "2026-01-01T00:01:00Z", "status": "pending-upgrade", "chart": "curie-0.8.6", "app_version": "0.8.6", "description": "Preparing upgrade"},
            ]
        elif scenario == "helm-failed-successor-with-loss" and (root / "upgraded").exists():
            rows = [
                {"revision": 12, "updated": "2026-01-01T00:00:00Z", "status": "deployed", "chart": "curie-0.8.6", "app_version": "0.8.6", "description": "Upgrade complete"},
                {"revision": 13, "updated": "2026-01-01T00:01:00Z", "status": "failed", "chart": "curie-0.8.6", "app_version": "0.8.6", "description": "Credential upgrade failed"},
            ]
        elif scenario == "failed-head-then-success" and (root / "upgraded").exists():
            rows = [
                {"revision": 12, "updated": "2026-01-01T00:00:00Z", "status": "superseded", "chart": "curie-0.8.6", "app_version": "0.8.6", "description": "Upgrade complete"},
                {"revision": 13, "updated": "2026-01-01T00:01:00Z", "status": "failed", "chart": "curie-0.8.6", "app_version": "0.8.6", "description": "Upgrade failed"},
                {"revision": 14, "updated": "2026-01-01T00:02:00Z", "status": "deployed", "chart": "curie-0.8.6", "app_version": "0.8.6", "description": "Upgrade complete"},
            ]
        elif scenario == "failed-head-then-success":
            rows = [
                {"revision": 12, "updated": "2026-01-01T00:00:00Z", "status": "deployed", "chart": "curie-0.8.6", "app_version": "0.8.6", "description": "Upgrade complete"},
                {"revision": 13, "updated": "2026-01-01T00:01:00Z", "status": "failed", "chart": "curie-0.8.6", "app_version": "0.8.6", "description": "Upgrade failed"},
            ]
        elif scenario == "restore-create-then-drift" and (root / "create-succeeded").exists():
            rows = [
                {"revision": 12, "status": "superseded", "chart": "curie-0.8.6", "description": "Upgrade complete"},
                {"revision": 13, "status": "superseded", "chart": "curie-0.8.6", "description": "credential upgrade completed"},
                {"revision": 14, "status": "deployed", "chart": "curie-0.8.6", "description": "external upgrade completed"},
            ]
        elif scenario == "create-then-drift" and (root / "create-succeeded").exists():
            rows = [
                {"revision": 12, "status": "superseded", "chart": "curie-0.8.6", "description": "Upgrade complete"},
                {"revision": 13, "status": "deployed", "chart": "curie-0.8.6", "description": "external upgrade completed"},
            ]
        elif scenario in ("revision-drift", "pre-missing-write-fence-drift") and count > 0:
            rows = [
                {"revision": 12, "status": "superseded", "chart": "curie-0.8.6", "description": "Upgrade complete"},
                {"revision": 13, "status": "deployed", "chart": "curie-0.8.6", "description": "Upgrade complete"},
            ]
        elif scenario == "post-missing-write-fence-drift" and (root / "upgraded").exists() and count >= 3:
            rows = [
                {"revision": 12, "status": "superseded", "chart": "curie-0.8.6", "description": "Upgrade complete"},
                {"revision": 13, "status": "superseded", "chart": "curie-0.8.6", "description": "credential upgrade completed"},
                {"revision": 14, "status": "deployed", "chart": "curie-0.8.6", "description": "external upgrade completed"},
            ]
        elif scenario == "post-upgrade-interleaving" and (root / "upgraded").exists():
            rows = [
                {"revision": 12, "status": "superseded", "chart": "curie-0.8.6", "description": "Upgrade complete"},
                {"revision": 13, "status": "superseded", "chart": "curie-0.8.6", "description": "external upgrade completed"},
                {"revision": 14, "status": "deployed", "chart": "curie-0.8.6", "description": "credential upgrade completed"},
            ]
        elif scenario == "rollout-revision-drift" and (root / "rollout-started").exists():
            rows = [
                {"revision": 12, "status": "superseded", "chart": "curie-0.8.6", "description": "Upgrade complete"},
                {"revision": 13, "status": "superseded", "chart": "curie-0.8.6", "description": "credential upgrade completed"},
                {"revision": 14, "status": "deployed", "chart": "curie-0.8.6", "description": "external upgrade completed"},
            ]
        elif scenario == "helm-upgrade-hang" and (root / "upgraded").exists():
            rows = [
                {"revision": 12, "status": "deployed", "chart": "curie-0.8.6", "description": "Upgrade complete"},
            ]
        elif (root / "upgraded").exists():
            rows = [
                {"revision": 12, "status": "superseded", "chart": "curie-0.8.6", "description": "Upgrade complete"},
                {"revision": 13, "status": "deployed", "chart": "curie-0.8.6", "description": "Upgrade complete"},
            ]
        else:
            rows = [
                {"revision": 12, "status": "deployed", "chart": "curie-0.8.6", "description": "Upgrade complete"},
            ]
        head = max(rows, key=lambda row: row["revision"])
        event("helm:head:" + str(head["revision"]) + ":" + head["status"])
        deployed = next(row["revision"] for row in rows if row["status"] == "deployed")
        event("helm:deployed:" + str(deployed))
        print(json.dumps(rows))
        raise SystemExit(0)
    if verb == "get" and len(args) > 1 and args[1] == "values":
        event("helm:get-values")
        if scenario == "sandbox-disabled":
            print(json.dumps({"agentSandbox": {"deploy": False}}))
        elif scenario == "active-agent-omitted":
            print(json.dumps({"agentSandbox": {"deploy": True, "connectorSecrets": {"red": {"EXAMPLE_SETTING": "placeholder"}}}}))
        else:
            print("{}")
        raise SystemExit(0)
    if verb == "get" and len(args) > 1 and args[1] == "manifest":
        phase = "recovered" if (root / "rolled-back").exists() else ("post" if (root / "upgraded").exists() else "pre")
        event("helm:get-manifest:" + phase)
        if scenario in ("empty", "sandbox-disabled"):
            body = ""
        elif scenario == "incomplete":
            body = (root / "incomplete.yaml").read_text()
        elif scenario == "active-agent-omitted" or (scenario in ("ownership-loss", "restore-create-then-drift") and phase == "post") or (scenario == "create-then-drift" and (root / "create-succeeded").exists()):
            body = (root / "dropped.yaml").read_text()
        else:
            body = (root / "pre.yaml").read_text()
        sys.stdout.write(body)
        raise SystemExit(0)
    if verb == "upgrade":
        event("helm:upgrade")
        (root / "upgraded").touch()
        if scenario in ("ownership-loss", "unreconciled", "helm-upgrade-hang", "post-missing-write-fence-drift", "restore-create-then-drift", "helm-failed-successor-with-loss"):
            set_live("acme-platform-agent-red-runner", False)
            set_live("acme-platform-agent-red-runner-pool", False)
        if scenario == "helm-upgrade-hang":
            (root / "hung-helm.pid").write_text(str(os.getpid()))
            print("helm-hang-stdout-placeholder", flush=True)
            print("helm-hang-stderr-placeholder", file=sys.stderr, flush=True)
            time.sleep(3)
            raise SystemExit(124)
        if scenario == "helm-sensitive-stderr":
            print(os.environ["FAKE_SENSITIVE_SENTINEL"], file=sys.stderr)
            raise SystemExit(1)
        if scenario == "helm-failed-successor-with-loss":
            print("placeholder credential upgrade failure", file=sys.stderr)
            raise SystemExit(1)
        raise SystemExit(0)
    if verb == "rollback":
        target = args[2] if len(args) > 2 else "missing"
        event("helm:rollback:" + target)
        (root / "rolled-back").touch()
        for name in objects:
            set_live(name)
        raise SystemExit(0)
    print("unsupported fake helm invocation: " + " ".join(args), file=sys.stderr)
    raise SystemExit(1)

if tool == "kubectl":
    if args == ["-n", os.environ["FAKE_CLUSTER_NAMESPACE"], "get", "secret", "acme-github-app", "-o", "json"]:
        print((root / "github-app-secret.json").read_text())
        raise SystemExit(0)
    joined = " ".join(args)
    if args and args[0] == "proxy":
        event("kubectl:proxy")
        print("placeholder proxy is forbidden", file=sys.stderr)
        raise SystemExit(1)
    if " delete " in " " + joined + " ":
        event("kubectl:delete")
        print("placeholder delete is forbidden", file=sys.stderr)
        raise SystemExit(1)
    if " rollout restart " in " " + joined + " ":
        event("kubectl:rollout-restart")
        (root / "rollout-started").touch()
        if scenario in ("rollout-loss", "rollout-revision-drift", "rollout-restart-failure"):
            set_live("acme-platform-agent-red-runner", False)
            set_live("acme-platform-agent-red-runner-pool", False)
        if scenario == "rollout-restart-failure":
            print("placeholder rollout restart failure", file=sys.stderr)
            raise SystemExit(1)
        raise SystemExit(0)
    if " rollout status " in " " + joined + " ":
        event("kubectl:rollout-status")
        if scenario == "rollout-status-failure":
            set_live("acme-platform-agent-red-runner", False)
            set_live("acme-platform-agent-red-runner-pool", False)
            print("placeholder rollout status failure", file=sys.stderr)
            raise SystemExit(1)
        (root / "rollout-finished").touch()
        raise SystemExit(0)
    if " get svc " in " " + joined + " ":
        event("kubectl:get-api-fullname")
        print("acme-platform-api")
        raise SystemExit(0)
    if " get " in " " + joined + " " and ("sandboxtemplate" in joined.lower() or "sandboxwarmpool" in joined.lower()):
        event("kubectl:get-sandboxes:" + ("post" if (root / "upgraded").exists() else "pre"))
        if scenario == "kubectl-timeout":
            print("request deadline exceeded", file=sys.stderr)
            raise SystemExit(124)
        if scenario == "kubectl-hang":
            (root / "hung-kubectl.pid").write_text(str(os.getpid()))
            time.sleep(30)
        print(json.dumps(sandbox_list()))
        raise SystemExit(0)
    if " create " in " " + joined + " " or " apply " in " " + joined + " ":
        verb = "apply" if " apply " in " " + joined + " " else "create"
        source = None
        for index, value in enumerate(args[:-1]):
            if value in ("-f", "--filename"):
                source = args[index + 1]
                break
        if source is None:
            print("fake kubectl expected -f", file=sys.stderr)
            raise SystemExit(1)
        body = sys.stdin.read() if source == "-" else Path(source).read_text()
        kind = re.search(r"(?m)^kind:\s*(\S+)\s*$", body)
        metadata_block = re.search(r"(?m)^metadata:\s*\n((?:^[ \t]+[^\n]*\n?)*)", body)
        name = re.search(r"(?m)^[ \t]+name:\s*(\S+)\s*$", metadata_block.group(1)) if metadata_block else None
        if not kind or not name:
            print("fake kubectl could not identify created sandbox", file=sys.stderr)
            raise SystemExit(1)
        kind = kind.group(1)
        name = name.group(1)
        event("kubectl:" + verb + ":" + kind + ":" + name)
        if verb == "create":
            (root / ("created__" + name + ".yaml")).write_text(body)
        if scenario == "concurrent-create" and not (root / "concurrent-create-seen").exists():
            (root / "concurrent-create-seen").touch()
            set_live(name)
            print("already exists", file=sys.stderr)
            raise SystemExit(1)
        if scenario != "unreconciled":
            set_live(name)
        if verb == "create":
            operation = re.search(r"(?m)^[ \t]+curietech\.ai/github-app-recovery:\s*(\S+)\s*$", body)
            objects[name]["metadata"]["uid"] = "00000000-0000-4000-8000-000000000001"
            objects[name]["metadata"]["resourceVersion"] = "101"
            if operation:
                objects[name]["metadata"]["annotations"]["curietech.ai/github-app-recovery"] = operation.group(1)
            (root / ("created-json__" + name)).write_text(json.dumps(objects[name]))
            print(json.dumps(objects[name]))
            if scenario in ("create-then-drift", "history-failure-after-create", "restore-create-then-drift"):
                (root / "create-succeeded").touch()
        raise SystemExit(0)
    print("unsupported fake kubectl invocation: " + joined, file=sys.stderr)
    raise SystemExit(1)

raise SystemExit(1)
"#;

struct FakeCluster {
    dir: tempfile::TempDir,
    github: MockServer,
}

impl FakeCluster {
    fn new(scenario: &str) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin_dir = dir.path().join("bin");
        fs::create_dir(&bin_dir).expect("create shim dir");
        for tool in ["helm", "kubectl"] {
            let path = bin_dir.join(tool);
            fs::write(&path, TOOL_SHIM).expect("write tool shim");
            let mut permissions = fs::metadata(&path).expect("stat shim").permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&path, permissions).expect("chmod shim");
        }
        fs::write(dir.path().join("pre.yaml"), PRE_MANIFEST).expect("write pre manifest");
        fs::write(dir.path().join("dropped.yaml"), DROPPED_MANIFEST)
            .expect("write dropped manifest");
        fs::write(dir.path().join("incomplete.yaml"), INCOMPLETE_MANIFEST)
            .expect("write incomplete manifest");

        for name in [
            "acme-platform-runner",
            "acme-platform-runner-pool",
            "acme-platform-agent-red-runner",
            "acme-platform-agent-red-runner-pool",
        ] {
            if scenario != "sandbox-disabled"
                && (scenario != "pre-missing"
                    && scenario != "concurrent-create"
                    && scenario != "pre-missing-write-fence-drift"
                    && scenario != "create-then-drift"
                    && scenario != "history-failure-before-write"
                    && scenario != "history-failure-after-create"
                    || !name.contains("agent-red"))
            {
                fs::write(dir.path().join(format!("live__{name}")), "").expect("seed live object");
            }
        }
        // The released identity guard authenticates the BYO key before the
        // sandbox barrier. Generate a real PEM and mock only GitHub's documented
        // GET /app response, as in github_app_identity (issue #2269).
        // https://docs.github.com/en/rest/apps/apps#get-the-authenticated-app
        let key = Command::new("openssl")
            .args(["genrsa", "2048"])
            .output()
            .expect("generate test App PEM");
        assert!(key.status.success(), "openssl genrsa failed");
        let secret = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "acme-github-app"},
            "data": {
                "app-pem": base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    &key.stdout,
                )
            }
        });
        let secret_path = dir.path().join("github-app-secret.json");
        fs::write(&secret_path, secret.to_string()).expect("write test App Secret");
        fs::set_permissions(&secret_path, fs::Permissions::from_mode(0o600))
            .expect("protect test App Secret");
        let github = serve(|request: &Request| {
            if request.method == "GET"
                && request.path == "/app"
                && request
                    .header("authorization")
                    .is_some_and(|value| value.starts_with("Bearer "))
            {
                Response::json(200, r#"{"id":1234567,"name":"acme-bot"}"#)
            } else {
                Response::json(401, r#"{"message":"Unauthorized"}"#)
            }
        });
        Self { dir, github }
    }

    fn state(&self) -> &Path {
        self.dir.path()
    }

    fn child_path(&self) -> std::ffi::OsString {
        let mut dirs = vec![self.state().join("bin")];
        if let Some(existing) = std::env::var_os("PATH") {
            dirs.extend(std::env::split_paths(&existing));
        }
        std::env::join_paths(dirs).expect("join PATH")
    }

    fn run(&self, scenario: &str, disconnect: bool) -> Output {
        self.run_target(scenario, disconnect, RELEASE, NAMESPACE)
    }

    fn run_target(
        &self,
        scenario: &str,
        disconnect: bool,
        release: &str,
        namespace: &str,
    ) -> Output {
        let mut args = vec![
            "cluster",
            "github-app",
            "--namespace",
            namespace,
            "--release",
            release,
            "--chart",
            "charts/curie",
            "--json",
        ];
        if disconnect {
            args.push("--disconnect");
        } else {
            args.extend([
                "--app-id",
                "1234567",
                "--existing-secret",
                "acme-github-app",
                "--existing-secret-key",
                "app-pem",
            ]);
        }
        let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
        command
            .args(&args)
            .env("PATH", self.child_path())
            .env("FAKE_CLUSTER_STATE", self.state())
            .env("FAKE_CLUSTER_SCENARIO", scenario)
            .env("FAKE_CLUSTER_NAMESPACE", namespace)
            .env("CURIE_GITHUB_API_URL", &self.github.base_url)
            .env("FAKE_SENSITIVE_SENTINEL", sensitive_stderr_sentinel());
        if scenario == "helm-upgrade-hang" {
            command.env("CURIE_TEST_GITHUB_APP_HELM_TIMEOUT_MS", "150");
        }
        command
            .output()
            .unwrap_or_else(|error| panic!("run curie {}: {error}", args.join(" ")))
    }

    fn events(&self) -> Vec<String> {
        read_lines(self.state().join("events.log"))
    }

    fn raw(&self) -> Vec<String> {
        read_lines(self.state().join("raw.log"))
    }
}

fn sensitive_stderr_sentinel() -> String {
    format!(
        "-----{} PRIVATE KEY----- placeholder-only-sensitive-stderr",
        "BEGIN"
    )
}

fn read_lines(path: PathBuf) -> Vec<String> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

fn combined(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn process_exited(pid: u32) -> bool {
    let process = PathBuf::from("/proc").join(pid.to_string());
    let deadline = Instant::now() + Duration::from_secs(2);
    while process.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    !process.exists()
}

fn stdout_json(output: &Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(stdout.trim())
        .unwrap_or_else(|error| panic!("stdout must be one JSON object: {error}; stdout: {stdout}"))
}

fn position(events: &[String], expected: &str) -> usize {
    events
        .iter()
        .position(|event| event == expected)
        .unwrap_or_else(|| panic!("missing event `{expected}`: {events:?}"))
}

fn assert_revisioned_pre_manifest_read(raw: &[String]) {
    assert!(
        raw.iter().any(|line| {
            line.starts_with(&format!("helm get manifest {RELEASE} "))
                && (line.contains(&format!("--revision {REVISION}"))
                    || line.contains(&format!("--revision={REVISION}")))
        }),
        "the pre-mutation desired set was not read from exact Helm revision {REVISION}: {raw:?}"
    );
}

fn assert_recreated_as_helm_owned(cluster: &FakeCluster, names: &[&str]) {
    for name in names {
        let path = cluster.state().join(format!("created__{name}.yaml"));
        let body = fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!("read recreated object body {}: {error}", path.display())
        });
        for expected in [
            "app.kubernetes.io/managed-by: Helm",
            "meta.helm.sh/release-name: acme-platform",
            "meta.helm.sh/release-namespace: acme-system",
        ] {
            assert!(
                body.lines().any(|line| line.trim() == expected),
                "recreated {name} must preserve Helm ownership `{expected}`: {body}"
            );
        }
    }
}

fn recovery_operation_from_created(cluster: &FakeCluster, name: &str) -> String {
    let path = cluster.state().join(format!("created__{name}.yaml"));
    let body = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read recovery body {}: {error}", path.display()));
    let operation = body
        .lines()
        .find_map(|line| {
            line.trim()
                .strip_prefix("curietech.ai/github-app-recovery:")
                .map(str::trim)
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| panic!("created {name} lacks a recovery-operation annotation: {body}"));
    assert!(
        uuid::Uuid::parse_str(operation).is_ok(),
        "the recovery-operation annotation must be a unique operation identifier: {body}"
    );
    operation.to_string()
}

fn assert_history_inspection_without_rollback(value: &serde_json::Value) {
    let fix = value
        .get("fix")
        .and_then(|fix| fix.as_str())
        .unwrap_or_default();
    let lower = fix.to_lowercase();
    assert!(
        fix.contains("helm history acme-platform -n acme-system")
            && lower.contains("no automatic")
            && lower.contains("rollback")
            && !fix.contains("helm rollback"),
        "the failure must direct history inspection and explicitly refuse rollback: {value}"
    );
}

fn has_request_timeout(command: &str) -> bool {
    command
        .split_whitespace()
        .any(|arg| arg == "--request-timeout" || arg.starts_with("--request-timeout="))
}

fn assert_no_success(output: &Output, events: &[String]) {
    let value = stdout_json(output);
    assert!(
        value.get("github_app_configured").is_none(),
        "a failed reconciliation reported success: {value}"
    );
    assert!(
        !combined(output).contains("GitHub App configured"),
        "a failed reconciliation printed the success note: {}",
        combined(output)
    );
    assert!(
        !events
            .iter()
            .any(|event| event == "kubectl:rollout-restart"),
        "the API rolled before the sandbox recovery gate passed: {events:?}"
    );
}

fn assert_rollout_failure_recovers_sandbox_loss(scenario: &str, failure_event: &str) {
    let cluster = FakeCluster::new(scenario);
    let output = cluster.run(scenario, false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "a failed API rollout must return nonzero: {}",
        combined(&output)
    );
    let failure = position(&events, failure_event);
    let template = position(
        &events,
        "kubectl:create:SandboxTemplate:acme-platform-agent-red-runner",
    );
    let pool = position(
        &events,
        "kubectl:create:SandboxWarmPool:acme-platform-agent-red-runner-pool",
    );
    let verified = events
        .iter()
        .rposition(|event| event == "kubectl:get-sandboxes:post")
        .unwrap_or_else(|| panic!("missing post-rollout-failure verification: {events:?}"));
    assert!(
        failure < template && template < pool && pool < verified,
        "rollout failure must still restore template-before-pool and verify: {events:?}"
    );
    let recovery_reads = events[failure + 1..]
        .iter()
        .filter(|event| event.as_str() == "kubectl:get-sandboxes:post")
        .count();
    assert!(
        (2..=4).contains(&recovery_reads),
        "rollout-failure reconciliation must verify within its bounded retry budget: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| event.starts_with("helm:rollback:")),
        "the CLI must not automatically roll back after rollout failure: {events:?}"
    );
    let value = stdout_json(&output);
    let error = value
        .get("error")
        .and_then(|error| error.as_str())
        .unwrap_or_default()
        .to_lowercase();
    assert!(
        error.contains("rollout") && error.contains("sandbox"),
        "the typed failure must report both rollout failure and sandbox recovery: {value}"
    );
    let recovery =
        format!("helm rollback {RELEASE} {REVISION} -n {NAMESPACE} --wait --timeout 180s");
    assert!(
        value
            .get("fix")
            .and_then(|fix| fix.as_str())
            .is_some_and(|fix| fix.contains(&recovery)),
        "rollout failure must provide deterministic operator recovery: {value}"
    );
    assert!(
        value.get("github_app_configured").is_none()
            && !combined(&output).contains("GitHub App configured"),
        "rollout failure emitted success: {value}"
    );
}

fn assert_pending_helm_head_blocks_all_mutation(scenario: &str) {
    let cluster = FakeCluster::new(scenario);
    let output = cluster.run(scenario, false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "a higher pending Helm head must return nonzero"
    );
    assert!(
        events
            .iter()
            .any(|event| event == "helm:head:13:pending-upgrade"),
        "the fixture did not expose pending-upgrade revision 13: {events:?}"
    );
    assert!(
        !events.iter().any(|event| {
            event.starts_with("kubectl:create:")
                || event.starts_with("kubectl:apply:")
                || event == "helm:upgrade"
                || event == "kubectl:rollout-restart"
        }),
        "the pending Helm operation did not block every mutation path: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| event.starts_with("helm:rollback:")),
        "a pending Helm operation must never trigger rollback: {events:?}"
    );
    let value = stdout_json(&output);
    let error = value
        .get("error")
        .and_then(|error| error.as_str())
        .unwrap_or_default()
        .to_lowercase();
    assert!(
        error.contains("pending-upgrade") && error.contains("13"),
        "the refusal must identify the pending head revision and status: {value}"
    );
    assert_history_inspection_without_rollback(&value);
    assert!(
        value.get("github_app_configured").is_none()
            && !combined(&output).contains("GitHub App configured"),
        "pending Helm state emitted false success: {value}"
    );
}

#[test]
fn connect_preserves_every_manifest_pair_and_gates_api_rollout_on_post_upgrade_verification() {
    let cluster = FakeCluster::new("healthy");
    let output = cluster.run("healthy", false);
    let events = cluster.events();
    assert!(
        output.status.success(),
        "healthy connect must succeed: {}; events: {events:?}",
        combined(&output)
    );
    assert_eq!(
        stdout_json(&output),
        serde_json::json!({"github_app_configured": true})
    );
    assert_revisioned_pre_manifest_read(&cluster.raw());

    let pre_manifest = position(&events, "helm:get-manifest:pre");
    let upgrade = position(&events, "helm:upgrade");
    let post_manifest = position(&events, "helm:get-manifest:post");
    let post_verify = events
        .iter()
        .enumerate()
        .skip(post_manifest + 1)
        .find_map(|(index, event)| (event == "kubectl:get-sandboxes:post").then_some(index))
        .unwrap_or_else(|| panic!("missing post-upgrade live verification: {events:?}"));
    let rollout = position(&events, "kubectl:rollout-restart");
    assert!(
        pre_manifest < upgrade && upgrade < post_manifest && post_manifest < post_verify && post_verify < rollout,
        "required order is snapshot -> Helm mutation -> reconcile/verify -> API rollout: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| event.starts_with("kubectl:create:")),
        "already-live objects must not be overwritten: {events:?}"
    );
}

#[test]
fn a_pair_deleted_during_api_rollout_is_recreated_and_verified_before_success() {
    let cluster = FakeCluster::new("rollout-loss");
    let output = cluster.run("rollout-loss", false);
    let events = cluster.events();
    assert!(
        output.status.success(),
        "rollout-time loss must be repaired before success: {}; events: {events:?}",
        combined(&output)
    );
    let rollout = position(&events, "kubectl:rollout-restart");
    let template = position(
        &events,
        "kubectl:create:SandboxTemplate:acme-platform-agent-red-runner",
    );
    let pool = position(
        &events,
        "kubectl:create:SandboxWarmPool:acme-platform-agent-red-runner-pool",
    );
    let final_verify = events
        .iter()
        .rposition(|event| event == "kubectl:get-sandboxes:post")
        .unwrap_or_else(|| panic!("missing final live verification: {events:?}"));
    assert!(
        rollout < template && template < pool && pool < final_verify,
        "the post-rollout barrier must repair template-before-pool and verify before success: {events:?}"
    );
    assert_eq!(
        stdout_json(&output),
        serde_json::json!({"github_app_configured": true})
    );
}

#[test]
fn rollout_restart_failure_still_runs_bounded_sandbox_recovery_before_returning_error() {
    assert_rollout_failure_recovers_sandbox_loss(
        "rollout-restart-failure",
        "kubectl:rollout-restart",
    );
}

#[test]
fn rollout_status_failure_still_runs_bounded_sandbox_recovery_before_returning_error() {
    assert_rollout_failure_recovers_sandbox_loss(
        "rollout-status-failure",
        "kubectl:rollout-status",
    );
}

#[test]
fn a_pending_helm_head_at_initial_snapshot_blocks_every_mutation() {
    assert_pending_helm_head_blocks_all_mutation("pending-head-initial");
}

#[test]
fn a_pending_helm_head_appearing_at_the_write_fence_blocks_every_mutation() {
    assert_pending_helm_head_blocks_all_mutation("pending-head-write-fence");
}

#[test]
fn a_failed_higher_head_allows_the_next_successful_revision_and_normal_rollout() {
    let cluster = FakeCluster::new("failed-head-then-success");
    let output = cluster.run("failed-head-then-success", false);
    let events = cluster.events();
    assert!(
        output.status.success(),
        "a stable failed head must not block the next Helm revision: {}; events: {events:?}",
        combined(&output)
    );
    let failed_head = position(&events, "helm:head:13:failed");
    let upgrade = position(&events, "helm:upgrade");
    let deployed = position(&events, "helm:head:14:deployed");
    let rollout = position(&events, "kubectl:rollout-restart");
    assert!(
        failed_head < upgrade && upgrade < deployed && deployed < rollout,
        "the failed head must remain stable through preflight, then Helm revision 14 must precede rollout: {events:?}"
    );
    assert!(
        !events.iter().any(|event| {
            event.starts_with("kubectl:create:")
                || event.starts_with("kubectl:apply:")
                || event.starts_with("helm:rollback:")
        }),
        "the healthy failed-head path unexpectedly rewrote or rolled back sandbox state: {events:?}"
    );
    let raw = cluster.raw();
    assert_revisioned_pre_manifest_read(&raw);
    assert!(
        raw.iter().any(|line| {
            line.starts_with(&format!("helm get manifest {RELEASE} "))
                && (line.contains("--revision 14") || line.contains("--revision=14"))
        }),
        "the post-upgrade sandbox inventory was not read from successful revision 14: {raw:?}"
    );
    assert_eq!(
        stdout_json(&output),
        serde_json::json!({"github_app_configured": true})
    );
}

#[test]
fn a_revision_drift_before_mutation_refuses_without_upgrading_or_rolling_back_stale_state() {
    let cluster = FakeCluster::new("revision-drift");
    let output = cluster.run("revision-drift", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "a revision that moved after snapshot must not report success: {}",
        combined(&output)
    );
    assert!(
        events
            .iter()
            .filter(|event| *event == "helm:history")
            .count()
            >= 2,
        "the release revision was never rechecked immediately before mutation: {events:?}"
    );
    assert!(
        !events.iter().any(|event| event == "helm:upgrade"),
        "credential mutation ran against a stale revision snapshot: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| event.starts_with("helm:rollback:")),
        "the drift path must not roll back revision 12 after another actor deployed 13: {events:?}"
    );
    let value = stdout_json(&output);
    let error = value
        .get("error")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    assert!(
        error.contains("12") && error.contains("13"),
        "the refusal must name the captured and current revisions: {value}"
    );
    assert!(
        value.get("github_app_configured").is_none(),
        "revision drift emitted success JSON: {value}"
    );
}

#[test]
fn preflight_repair_refuses_to_create_when_the_release_drifts_at_its_write_fence() {
    let cluster = FakeCluster::new("pre-missing-write-fence-drift");
    let output = cluster.run("pre-missing-write-fence-drift", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "a preflight repair must not write objects from a stale manifest: {}",
        combined(&output)
    );
    position(&events, "helm:deployed:13");
    assert!(
        !events.iter().any(|event| {
            event.starts_with("kubectl:create:") || event.starts_with("kubectl:apply:")
        }),
        "preflight revision drift must prevent every stale sandbox write: {events:?}"
    );
    assert!(
        !events.iter().any(|event| event == "helm:upgrade"),
        "credential mutation ran after preflight revision drift: {events:?}"
    );
    let value = stdout_json(&output);
    let error = value
        .get("error")
        .and_then(|error| error.as_str())
        .unwrap_or_default();
    assert!(
        error.contains("12") && error.contains("13") && error.to_lowercase().contains("revision"),
        "the write-fence refusal must identify captured revision 12 and current revision 13: {value}"
    );
    assert!(
        value.get("github_app_configured").is_none(),
        "preflight write-fence drift emitted success: {value}"
    );
}

#[test]
fn history_failure_before_recovery_write_refuses_without_writing_or_rollback_guidance() {
    let cluster = FakeCluster::new("history-failure-before-write");
    let output = cluster.run("history-failure-before-write", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "a failed pre-write history fence must return nonzero"
    );
    assert!(
        !events.iter().any(|event| {
            event.starts_with("kubectl:create:") || event.starts_with("kubectl:apply:")
        }),
        "a sandbox was written after the history fence became unreadable: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| event.starts_with("helm:rollback:")),
        "an unreadable write fence must never trigger automatic rollback: {events:?}"
    );
    let value = stdout_json(&output);
    assert_history_inspection_without_rollback(&value);
    assert!(
        value.get("github_app_configured").is_none()
            && !events.iter().any(|event| event == "helm:upgrade")
            && !events
                .iter()
                .any(|event| event == "kubectl:rollout-restart"),
        "the failed history fence continued toward mutation or success: {events:?}; {value}"
    );
}

#[test]
fn history_failure_after_create_preserves_operation_marker_guidance_and_stops() {
    let cluster = FakeCluster::new("history-failure-after-create");
    let output = cluster.run("history-failure-after-create", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "an unreadable post-create history fence must return nonzero"
    );
    let creates = events
        .iter()
        .filter(|event| event.starts_with("kubectl:create:"))
        .map(String::as_str)
        .collect::<Vec<_>>();
    assert_eq!(
        creates,
        vec!["kubectl:create:SandboxTemplate:acme-platform-agent-red-runner"],
        "post-create history failure must stop after the single identifiable write: {events:?}"
    );
    let operation = recovery_operation_from_created(&cluster, "acme-platform-agent-red-runner");
    let value = stdout_json(&output);
    let guidance = format!(
        "{} {}",
        value
            .get("error")
            .and_then(|error| error.as_str())
            .unwrap_or_default(),
        value
            .get("fix")
            .and_then(|fix| fix.as_str())
            .unwrap_or_default()
    );
    assert!(
        guidance.contains("acme-platform-agent-red-runner")
            && guidance.contains("curietech.ai/github-app-recovery")
            && guidance.contains(&operation)
            && guidance.contains("helm history acme-platform -n acme-system"),
        "the failure must retain exact object, operation marker, and history inspection guidance: {value}"
    );
    assert!(
        !guidance.contains("helm rollback")
            && !events
                .iter()
                .any(|event| event.starts_with("helm:rollback:")),
        "post-create uncertainty must not advertise or perform rollback: {value}; {events:?}"
    );
    assert!(
        value.get("github_app_configured").is_none()
            && !events.iter().any(|event| event == "helm:upgrade")
            && !events
                .iter()
                .any(|event| event == "kubectl:rollout-restart"),
        "post-create history failure continued toward mutation, rollout, or success: {events:?}; {value}"
    );
}

#[test]
fn drift_immediately_after_create_never_proxies_deletes_or_continues_stale_recovery() {
    let cluster = FakeCluster::new("create-then-drift");
    let output = cluster.run("create-then-drift", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "revision drift immediately after create must return nonzero: {}",
        combined(&output)
    );
    let create = position(
        &events,
        "kubectl:create:SandboxTemplate:acme-platform-agent-red-runner",
    );
    let drift = position(&events, "helm:deployed:13");
    assert!(
        create < drift,
        "the fixture must advance Helm only after kubectl reports a successful create: {events:?}"
    );
    let creates = events
        .iter()
        .filter(|event| event.starts_with("kubectl:create:"))
        .map(String::as_str)
        .collect::<Vec<_>>();
    assert_eq!(
        creates,
        vec!["kubectl:create:SandboxTemplate:acme-platform-agent-red-runner"],
        "the command continued stale recovery writes after post-create drift: {events:?}"
    );
    recovery_operation_from_created(&cluster, "acme-platform-agent-red-runner");
    assert!(
        !events.iter().any(|event| event == "kubectl:proxy")
            && !cluster
                .raw()
                .iter()
                .any(|line| line.starts_with("kubectl proxy ")),
        "post-create drift must never start a Kubernetes API proxy: {events:?}"
    );
    assert!(
        !events.iter().any(|event| event == "kubectl:delete")
            && !cluster
                .raw()
                .iter()
                .any(|line| line.starts_with("kubectl delete ")),
        "post-create drift must preserve the identifiable object for operator recovery: {events:?}"
    );
    assert!(
        !events.iter().any(|event| event == "helm:upgrade")
            && !events
                .iter()
                .any(|event| event == "kubectl:rollout-restart"),
        "credential mutation or rollout continued after post-create drift: {events:?}"
    );
    let value = stdout_json(&output);
    let error = value
        .get("error")
        .and_then(|error| error.as_str())
        .unwrap_or_default();
    assert!(
        error.contains("12") && error.contains("13") && error.to_lowercase().contains("revision"),
        "the loud failure must identify the post-create revision drift: {value}"
    );
    assert!(
        value.get("github_app_configured").is_none()
            && !combined(&output).contains("GitHub App configured"),
        "post-create drift emitted success: {value}"
    );
}

#[test]
fn history_failure_immediately_after_helm_mutation_stops_without_rollback_guidance() {
    let cluster = FakeCluster::new("history-failure-after-helm");
    let output = cluster.run("history-failure-after-helm", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "an unreadable post-mutation revision must return nonzero"
    );
    let upgrade = position(&events, "helm:upgrade");
    let failed_history = events
        .iter()
        .rposition(|event| event == "helm:history")
        .unwrap_or_else(|| panic!("missing post-mutation history read: {events:?}"));
    assert!(
        upgrade < failed_history,
        "the fixture did not fail history after the credential mutation: {events:?}"
    );
    assert!(
        !events[upgrade + 1..].iter().any(|event| {
            event.starts_with("kubectl:create:")
                || event.starts_with("kubectl:apply:")
                || event == "kubectl:rollout-restart"
        }),
        "the command continued unsafe writes or rollout without a post-mutation revision: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| event.starts_with("helm:rollback:")),
        "post-mutation uncertainty must not trigger rollback: {events:?}"
    );
    let value = stdout_json(&output);
    assert_history_inspection_without_rollback(&value);
    assert!(
        value.get("github_app_configured").is_none()
            && !combined(&output).contains("GitHub App configured"),
        "post-mutation history failure emitted success: {value}"
    );
}

#[test]
fn post_upgrade_revision_interleaving_fails_without_rolling_back_stale_state() {
    let cluster = FakeCluster::new("post-upgrade-interleaving");
    let output = cluster.run("post-upgrade-interleaving", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "an intervening revision after the final preflight must not report success: {}",
        combined(&output)
    );
    assert!(
        events.iter().any(|event| event == "helm:upgrade"),
        "the scenario never reached the credential Helm attempt: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| *event == "helm:history")
            .count(),
        3,
        "snapshot, immediate pre-mutation recheck, and post-attempt read must all occur: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| event.starts_with("helm:rollback:")),
        "revision 12 is stale once the release reaches 14 and must never be rolled back automatically: {events:?}"
    );
    let drift = position(&events, "helm:deployed:14");
    assert!(
        !events[drift + 1..].iter().any(|event| {
            event.starts_with("kubectl:create:") || event.starts_with("kubectl:apply:")
        }),
        "no stale revision-12 sandbox object may be written after revision 14 is observed: {events:?}"
    );
    let value = stdout_json(&output);
    let error = value
        .get("error")
        .and_then(|error| error.as_str())
        .unwrap_or_default();
    assert!(
        error.contains("12") && error.contains("14"),
        "the failure must name captured revision 12 and observed revision 14: {value}"
    );
    assert!(
        value
            .get("fix")
            .and_then(|fix| fix.as_str())
            .is_some_and(|fix| fix.contains("inspect") && !fix.contains("helm rollback")),
        "interleaved state needs an inspection action, never a stale rollback command: {value}"
    );
    assert_no_success(&output, &events);
}

#[test]
fn post_upgrade_repair_refuses_to_create_when_the_release_drifts_at_its_write_fence() {
    let cluster = FakeCluster::new("post-missing-write-fence-drift");
    let output = cluster.run("post-missing-write-fence-drift", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "post-upgrade repair must not write revision-13 objects after revision 14 deploys: {}",
        combined(&output)
    );
    position(&events, "helm:deployed:14");
    assert!(
        !events.iter().any(|event| {
            event.starts_with("kubectl:create:") || event.starts_with("kubectl:apply:")
        }),
        "post-upgrade revision drift must prevent every stale sandbox write: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| event == "kubectl:rollout-restart"),
        "the API rollout started after the post-upgrade write fence failed: {events:?}"
    );
    let value = stdout_json(&output);
    let error = value
        .get("error")
        .and_then(|error| error.as_str())
        .unwrap_or_default();
    assert!(
        error.contains("13") && error.contains("14") && error.to_lowercase().contains("revision"),
        "the refusal must identify post-upgrade revision 13 and current revision 14: {value}"
    );
    assert!(
        value.get("github_app_configured").is_none(),
        "post-upgrade write-fence drift emitted success: {value}"
    );
}

#[test]
fn final_revision_drift_during_api_rollout_fails_without_stale_reconcile_or_rollback() {
    let cluster = FakeCluster::new("rollout-revision-drift");
    let output = cluster.run("rollout-revision-drift", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "release revision 14 appearing during API rollout must prevent success: {}",
        combined(&output)
    );
    let rollout = position(&events, "kubectl:rollout-restart");
    let drift = position(&events, "helm:deployed:14");
    assert!(
        rollout < drift,
        "the simulated revision drift must occur during the API rollout window: {events:?}"
    );
    assert!(
        !events[drift + 1..].iter().any(|event| {
            event.starts_with("kubectl:create:") || event.starts_with("kubectl:apply:")
        }),
        "captured revision-13 objects were written after current revision 14 was observed: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| event.starts_with("helm:rollback:")),
        "rollout-time drift must never roll back another actor's revision: {events:?}"
    );
    let value = stdout_json(&output);
    let error = value
        .get("error")
        .and_then(|error| error.as_str())
        .unwrap_or_default();
    assert!(
        error.contains("14") && error.to_lowercase().contains("revision"),
        "the failure must name the newly observed revision 14: {value}"
    );
    assert!(
        value
            .get("fix")
            .and_then(|fix| fix.as_str())
            .is_some_and(|fix| !fix.contains("helm rollback")),
        "rollout-time drift requires inspection, never stale rollback guidance: {value}"
    );
    assert!(
        value.get("github_app_configured").is_none()
            && !combined(&output).contains("GitHub App configured"),
        "rollout-time revision drift emitted success: {value}"
    );
}

#[test]
fn history_failure_after_completed_rollout_stops_without_rollback_or_false_success() {
    let cluster = FakeCluster::new("history-failure-after-rollout");
    let output = cluster.run("history-failure-after-rollout", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "an unreadable post-rollout revision must return nonzero"
    );
    let status = position(&events, "kubectl:rollout-status");
    let failed_history = events
        .iter()
        .rposition(|event| event == "helm:history")
        .unwrap_or_else(|| panic!("missing history read after rollout: {events:?}"));
    assert!(
        status < failed_history,
        "the fixture did not fail history after rollout status completed: {events:?}"
    );
    assert!(
        !events[status + 1..].iter().any(|event| {
            event.starts_with("kubectl:create:") || event.starts_with("kubectl:apply:")
        }),
        "the command wrote sandbox state after losing the post-rollout revision fence: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| event.starts_with("helm:rollback:")),
        "post-rollout uncertainty must not trigger rollback: {events:?}"
    );
    let value = stdout_json(&output);
    assert_history_inspection_without_rollback(&value);
    assert!(
        value.get("github_app_configured").is_none()
            && !combined(&output).contains("GitHub App configured"),
        "post-rollout history failure emitted false success: {value}"
    );
}

#[test]
fn a_divergent_live_object_is_refused_without_overwrite_or_credential_mutation() {
    let cluster = FakeCluster::new("divergent-live");
    let output = cluster.run("divergent-live", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "same-name live state with divergent desired content must fail closed"
    );
    assert!(
        !events.iter().any(|event| event == "helm:upgrade"),
        "a divergent live sandbox reached the credential mutation: {events:?}"
    );
    assert!(
        !events.iter().any(|event| {
            event.starts_with("kubectl:create:") || event.starts_with("kubectl:apply:")
        }),
        "the command overwrote a divergent live object: {events:?}"
    );
    let value = stdout_json(&output);
    assert!(
        value
            .get("error")
            .and_then(|value| value.as_str())
            .is_some_and(|error| error.contains("acme-platform-agent-red-runner")),
        "the refusal must name the divergent placeholder object: {value}"
    );
}

#[test]
fn a_live_object_missing_helm_ownership_is_refused_before_mutation_without_overwrite() {
    let cluster = FakeCluster::new("wrong-live-ownership");
    let output = cluster.run("wrong-live-ownership", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "an expected live sandbox without exact Helm ownership must fail closed"
    );
    assert!(
        !events.iter().any(|event| event == "helm:upgrade"),
        "invalid Helm ownership reached the credential mutation: {events:?}"
    );
    assert!(
        !events.iter().any(|event| {
            event.starts_with("kubectl:create:") || event.starts_with("kubectl:apply:")
        }),
        "invalid Helm ownership was overwritten during preflight: {events:?}"
    );
    let value = stdout_json(&output);
    assert!(
        value
            .get("error")
            .and_then(|value| value.as_str())
            .is_some_and(|error| {
                error.contains("acme-platform-agent-red-runner")
                    && error.to_lowercase().contains("ownership")
            }),
        "the refusal must name the object and explain its invalid ownership: {value}"
    );
}

#[test]
fn manifest_ownership_loss_restores_live_state_without_automatic_rollback_and_returns_recovery() {
    let cluster = FakeCluster::new("ownership-loss");
    let output = cluster.run("ownership-loss", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "dropping an active pair from Helm ownership must fail: {}",
        combined(&output)
    );
    let raw = cluster.raw();
    assert_revisioned_pre_manifest_read(&raw);
    assert!(
        !events
            .iter()
            .any(|event| event.starts_with("helm:rollback:")),
        "the command must not roll back release state another actor may have changed: {events:?}"
    );
    let post_manifest = position(&events, "helm:get-manifest:post");
    let template = position(
        &events,
        "kubectl:create:SandboxTemplate:acme-platform-agent-red-runner",
    );
    let pool = position(
        &events,
        "kubectl:create:SandboxWarmPool:acme-platform-agent-red-runner-pool",
    );
    let final_live = events
        .iter()
        .rposition(|event| event == "kubectl:get-sandboxes:post")
        .unwrap_or_else(|| panic!("missing restoration verification: {events:?}"));
    assert!(
        post_manifest < template && template < pool && pool < final_live,
        "lost ownership must restore the captured live objects template-before-pool and verify them: {events:?}"
    );
    let value = stdout_json(&output);
    let error = value
        .get("error")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_lowercase();
    assert!(
        error.contains("ownership") || error.contains("removed"),
        "the failure must explain the Helm ownership loss: {value}"
    );
    let recovery =
        format!("helm rollback {RELEASE} {REVISION} -n {NAMESPACE} --wait --timeout 180s");
    assert!(
        value
            .get("fix")
            .and_then(|fix| fix.as_str())
            .is_some_and(|fix| fix.contains(&recovery)),
        "the nonzero result must provide the exact operator-controlled rollback: {value}"
    );
    for name in [
        "acme-platform-agent-red-runner",
        "acme-platform-agent-red-runner-pool",
    ] {
        assert!(
            cluster.state().join(format!("live__{name}")).exists(),
            "live restoration did not recreate {name}"
        );
    }
    assert_recreated_as_helm_owned(
        &cluster,
        &[
            "acme-platform-agent-red-runner",
            "acme-platform-agent-red-runner-pool",
        ],
    );
    assert!(
        !error.contains("rolled back"),
        "the error falsely claims an automatic rollback occurred: {value}"
    );
    assert_no_success(&output, &events);
}

#[test]
fn restore_create_then_drift_retains_operation_marker_guidance_without_more_writes() {
    let cluster = FakeCluster::new("restore-create-then-drift");
    let output = cluster.run("restore-create-then-drift", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "revision drift during ownership-loss restoration must return nonzero"
    );
    let post_manifest = position(&events, "helm:get-manifest:post");
    let create = position(
        &events,
        "kubectl:create:SandboxTemplate:acme-platform-agent-red-runner",
    );
    let drift = position(&events, "helm:deployed:14");
    assert!(
        post_manifest < create && create < drift,
        "the fixture must drift only after the restore-path create succeeds: {events:?}"
    );
    let creates = events
        .iter()
        .filter(|event| event.starts_with("kubectl:create:"))
        .map(String::as_str)
        .collect::<Vec<_>>();
    assert_eq!(
        creates,
        vec!["kubectl:create:SandboxTemplate:acme-platform-agent-red-runner"],
        "restore-path drift must stop before a second stale sandbox write: {events:?}"
    );
    let operation = recovery_operation_from_created(&cluster, "acme-platform-agent-red-runner");
    let value = stdout_json(&output);
    let guidance = format!(
        "{} {}",
        value
            .get("error")
            .and_then(|error| error.as_str())
            .unwrap_or_default(),
        value
            .get("fix")
            .and_then(|fix| fix.as_str())
            .unwrap_or_default()
    );
    assert!(
        guidance.contains("acme-platform-agent-red-runner")
            && guidance.contains("curietech.ai/github-app-recovery")
            && guidance.contains(&operation)
            && guidance.contains("kubectl get sandboxtemplates.extensions.agents.x-k8s.io/acme-platform-agent-red-runner"),
        "restore-path drift must retain exact operation-marked object inspection guidance: {value}"
    );
    assert!(
        !guidance.contains("helm rollback")
            && !events
                .iter()
                .any(|event| event.starts_with("helm:rollback:")),
        "restore-path drift must not advertise or perform rollback: {value}; {events:?}"
    );
    assert!(
        value.get("github_app_configured").is_none()
            && !events
                .iter()
                .any(|event| event == "kubectl:rollout-restart")
            && !combined(&output).contains("GitHub App configured"),
        "restore-path drift continued to rollout or success: {events:?}; {value}"
    );
}

#[test]
fn a_live_only_missing_pair_is_recreated_template_before_pool_then_verified() {
    let cluster = FakeCluster::new("pre-missing");
    let output = cluster.run("pre-missing", false);
    let events = cluster.events();
    assert!(
        output.status.success(),
        "manifest-backed live repair must allow the connect to continue: {}; events: {events:?}",
        combined(&output)
    );
    let template = position(
        &events,
        "kubectl:create:SandboxTemplate:acme-platform-agent-red-runner",
    );
    let pool = position(
        &events,
        "kubectl:create:SandboxWarmPool:acme-platform-agent-red-runner-pool",
    );
    let upgrade = position(&events, "helm:upgrade");
    assert!(
        template < pool && pool < upgrade,
        "pre-existing loss must be repaired template-before-pool and verified before mutation: {events:?}"
    );
    assert!(
        events[..upgrade]
            .iter()
            .filter(|event| event.as_str() == "kubectl:get-sandboxes:pre")
            .count()
            >= 2,
        "the pre-mutation repair was not verified before Helm ran: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| event.starts_with("kubectl:apply:")),
        "recovery must create only absent objects, never apply over live objects: {events:?}"
    );
    let raw = cluster.raw();
    let sandbox_gets: Vec<&String> = raw
        .iter()
        .filter(|line| line.starts_with("kubectl get sandboxtemplates.extensions.agents.x-k8s.io,"))
        .collect();
    assert!(
        !sandbox_gets.is_empty()
            && sandbox_gets
                .iter()
                .all(|command| has_request_timeout(command)),
        "every sandbox list must have an explicit request timeout: {raw:?}"
    );
    let sandbox_creates: Vec<&String> = raw
        .iter()
        .filter(|line| line.starts_with("kubectl create "))
        .collect();
    assert!(
        !sandbox_creates.is_empty()
            && sandbox_creates
                .iter()
                .all(|command| has_request_timeout(command)),
        "every sandbox create must have an explicit request timeout: {raw:?}"
    );
}

#[test]
fn a_concurrent_creator_wins_the_race_and_a_relist_allows_recovery_to_continue() {
    let cluster = FakeCluster::new("concurrent-create");
    let output = cluster.run("concurrent-create", false);
    let events = cluster.events();
    assert!(
        output.status.success(),
        "create nonzero is recoverable when a relist proves the object now exists: {}; events: {events:?}",
        combined(&output)
    );
    let first_create = position(
        &events,
        "kubectl:create:SandboxTemplate:acme-platform-agent-red-runner",
    );
    let pool_create = position(
        &events,
        "kubectl:create:SandboxWarmPool:acme-platform-agent-red-runner-pool",
    );
    assert!(
        events[first_create + 1..pool_create]
            .iter()
            .any(|event| event == "kubectl:get-sandboxes:pre"),
        "a failed create was not relisted before continuing: {events:?}"
    );
}

#[test]
fn unreconciled_post_upgrade_loss_is_bounded_actionable_and_blocks_success() {
    let cluster = FakeCluster::new("unreconciled");
    let output = cluster.run("unreconciled", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "a create that never becomes observable must fail: {}",
        combined(&output)
    );
    let creates = events
        .iter()
        .filter(|event| event.starts_with("kubectl:create:"))
        .count();
    assert_eq!(
        creates, 6,
        "three exact recovery rounds over two absent objects must issue six creates: {events:?}"
    );
    let value = stdout_json(&output);
    let fix = value
        .get("fix")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    let recovery =
        format!("helm rollback {RELEASE} {REVISION} -n {NAMESPACE} --wait --timeout 180s");
    assert!(
        fix.contains(&recovery),
        "the bounded failure must carry the deterministic recovery command `{recovery}`: {value}"
    );
    let error = value
        .get("error")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    assert!(
        error.contains("acme-platform-agent-red-runner")
            || error.contains("acme-platform-agent-red-runner-pool"),
        "the failure must name an unreconciled placeholder object: {value}"
    );
    assert_no_success(&output, &events);
}

#[test]
fn a_kubectl_deadline_is_explicit_and_returns_an_actionable_nonzero_result() {
    let cluster = FakeCluster::new("kubectl-timeout");
    let output = cluster.run("kubectl-timeout", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "a sandbox read deadline must return nonzero rather than report success"
    );
    let raw = cluster.raw();
    let command = raw
        .iter()
        .find(|line| line.starts_with("kubectl get sandboxtemplates.extensions.agents.x-k8s.io,"))
        .unwrap_or_else(|| panic!("sandbox list was never attempted: {raw:?}"));
    assert!(
        has_request_timeout(command),
        "the kubectl request had no explicit request-timeout: {command}"
    );
    let value = stdout_json(&output);
    let error = value
        .get("error")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_lowercase();
    assert!(
        error.contains("timed out") || error.contains("deadline"),
        "the bounded wall-clock/request deadline must be visible: {value}"
    );
    assert_history_inspection_without_rollback(&value);
    assert_no_success(&output, &events);
}

#[test]
fn a_hung_kubectl_is_killed_by_the_wall_clock_deadline_and_returns_actionable_failure() {
    let cluster = FakeCluster::new("kubectl-hang");
    let started = Instant::now();
    let output = cluster.run("kubectl-hang", false);
    let elapsed = started.elapsed();
    let events = cluster.events();
    assert!(
        elapsed >= Duration::from_secs(6) && elapsed < Duration::from_secs(12),
        "the CLI must enforce its approximately seven-second process deadline; elapsed={elapsed:?}"
    );
    assert!(
        !output.status.success(),
        "a hung kubectl must return actionable nonzero"
    );
    let value = stdout_json(&output);
    assert!(
        value
            .get("error")
            .and_then(|error| error.as_str())
            .is_some_and(|error| error.contains("wall-clock") || error.contains("timed out")),
        "the outer process deadline must be visible in the error: {value}"
    );
    assert_history_inspection_without_rollback(&value);

    let pid: u32 = fs::read_to_string(cluster.state().join("hung-kubectl.pid"))
        .expect("hanging kubectl wrote its pid")
        .trim()
        .parse()
        .expect("kubectl pid is numeric");
    assert!(
        process_exited(pid),
        "timed-out kubectl child {pid} remained alive after the CLI returned"
    );
    assert_no_success(&output, &events);
}

#[test]
fn agent_sandbox_disabled_with_no_resources_preserves_the_sibling_credential_path() {
    let cluster = FakeCluster::new("sandbox-disabled");
    let output = cluster.run("sandbox-disabled", false);
    let events = cluster.events();
    assert!(
        output.status.success(),
        "agentSandbox.deploy=false has no sandbox set to preserve: {}; events: {events:?}",
        combined(&output)
    );
    assert!(
        events.iter().any(|event| event == "helm:get-values"),
        "the zero-resource path did not prove agentSandbox.deploy=false from Helm values: {events:?}"
    );
    assert!(
        position(&events, "helm:upgrade") < position(&events, "kubectl:rollout-restart"),
        "the disabled sibling path did not retain Helm-before-rollout ordering: {events:?}"
    );
    assert_eq!(
        stdout_json(&output),
        serde_json::json!({"github_app_configured": true})
    );
}

#[test]
fn active_connector_agent_missing_from_the_captured_manifest_refuses_before_mutation() {
    let cluster = FakeCluster::new("active-agent-omitted");
    let output = cluster.run("active-agent-omitted", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "an active connector agent absent from revision 12's manifest must fail"
    );
    assert!(
        events.iter().any(|event| event == "helm:get-values"),
        "active per-agent inventory was not derived independently from Helm values: {events:?}"
    );
    assert!(
        !events.iter().any(|event| event == "helm:upgrade"),
        "credential mutation ran without the active agent's sandbox pair: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| event.starts_with("helm:rollback:")),
        "a pre-mutation inventory refusal must not roll back unrelated state: {events:?}"
    );
    let value = stdout_json(&output);
    let error = value
        .get("error")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    assert!(
        error.contains("red")
            && (error.contains("acme-platform-agent-red-runner")
                || error.contains("SandboxTemplate/SandboxWarmPool")),
        "the refusal must name the missing placeholder agent or pair: {value}"
    );
    assert!(
        value.get("github_app_configured").is_none(),
        "the refused inventory emitted success JSON: {value}"
    );
}

#[test]
fn failed_helm_successor_restores_the_stable_active_revision_before_returning_error() {
    let cluster = FakeCluster::new("helm-failed-successor-with-loss");
    let output = cluster.run("helm-failed-successor-with-loss", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "a failed credential Helm revision must return nonzero"
    );
    let upgrade = position(&events, "helm:upgrade");
    let failed_head = events
        .iter()
        .enumerate()
        .skip(upgrade + 1)
        .find_map(|(index, event)| (event == "helm:head:13:failed").then_some(index))
        .unwrap_or_else(|| panic!("missing failed Helm successor after mutation: {events:?}"));
    let post_manifest = events
        .iter()
        .enumerate()
        .skip(failed_head + 1)
        .find_map(|(index, event)| (event == "helm:get-manifest:post").then_some(index))
        .unwrap_or_else(|| panic!("active revision manifest was not reread: {events:?}"));
    let template = position(
        &events,
        "kubectl:create:SandboxTemplate:acme-platform-agent-red-runner",
    );
    let pool = position(
        &events,
        "kubectl:create:SandboxWarmPool:acme-platform-agent-red-runner-pool",
    );
    let verified = events
        .iter()
        .rposition(|event| event == "kubectl:get-sandboxes:post")
        .unwrap_or_else(|| panic!("missing final failed-successor verification: {events:?}"));
    assert!(
        upgrade < failed_head
            && failed_head < post_manifest
            && post_manifest < template
            && template < pool
            && pool < verified,
        "stable failed successor recovery must reread active revision 12, restore template-before-pool, and verify: {events:?}"
    );
    assert!(
        events[upgrade + 1..=verified]
            .iter()
            .filter(|event| event.as_str() == "helm:head:13:failed")
            .count()
            >= 5,
        "failed head 13 was not repeatedly fenced as stable during recovery: {events:?}"
    );
    let revision_12_manifests = cluster
        .raw()
        .into_iter()
        .filter(|line| {
            line.starts_with(&format!("helm get manifest {RELEASE} "))
                && (line.contains("--revision 12") || line.contains("--revision=12"))
        })
        .count();
    assert!(
        revision_12_manifests >= 2,
        "recovery must use the immutable active revision 12 manifest before and after the failed successor"
    );
    assert_recreated_as_helm_owned(
        &cluster,
        &[
            "acme-platform-agent-red-runner",
            "acme-platform-agent-red-runner-pool",
        ],
    );
    recovery_operation_from_created(&cluster, "acme-platform-agent-red-runner");
    recovery_operation_from_created(&cluster, "acme-platform-agent-red-runner-pool");
    assert!(
        !events
            .iter()
            .any(|event| event.starts_with("helm:rollback:")),
        "the stable failed successor must not be rolled back automatically: {events:?}"
    );
    let value = stdout_json(&output);
    let error = value
        .get("error")
        .and_then(|error| error.as_str())
        .unwrap_or_default()
        .to_lowercase();
    assert!(
        error.contains("helm mutation failed")
            && error.contains("sandbox")
            && (error.contains("restored") || error.contains("reconciled")),
        "the typed failure must report failed Helm mutation and completed sandbox recovery: {value}"
    );
    let recovery =
        format!("helm rollback {RELEASE} {REVISION} -n {NAMESPACE} --wait --timeout 180s");
    assert!(
        value
            .get("fix")
            .and_then(|fix| fix.as_str())
            .is_some_and(|fix| fix.contains(&recovery)),
        "the stable failed successor must provide exact prior-revision rollback guidance: {value}"
    );
    assert!(
        value.get("github_app_configured").is_none()
            && !events
                .iter()
                .any(|event| event == "kubectl:rollout-restart")
            && !combined(&output).contains("GitHub App configured"),
        "failed Helm successor recovery rolled the API or emitted success: {events:?}; {value}"
    );
}

#[test]
fn helm_failure_output_cannot_echo_sensitive_stderr_into_the_json_error() {
    let cluster = FakeCluster::new("helm-sensitive-stderr");
    let output = cluster.run("helm-sensitive-stderr", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "the injected Helm failure must return nonzero"
    );
    let sentinel = sensitive_stderr_sentinel();
    assert!(
        !combined(&output).contains(&sentinel),
        "the CLI echoed sensitive-looking Helm stderr into its user-visible error"
    );
    let value = stdout_json(&output);
    assert!(
        value
            .get("error")
            .and_then(|value| value.as_str())
            .is_some_and(|error| error.contains("Helm mutation failed")),
        "the scrubbed error must still identify the failing operation: {value}"
    );
    assert_no_success(&output, &events);
}

#[test]
fn a_hung_helm_upgrade_times_out_restores_live_pairs_and_returns_recovery() {
    let cluster = FakeCluster::new("helm-upgrade-hang");
    let started = Instant::now();
    let output = cluster.run("helm-upgrade-hang", false);
    let elapsed = started.elapsed();
    let events = cluster.events();
    assert!(
        elapsed < Duration::from_secs(2),
        "debug timeout override must bound the hanging Helm child near 150ms; elapsed={elapsed:?}"
    );
    assert!(
        !output.status.success(),
        "a timed-out Helm upgrade must return nonzero"
    );
    let pid: u32 = fs::read_to_string(cluster.state().join("hung-helm.pid"))
        .expect("hanging Helm shim wrote its pid")
        .trim()
        .parse()
        .expect("Helm pid is numeric");
    assert!(
        process_exited(pid),
        "timed-out Helm child {pid} remained alive after the CLI returned"
    );
    assert!(
        events
            .iter()
            .filter(|event| event.as_str() == "helm:deployed:12")
            .count()
            >= 3
            && !events.iter().any(|event| event == "helm:deployed:13"),
        "post-timeout history must still identify captured revision 12: {events:?}"
    );
    let template = position(
        &events,
        "kubectl:create:SandboxTemplate:acme-platform-agent-red-runner",
    );
    let pool = position(
        &events,
        "kubectl:create:SandboxWarmPool:acme-platform-agent-red-runner-pool",
    );
    let verified = events
        .iter()
        .rposition(|event| event == "kubectl:get-sandboxes:post")
        .unwrap_or_else(|| panic!("missing post-timeout live verification: {events:?}"));
    assert!(
        position(&events, "helm:upgrade") < template && template < pool && pool < verified,
        "timeout recovery must restore template-before-pool and verify: {events:?}"
    );
    assert_recreated_as_helm_owned(
        &cluster,
        &[
            "acme-platform-agent-red-runner",
            "acme-platform-agent-red-runner-pool",
        ],
    );
    assert!(
        !events
            .iter()
            .any(|event| event.starts_with("helm:rollback:")),
        "the CLI must not automatically roll back an uncertain Helm attempt: {events:?}"
    );
    let value = stdout_json(&output);
    let recovery =
        format!("helm rollback {RELEASE} {REVISION} -n {NAMESPACE} --wait --timeout 180s");
    assert!(
        value
            .get("fix")
            .and_then(|fix| fix.as_str())
            .is_some_and(|fix| fix.contains(&recovery)),
        "the timed-out mutation must provide exact deterministic recovery: {value}"
    );
    let text = combined(&output);
    assert!(
        !text.contains("helm-hang-stdout-placeholder")
            && !text.contains("helm-hang-stderr-placeholder"),
        "raw Helm output escaped into the structured failure"
    );
    assert_no_success(&output, &events);
}

#[test]
fn an_empty_deployed_manifest_refuses_before_the_credential_mutation() {
    let cluster = FakeCluster::new("empty");
    let output = cluster.run("empty", false);
    let events = cluster.events();
    assert!(!output.status.success(), "empty expected set must fail");
    assert_no_success(&output, &events);
    assert!(
        !events.iter().any(|event| event == "helm:upgrade"),
        "an empty expected sandbox set reached the credential mutation: {events:?}"
    );
    let value = stdout_json(&output);
    assert!(
        value
            .get("fix")
            .and_then(|value| value.as_str())
            .is_some_and(|fix| fix.contains("helm get manifest")),
        "empty-manifest refusal must say how to inspect the release: {value}"
    );
}

#[test]
fn copy_paste_history_hint_shell_quotes_crafted_target_arguments() {
    let release = "acme release;false";
    let namespace = "acme-system;false";

    let history_cluster = FakeCluster::new("history-failure");
    let history_output = history_cluster.run_target("history-failure", false, release, namespace);
    assert!(!history_output.status.success());
    let history_fix = stdout_json(&history_output)
        .get("fix")
        .and_then(|fix| fix.as_str())
        .unwrap_or_default()
        .to_string();
    assert!(
        history_fix.contains("helm history 'acme release;false' -n 'acme-system;false' -o json"),
        "the history recovery hint is not safe to copy into a shell: {history_fix}"
    );
}

#[test]
fn copy_paste_manifest_hint_shell_quotes_crafted_target_arguments() {
    let release = "acme release;false";
    let namespace = "acme-system;false";
    let manifest_cluster = FakeCluster::new("empty");
    let manifest_output = manifest_cluster.run_target("empty", false, release, namespace);
    assert!(!manifest_output.status.success());
    let manifest_fix = stdout_json(&manifest_output)
        .get("fix")
        .and_then(|fix| fix.as_str())
        .unwrap_or_default()
        .to_string();
    assert!(
        manifest_fix.contains(
            "helm get manifest 'acme release;false' -n 'acme-system;false' --revision 12"
        ),
        "the manifest recovery hint is not safe to copy into a shell: {manifest_fix}"
    );
}

#[test]
fn an_incomplete_template_pool_reference_refuses_before_mutation() {
    let cluster = FakeCluster::new("incomplete");
    let output = cluster.run("incomplete", false);
    let events = cluster.events();
    assert!(
        !output.status.success(),
        "incomplete expected set must fail"
    );
    assert_no_success(&output, &events);
    assert!(
        !events.iter().any(|event| event == "helm:upgrade"),
        "an invalid template/pool pair reached the credential mutation: {events:?}"
    );
}

#[test]
fn disconnect_uses_the_same_sandbox_barrier_and_retains_its_sibling_output() {
    let cluster = FakeCluster::new("healthy");
    let output = cluster.run("healthy", true);
    let events = cluster.events();
    assert!(
        output.status.success(),
        "healthy disconnect must succeed through the same barrier: {}; events: {events:?}",
        combined(&output)
    );
    assert_eq!(
        stdout_json(&output),
        serde_json::json!({"github_app_configured": false})
    );
    assert!(
        position(&events, "helm:get-manifest:pre") < position(&events, "helm:upgrade")
            && position(&events, "helm:get-manifest:post")
                < position(&events, "kubectl:rollout-restart"),
        "disconnect bypassed the shared sandbox recovery barrier: {events:?}"
    );
}
