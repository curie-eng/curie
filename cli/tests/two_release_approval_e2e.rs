//! Issue #2307: exercise the real CLI and two release driver, substituting
//! external cluster tools and the API HTTP response boundary. The driver and
//! its embedded Python parsers execute unchanged. Workflow security and path
//! configuration remain declarative artifact checks (#2307, #3838).

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Output};

use serde_json::{json, Value};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn output_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn workflow() -> Value {
    let text =
        fs::read_to_string(repo_root().join(".github/workflows/two-release-approval-e2e.yaml"))
            .expect("read the two release workflow");
    serde_norway::from_str(&text).expect("parse the workflow configuration")
}

const FORBIDDEN_KIND_NAMES: &[&str] = &[
    "dark-factory",
    "curie-2589-resume",
    "curie-pilot3-verify",
    "curie-sre-demo",
    "curie-e2e",
];

// These tools substitute external processes and HTTP responses, never the
// driver functions or parsers. Unknown API requests fail instead of reaching
// another job's service at the driver's fixed ports.
const TOOL_FIXTURE: &str = r#"#!/usr/bin/python3
import io
import json
import os
import pathlib
import sys
import urllib.error
import urllib.parse
import urllib.request

tool = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]

def record(event):
    with open(os.environ["CURIE_TEST_TRACE"], "a") as trace:
        trace.write(json.dumps(event) + "\n")

def fixture_response(request, timeout):
    url = urllib.parse.urlparse(request.full_url)
    if url.hostname != "127.0.0.1" or url.port not in (18080, 18081):
        raise AssertionError(f"unexpected API address: {request.full_url}")
    owner = url.port == 18080
    path = url.path
    method = request.get_method()
    principal = request.get_header("X-curie-approval-principal")
    record({"tool": "api", "owner": owner, "path": path,
            "method": method, "principal": principal})
    if owner and path == "/approvals" and method == "POST":
        status = int(os.environ.get("CURIE_TEST_CREATE_STATUS", "201"))
        body = {"id": "approval-fixture", "status": "pending"}
    elif not owner and path == "/approvals/principals/operator" and method == "POST":
        status, body = 201, {"token": "fixture-principal"}
    elif path == "/approvals/approval-fixture" and method == "GET":
        status, body = (200, {"status": "pending"}) if owner else (404, {"detail": "approval not found"})
    elif path == "/approvals/approval-fixture/resolve" and method == "POST":
        if owner:
            status = int(os.environ.get("CURIE_TEST_RESOLVE_STATUS", "401"))
            body = {"status": os.environ.get("CURIE_TEST_RESOLVE_ROW", "approved")}
        else:
            if principal != "fixture-principal":
                raise AssertionError("consumer resolve must carry its operator principal")
            status, body = 404, {"detail": "approval not found"}
    else:
        raise AssertionError(f"unexpected API request: {method} {request.full_url}")
    response = io.BytesIO(json.dumps(body).encode())
    response.status = status
    if status >= 400:
        raise urllib.error.HTTPError(request.full_url, status, "fixture response", {}, response)
    return response

if tool == "python3":
    if not args or args[0] != "-":
        os.execv("/usr/bin/python3", ["python3", *args])
    program = sys.stdin.read()
    if len(args) > 1 and args[1].startswith("http://"):
        urllib.request.urlopen = fixture_response
    sys.argv = args
    exec(compile(program, "<driver stdin>", "exec"), {"__name__": "__main__"})
    raise SystemExit(0)

event = {"tool": tool, "args": args}
if tool == "helm" and args and args[0] == "install":
    event["values"] = [
        {"path": args[i + 1], "mode": pathlib.Path(args[i + 1]).stat().st_mode & 0o777,
         "body": pathlib.Path(args[i + 1]).read_text()}
        for i, arg in enumerate(args[:-1]) if arg == "-f"
    ]
record(event)

if tool == "kind" and args[:2] == ["get", "clusters"]:
    if os.environ.get("CURIE_TEST_EXISTING") == "1":
        print(os.environ["CURIE_TWO_RELEASE_KIND_CLUSTER"])
elif tool == "kubectl" and "get" in args and "nodes" in args:
    if os.environ.get("CURIE_TEST_OWNED") == "1":
        print("node/fixture-control-plane")
elif tool == "kubectl" and args[:2] == ["label", "nodes"]:
    if os.environ.get("CURIE_TEST_LABEL_FAIL") == "1":
        raise SystemExit(73)
elif tool == "helm" and args[:2] == ["install", "consumer2307"]:
    if os.environ.get("CURIE_TEST_INSTALL_FAIL") == "1":
        raise SystemExit(74)
"#;

struct Driver {
    dir: tempfile::TempDir,
}

impl Driver {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("create driver fixture directory");
        fs::create_dir(dir.path().join("tmp")).expect("create private temporary directory");
        for tool in ["kind", "kubectl", "helm", "curl", "python3"] {
            let path = dir.path().join(tool);
            fs::write(&path, TOOL_FIXTURE).expect("write external tool fixture");
            fs::set_permissions(path, fs::Permissions::from_mode(0o755))
                .expect("make external tool fixture executable");
        }
        Self { dir }
    }

    fn run(&self, phase: &str, overrides: &[(&str, &str)]) -> Output {
        let mut paths = vec![self.dir.path().to_path_buf()];
        paths.extend(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        ));
        let path = std::env::join_paths(paths).expect("join fixture tool PATH");
        Command::new(env!("CARGO_BIN_EXE_curie"))
            .args(["dev", "two-release-approval-e2e"])
            .current_dir(repo_root())
            .env("PATH", path)
            .env("TMPDIR", self.dir.path().join("tmp"))
            .env("CURIE_TEST_TRACE", self.dir.path().join("trace"))
            .env("GITHUB_OUTPUT", self.dir.path().join("output"))
            .env("GITHUB_STEP_SUMMARY", self.dir.path().join("summary"))
            .env("CURIE_TWO_RELEASE_PHASE", phase)
            .env(
                "CURIE_TWO_RELEASE_KIND_CLUSTER",
                "curie-fixture-two-release",
            )
            .env("CURIE_TWO_RELEASE_ALLOW_LIVE", "0")
            .env("CURIE_TWO_RELEASE_REQUIRED", "0")
            .env("CURIE_TWO_RELEASE_FORCE", "0")
            .env("CURIE_TWO_RELEASE_KEEP", "0")
            .env("CURIE_API_KEY", "fixture-api-key")
            .env("CURIE_API_IMAGE", "")
            .env("CURIE_DISPATCHER_IMAGE", "")
            .env("CI_SLACK_APP_TOKEN", "")
            .env("CI_SLACK_BOT_TOKEN", "")
            .env("CURIE_APPROVAL_PRINCIPAL", "")
            .envs(overrides.iter().copied())
            .output()
            .expect("run the real two release CLI command")
    }

    fn text(&self, name: &str) -> String {
        fs::read_to_string(self.dir.path().join(name)).unwrap_or_default()
    }

    fn events(&self, tool: &str) -> Vec<Value> {
        self.text("trace")
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("parse fixture trace event"))
            .filter(|event| event["tool"] == tool)
            .collect()
    }

    fn assert_temporary_values_removed(&self) {
        assert_eq!(
            fs::read_dir(self.dir.path().join("tmp"))
                .expect("list owned temporary directory")
                .count(),
            0,
            "the driver must clean its private values and kubeconfig files"
        );
    }
}

#[test]
fn cli_prereqs_keep_helm_ready_and_report_missing_live_credentials() {
    for required in ["0", "1"] {
        let driver = Driver::new();
        let output = driver.run("prereqs", &[("CURIE_TWO_RELEASE_REQUIRED", required)]);
        assert!(output.status.success(), "{}", output_text(&output));
        assert_eq!(
            driver.text("output"),
            "ready=true\nskip_reason=live envelope BLOCKED: missing dedicated CI Slack app\n"
        );
        let summary = driver.text("summary");
        assert!(summary.contains("BLOCKED"), "{summary}");
        assert!(summary.contains("CI_SLACK_APP_TOKEN"), "{summary}");
        assert!(summary.contains("CI_SLACK_BOT_TOKEN"), "{summary}");
        assert!(
            driver.text("trace").is_empty(),
            "prereqs must not touch external services"
        );
    }
}

#[test]
fn cli_prereqs_accept_dedicated_tokens_without_claiming_a_live_pass() {
    let driver = Driver::new();
    let output = driver.run(
        "prereqs",
        &[
            ("CI_SLACK_APP_TOKEN", "fixture-app-token"),
            ("CI_SLACK_BOT_TOKEN", "fixture-bot-token"),
        ],
    );
    assert!(output.status.success(), "{}", output_text(&output));
    assert_eq!(driver.text("output"), "ready=true\n");
    assert!(!driver.text("summary").contains("PASS"));
    assert!(driver.text("trace").is_empty());
}

#[test]
fn cli_run_requires_live_opt_in_before_any_external_action() {
    let driver = Driver::new();
    let output = driver.run("run", &[]);
    assert!(!output.status.success(), "{}", output_text(&output));
    assert!(output_text(&output).contains("CURIE_TWO_RELEASE_ALLOW_LIVE=1"));
    assert!(driver.text("trace").is_empty());
}

#[test]
fn cli_run_refuses_another_jobs_cluster_name_before_any_external_action() {
    for name in FORBIDDEN_KIND_NAMES {
        let driver = Driver::new();
        let output = driver.run(
            "run",
            &[
                ("CURIE_TWO_RELEASE_ALLOW_LIVE", "1"),
                ("CURIE_TWO_RELEASE_KIND_CLUSTER", name),
            ],
        );
        assert!(!output.status.success(), "{}", output_text(&output));
        assert!(output_text(&output).contains("refusing another job's kind cluster name"));
        assert!(driver.text("trace").is_empty());
    }
}

#[test]
fn force_recreate_refuses_a_cluster_without_the_job_ownership_label() {
    let driver = Driver::new();
    let output = driver.run(
        "run",
        &[
            ("CURIE_TWO_RELEASE_ALLOW_LIVE", "1"),
            ("CURIE_TWO_RELEASE_FORCE", "1"),
            ("CURIE_TEST_EXISTING", "1"),
        ],
    );
    assert!(!output.status.success(), "{}", output_text(&output));
    assert!(output_text(&output).contains("refusing to delete"));
    let commands = driver.events("kind");
    assert!(
        !commands
            .iter()
            .any(|event| matches!(event["args"][0].as_str(), Some("delete" | "create"))),
        "{commands:?}"
    );
    let kubectl = driver.events("kubectl");
    assert!(
        kubectl.iter().any(|event| event["args"]
            .as_array()
            .unwrap()
            .contains(&json!("curie.example.com/two-release-job=2307"))),
        "{kubectl:?}"
    );
    driver.assert_temporary_values_removed();
}

#[test]
fn labeling_failure_deletes_the_new_owned_cluster_even_after_force_recreate() {
    for existing in ["0", "1"] {
        let driver = Driver::new();
        let output = driver.run(
            "run",
            &[
                ("CURIE_TWO_RELEASE_ALLOW_LIVE", "1"),
                ("CURIE_TWO_RELEASE_FORCE", "1"),
                ("CURIE_TEST_EXISTING", existing),
                ("CURIE_TEST_OWNED", "1"),
                ("CURIE_TEST_LABEL_FAIL", "1"),
            ],
        );
        assert!(!output.status.success(), "{}", output_text(&output));
        let kind = driver.events("kind");
        let mutations: Vec<_> = kind
            .iter()
            .filter(|event| matches!(event["args"][0].as_str(), Some("delete" | "create")))
            .map(|event| event["args"][0].as_str().unwrap())
            .collect();
        let expected = if existing == "1" {
            vec!["delete", "create", "delete"]
        } else {
            vec!["create", "delete"]
        };
        assert_eq!(mutations, expected, "{kind:?}");
        assert!(driver.events("helm").is_empty());
        driver.assert_temporary_values_removed();
    }
}

#[test]
fn actual_helm_installs_use_consumer_overlay_and_private_credential_values() {
    let driver = Driver::new();
    let output = driver.run(
        "run",
        &[
            ("CURIE_TWO_RELEASE_ALLOW_LIVE", "1"),
            ("CURIE_TEST_INSTALL_FAIL", "1"),
        ],
    );
    assert!(
        !output.status.success(),
        "the installation failure must propagate"
    );
    let helm = driver.events("helm");
    let installs: Vec<_> = helm
        .iter()
        .filter(|event| event["args"][0] == "install")
        .collect();
    assert_eq!(installs.len(), 2, "{helm:?}");
    assert!(!installs[0]["args"]
        .as_array()
        .unwrap()
        .contains(&json!("--skip-crds")));
    assert!(installs[1]["args"]
        .as_array()
        .unwrap()
        .contains(&json!("--skip-crds")));
    assert!(installs[1]["args"].as_array().unwrap().iter().any(|arg| arg
        .as_str()
        .unwrap()
        .ends_with("values-e2e-two-release-consumer.yaml")));
    for install in installs {
        let args = install["args"].to_string();
        for forbidden in [
            "fixture-api-key",
            "xapp-fixture-2307",
            "xoxb-fixture-2307",
            "api.apiKey=",
            "approvalChatAttesterSecret=",
            "appToken=",
            "botToken=",
        ] {
            assert!(
                !args.contains(forbidden),
                "credential on Helm arguments: {args}"
            );
        }
        let values: Vec<Value> = install["values"]
            .as_array()
            .unwrap()
            .iter()
            .map(|file| serde_norway::from_str(file["body"].as_str().unwrap()).unwrap())
            .collect();
        let api = values
            .iter()
            .find(|value| value["api"]["apiKey"] == "fixture-api-key")
            .expect("install carries API credential values");
        assert_eq!(api["security"]["allowDevDefaults"], true);
        assert_ne!(
            api["api"]["approvalChatAttesterSecret"],
            api["api"]["apiKey"]
        );
        let slack = values
            .iter()
            .find(|value| value["dispatcher"]["slack"]["appToken"] == "xapp-fixture-2307")
            .expect("install carries Slack fixture values");
        assert_eq!(
            slack["dispatcher"]["slack"]["botToken"],
            "xoxb-fixture-2307"
        );
        let mut credential_mode_checks = 0;
        for (file, value) in install["values"].as_array().unwrap().iter().zip(&values) {
            if value["api"]["apiKey"] == "fixture-api-key"
                || value["dispatcher"]["slack"]["appToken"] == "xapp-fixture-2307"
            {
                let path = std::path::Path::new(file["path"].as_str().unwrap());
                assert!(
                    path.starts_with(driver.dir.path().join("tmp")),
                    "credential values must belong to the fixture temporary directory: {path:?}"
                );
                assert_eq!(file["mode"], 0o600, "{file}");
                credential_mode_checks += 1;
            }
        }
        assert_eq!(
            credential_mode_checks, 2,
            "both API and Slack credential files must have their private mode checked"
        );
    }
    assert!(!output_text(&output).contains("fixture-api-key"));
    assert!(driver.events("api").is_empty());
    driver.assert_temporary_values_removed();
}

#[test]
fn successful_create_and_single_resolves_emit_honest_summary_and_clean_up() {
    for (status, row) in [
        ("200", "PASS"),
        ("201", "PASS"),
        ("401", "BLOCKED"),
        ("403", "BLOCKED"),
    ] {
        let driver = Driver::new();
        let output = driver.run(
            "run",
            &[
                ("CURIE_TWO_RELEASE_ALLOW_LIVE", "1"),
                ("CURIE_TEST_RESOLVE_STATUS", status),
            ],
        );
        assert!(output.status.success(), "{}", output_text(&output));
        let api = driver.events("api");
        assert_eq!(
            api.iter()
                .filter(|event| event["path"] == "/approvals" && event["method"] == "POST")
                .count(),
            1,
            "{api:?}"
        );
        let resolves: Vec<_> = api
            .iter()
            .filter(|event| event["path"] == "/approvals/approval-fixture/resolve")
            .collect();
        assert_eq!(resolves.len(), 2, "{api:?}");
        assert_eq!(resolves[0]["owner"], false);
        assert_eq!(resolves[0]["principal"], "fixture-principal");
        assert_eq!(resolves[1]["owner"], true);
        assert!(resolves.iter().all(|event| event["method"] == "POST"));
        let summary = driver.text("summary");
        for required in ["helm two-release", "API isolation: PASS", "API isolation one-shot B (404 approval not found): PASS", "API isolation one-shot A (still pending): PASS", "B one-shot resolve miss (404 approval not found, operator principal, owner row still pending): PASS", "deployed dispatcher envelope ownership: BLOCKED", "live Slack owner-only envelope: BLOCKED"] {
            assert!(summary.contains(required), "{summary}");
        }
        assert!(
            summary.contains(&format!("A resolve-once: {row}")),
            "{summary}"
        );
        assert!(
            !summary.contains("dispatcher envelope ownership: PASS"),
            "{summary}"
        );
        let kubectl = driver.events("kubectl");
        for deployment in [
            "deploy/owner2307-curie-dispatcher",
            "deploy/consumer2307-curie-dispatcher",
        ] {
            assert!(
                kubectl.iter().any(|event| event["args"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(deployment))
                    && event["args"]
                        .as_array()
                        .unwrap()
                        .contains(&json!("rollout"))),
                "{kubectl:?}"
            );
        }
        assert_eq!(
            driver
                .events("kind")
                .iter()
                .filter(|event| event["args"][0] == "delete")
                .count(),
            1
        );
        assert_eq!(
            driver
                .events("helm")
                .iter()
                .filter(|event| event["args"][0] == "uninstall")
                .count(),
            2
        );
        driver.assert_temporary_values_removed();
    }
}

#[test]
fn failed_create_or_invalid_owner_response_cannot_emit_a_pass_summary() {
    for overrides in [
        vec![("CURIE_TEST_CREATE_STATUS", "500")],
        vec![("CURIE_TEST_RESOLVE_STATUS", "500")],
        vec![
            ("CURIE_TEST_RESOLVE_STATUS", "200"),
            ("CURIE_TEST_RESOLVE_ROW", "pending"),
        ],
    ] {
        let driver = Driver::new();
        let mut env = vec![("CURIE_TWO_RELEASE_ALLOW_LIVE", "1")];
        env.extend(overrides);
        let output = driver.run("run", &env);
        assert!(!output.status.success(), "{}", output_text(&output));
        assert!(driver.text("summary").is_empty());
        assert_eq!(
            driver
                .events("kind")
                .iter()
                .filter(|event| event["args"][0] == "delete")
                .count(),
            1
        );
        driver.assert_temporary_values_removed();
    }
}

#[test]
fn workflow_declares_least_privilege_contents_read_permissions() {
    assert_eq!(
        workflow()["permissions"],
        json!({"contents": "read"}),
        "#2307 requires explicit read only repository permissions"
    );
}

#[test]
fn workflow_pairs_every_checkout_with_persist_credentials_false() {
    let workflow = workflow();
    let mut checkouts = 0;
    for job in workflow["jobs"]
        .as_object()
        .expect("workflow defines jobs")
        .values()
    {
        for step in job["steps"].as_array().expect("job defines steps") {
            if step["uses"]
                .as_str()
                .is_some_and(|action| action.starts_with("actions/checkout@"))
            {
                checkouts += 1;
                assert_eq!(
                    step["with"]["persist-credentials"], false,
                    "#2307 checkout credentials must not persist: {step}"
                );
            }
        }
    }
    assert!(
        checkouts > 0,
        "checkout security coverage must not be vacuous"
    );
}

#[test]
fn workflow_passes_secrets_through_env_instead_of_run_commands() {
    let workflow = workflow();
    for job in workflow["jobs"]
        .as_object()
        .expect("workflow defines jobs")
        .values()
    {
        for step in job["steps"].as_array().expect("job defines steps") {
            if let Some(run) = step["run"].as_str() {
                assert!(
                    !run.contains("secrets."),
                    "#2307 secrets must not appear in script command text: {run}"
                );
            }
        }
    }
}
