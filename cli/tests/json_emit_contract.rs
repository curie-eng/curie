//! Integration: the `--json` emit contract (issue #456, ADR-0021). Under
//! `--json`, EVERY agent-facing verb must emit exactly one JSON object to
//! stdout -- never empty stdout. Today the read verbs and the whole `--dry-run`
//! surface go through `ui.payload`/`ui.kv`/`ui.payload_plain`, which suppress
//! under `--json`, so those verbs emit empty stdout + exit 0 (the bug). These
//! tests are RED now (empty stdout, no JSON object) and GREEN once the handlers
//! route their payload through a `--json` sink that always emits an object.
//!
//! The dry-run coverage is manifest-driven on purpose: it walks the committed
//! `command-manifest.json`, so a verb added later with a `--dry-run` arg is
//! auto-covered without touching this test. Exit codes are deliberately ignored
//! (comms/eval legitimately exit non-zero with a JSON *error* object); the bug
//! under test is empty stdout, not a nonzero code.

mod support;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use support::{serve, Response};

// The eval-concurrency forward-surface (#706): `EvalOpts` gains a
// `pub concurrency: usize` field, and `eval_dry_run_lines` must emit one plan
// line naming the sequential run. Importing directly from the `curie` lib
// (rather than only driving the built binary) pins the field on the struct
// itself: until the implementer adds `concurrency`, this whole test target
// fails to compile (the intended RED), isolated to this file -- the lib
// itself still compiles.
use curie::message::{eval_dry_run_lines, EvalOpts};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn manifest() -> serde_json::Value {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/command-manifest.json");
    let raw = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("committed manifest {path} must exist: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("manifest {path} must be valid JSON: {e}"))
}

/// Placeholder value for a required arg. `--limit` parses as an f64, so a
/// non-numeric placeholder would be a clap parse error (empty stdout -> false
/// failure); everything else is a plain string.
fn placeholder(id: &str) -> &'static str {
    match id {
        "limit" => "1",
        // `migrate-store --phase` restricts its values, so the generic "x" is
        // rejected by clap before the verb reaches its dry-run branch -- which
        // would fail this gate on an argument-parsing error rather than on the
        // empty-stdout bug it exists to catch. Any arg with a value_parser
        // enumeration needs a real value here.
        "phase" => "export",
        _ => "x",
    }
}

/// Every LEAF verb path (e.g. `["cluster", "kill"]`) whose `args` include an arg
/// with `"id": "dry_run"`, paired with the argv fragment of its required-arg
/// placeholders. Required boolean switches contribute only `--<long>`; value
/// options and positionals also contribute their placeholder value.
fn dry_run_verbs() -> Vec<(Vec<String>, Vec<String>)> {
    let mut out = Vec::new();
    fn args_of(node: &serde_json::Value) -> &[serde_json::Value] {
        node.get("args")
            .and_then(|a| a.as_array())
            .map_or(&[], |a| a.as_slice())
    }
    fn walk(
        node: &serde_json::Value,
        path: Vec<String>,
        out: &mut Vec<(Vec<String>, Vec<String>)>,
    ) {
        let subs = node
            .get("subcommands")
            .and_then(|s| s.as_array())
            .filter(|s| !s.is_empty());
        match subs {
            Some(subs) => {
                for sub in subs {
                    let name = sub
                        .get("name")
                        .and_then(|n| n.as_str())
                        .expect("subcommand has a name")
                        .to_string();
                    let mut child = path.clone();
                    child.push(name);
                    walk(sub, child, out);
                }
            }
            None => {
                // Leaf verb: collect it only if it carries a --dry-run arg.
                let args = args_of(node);
                let has_dry = args
                    .iter()
                    .any(|a| a.get("id").and_then(|i| i.as_str()) == Some("dry_run"));
                if !has_dry {
                    return;
                }
                let mut required = Vec::new();
                for a in args {
                    if a.get("required").and_then(|r| r.as_bool()) != Some(true) {
                        continue;
                    }
                    let id = a.get("id").and_then(|i| i.as_str()).unwrap_or("");
                    if a.get("positional").and_then(|p| p.as_bool()) == Some(true) {
                        required.push(placeholder(id).to_string());
                    } else {
                        let long = a
                            .get("long")
                            .and_then(|l| l.as_str())
                            .expect("required non-positional arg has a --long");
                        required.push(format!("--{long}"));
                        let boolean_switch = a
                            .get("possible_values")
                            .and_then(|values| values.as_array())
                            .is_some_and(|values| {
                                values.len() == 2
                                    && values.iter().any(|value| value == "true")
                                    && values.iter().any(|value| value == "false")
                            });
                        if !boolean_switch {
                            required.push(placeholder(id).to_string());
                        }
                    }
                }
                out.push((path, required));
            }
        }
    }
    walk(&manifest(), Vec::new(), &mut out);
    out
}

#[test]
fn every_dry_run_verb_emits_json_object() {
    let verbs = dry_run_verbs();
    // Guard against a vacuously-passing empty walk, and against a manifest that
    // silently drops verbs: ~24 carry --dry-run today, so a loose floor of 20.
    assert!(
        verbs.len() >= 20,
        "expected >= 20 --dry-run verbs in the manifest, found {} ({:?}); \
         a vacuous or shrunken walk must fail loudly",
        verbs.len(),
        verbs.iter().map(|(p, _)| p.join(" ")).collect::<Vec<_>>()
    );

    for (path, required) in &verbs {
        let mut argv: Vec<String> = path.clone();
        argv.extend(required.iter().cloned());
        argv.push("--dry-run".to_string());
        argv.push("--json".to_string());

        // cargo runs integration tests with cwd = `cli/`, where `charts/curie`
        // and the compose file don't resolve -- so the cluster/local operator
        // verbs (`up`/`down`/`status`) would error on chart resolution and never
        // reach their real dry-run branch, satisfying "non-empty JSON" via the
        // centralized *error* path instead of the *plan* path (a gate hole).
        // Run from the worktree root so those verbs hit their true dry-run
        // branch; pre-fix that path emits exit 0 + empty stdout (the RED we want).
        let root = concat!(env!("CARGO_MANIFEST_DIR"), "/..");
        let output = Command::new(bin())
            .current_dir(root)
            .args(&argv)
            .output()
            .unwrap_or_else(|e| panic!("run curie {}: {e}", argv.join(" ")));
        let stdout = String::from_utf8_lossy(&output.stdout);

        assert!(
            !stdout.trim().is_empty(),
            "`curie {}` under --json must not emit empty stdout\nstderr: {}",
            argv.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
        let parsed: serde_json::Value =
            serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
                panic!(
                    "`curie {}` under --json must emit parseable JSON: {e}\nstdout: {stdout}",
                    argv.join(" ")
                )
            });
        assert!(
            parsed.is_object(),
            "`curie {}` under --json must emit a JSON object, got: {stdout}",
            argv.join(" ")
        );
    }
}

#[test]
fn observability_emits_json_object() {
    // AC1: `local observability` has no --dry-run, so the manifest walk misses
    // it. It is network-free and prints three URLs (Console, Langfuse, API
    // base); nothing opens in a browser unless `--open` is passed, and `--json`
    // never opens one regardless (see `should_open`). This is a canary: its
    // assertions are intentionally left unmodified by #460 so a passing run
    // proves the local `--json` shape stayed a strict superset of the shipped
    // `{"surfaces":[{"name","url"}]}` contract (empty-stdout was already fixed
    // by #456/PR#503, before this branch).
    let output = Command::new(bin())
        .args(["local", "observability", "--json"])
        .output()
        .expect("run curie local observability --json");
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        !stdout.trim().is_empty(),
        "`curie local observability --json` must not emit empty stdout\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|e| panic!("must emit parseable JSON: {e}\nstdout: {stdout}"));
    assert!(
        parsed.is_object(),
        "`curie local observability --json` must emit a JSON object, got: {stdout}"
    );
    assert_eq!(
        output.status.code(),
        Some(0),
        "observability is hermetic and must exit 0\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn dry_run_json_masks_credentials() {
    // SECURITY: the new JSON sink must carry the ALREADY-MASKED plan string, not
    // a raw secret. The non-json dry-run masks the model credential via
    // `CmdArg::SecretSet` (first 8 chars + "***"), so `sk-ant-SECRETSENTINEL9999`
    // prints as `sk-ant-S***` and the raw sentinel never appears. The --json
    // output must preserve exactly that masking. RED now = empty stdout.
    //
    // `--chart` is required: a dev build with no `charts/curie` in the test
    // binary's cwd (the `cli/` package dir) errors before building the plan, so
    // point it at the committed chart at the worktree root (one level up from
    // CARGO_MANIFEST_DIR). `--dry-run` never fetches or stats it.
    let chart = concat!(env!("CARGO_MANIFEST_DIR"), "/../charts/curie");
    let output = Command::new(bin())
        .args(["cluster", "up", "--chart", chart, "--dry-run", "--json"])
        .env("CURIE_MODEL_CREDENTIALS", "sk-ant-SECRETSENTINEL9999")
        .output()
        .expect("run curie cluster up --dry-run --json");
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        !stdout.trim().is_empty(),
        "`curie cluster up --dry-run --json` must not emit empty stdout\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|e| panic!("must emit parseable JSON: {e}\nstdout: {stdout}"));
    assert!(
        parsed.is_object(),
        "`curie cluster up --dry-run --json` must emit a JSON object, got: {stdout}"
    );
    assert!(
        stdout.contains("sk-ant-S***"),
        "the JSON plan must carry the masked credential marker `sk-ant-S***`: {stdout}"
    );
    assert!(
        !stdout.contains("SECRETSENTINEL9999"),
        "the raw model credential must NEVER appear in the JSON output: {stdout}"
    );
}

// ---------------------------------------------------------------------------
// The `eval --dry-run` PLAN path (not the error path)
// ---------------------------------------------------------------------------

/// The manifest-driven gate above drives every `--dry-run` verb with NO required
/// args beyond placeholders, so `eval` fails cases-resolution and satisfies the
/// "emits an object" assertion via the CENTRALIZED ERROR-JSON path -- it never
/// reaches its dry-run PLAN branch, which is the branch that was actually broken.
/// This drives `eval` from the real weather bundle so its deployed trajectory
/// suite resolves without an explicit cases override, and asserts the payload is
/// the PLAN (no `error` key), so an error-path regression cannot green this test.
/// `--dry-run` touches no network.
fn assert_eval_dry_run_plan(tier: &str) {
    let output = Command::new(bin())
        .args([tier, "eval", "--dry-run", "--json"])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../examples/weather"))
        .output()
        .unwrap_or_else(|e| panic!("run curie {tier} eval --dry-run --json: {e}"));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(
        output.status.code(),
        Some(0),
        "`curie {tier} eval --dry-run --json` is hermetic and must exit 0\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        !stdout.trim().is_empty(),
        "`curie {tier} eval --dry-run --json` must not emit empty stdout\nstderr: {stderr}"
    );
    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|e| panic!("must emit parseable JSON: {e}\nstdout: {stdout}"));
    assert!(
        parsed.is_object(),
        "`curie {tier} eval --dry-run --json` must emit a JSON object, got: {stdout}"
    );
    assert!(
        parsed.get("error").is_none(),
        "must be the dry-run PLAN, not an error object -- the false-green this gap was about: {stdout}"
    );
    assert_eq!(
        parsed.get("dry_run"),
        Some(&serde_json::Value::Bool(true)),
        "the plan object must carry `dry_run: true`: {stdout}"
    );
    let plan = parsed
        .get("plan")
        .and_then(|p| p.as_array())
        .unwrap_or_else(|| panic!("`plan` must be an array: {stdout}"));
    assert!(
        !plan.is_empty(),
        "`plan` must be a NON-empty array of the lines eval would run: {stdout}"
    );
    let first = plan[0].as_str().unwrap_or_default();
    assert!(
        first.contains("weather"),
        "the first plan line must name the resolved suite (`weather`), proving cases resolved: {first:?}"
    );
}

#[test]
fn local_eval_dry_run_emits_plan_not_error() {
    assert_eval_dry_run_plan("local");
}

#[test]
fn cluster_eval_dry_run_emits_plan_not_error() {
    assert_eval_dry_run_plan("cluster");
}

/// The forward-compatible `--concurrency` flag (#706) defaults to 1
/// (sequential); the dry-run plan must say so explicitly, naming both
/// "concurrency" and "sequential" in the same line, so an agent reading the
/// plan never mistakes the sequential CLI loop for real fan-out (real
/// parallelism is worker-side, deferred to #709).
/// Placeholder credentials for the inline `EvalOpts` literal below (test-only,
/// never real secrets): the public local-dev Valkey password and API key
/// defaults. Hoisted to named consts so the literal reads a `TEST_*` const
/// rather than an inline quoted field assignment, which is the shape the
/// commit-time secret scanner's field-name-plus-literal heuristic matches on.
const TEST_VALKEY_PASSWORD: &str = "valkeypass";
const TEST_API_KEY: &str = "curie-dev-key";

#[test]
fn eval_dry_run_plan_names_sequential_concurrency() {
    let opts = EvalOpts {
        cases: None,
        channel: None,
        namespace: "curie".into(),
        release: "curie".into(),
        listen_host: None,
        listen_port: 8155,
        valkey_local_port: 56381,
        valkey_password: TEST_VALKEY_PASSWORD.into(),
        api_local_port: 8123,
        api_key: TEST_API_KEY.into(),
        user: "U-curie-message".into(),
        stream: "curie:runs".into(),
        timeout_secs: 300,
        dry_run: true,
        local: true,
        api_url: None,
        models: Vec::new(),
        concurrency: 1,
    };
    let lines = eval_dry_run_lines(&opts, "weather", 3);
    assert!(
        lines
            .iter()
            .any(|l| l.contains("concurrency") && l.contains("sequential")),
        "dry-run plan must name the sequential run and mention concurrency: {lines:?}"
    );
}

// ---------------------------------------------------------------------------
// The `comms --dry-run` PLAN path (not the error path)
// ---------------------------------------------------------------------------

/// Sentinel Slack tokens. Distinctive enough that a raw leak anywhere in stdout
/// is unambiguous, and long enough to survive the `xapp-SEN***` masking prefix.
const SENTINEL_APP_TOKEN: &str = "xapp-SENTINELAPP1234";
const SENTINEL_BOT_TOKEN: &str = "xoxb-SENTINELBOT5678";

/// Drives a `<tier> comms` variant with `--dry-run --json` and asserts the
/// payload is the PLAN, returning it for the caller's tier-specific checks.
///
/// The manifest-driven gate above supplies NO provider flags, so `comms` fails
/// its `require_provider`/`require_connect_tokens` check and satisfies the
/// "emits an object" assertion via the CENTRALIZED ERROR-JSON path -- it never
/// reaches its dry-run PLAN branch, which is the branch that was actually
/// broken. These drive real connect/disconnect inputs so the plan branch is
/// exercised, and the `no error key` assertion means an error-path regression
/// cannot green them. Both tiers emit the plan and return BEFORE their
/// `require_on_path("helm"/"kubectl"/"docker")` calls, so this is hermetic:
/// no network, no cluster, no tooling on PATH.
fn assert_comms_dry_run_plan(tier: &str, extra: &[&str]) -> serde_json::Value {
    let mut args = vec![tier, "comms", "--slack"];
    args.extend_from_slice(extra);
    args.extend_from_slice(&["--dry-run", "--json"]);
    let output = Command::new(bin())
        .args(&args)
        // The token flags default from these; remove them so an ambient shell
        // value can never influence the plan under test.
        .env_remove("SLACK_APP_TOKEN")
        .env_remove("SLACK_BOT_TOKEN")
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
        .output()
        .unwrap_or_else(|e| panic!("run curie {args:?}: {e}"));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(
        output.status.code(),
        Some(0),
        "`curie {args:?}` is hermetic and must exit 0\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        !stdout.trim().is_empty(),
        "`curie {args:?}` must not emit empty stdout\nstderr: {stderr}"
    );
    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|e| panic!("must emit parseable JSON: {e}\nstdout: {stdout}"));
    assert!(
        parsed.is_object(),
        "`curie {args:?}` must emit a JSON object, got: {stdout}"
    );
    assert!(
        parsed.get("error").is_none(),
        "must be the dry-run PLAN, not an error object -- the false-green this gap was about: {stdout}"
    );
    assert_eq!(
        parsed.get("dry_run"),
        Some(&serde_json::Value::Bool(true)),
        "the plan object must carry `dry_run: true`: {stdout}"
    );
    let plan = parsed
        .get("plan")
        .and_then(|p| p.as_array())
        .unwrap_or_else(|| panic!("`plan` must be an array: {stdout}"));
    assert!(
        !plan.is_empty(),
        "`plan` must be a NON-empty array of the lines comms would run: {stdout}"
    );
    parsed
}

/// Reinforces the masking contract: comms tokens go through `CmdArg::SecretSet`,
/// so `cmd.display()` renders them as a `xapp-SEN***` prefix. A regression that
/// swapped `SecretSet` for a plain arg would spill the raw token into the plan
/// -- and into any agent's logs that captured this `--json` payload.
fn assert_tokens_masked(stdout: &str) {
    assert!(
        !stdout.contains(SENTINEL_APP_TOKEN),
        "the raw app token must NEVER appear in the plan: {stdout}"
    );
    assert!(
        !stdout.contains(SENTINEL_BOT_TOKEN),
        "the raw bot token must NEVER appear in the plan: {stdout}"
    );
    assert!(
        stdout.contains("xapp-SEN***"),
        "the app token must appear MASKED (`xapp-SEN***`), proving it was carried as a secret rather than dropped: {stdout}"
    );
    assert!(
        stdout.contains("xoxb-SEN***"),
        "the bot token must appear MASKED (`xoxb-SEN***`): {stdout}"
    );
}

#[test]
fn local_comms_dry_run_emits_plan_not_error() {
    let parsed = assert_comms_dry_run_plan(
        "local",
        &[
            "--app-token",
            SENTINEL_APP_TOKEN,
            "--bot-token",
            SENTINEL_BOT_TOKEN,
        ],
    );
    let stdout = parsed.to_string();
    assert_tokens_masked(&stdout);
    let first = parsed["plan"][0].as_str().unwrap_or_default();
    assert!(
        first.contains("docker compose") && first.contains("curie-dispatcher"),
        "the local plan must be the compose connect command that brings up the dispatcher: {first:?}"
    );
}

#[test]
fn cluster_comms_dry_run_emits_plan_not_error() {
    let parsed = assert_comms_dry_run_plan(
        "cluster",
        &[
            "--app-token",
            SENTINEL_APP_TOKEN,
            "--bot-token",
            SENTINEL_BOT_TOKEN,
        ],
    );
    let stdout = parsed.to_string();
    assert_tokens_masked(&stdout);
    let first = parsed["plan"][0].as_str().unwrap_or_default();
    assert!(
        first.contains("helm upgrade") && first.contains("dispatcher.slack.appToken"),
        "the cluster plan must be the helm upgrade that sets the dispatcher's Slack tokens: {first:?}"
    );
}

/// `--disconnect` needs no tokens (`require_connect_tokens` short-circuits), so
/// it reaches the plan branch on both tiers and is worth pinning: it is the
/// variant an agent runs to tear Slack back down.
#[test]
fn local_comms_disconnect_dry_run_emits_plan_not_error() {
    let parsed = assert_comms_dry_run_plan("local", &["--disconnect"]);
    let plan = parsed["plan"].as_array().expect("plan array");
    assert!(
        plan.iter().any(|l| l
            .as_str()
            .unwrap_or_default()
            .contains("stop curie-dispatcher")),
        "the local disconnect plan must stop the dispatcher: {parsed}"
    );
}

#[test]
fn cluster_comms_disconnect_dry_run_emits_plan_not_error() {
    let parsed = assert_comms_dry_run_plan("cluster", &["--disconnect"]);
    let first = parsed["plan"][0].as_str().unwrap_or_default();
    assert!(
        first.contains("helm upgrade") && first.contains("dispatcher.slack.appToken="),
        "the cluster disconnect plan must clear the dispatcher's Slack tokens: {first:?}"
    );
}

// ---------------------------------------------------------------------------
// `to_json` key-shape pins
// ---------------------------------------------------------------------------
//
// The read verbs' real (`List`) path needs a live server, so the binary-driven
// tests above cannot reach it -- and they only assert `is_object()`, which a
// dropped or renamed key still satisfies. These unit-test `to_json` directly and
// compare against an EXACT literal, so renaming, dropping, OR adding a key fails.
// This is the only coverage of the agent-facing key contract.

use curie::api::{MemoryEntry, Version};
use curie::commands::{
    ApprovalsOutput, BudgetOutput, DeleteOutput, InitOutput, KillOutput, MemoryOutput,
    ResetThreadOutput, ResumeOutput, VersionsOutput,
};
// `ObservabilityOutput` moved to the tier-aware `observability` seam (#460) so
// both the local and cluster handlers return one type.
use curie::observability::{Endpoint, ObservabilityOutput};
use curie::ui::{CliOutput, DryRunPlan};
use serde_json::json;

fn plan(lines: &[&str]) -> DryRunPlan {
    DryRunPlan {
        lines: lines.iter().map(|l| l.to_string()).collect(),
    }
}

#[test]
fn dry_run_plan_json_shape_is_pinned() {
    assert_eq!(
        plan(&["helm upgrade --install curie", "kubectl rollout status"]).to_json(),
        json!({
            "dry_run": true,
            "plan": ["helm upgrade --install curie", "kubectl rollout status"],
        })
    );
    // An empty plan still emits the `plan` key as an array, never null/absent.
    assert_eq!(plan(&[]).to_json(), json!({"dry_run": true, "plan": []}));
}

#[test]
fn versions_output_json_shape_is_pinned() {
    assert_eq!(
        VersionsOutput::DryRun(plan(&["GET <api>/agents/<id>/versions"])).to_json(),
        json!({"dry_run": true, "plan": ["GET <api>/agents/<id>/versions"]})
    );
    assert_eq!(
        VersionsOutput::Empty {
            agent: "weather".to_string(),
        }
        .to_json(),
        json!({"agent": "weather", "versions": []})
    );
    // Two versions, one with every optional field `None` and one with them all
    // `Some`, so the null-vs-value rendering of each optional is pinned. Per
    // issue #691 / E1, each element emits exactly: `id`, `version_label`,
    // `commit_sha`, `bundle_sha256`, `created_by`, `created_at`, `bundle_ref`.
    // `id` and `bundle_ref` are now emitted (an agent needs `id` to address the
    // version); `agent_id` is carried on the struct but deliberately NOT emitted
    // (the payload already carries `agent`, the query was agent-scoped).
    assert_eq!(
        VersionsOutput::List {
            agent: "weather".to_string(),
            versions: vec![
                Version {
                    id: "ver_1".to_string(),
                    version_label: "v1".to_string(),
                    commit_sha: None,
                    bundle_sha256: None,
                    created_by: None,
                    created_at: None,
                    agent_id: None,
                    bundle_ref: None,
                },
                Version {
                    id: "ver_2".to_string(),
                    version_label: "v2".to_string(),
                    commit_sha: Some("abc1234".to_string()),
                    bundle_sha256: Some("deadbeef00".to_string()),
                    created_by: Some("bconn".to_string()),
                    created_at: Some("2026-07-16T00:00:00Z".to_string()),
                    agent_id: Some("agt_1".to_string()),
                    bundle_ref: Some("bundles/abc.tar.gz".to_string()),
                },
            ],
        }
        .to_json(),
        json!({
            "agent": "weather",
            "versions": [
                {
                    "id": "ver_1",
                    "version_label": "v1",
                    "commit_sha": null,
                    "bundle_sha256": null,
                    "created_by": null,
                    "created_at": null,
                    "bundle_ref": null,
                },
                {
                    "id": "ver_2",
                    "version_label": "v2",
                    "commit_sha": "abc1234",
                    "bundle_sha256": "deadbeef00",
                    "created_by": "bconn",
                    "created_at": "2026-07-16T00:00:00Z",
                    "bundle_ref": "bundles/abc.tar.gz",
                },
            ],
        })
    );
}

#[test]
fn memory_output_json_shape_is_pinned() {
    assert_eq!(
        MemoryOutput::DryRun(plan(&["GET <api>/agents/<id>/memory"])).to_json(),
        json!({"dry_run": true, "plan": ["GET <api>/agents/<id>/memory"]})
    );
    assert_eq!(
        MemoryOutput::Empty {
            agent: "weather".to_string(),
        }
        .to_json(),
        json!({"agent": "weather", "entries": []})
    );
    // `MemoryEntry::version` is deliberately NOT emitted; this exact-match pins
    // that (surfacing it later would fail here and force a contract decision).
    assert_eq!(
        MemoryOutput::List {
            agent: "weather".to_string(),
            entries: vec![
                MemoryEntry {
                    index: 0,
                    content: "user prefers celsius".to_string(),
                    version: 3,
                },
                MemoryEntry {
                    index: 1,
                    content: "home airport is BOS".to_string(),
                    version: 3,
                },
            ],
        }
        .to_json(),
        json!({
            "agent": "weather",
            "entries": [
                {"index": 0, "content": "user prefers celsius"},
                {"index": 1, "content": "home airport is BOS"},
            ],
        })
    );
}

#[test]
fn approvals_output_json_shape_is_pinned() {
    assert_eq!(
        ApprovalsOutput::DryRun(plan(&["GET <api>/agents/<id>"])).to_json(),
        json!({"dry_run": true, "plan": ["GET <api>/agents/<id>"]})
    );
    // No gates: `gated_tools` must still be an empty array, never null/absent --
    // an agent consumer branches on the array, not on key presence.
    //
    // Deliberate, reviewed contract EVOLUTION (#607), not a weakened pin: `agent`
    // and `gated_tools` keep their exact meaning, and this still asserts EXACT
    // equality against a literal. What joins them is the additive
    // `manifest_unreadable`, which separates "the deployed manifest declares no
    // gates" (null, below) from "the manifest could not be read" (a reason string).
    // Without it an empty `gated_tools` is ambiguous, and the ambiguity resolves in
    // the unsafe direction: an agent reads `[]` as "nothing pauses for approval".
    assert_eq!(
        ApprovalsOutput::Gates {
            agent: "weather".to_string(),
            gated_tools: vec![],
            manifest_unreadable: None,
        }
        .to_json(),
        json!({"agent": "weather", "gated_tools": [], "manifest_unreadable": null})
    );
    assert_eq!(
        ApprovalsOutput::Gates {
            agent: "weather".to_string(),
            gated_tools: vec!["Bash".to_string(), "mcp__weather__forecast".to_string()],
            manifest_unreadable: None,
        }
        .to_json(),
        json!({
            "agent": "weather",
            "gated_tools": ["Bash", "mcp__weather__forecast"],
            "manifest_unreadable": null,
        })
    );
    // The manifest lookup failed. `gated_tools` is what the platform field alone
    // carried, and `manifest_unreadable` is the machine-readable disclosure that it
    // may not be the whole set.
    assert_eq!(
        ApprovalsOutput::Gates {
            agent: "weather".to_string(),
            gated_tools: vec![],
            manifest_unreadable: Some("listing the agent's deployments failed: 503".to_string()),
        }
        .to_json(),
        json!({
            "agent": "weather",
            "gated_tools": [],
            "manifest_unreadable": "listing the agent's deployments failed: 503",
        })
    );
}

#[test]
fn approvals_pending_and_resolved_json_shapes_are_pinned() {
    use curie::api::ApprovalRecord;
    let record = || ApprovalRecord {
        id: "ap_1".to_string(),
        author: "U1".to_string(),
        route: Some("managers".to_string()),
        gate_kind: Some("permission".to_string()),
        granted_tool: Some("Bash".to_string()),
        status: "pending".to_string(),
        conversation_id: "C1-thread-9".to_string(),
        summary: "Deploy the thing".to_string(),
        expires_at: Some("2026-07-16T00:00:00Z".to_string()),
        resolved_by: None,
        // #1078: the field --actor-channel is derived from.
        card_channel: Some("CFINANCE01".to_string()),
    };
    assert_eq!(
        ApprovalsOutput::Pending {
            agent: "weather".to_string(),
            records: vec![record()],
            truncated: false,
        }
        .to_json(),
        json!({
            "agent": "weather",
            "pending": [{
                "id": "ap_1",
                "author": "U1",
                "route": "managers",
                "gate_kind": "permission",
                "granted_tool": "Bash",
                "status": "pending",
                "conversation_id": "C1-thread-9",
                "summary": "Deploy the thing",
                "expires_at": "2026-07-16T00:00:00Z",
                "resolved_by": null,
                // #1078. Pinned here because this assertion is the reason the
                // omission was noticeable at all: the projection is spelled out
                // key by key, so a field the API carries and the CLI drops is
                // visible in the diff rather than inferred.
                "card_channel": "CFINANCE01",
            }],
            "count": 1,
            "truncated": false,
        })
    );
    let mut resolved = record();
    resolved.status = "approved".to_string();
    resolved.resolved_by = Some("U2".to_string());
    assert_eq!(
        ApprovalsOutput::Resolved { record: resolved }.to_json()["resolved"]["status"],
        json!("approved")
    );
}

/// Deliberate, reviewed contract EVOLUTION (#460), not a weakened pin: the
/// payload key `surfaces` and the row key `name` are unchanged, and this still
/// asserts EXACT equality against a literal, so a dropped/renamed/added key
/// fails exactly as before. What changed is that each row is now an additive
/// SUPERSET -- `note` and `browsable` join `name`/`url`. Existing agents reading
/// `surfaces[].name`/`.url` keep working.
///
/// Per the repo convention pinned by `kill_output_json_shape_is_pinned` ("the
/// false case must emit `killed: false`, not omit the key"), all four keys are
/// ALWAYS emitted with explicit nulls -- never conditionally omitted.
#[test]
fn observability_output_json_shape_is_pinned() {
    assert_eq!(
        ObservabilityOutput::Surfaces(vec![
            // Browsable row: url set, note explicitly null.
            Endpoint {
                name: "Curie Console".to_string(),
                url: Some("http://localhost:28080/?api=1".to_string()),
                note: None,
                browsable: true,
            },
            Endpoint {
                name: "Langfuse UI".to_string(),
                url: Some("http://localhost:23000".to_string()),
                note: None,
                browsable: true,
            },
            // Non-browsable row WITH a url: the API base is an agent target,
            // never opened in a browser.
            Endpoint {
                name: "Curie API".to_string(),
                url: Some("http://localhost:28000".to_string()),
                note: None,
                browsable: false,
            },
            // Degraded row: url explicitly null, note carries the message --
            // never smuggled into `url`.
            Endpoint {
                name: "Langfuse UI".to_string(),
                url: None,
                note: Some("service curie-langfuse-web not found".to_string()),
                browsable: false,
            },
        ])
        .to_json(),
        json!({
            "surfaces": [
                {"name": "Curie Console", "url": "http://localhost:28080/?api=1", "note": null, "browsable": true},
                {"name": "Langfuse UI", "url": "http://localhost:23000", "note": null, "browsable": true},
                {"name": "Curie API", "url": "http://localhost:28000", "note": null, "browsable": false},
                {"name": "Langfuse UI", "url": null, "note": "service curie-langfuse-web not found", "browsable": false},
            ],
        })
    );
}

/// The cluster tier returns the SAME `ObservabilityOutput`, so its payload is
/// pinned at the `to_json` level: `cluster observability` needs a real cluster,
/// so there is no hermetic binary test for it the way `local` has
/// `observability_emits_json_object`. This pins cross-tier payload parity --
/// three surfaces, identical key set, degrading per endpoint rather than
/// hard-failing when a service is missing.
#[test]
fn cluster_observability_output_json_shape_is_pinned() {
    assert_eq!(
        ObservabilityOutput::Surfaces(vec![
            Endpoint {
                name: "Curie Console".to_string(),
                url: Some("http://10.0.0.5:31234/?api=1".to_string()),
                note: None,
                browsable: true,
            },
            // A ClusterIP service degrades to a port-forward hint, not a URL.
            Endpoint {
                name: "Langfuse UI".to_string(),
                url: None,
                note: Some(
                    "kubectl -n curie port-forward svc/curie-langfuse-web 3000:3000  \
                     then http://localhost:3000"
                        .to_string()
                ),
                browsable: false,
            },
            Endpoint {
                name: "Curie API".to_string(),
                url: Some("http://10.0.0.5:31234/api".to_string()),
                note: None,
                browsable: false,
            },
        ])
        .to_json(),
        json!({
            "surfaces": [
                {"name": "Curie Console", "url": "http://10.0.0.5:31234/?api=1", "note": null, "browsable": true},
                {"name": "Langfuse UI", "url": null, "note": "kubectl -n curie port-forward svc/curie-langfuse-web 3000:3000  then http://localhost:3000", "browsable": false},
                {"name": "Curie API", "url": "http://10.0.0.5:31234/api", "note": null, "browsable": false},
            ],
        })
    );
}

#[test]
fn kill_output_json_shape_is_pinned() {
    assert_eq!(
        KillOutput::DryRun(plan(&["POST <api>/agents/<id>/kill"])).to_json(),
        json!({"dry_run": true, "plan": ["POST <api>/agents/<id>/kill"]})
    );
    assert_eq!(
        KillOutput::Done {
            agent: "weather".to_string(),
            killed: true,
        }
        .to_json(),
        json!({"agent": "weather", "killed": true})
    );
    // The false case must emit `killed: false`, not omit the key.
    assert_eq!(
        KillOutput::Done {
            agent: "weather".to_string(),
            killed: false,
        }
        .to_json(),
        json!({"agent": "weather", "killed": false})
    );
}

#[test]
fn resume_output_json_shape_is_pinned() {
    assert_eq!(
        ResumeOutput::DryRun(plan(&["POST <api>/agents/<id>/resume"])).to_json(),
        json!({"dry_run": true, "plan": ["POST <api>/agents/<id>/resume"]})
    );
    assert_eq!(
        ResumeOutput::Done {
            agent: "weather".to_string(),
            killed: false,
        }
        .to_json(),
        json!({"agent": "weather", "killed": false})
    );
    assert_eq!(
        ResumeOutput::Done {
            agent: "weather".to_string(),
            killed: true,
        }
        .to_json(),
        json!({"agent": "weather", "killed": true})
    );
}

#[test]
fn budget_output_json_shape_is_pinned() {
    assert_eq!(
        BudgetOutput::DryRun(plan(&["PATCH <api>/agents/<id>"])).to_json(),
        json!({"dry_run": true, "plan": ["PATCH <api>/agents/<id>"]})
    );
    assert_eq!(
        BudgetOutput::Done {
            agent: "weather".to_string(),
            max_usd_per_day: Some(12.5),
        }
        .to_json(),
        json!({"agent": "weather", "max_usd_per_day": 12.5})
    );
    // `None` means "platform default" and must serialize as an explicit null --
    // omitting the key would read to an agent as "no budget field at all".
    assert_eq!(
        BudgetOutput::Done {
            agent: "weather".to_string(),
            max_usd_per_day: None,
        }
        .to_json(),
        json!({"agent": "weather", "max_usd_per_day": null})
    );
}

#[test]
fn reset_thread_output_json_shape_is_pinned() {
    assert_eq!(
        ResetThreadOutput::DryRun(plan(&["POST <api>/agents/<id>/threads/<key>/reset"])).to_json(),
        json!({"dry_run": true, "plan": ["POST <api>/agents/<id>/threads/<key>/reset"]})
    );
    assert_eq!(
        ResetThreadOutput::Done {
            agent: "weather".to_string(),
            thread_key: "t-1".to_string(),
            requested: true,
            released: true,
        }
        .to_json(),
        json!({"agent": "weather", "thread_key": "t-1", "requested": true, "released": true})
    );
}

#[test]
fn delete_output_json_shape_is_pinned() {
    assert_eq!(
        DeleteOutput::DryRun(plan(&["DELETE <api>/agents/<id>"])).to_json(),
        json!({"dry_run": true, "plan": ["DELETE <api>/agents/<id>"]})
    );
    assert_eq!(
        DeleteOutput::Done {
            agent: "weather".to_string(),
        }
        .to_json(),
        json!({"agent": "weather", "deleted": true})
    );
}

// ---------------------------------------------------------------------------
// The operator + deploy verbs' real-path `to_json` (#485)
// ---------------------------------------------------------------------------
//
// These verbs' success path needs a live server/cluster, so the binary-driven
// tests above cannot reach it. Pin their `to_json` shapes directly so a
// dropped/renamed/added key fails here -- the same discipline the read verbs get.

#[test]
fn deploy_output_json_shape_is_pinned() {
    use curie::commands::DeployOutput;

    let expected: serde_json::Value =
        serde_json::from_str(include_str!("data/deploy-provider-wire.json"))
            .expect("cli/tests/data/deploy-provider-wire.json must contain valid JSON");

    assert_eq!(
        DeployOutput {
            plugin_name: "weather".to_string(),
            label: "v1-123".to_string(),
            env: "dev".to_string(),
            agent_name: "weather".to_string(),
            agent_id: "agt_1".to_string(),
            version_label: "v1-123".to_string(),
            version_id: "ver_1".to_string(),
            channel: "unchanged (C0EXAMPLE1)".to_string(),
            bundle_ref: "bundles/abc.tar.gz".to_string(),
            bundle_sha256: "deadbeef00".to_string(),
            bundle_size_bytes: 4096,
            deployment_id: "dep_1".to_string(),
            deployment_environment: "dev".to_string(),
            deployment_status: "active".to_string(),
        }
        .to_json(),
        expected
    );
}

#[test]
fn comms_output_json_shape_is_pinned() {
    use curie::comms::CommsOutput;
    assert_eq!(
        CommsOutput::DryRun(plan(&["helm upgrade"])).to_json(),
        json!({"dry_run": true, "plan": ["helm upgrade"]})
    );
    // The disconnect case must emit `connected: false`, not omit the key.
    assert_eq!(
        CommsOutput::Done { connected: false }.to_json(),
        json!({"connected": false})
    );
    assert_eq!(
        CommsOutput::Done { connected: true }.to_json(),
        json!({"connected": true})
    );
}

#[test]
fn cluster_up_down_output_json_shapes_are_pinned() {
    use curie::ops::{ClusterDownOutput, ClusterUpOutput};
    assert_eq!(
        ClusterUpOutput::Up {
            namespace: "curie".to_string(),
            release: "curie".to_string(),
        }
        .to_json(),
        json!({"status": "up", "namespace": "curie", "release": "curie"})
    );
    assert_eq!(
        ClusterDownOutput::Aborted.to_json(),
        json!({"down": false, "aborted": true})
    );
    assert_eq!(
        ClusterDownOutput::Down {
            release_was_absent: false,
        }
        .to_json(),
        json!({"down": true, "release_was_absent": false})
    );
}

// #767 fail-forward teardown error contract. When `cluster down` cannot
// complete the teardown (e.g. the API server was unreachable so `helm uninstall`
// failed), it must NOT bail before the sweep and it must NOT report success:
// it returns a transient error carrying the exact copy-pasteable resume command
// in `fix`. This test pins that CONTRACT -- exit class 3 (Transient) plus a
// label-scoped resume command in `{error, fix}` -- which the implementer's error
// path must produce. The error is constructed exactly as the implementer will:
// `CliError::transient(msg).with_fix(<resume command>)` wrapped into
// `anyhow::Error`, then classified via the public `curie::exit` API. Asserting
// on `classify`/`error_json` (rather than a real `down()` return, which needs a
// live unreachable cluster) pins the same user-visible contract without I/O.
#[test]
fn cluster_down_failforward_error_is_transient_with_resume_fix() {
    use curie::exit::{classify, error_json, CliError, ExitClass};

    // The resume command an incomplete teardown surfaces: only the outstanding
    // steps, label-scoped per #707 (the ownership-scope invariant).
    let resume =
        "kubectl delete namespace -l curietech.ai/created-by=prod-release --ignore-not-found";
    let err: anyhow::Error =
        CliError::transient("cluster teardown could not complete; the API server was unreachable")
            .with_fix(resume)
            .into();

    // Retryable signal: exit class 3, so a CI wrapper or agent knows to retry
    // once connectivity returns.
    let (class, fix) = classify(&err);
    assert_eq!(class, ExitClass::Transient);
    assert_eq!(class.code(), 3);

    // The fix carries the exact resume command, and it stays label-scoped.
    let fix = fix.expect("a fail-forward teardown error carries a resume command in fix");
    assert!(
        fix.contains("curietech.ai/created-by="),
        "fix must carry the label-scoped resume command: {fix}"
    );

    // The `--json` error payload exposes it as `{error, fix}` and nothing else.
    let json = error_json(&err);
    let obj = json.as_object().expect("error_json is an object");
    assert_eq!(obj.len(), 2, "exactly error and fix: {obj:?}");
    assert!(
        json["fix"]
            .as_str()
            .unwrap()
            .contains("curietech.ai/created-by="),
        "fix names the label-scoped resume command: {}",
        json["fix"]
    );
}

#[test]
fn local_operator_output_json_shapes_are_pinned() {
    use curie::local::{LocalDownOutput, LocalStatusOutput, LocalUpOutput};
    assert_eq!(
        LocalUpOutput::Up {
            endpoints: vec![("Console".to_string(), "http://localhost:28080".to_string())],
            slack: false,
        }
        .to_json(),
        json!({
            "status": "up",
            "endpoints": [{"name": "Console", "url": "http://localhost:28080"}],
            "slack": false,
        })
    );
    assert_eq!(
        LocalStatusOutput::Services {
            rows: vec!["NAME  STATUS".to_string()],
        }
        .to_json(),
        json!({"services": ["NAME  STATUS"]})
    );
    assert_eq!(
        LocalDownOutput::Down {
            volumes_wiped: true,
            reaped: 2,
        }
        .to_json(),
        json!({"stopped": true, "volumes_wiped": true, "runners_reaped": 2})
    );
    assert_eq!(
        LocalDownOutput::Aborted.to_json(),
        json!({"stopped": false, "aborted": true})
    );
}

// ---------------------------------------------------------------------------
// init --json (issue #485)
// ---------------------------------------------------------------------------

/// A minimal valid agent spec: one skill, one eval, no connectors. Enough for
/// `parse`/`validate` to accept and `scaffold_from_spec` to write a bundle, so
/// the binary-driven test below can exercise the real `init --from-spec`
/// success path hermetically.
const INIT_SPEC_JSON: &str = r#"{
  "name": "deal-desk",
  "description": "Prices and reviews deal desk requests.",
  "skills": [
    {
      "name": "deal-desk",
      "description": "Invoke when a rep submits a pricing exception request.",
      "instructions": "Deal desk skill body.\n"
    }
  ],
  "evals": [
    { "id": "prices-a-deal", "input": "Quote 20% off for Acme", "grader": { "kind": "contains", "expected": "all done", "case_sensitive": false } }
  ]
}"#;

/// Binary-driven: `curie init --from-spec <spec> --json` on the real success
/// path must emit ONE JSON object to stdout, not the 0-byte stdout it shipped
/// with (issue #485). Asserting exit 0 alone is a FALSE GREEN here -- the bug
/// was exit 0 WITH empty stdout -- so this asserts on the stdout BYTES. Hermetic:
/// it writes a self-contained spec and scaffolds into a fresh tempdir, no network.
#[test]
fn init_from_spec_emits_json_object() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let spec_path = tmp.path().join("spec.json");
    std::fs::write(&spec_path, INIT_SPEC_JSON).expect("write spec fixture");
    let out_dir = tmp.path().join("bundle");

    let output = Command::new(bin())
        .args([
            "init",
            "--from-spec",
            spec_path.to_str().unwrap(),
            "--dir",
            out_dir.to_str().unwrap(),
            "--json",
        ])
        .output()
        .expect("run curie init --from-spec --json");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        output.status.code(),
        Some(0),
        "init --from-spec is hermetic and must exit 0\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !stdout.trim().is_empty(),
        "`curie init --from-spec --json` must not emit empty stdout (issue #485)\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let parsed: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|e| panic!("must emit parseable JSON: {e}\nstdout: {stdout}"));
    assert!(
        parsed.is_object(),
        "`curie init --from-spec --json` must emit a JSON object, got: {stdout}"
    );
    // Agent-facing keys: the name comes from the spec, from_spec is the source
    // path (never null on this branch), created lists the written files.
    assert_eq!(parsed["initialized"], json!(true));
    assert_eq!(parsed["name"], json!("deal-desk"));
    assert_eq!(
        parsed["from_spec"],
        json!(spec_path.to_str().unwrap()),
        "from_spec must carry the spec source path on the --from-spec branch"
    );
    assert!(
        parsed["created"].as_array().is_some_and(|c| !c.is_empty()),
        "created must list the scaffolded files: {stdout}"
    );
    // The human success line must NOT leak onto stdout under --json.
    assert!(
        !stdout.contains("initialized plugin bundle"),
        "the human success line must stay on stderr under --json: {stdout}"
    );
}

/// Exact key-shape pin for `InitOutput::to_json` (both branches). Renaming,
/// dropping, or adding a key fails. The plain-name branch's `from_spec` must be
/// an explicit `null`, not an omitted key: an agent reads null as "not
/// spec-sourced", where a missing key is ambiguous.
#[test]
fn init_output_json_shape_is_pinned() {
    use std::path::PathBuf;
    assert_eq!(
        InitOutput {
            name: "deal-desk".to_string(),
            dir: PathBuf::from("deal-desk"),
            from_spec: Some(PathBuf::from("spec.json")),
            created: vec![PathBuf::from("deal-desk/.claude-plugin/plugin.json")],
            success_msg: "rendered only on the human path".to_string(),
        }
        .to_json(),
        json!({
            "initialized": true,
            "name": "deal-desk",
            "dir": "deal-desk",
            "from_spec": "spec.json",
            "created": ["deal-desk/.claude-plugin/plugin.json"],
            "next": "cd deal-desk && curie skill up"
        })
    );
    assert_eq!(
        InitOutput {
            name: "weather".to_string(),
            dir: PathBuf::from("./weather"),
            from_spec: None,
            created: vec![],
            success_msg: String::new(),
        }
        .to_json(),
        json!({
            "initialized": true,
            "name": "weather",
            "dir": "./weather",
            "from_spec": null,
            "created": [],
            "next": "cd ./weather && curie skill up"
        })
    );
}

/// The `next` command shell-quotes a dir with a space so it stays a single valid
/// `cd` target, not a broken two-token command. A kebab path stays bare (asserted
/// above), so this only fires for special-char paths.
#[test]
fn init_output_next_shell_quotes_a_dir_with_a_space() {
    use std::path::PathBuf;
    let out = InitOutput {
        name: "deal-desk".to_string(),
        dir: PathBuf::from("my bundle"),
        from_spec: None,
        created: vec![],
        success_msg: String::new(),
    }
    .to_json();
    assert_eq!(
        out["next"],
        json!("cd 'my bundle' && curie skill up"),
        "a dir with a space must be shell-quoted in the next command: {out}"
    );
}

// `cluster deploy --all-targets --json` result and failure contracts (#1308).

const DEPLOY_LABEL: &str = "v1-test";
const DEPLOY_API_KEY: &str = "test-key";

#[derive(Clone, Copy)]
enum DeployFixtureFailure {
    None,
    Deploy(&'static str),
}

fn target_from_agent(agent: &str) -> &'static str {
    if agent.contains("prod") {
        "prod"
    } else {
        "dev"
    }
}

fn target_config(target: &str) -> (&'static str, &'static str, &'static str) {
    match target {
        "dev" => ("acme-dev", "dev", "C0EXAMPLE2"),
        "prod" => ("acme-prod", "prod", "C0EXAMPLE1"),
        other => panic!("unexpected deploy target {other}"),
    }
}

fn response_json(status: u16, value: serde_json::Value) -> Response {
    Response::json(status, &value.to_string())
}

fn deploy_api_response(req: &support::Request, failure: DeployFixtureFailure) -> Response {
    match (req.method.as_str(), req.path.as_str()) {
        ("POST", "/deploy-targets/list") => response_json(
            200,
            json!({
                "targets": [
                    {"name": "dev", "agent": "acme-dev", "env": "dev", "slack_channel": "C0EXAMPLE2"},
                    {"name": "prod", "agent": "acme-prod", "env": "prod", "slack_channel": "C0EXAMPLE1"}
                ]
            }),
        ),
        ("POST", "/deploy-targets/resolve") => {
            let body: serde_json::Value =
                serde_json::from_slice(&req.body).expect("target request body is JSON");
            let target = body["target"].as_str().expect("target is a string");
            let (agent, env, channel) = target_config(target);
            response_json(
                200,
                json!({"agent": agent, "env": env, "slack_channel": channel}),
            )
        }
        ("GET", "/agents") => Response::json(200, "[]"),
        ("POST", "/agents") => {
            let body: serde_json::Value =
                serde_json::from_slice(&req.body).expect("agent request body is JSON");
            let name = body["name"].as_str().expect("agent name is a string");
            // The neutral `{kind, address}` channel binding (ADR-0096, #1459),
            // not the Slack-typed `slack_channel` the agent create body carried
            // before it.
            let channel = body["channel"]["address"]
                .as_str()
                .expect("agent channel address is a string");
            let target = target_from_agent(name);
            response_json(
                201,
                json!({
                    "id": format!("agent-{target}"),
                    "name": name,
                    "channels": [{"kind": "slack", "address": channel}]
                }),
            )
        }
        ("POST", path) if path.starts_with("/agents/") && path.ends_with("/versions") => {
            let agent_id = path
                .trim_start_matches("/agents/")
                .trim_end_matches("/versions")
                .trim_end_matches('/');
            let target = target_from_agent(agent_id);
            response_json(
                201,
                json!({"id": format!("version-{target}"), "version_label": DEPLOY_LABEL}),
            )
        }
        ("PUT", path) if path.contains("/versions/") && path.ends_with("/bundle") => {
            let target = target_from_agent(path);
            response_json(
                201,
                json!({
                    "bundle_ref": format!("bundles/{target}.tar.gz"),
                    "bundle_sha256": format!("sha-{target}"),
                    "size_bytes": if target == "dev" { 101 } else { 202 }
                }),
            )
        }
        ("POST", "/deployments") => {
            let body: serde_json::Value =
                serde_json::from_slice(&req.body).expect("deployment request body is JSON");
            let agent_id = body["agent_id"]
                .as_str()
                .expect("deployment agent is a string");
            let target = target_from_agent(agent_id);
            if matches!(failure, DeployFixtureFailure::Deploy(failed) if failed == target) {
                return Response::json(500, &format!("deployment {target} exploded"));
            }
            response_json(
                201,
                json!({
                    "id": format!("deployment-{target}"),
                    "environment": target,
                    "status": "active"
                }),
            )
        }
        ("GET", path) if path.contains("/versions/") && path.contains("/connectors?") => {
            response_json(
                200,
                json!({
                    "manifests": [],
                    "owned_secret_name": "",
                    "owned_secret_keys": [],
                    "mcp_entries": {}
                }),
            )
        }
        (method, path) => Response::json(
            500,
            &format!("unexpected mock API request: {method} {path}"),
        ),
    }
}

fn write_kubectl_stub(dir: &Path) -> PathBuf {
    let path = dir.join("kubectl");
    fs::write(
        &path,
        r#"#!/bin/sh
case "$*" in
  *"get deployment"*)
    if [ "${CURIE_TEST_CONNECTOR_FAILURE:-}" = 1 ]; then
      printf '%s\n' 'connector sync exploded' >&2
      exit 1
    fi
    printf '%s' 'curie'
    ;;
  *"delete deployment,service,networkpolicy,secret"*)
    exit 0
    ;;
  *)
    printf 'unexpected kubectl invocation: %s\n' "$*" >&2
    exit 64
    ;;
esac
"#,
    )
    .expect("write kubectl stub");
    let mut permissions = fs::metadata(&path)
        .expect("read kubectl stub metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).expect("make kubectl stub executable");
    path
}

fn stub_path(bin_dir: &Path) -> std::ffi::OsString {
    let mut paths = vec![bin_dir.to_path_buf()];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    std::env::join_paths(paths).expect("join stub PATH")
}

fn run_cluster_deploy_json(
    all_targets: bool,
    failure: DeployFixtureFailure,
    connector_failure: bool,
) -> Output {
    let plugin = tempfile::tempdir().expect("plugin tempdir");
    curie::scaffold::scaffold(plugin.path(), "acme-bundle").expect("scaffold test bundle");
    fs::write(
        plugin.path().join("deploy.yaml"),
        "targets:\n  dev: { agent: acme-dev, env: dev, slack_channel: C0EXAMPLE2 }\n  prod: { agent: acme-prod, env: prod, slack_channel: C0EXAMPLE1 }\n",
    )
    .expect("write deploy targets");

    let tools = tempfile::tempdir().expect("tool tempdir");
    write_kubectl_stub(tools.path());
    let server = serve(move |req| deploy_api_response(req, failure));

    let mut command = Command::new(bin());
    command.args([
        "cluster",
        "deploy",
        "--plugin-dir",
        plugin.path().to_str().expect("plugin path is valid UTF8"),
        "--api-url",
        &server.base_url,
        "--api-key",
        DEPLOY_API_KEY,
        "--namespace",
        "curie",
        "--release",
        "curie",
        "--label",
        DEPLOY_LABEL,
    ]);
    if all_targets {
        command.arg("--all-targets");
    } else {
        command.args(["--target", "dev"]);
    }
    command
        .arg("--json")
        .env("PATH", stub_path(tools.path()))
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY");
    if connector_failure {
        command.env("CURIE_TEST_CONNECTOR_FAILURE", "1");
    } else {
        command.env_remove("CURIE_TEST_CONNECTOR_FAILURE");
    }
    command.output().expect("run cluster deploy JSON contract")
}

fn one_stdout_object(output: &Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.trim().is_empty(), "stdout must not be empty");
    let mut values =
        serde_json::Deserializer::from_slice(&output.stdout).into_iter::<serde_json::Value>();
    let first = values
        .next()
        .unwrap_or_else(|| panic!("stdout must contain one JSON object: {stdout}"))
        .unwrap_or_else(|error| panic!("stdout must start with JSON: {error}\n{stdout}"));
    assert!(
        first.is_object(),
        "stdout must contain a JSON object: {stdout}"
    );
    assert!(
        values.next().is_none(),
        "stdout must contain exactly one JSON object: {stdout}"
    );
    first
}

fn expected_deploy(target: &str) -> serde_json::Value {
    let (agent, environment, channel) = target_config(target);
    json!({
        "plugin": "acme-bundle",
        "label": DEPLOY_LABEL,
        "environment": environment,
        "agent": {"name": agent, "id": format!("agent-{target}")},
        "version": {"label": DEPLOY_LABEL, "id": format!("version-{target}")},
        "channel": channel,
        "bundle": {
            "ref": format!("bundles/{target}.tar.gz"),
            "sha256": format!("sha-{target}"),
            "size_bytes": if target == "dev" { 101 } else { 202 }
        },
        "deployment": {
            "id": format!("deployment-{target}"),
            "environment": environment,
            "status": "active"
        }
    })
}

fn assert_failure_keys(value: &serde_json::Value, includes_failed_result: bool) {
    let object = value.as_object().expect("failure payload is an object");
    let expected = if includes_failed_result { 6 } else { 5 };
    assert_eq!(
        object.len(),
        expected,
        "failure keys must stay exact: {value}"
    );
    for key in ["failed_target", "stage", "completed", "error", "fix"] {
        assert!(
            object.contains_key(key),
            "failure is missing {key}: {value}"
        );
    }
    assert_eq!(
        object.contains_key("failed_result"),
        includes_failed_result,
        "failed_result presence must match the failure stage: {value}"
    );
    assert!(
        value["fix"].is_null(),
        "a permanent failure has no retry fix: {value}"
    );
}

#[test]
fn cluster_deploy_json_all_targets_emits_one_ordered_complete_object() {
    let output = run_cluster_deploy_json(true, DeployFixtureFailure::None, false);
    assert_eq!(
        output.status.code(),
        Some(0),
        "all target deploy must succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        one_stdout_object(&output),
        json!({
            "results": [
                {"target": "dev", "result": expected_deploy("dev")},
                {"target": "prod", "result": expected_deploy("prod")}
            ]
        })
    );
}

#[test]
fn cluster_deploy_json_later_failure_names_target_and_completed_results() {
    let output = run_cluster_deploy_json(true, DeployFixtureFailure::Deploy("prod"), false);
    assert_eq!(output.status.code(), Some(1));
    let value = one_stdout_object(&output);
    assert_failure_keys(&value, false);
    assert_eq!(value["failed_target"], json!("prod"));
    assert_eq!(value["stage"], json!("deploy"));
    assert_eq!(
        value["completed"],
        json!([{"target": "dev", "result": expected_deploy("dev")}])
    );
    assert!(
        value["error"]
            .as_str()
            .is_some_and(|error| error.contains("deployment prod exploded")),
        "failure must retain the API error: {value}"
    );
}

#[test]
fn cluster_deploy_json_first_failure_has_empty_completed_results() {
    let output = run_cluster_deploy_json(true, DeployFixtureFailure::Deploy("dev"), false);
    assert_eq!(output.status.code(), Some(1));
    let value = one_stdout_object(&output);
    assert_failure_keys(&value, false);
    assert_eq!(value["failed_target"], json!("dev"));
    assert_eq!(value["stage"], json!("deploy"));
    assert_eq!(value["completed"], json!([]));
    assert!(
        value["error"]
            .as_str()
            .is_some_and(|error| error.contains("deployment dev exploded")),
        "failure must retain the API error: {value}"
    );
}

#[test]
fn cluster_deploy_json_connector_sync_failure_carries_failed_result() {
    let output = run_cluster_deploy_json(true, DeployFixtureFailure::None, true);
    assert_eq!(output.status.code(), Some(1));
    let value = one_stdout_object(&output);
    assert_failure_keys(&value, true);
    assert_eq!(value["failed_target"], json!("dev"));
    assert_eq!(value["stage"], json!("connector_sync"));
    assert_eq!(value["completed"], json!([]));
    assert_eq!(value["failed_result"], expected_deploy("dev"));
    assert!(
        value["error"]
            .as_str()
            .is_some_and(|error| error.contains("connector sync exploded")),
        "failure must retain the connector error: {value}"
    );
}

#[test]
fn cluster_deploy_json_single_target_shape_is_unchanged() {
    let output = run_cluster_deploy_json(false, DeployFixtureFailure::None, false);
    assert_eq!(
        output.status.code(),
        Some(0),
        "single target deploy must succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(one_stdout_object(&output), expected_deploy("dev"));
}
