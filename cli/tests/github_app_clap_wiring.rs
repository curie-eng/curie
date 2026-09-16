//! Integration: the clap-to-`GithubAppOpts` wiring of `cluster github-app`'s
//! BYO Secret flags (issue #1255).
//!
//! Every other test for this verb lives in `cli/src/github_app.rs`'s unit
//! module and builds a `GithubAppOpts` by hand, so it proves what the command
//! builders do with a field but never that the FLAG reaches that field. The
//! match arm in `main.rs` destructures the clap variant and then constructs
//! `GithubAppOpts` by name; Rust makes the destructuring half exhaustive, but
//! nothing makes the construction half exhaustive. A field pulled out of the
//! pattern and never placed into the struct compiles perfectly clean, and the
//! flag is silently dropped: `--existing-secret my-github-app` would parse,
//! validate, print "GitHub App configured", and configure nothing.
//!
//! These tests close that gap by driving the built binary and asserting on the
//! `--dry-run --json` plan, which carries the exact helm argv that would run.
//! Assertions are on WHOLE whitespace-separated tokens of the plan, never a
//! substring of the joined line: `contains("api.githubAppExistingSecret=")` is
//! also satisfied by the disconnect clear and by any other Secret name, so it
//! tests for a prefix rather than for the value that was set (#1263).
//!
//! No cluster and no network. `--dry-run` returns before any helm or kubectl
//! process is spawned, and an explicit `--existing-secret` is the one connect
//! path that makes no `helm get values` read (see `needs_byo_conflict_check`),
//! so the values-read step this verb gained cannot reach out either. `--chart`
//! is passed explicitly because chart resolution otherwise looks for
//! `charts/curie` relative to the test process's cwd, which is the `cli`
//! package root.

mod support;

use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

/// Run the binary with `argv` and return every whitespace-separated token of
/// its `--dry-run --json` plan, flattened across plan lines.
fn dry_run_plan_tokens(argv: &[&str]) -> Vec<String> {
    let output = Command::new(bin())
        .args(argv)
        .output()
        .unwrap_or_else(|e| panic!("run curie {}: {e}", argv.join(" ")));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "curie {} must exit 0; stdout: {stdout}; stderr: {stderr}",
        argv.join(" ")
    );
    let value: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("stdout must be one JSON object: {e}; stdout: {stdout}"));
    let lines = value
        .get("plan")
        .and_then(|p| p.as_array())
        .unwrap_or_else(|| panic!("dry-run output must carry a plan: {value}"));
    lines
        .iter()
        .filter_map(|l| l.as_str())
        .flat_map(|l| l.split_whitespace())
        .map(str::to_string)
        .collect()
}

/// The plan of a BYO connect that names a NON-default data key. Non-default on
/// purpose: `--existing-secret-key privateKey` would also pass if the flag were
/// dropped on the floor, because `privateKey` is the value the default supplies
/// (#1263).
fn byo_plan_tokens() -> Vec<String> {
    dry_run_plan_tokens(&[
        "cluster",
        "github-app",
        "--app-id",
        "1",
        "--existing-secret",
        "my-github-app",
        "--existing-secret-key",
        "app-pem",
        "--chart",
        "charts/curie",
        "--dry-run",
        "--json",
    ])
}

/// The whole plan token immediately preceding `value`.
fn token_before(tokens: &[String], value: &str) -> String {
    let at = tokens
        .iter()
        .position(|t| t == value)
        .unwrap_or_else(|| panic!("no plan token equal to `{value}`: {tokens:?}"));
    assert!(at > 0, "`{value}` has no preceding token: {tokens:?}");
    tokens[at - 1].clone()
}

#[test]
fn the_existing_secret_flags_reach_the_helm_plan_not_just_the_parser() {
    // The silent-drop failure mode: both flags parse, the verb reports
    // success, and the release is never pointed at the operator's Secret. The
    // only place that is visible is the argv the verb would actually run.
    let tokens = byo_plan_tokens();
    assert!(
        tokens
            .iter()
            .any(|t| t == "api.githubAppExistingSecret=my-github-app"),
        "--existing-secret never reached the helm plan: {tokens:?}"
    );
    assert!(
        tokens
            .iter()
            .any(|t| t == "api.githubAppExistingSecretKey=app-pem"),
        "--existing-secret-key never reached the helm plan: {tokens:?}"
    );
}

#[test]
fn the_wired_byo_flags_are_set_as_strings() {
    // An all-digit Secret name or data key parsed by `--set` round-trips
    // through --reuse-values as a float64 and renders in scientific notation
    // -- #1236's App-ID bug in a new field. The clap layer is where a
    // hand-written `--set` would land, so it is asserted here too.
    let tokens = byo_plan_tokens();
    assert_eq!(
        token_before(&tokens, "api.githubAppExistingSecret=my-github-app"),
        "--set-string",
        "the Secret name must not be helm-typed: {tokens:?}"
    );
    assert_eq!(
        token_before(&tokens, "api.githubAppExistingSecretKey=app-pem"),
        "--set-string",
        "the data key must not be helm-typed: {tokens:?}"
    );
}

#[test]
fn the_wired_byo_connect_never_asks_helm_to_read_a_pem() {
    // The security property, asserted at the real entry point: on the BYO path
    // no file path is handed to helm, so no PEM can be copied into release
    // history where `helm get values` prints it back (#1236).
    let tokens = byo_plan_tokens();
    assert!(
        !tokens.iter().any(|t| t == "--set-file"),
        "the wired BYO plan makes helm read a file off disk: {tokens:?}"
    );
    assert!(
        tokens.iter().any(|t| t == "api.githubAppPrivateKey="),
        "the wired BYO plan must clear the inline key: {tokens:?}"
    );
}

#[test]
fn the_wired_byo_connect_still_rolls_the_api() {
    // A secretKeyRef env var is resolved once at pod start, so the helm
    // upgrade alone leaves the running API on the old credential while the CLI
    // reports success. AC1 says the BYO path includes the rollout.
    let tokens = byo_plan_tokens();
    assert!(
        tokens.iter().any(|t| t == "restart"),
        "the BYO plan never restarts the api deployment: {tokens:?}"
    );
    assert!(
        tokens.iter().any(|t| t == "status"),
        "the BYO plan never waits for the rollout: {tokens:?}"
    );
    assert!(
        tokens.iter().any(|t| t == "deployment/curie-api"),
        "the BYO plan rolls something other than the api: {tokens:?}"
    );
}

/// The AC2 conflict guard, driven through the real binary on a REAL (non
/// `--dry-run`) invocation against a FAKE helm release.
///
/// The guard's own tests call `guard_byo_key_conflict` directly, and the
/// `--dry-run` tests above only exercise an explicit `--existing-secret`
/// connect -- the one connect path where the guard is deliberately skipped.
/// Between them, nothing covered the CALL SITE in `github_app()`: deleting that
/// single `guard_byo_key_conflict(&opts, existing.as_ref())?` line left the
/// whole suite green while restoring this ticket's exact failure -- a
/// `--private-key` run against a BYO release reaching `Done { configured: true }`
/// over an unchanged live key. AGENTS.md requires a guard to be proven by
/// execution through the real consumer path, and the real consumer path is the
/// binary.
///
/// Real invocation, and still no cluster. `github_app()` validates the inputs
/// and tools, captures the deployed Helm revision, then reads that revision's
/// values before applying `guard_byo_key_conflict` and entering any sandbox or
/// Helm mutation path. A `helm` shim earlier on the CHILD's PATH answers those
/// exact-revision reads; the refusal then happens before anything is mutated.
/// `--dry-run` is deliberately NOT used here: it must never touch the network
/// (`cli/CLAUDE.md`), so it makes no values read and reaches no guard at all --
/// a dry run could not exercise this call site.
///
/// Every shim invocation is appended to a log, because exit codes alone cannot
/// tell the two outcomes apart: the shim refuses everything except the values
/// read, so a guard that stayed correctly quiet ALSO exits non-zero. The log is
/// what distinguishes "refused before any mutation" from "reached the upgrade".
#[cfg(unix)]
mod byo_conflict_guard_through_the_binary {
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::process::{Command, Output};

    /// Logs every invocation, answers the read-only release and sandbox reads
    /// required before a credential mutation, and refuses everything else --
    /// so no test can pass because the CLI ran some other command that happened
    /// to succeed, and no test can mutate anything.
    /// Installed under both names: the real path calls `require_on_path`
    /// ("kubectl") before running the upgrade, so a missing kubectl would abort
    /// the sibling paths one step early and for the wrong reason.
    const TOOL_SHIM: &str = r#"#!/usr/bin/env bash
tool=$(basename "$0")
echo "$tool $*" >> "$SHIM_LOG"
if [ "$tool" = "helm" ] && [ "$1" = "get" ] && [ "$2" = "values" ]; then
  echo "$FAKE_VALUES"
  exit 0
fi
if [ "$tool" = "helm" ] && [ "$1" = "history" ]; then
  echo '[{"revision":12,"status":"deployed","chart":"curie-0.8.6"}]'
  exit 0
fi
if [ "$tool" = "helm" ] && [ "$1" = "get" ] && [ "$2" = "manifest" ]; then
  cat <<'MANIFEST'
---
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxTemplate
metadata:
  name: curie-runner
  labels:
    app.kubernetes.io/component: agent-sandbox
    app.kubernetes.io/instance: curie
    app.kubernetes.io/managed-by: Helm
spec:
  service: true
---
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxWarmPool
metadata:
  name: curie-runner-pool
  labels:
    app.kubernetes.io/component: agent-sandbox
    app.kubernetes.io/instance: curie
    app.kubernetes.io/managed-by: Helm
spec:
  replicas: 0
  sandboxTemplateRef:
    name: curie-runner
MANIFEST
  exit 0
fi
if [ "$tool" = "kubectl" ] && [ "$1" = "get" ] \
  && [[ "$2" == sandboxtemplates.extensions.agents.x-k8s.io,* ]]; then
  echo '{"items":[{"apiVersion":"extensions.agents.x-k8s.io/v1beta1","kind":"SandboxTemplate","metadata":{"name":"curie-runner","labels":{"app.kubernetes.io/component":"agent-sandbox","app.kubernetes.io/instance":"curie","app.kubernetes.io/managed-by":"Helm"},"annotations":{"meta.helm.sh/release-name":"curie","meta.helm.sh/release-namespace":"curie"}},"spec":{"service":true}},{"apiVersion":"extensions.agents.x-k8s.io/v1beta1","kind":"SandboxWarmPool","metadata":{"name":"curie-runner-pool","labels":{"app.kubernetes.io/component":"agent-sandbox","app.kubernetes.io/instance":"curie","app.kubernetes.io/managed-by":"Helm"},"annotations":{"meta.helm.sh/release-name":"curie","meta.helm.sh/release-namespace":"curie"}},"spec":{"replicas":0,"sandboxTemplateRef":{"name":"curie-runner"}}}]}'
  exit 0
fi
if [ "$tool" = "kubectl" ] && echo "$*" | grep -q " get secret "; then
  echo "$FAKE_SECRET_JSON"
  exit 0
fi
echo "shim: refusing to execute: $tool $*" >&2
exit 1
"#;

    /// The refusal's own words. Asserted ABSENT on every sibling path, which is
    /// how "the guard stayed quiet" is told apart from "the guard fired".
    const REFUSAL_TEXT: &str = "already reads the GitHub App private key from Secret";

    /// A release that already resolves the App key from an operator-managed
    /// Secret under a NON-default data key. Non-default on purpose: asserting
    /// on `privateKey` would also pass if the read ignored the release and fell
    /// back to the chart default (#1263).
    const BYO_VALUES: &str = r#"{"api":{"githubAppExistingSecret":"my-github-app","githubAppExistingSecretKey":"app-pem"}}"#;

    /// What `--disconnect` leaves behind: the key is PRESENT and empty. That is
    /// a chart-held release, not a BYO one.
    const DISCONNECTED_VALUES: &str = r#"{"api":{"githubAppExistingSecret":""}}"#;

    /// A per-test fake cluster: its own temp dir holding the tool shims, their
    /// invocation log, and a real PEM file.
    ///
    /// Per-test on purpose. Cargo runs these concurrently in one process, so a
    /// shared shim or log path would be clobbered by whichever test wrote it
    /// last, and `std::env::set_var("PATH", ..)` would be a data race across the
    /// whole test binary. Every input travels to the CHILD instead, via
    /// `Command::env`.
    struct FakeRelease {
        dir: tempfile::TempDir,
        github: super::support::MockServer,
    }

    impl FakeRelease {
        fn new() -> Self {
            let dir = tempfile::tempdir().expect("tempdir");
            let shim_dir = dir.path().join("bin");
            std::fs::create_dir(&shim_dir).expect("create shim dir");
            for tool in ["helm", "kubectl"] {
                let path = shim_dir.join(tool);
                std::fs::write(&path, TOOL_SHIM).expect("write shim");
                let mut perms = std::fs::metadata(&path).expect("stat shim").permissions();
                perms.set_mode(0o755);
                std::fs::set_permissions(&path, perms).expect("chmod shim");
            }
            // A real RSA key: since #2269 the live path signs a JWT and
            // probes GitHub before helm upgrade, so a PEM-shaped placeholder
            // would be refused at signing and never reach the BYO-conflict
            // call site these tests exist to pin. Generated at runtime so
            // this file never carries key material.
            let pem = std::process::Command::new("openssl")
                .args(["genrsa", "2048"])
                .output()
                .unwrap_or_else(|e| panic!("openssl genrsa: {e}"));
            assert!(
                pem.status.success(),
                "openssl genrsa failed: {}",
                String::from_utf8_lossy(&pem.stderr)
            );
            std::fs::write(dir.path().join("app.pem"), &pem.stdout).expect("write pem fixture");
            let github = super::support::serve(|_req: &super::support::Request| {
                super::support::Response::json(200, r#"{"id":1234567,"name":"acme-bot"}"#)
            });
            Self { dir, github }
        }

        /// A real file, because `require_connect_inputs` stats the path long
        /// before the guard is consulted.
        fn private_key(&self) -> String {
            self.dir
                .path()
                .join("app.pem")
                .to_string_lossy()
                .into_owned()
        }

        fn log_path(&self) -> PathBuf {
            self.dir.path().join("invocations.log")
        }

        /// Every command the shims were asked to run, in order.
        fn invocations(&self) -> Vec<String> {
            match std::fs::read_to_string(self.log_path()) {
                Ok(body) => body.lines().map(str::to_string).collect(),
                Err(_) => Vec::new(),
            }
        }

        /// Run the binary with the shim dir PREPENDED to the child's PATH.
        /// Prepended rather than replacing: the shims must win over a real helm
        /// or kubectl, and the binary still needs the rest of PATH.
        fn secret_json(&self) -> String {
            let pem = std::fs::read(self.dir.path().join("app.pem")).expect("read pem");
            let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, pem);
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": "my-github-app"},
                "data": {"app-pem": encoded, "privateKey": encoded}
            })
            .to_string()
        }

        fn run(&self, values: &str, argv: &[&str]) -> Output {
            let mut dirs = vec![self.dir.path().join("bin")];
            if let Some(existing) = std::env::var_os("PATH") {
                dirs.extend(std::env::split_paths(&existing));
            }
            let path = std::env::join_paths(dirs).expect("join PATH");
            Command::new(super::bin())
                .args(argv)
                .env("PATH", path)
                .env("FAKE_VALUES", values)
                .env("FAKE_SECRET_JSON", self.secret_json())
                .env("CURIE_GITHUB_API_URL", &self.github.base_url)
                .env("SHIM_LOG", self.log_path())
                .output()
                .unwrap_or_else(|e| panic!("run curie {}: {e}", argv.join(" ")))
        }
    }

    fn combined(output: &Output) -> String {
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    }

    fn stdout_json(output: &Output) -> serde_json::Value {
        let stdout = String::from_utf8_lossy(&output.stdout);
        serde_json::from_str(stdout.trim())
            .unwrap_or_else(|e| panic!("stdout must be one JSON object: {e}; stdout: {stdout}"))
    }

    fn text_at(value: &serde_json::Value, key: &str) -> String {
        value
            .get(key)
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("the refusal payload must carry `{key}`: {value}"))
            .to_string()
    }

    /// Assert the run got as far as the helm upgrade, i.e. the guard let it
    /// through. The sibling paths all fail on the shim's refusal to execute
    /// that upgrade, so their EXIT CODE is non-zero either way -- only the
    /// invocation log can say which side of the guard they died on.
    fn assert_reached_the_upgrade(release: &FakeRelease, output: &Output) {
        let log = release.invocations();
        assert!(
            log.iter().any(|line| line.starts_with("helm upgrade")),
            "the guard blocked a path it must never block: {log:?}; output: {}",
            combined(output)
        );
        let text = combined(output);
        assert!(
            !text.contains(REFUSAL_TEXT),
            "the conflict guard fired on a path it must never fire on: {text}"
        );
    }

    #[test]
    fn a_byo_configured_release_refuses_a_chart_held_private_key_at_the_command_line() {
        // THIS IS THE TICKET, and this is the test that goes red if the
        // `guard_byo_key_conflict` call in `github_app()` is deleted. Without
        // it the CLI prints "GitHub App configured", returns
        // {"github_app_configured": true} and rolls the API -- while the pod
        // keeps signing with the OLD key, because the chart resolves
        // GITHUB_APP_PRIVATE_KEY from the BYO Secret whenever
        // api.githubAppExistingSecret is non-empty, so --set-file writes a
        // value nothing ever reads. The operator's next documented rotation
        // step is "delete the first key on GitHub", at which point every clone
        // 401s and nothing the CLI printed hinted at it.
        let release = FakeRelease::new();
        let key = release.private_key();
        let output = release.run(
            BYO_VALUES,
            &[
                "cluster",
                "github-app",
                "--app-id",
                "1234567",
                "--private-key",
                &key,
                "--chart",
                "charts/curie",
                "--json",
            ],
        );
        assert!(
            !output.status.success(),
            "a BYO release must refuse --private-key; status: {:?}; output: {}",
            output.status,
            combined(&output)
        );

        let value = stdout_json(&output);
        // Non-empty stdout is not the assertion: the refusal has to name the
        // Secret and the data key the release actually reads, or the operator
        // cannot act on it.
        let error = text_at(&value, "error");
        assert!(
            error.contains("my-github-app"),
            "the refusal must name the Secret the release reads: {error}"
        );
        assert!(
            error.contains("app-pem"),
            "the refusal must name the release's own data key: {error}"
        );
        let fix = text_at(&value, "fix");
        assert!(
            fix.contains("--existing-secret my-github-app"),
            "the fix must name the way forward: {fix}"
        );
        assert!(
            fix.contains("--disconnect"),
            "the fix must name the way back: {fix}"
        );

        // The precise failure being prevented is REACHING the success path.
        assert!(
            value.get("github_app_configured").is_none(),
            "the refused run reported the App as configured: {value}"
        );

        // And the refusal must land before any mutation. The exact-revision
        // guard first identifies Helm revision 12, then reads values from that
        // revision so the credential decision and sandbox inventory cannot
        // observe different release states. Those are the only two commands
        // allowed before this refusal; in particular no helm upgrade and no
        // kubectl mutation may run.
        let log = release.invocations();
        assert_eq!(
            log,
            vec![
                "helm history curie -n curie -o json --max 256",
                "helm get values curie -n curie --revision 12 -o json",
            ],
            "the guard must perform only the exact-revision read before refusing"
        );
    }

    #[test]
    fn a_release_with_an_empty_byo_secret_still_reaches_the_chart_held_upgrade() {
        // The control that keeps the guard from being a blanket refusal.
        // `--disconnect` writes api.githubAppExistingSecret="", so the key is
        // PRESENT and empty on every disconnected release. A guard firing on
        // presence rather than on a non-empty value would leave the operator no
        // CLI route back to a chart-held key at all.
        let release = FakeRelease::new();
        let key = release.private_key();
        let output = release.run(
            DISCONNECTED_VALUES,
            &[
                "cluster",
                "github-app",
                "--app-id",
                "1234567",
                "--private-key",
                &key,
                "--chart",
                "charts/curie",
                "--json",
            ],
        );
        assert_reached_the_upgrade(&release, &output);
    }

    #[test]
    fn a_byo_release_still_reaches_the_upgrade_for_a_re_pointed_existing_secret() {
        // Re-running `--existing-secret` on a BYO release IS the supported
        // rotation: the operator updated the Secret's contents and needs the
        // API rolled onto them. A guard that fired here would leave them no CLI
        // way to roll the API at all -- so the refusal above must be scoped to
        // the chart-held connect, proven against the very same BYO values.
        let release = FakeRelease::new();
        let output = release.run(
            BYO_VALUES,
            &[
                "cluster",
                "github-app",
                "--app-id",
                "1234567",
                "--existing-secret",
                "my-github-app",
                "--existing-secret-key",
                "app-pem",
                "--chart",
                "charts/curie",
                "--json",
            ],
        );
        assert_reached_the_upgrade(&release, &output);
    }

    #[test]
    fn a_byo_release_can_still_be_disconnected() {
        // Clearing a reference must always be possible. A guard that refused
        // `--disconnect` on a BYO release would make that release
        // unrecoverable through the CLI: the operator would have to hand-run
        // helm, which is the thing this verb exists to avoid.
        let release = FakeRelease::new();
        let output = release.run(
            BYO_VALUES,
            &[
                "cluster",
                "github-app",
                "--disconnect",
                "--chart",
                "charts/curie",
                "--json",
            ],
        );
        assert_reached_the_upgrade(&release, &output);
    }
}
