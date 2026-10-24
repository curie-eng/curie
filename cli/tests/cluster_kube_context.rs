//! Integration (#2723): `curie cluster` must target an EXPLICIT Kubernetes context.
//!
//! The regression: every `curie cluster` verb spawned `helm` and `kubectl` with the
//! ambient process env, so the operator's current-context (possibly production) was
//! silently the target, and an ambient `HELM_KUBECONTEXT` could even split helm from
//! kubectl onto different clusters. `cluster down --yes` therefore could uninstall a
//! release from the wrong cluster with no warning about which cluster it touched.
//!
//! Contract driven here, black box through the real `curie` binary with fake
//! `kubectl`/`helm` on PATH:
//! - `--context <NAME>` is resolved by reading the kubeconfig files, with no process
//!   spawned, BEFORE any mutation; an unknown name refuses with zero helm and zero
//!   kubectl calls (the fake logs every call, `config view` included).
//! - every helm/kubectl child sees a KUBECONFIG whose FIRST entry sets
//!   `current-context: <NAME>` and `HELM_KUBECONTEXT=<NAME>` (overriding ambient).
//! - stderr names the context and its cluster.
//! - with no `--context`, the current-context is pinned and named.
//!
//! Env is set per child `Command` (never process env), so tests run in parallel.

use crate::support::executable as test_executable;

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const LOG_PREFIX: &str = r#"#!/bin/sh
first="${KUBECONFIG%%:*}"
cur=""
if [ -n "$first" ] && [ -f "$first" ]; then
  cur=$(grep -E '^current-context:' "$first" | head -n1 | sed -e 's/^current-context:[[:space:]]*//' -e 's/["'"'"']//g')
fi
# The pin only selects a context; the operator's kubeconfig (clusters, users,
# exec credentials) must stay in the list behind it or real kubectl has no cluster.
case ":${KUBECONFIG#*:}:" in
  *":$CURIE_TEST_ORIGINAL_KUBECONFIG:"*) ;;
  *) cur="original-kubeconfig-dropped" ;;
esac
"#;

fn install_fakes(bin_dir: &Path, log: &Path) {
    let log = log.display();
    let kubectl = format!(
        r#"{LOG_PREFIX}printf 'kubectl\t%s\t%s\t%s\n' "$cur" "$HELM_KUBECONTEXT" "$*" >> '{log}'
if [ "$1" = get ] && [ "$2" = namespace ]; then
  echo '{{"apiVersion":"v1","kind":"Namespace","metadata":{{"name":"curie","labels":{{}},"uid":"uid-curie","resourceVersion":"17"}}}}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = jobs ]; then
  echo '{{"apiVersion":"v1","kind":"List","items":[]}}'
  exit 0
fi
echo 'namespace "x" deleted'
exit 0
"#
    );
    let helm = format!(
        r#"{LOG_PREFIX}printf 'helm\t%s\t%s\t%s\n' "$cur" "$HELM_KUBECONTEXT" "$*" >> '{log}'
echo 'release "curie" uninstalled'
exit 0
"#
    );
    test_executable::install_in(bin_dir, "kubectl", &kubectl);
    test_executable::install_in(bin_dir, "helm", &helm);
}

struct Run {
    out: Output,
    log: Vec<(String, String, String, String)>,
}

impl Run {
    fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.out.stderr).into_owned()
    }
}

fn run_curie(args: &[&str], ambient_helm_ctx: Option<&str>) -> Run {
    let tmp = tempfile::tempdir().expect("tempdir");
    let home = tmp.path().join("home");
    let bin_dir = tmp.path().join("bin");
    fs::create_dir_all(home.join(".kube")).unwrap();
    fs::create_dir_all(&bin_dir).unwrap();
    let kubeconfig = home.join(".kube").join("config");
    fs::write(
        &kubeconfig,
        "apiVersion: v1\nkind: Config\ncurrent-context: prod-ctx\ncontexts:\n- name: prod-ctx\n  context:\n    cluster: prod-cluster\n    user: prod-user\n- name: test-ctx\n  context:\n    cluster: test-cluster\n    user: test-user\nclusters: []\nusers: []\n",
    )
    .unwrap();
    let log = tmp.path().join("calls.log");
    install_fakes(&bin_dir, &log);

    let mut paths = vec![bin_dir.clone()];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_curie"));
    cmd.args(args)
        .current_dir(tmp.path())
        .env("PATH", std::env::join_paths(paths).unwrap())
        .env("HOME", &home)
        .env("KUBECONFIG", &kubeconfig)
        .env("CURIE_TEST_ORIGINAL_KUBECONFIG", &kubeconfig)
        .env("TMPDIR", tmp.path())
        .env_remove("HELM_KUBECONTEXT");
    if let Some(ctx) = ambient_helm_ctx {
        cmd.env("HELM_KUBECONTEXT", ctx);
    }
    let out = cmd.output().expect("spawn curie");
    let log = fs::read_to_string(&log)
        .unwrap_or_default()
        .lines()
        .map(|l| {
            let mut f = l.splitn(4, '\t').map(str::to_string);
            (
                f.next().unwrap_or_default(),
                f.next().unwrap_or_default(),
                f.next().unwrap_or_default(),
                f.next().unwrap_or_default(),
            )
        })
        .collect();
    Run { out, log }
}

fn assert_all_pinned(run: &Run, ctx: &str) {
    let helm = run.log.iter().filter(|l| l.0 == "helm").count();
    let kubectl = run.log.iter().filter(|l| l.0 == "kubectl").count();
    assert!(
        helm >= 1 && kubectl >= 1,
        "expected helm and kubectl calls; log={:?} stderr={}",
        run.log,
        run.stderr()
    );
    for (prog, cur, helm_ctx, args) in &run.log {
        assert_eq!(
            cur, ctx,
            "{prog} {args}: first KUBECONFIG entry must pin {ctx}"
        );
        assert_eq!(
            helm_ctx, ctx,
            "{prog} {args}: HELM_KUBECONTEXT must be {ctx}"
        );
    }
}

#[test]
fn explicit_context_pins_every_helm_and_kubectl_call() {
    for args in [
        ["cluster", "--context", "test-ctx", "down", "--yes"],
        ["cluster", "down", "--yes", "--context", "test-ctx"],
    ] {
        let run = run_curie(&args, None);
        assert!(
            run.out.status.success(),
            "{args:?} exit={:?} stderr={}",
            run.out.status,
            run.stderr()
        );
        assert_all_pinned(&run, "test-ctx");
        let err = run.stderr();
        assert!(
            err.contains("test-ctx"),
            "stderr must name the context: {err}"
        );
        assert!(
            err.contains("test-cluster"),
            "stderr must name the cluster: {err}"
        );
    }
}

#[test]
fn unknown_context_refuses_before_any_mutation() {
    let run = run_curie(&["cluster", "down", "--yes", "--context", "nope"], None);
    assert!(!run.out.status.success(), "unknown context must fail");
    assert!(run.stderr().contains("nope"), "stderr: {}", run.stderr());
    assert!(
        run.log.is_empty(),
        "no helm or non-config kubectl call may run: {:?}",
        run.log
    );
}

#[test]
fn explicit_context_overrides_ambient_helm_kubecontext() {
    let run = run_curie(
        &["cluster", "down", "--yes", "--context", "test-ctx"],
        Some("prod-ctx"),
    );
    assert!(run.out.status.success(), "stderr={}", run.stderr());
    let helm: Vec<_> = run.log.iter().filter(|l| l.0 == "helm").collect();
    assert!(!helm.is_empty(), "helm must run: {:?}", run.log);
    for (_, cur, helm_ctx, args) in helm {
        assert_eq!(helm_ctx, "test-ctx", "helm {args} kept ambient context");
        assert_eq!(cur, "test-ctx", "helm {args} kubeconfig not pinned");
    }
}

#[test]
fn no_context_pins_current_context() {
    let run = run_curie(&["cluster", "down", "--yes"], None);
    assert!(run.out.status.success(), "stderr={}", run.stderr());
    assert_all_pinned(&run, "prod-ctx");
    assert!(
        run.stderr().contains("prod-ctx"),
        "stderr: {}",
        run.stderr()
    );
}

// #2864: `cluster status` with no resolvable context and no cluster access must say
// that no context resolved and suggest `--context`, not blame convergence and
// recommend a mutating `cluster up`.

const NO_CURRENT_CONTEXT: &str = "apiVersion: v1\nkind: Config\ncontexts:\n- name: test-ctx\n  context:\n    cluster: test-cluster\n    user: test-user\nclusters: []\nusers: []\n";

/// Fakes where the cluster is unreachable (`unreachable`) or reachable with a
/// deployed release whose only pod is not ready.
fn install_status_fakes(bin_dir: &Path, log: &Path, unreachable: bool) {
    let log = log.display();
    let body = if unreachable {
        "echo 'The connection to the server localhost:8080 was refused' >&2\nexit 1\n".to_string()
    } else {
        r#"if [ "$1" = status ]; then
  printf 'NAME: curie\nSTATUS: deployed\nREVISION: 3\nCHART: curie-0.9.2\n'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = pods ]; then
  echo '{"items":[{"metadata":{"name":"curie-api-0"},"status":{"phase":"Running","containerStatuses":[{"name":"api","ready":false,"restartCount":4,"state":{"waiting":{"reason":"CrashLoopBackOff"}}}]}}]}'
  exit 0
fi
echo 'error: not faked' >&2
exit 1
"#
        .to_string()
    };
    for prog in ["kubectl", "helm"] {
        test_executable::install_in(
            bin_dir,
            prog,
            &format!("#!/bin/sh\nprintf '{prog}\\t%s\\n' \"$*\" >> '{log}'\n{body}"),
        );
    }
}

fn run_status(extra: &[&str], unreachable: bool) -> Output {
    let tmp = tempfile::tempdir().expect("tempdir");
    let home = tmp.path().join("home");
    let bin_dir = tmp.path().join("bin");
    fs::create_dir_all(home.join(".kube")).unwrap();
    fs::create_dir_all(&bin_dir).unwrap();
    let kubeconfig = home.join(".kube").join("config");
    fs::write(&kubeconfig, NO_CURRENT_CONTEXT).unwrap();
    install_status_fakes(&bin_dir, &tmp.path().join("calls.log"), unreachable);
    let mut paths = vec![bin_dir];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    let mut args = vec!["cluster"];
    args.extend_from_slice(extra);
    args.extend_from_slice(&["status", "--namespace", "curie", "--release", "curie"]);
    let out = Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(&args)
        .current_dir(tmp.path())
        .env("PATH", std::env::join_paths(paths).unwrap())
        .env("HOME", &home)
        .env("KUBECONFIG", &kubeconfig)
        .env("TMPDIR", tmp.path())
        .env_remove("HELM_KUBECONTEXT")
        .output()
        .expect("spawn curie");
    let after = fs::read_to_string(&kubeconfig).unwrap();
    assert_eq!(
        after, NO_CURRENT_CONTEXT,
        "the kubeconfig must not be altered"
    );
    out
}

#[test]
fn status_without_resolvable_context_suggests_context_not_cluster_up() {
    let out = run_status(&[], true);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "must fail: {err}");
    assert!(
        err.contains("no Kubernetes context"),
        "names the missing context: {err}"
    );
    assert!(err.contains("--context"), "suggests --context: {err}");
    assert!(err.contains("test-ctx"), "lists available contexts: {err}");
    assert!(
        !err.contains("cluster up"),
        "must not suggest an install: {err}"
    );
    assert!(
        !err.contains("has not converged"),
        "must not blame convergence: {err}"
    );
}

#[test]
fn status_with_explicit_context_keeps_the_convergence_diagnosis() {
    let out = run_status(&["--context", "test-ctx"], true);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "unreachable cluster must still fail: {err}"
    );
    assert!(err.contains("has not converged"), "{err}");
    assert!(!err.contains("no Kubernetes context"), "{err}");
}

#[test]
fn status_reachable_unhealthy_release_without_context_is_a_convergence_failure() {
    let out = run_status(&[], false);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "unhealthy release must fail: {err}");
    assert!(err.contains("has not converged"), "{err}");
    assert!(!err.contains("no Kubernetes context"), "{err}");
}

// #3568: `curie example sre-bot` spawns helm and kubectl against a cluster too, so it
// must pin the context exactly like `curie cluster`.

#[test]
fn example_sre_bot_explicit_context_pins_every_helm_and_kubectl_call() {
    for args in [
        [
            "example",
            "--context",
            "test-ctx",
            "sre-bot",
            "provision-observability",
            "--chart",
            ".",
        ],
        [
            "example",
            "sre-bot",
            "provision-observability",
            "--chart",
            ".",
            "--context",
            "test-ctx",
        ],
    ] {
        let run = run_curie(&args, Some("prod-ctx"));
        // The fake kubectl returns no node list, so the capacity preflight stops the
        // run after the first helm and kubectl calls; both must already be pinned.
        assert_all_pinned(&run, "test-ctx");
        let err = run.stderr();
        assert!(
            err.contains("test-ctx"),
            "stderr must name the context: {err}"
        );
        assert!(
            err.contains("test-cluster"),
            "stderr must name the cluster: {err}"
        );
    }
}

#[test]
fn example_sre_bot_without_context_pins_current_context() {
    let run = run_curie(
        &[
            "example",
            "sre-bot",
            "provision-observability",
            "--chart",
            ".",
        ],
        Some("test-ctx"),
    );
    assert_all_pinned(&run, "prod-ctx");
}

#[test]
fn example_sre_bot_unknown_context_refuses_before_any_call() {
    let run = run_curie(
        &[
            "example",
            "sre-bot",
            "provision-observability",
            "--chart",
            ".",
            "--context",
            "nope",
        ],
        None,
    );
    assert!(!run.out.status.success(), "unknown context must fail");
    assert!(run.stderr().contains("nope"), "stderr: {}", run.stderr());
    assert!(
        run.log.is_empty(),
        "no helm or kubectl call may run: {:?}",
        run.log
    );
}

#[test]
fn example_sre_bot_dry_run_names_the_context_and_prints_the_argv() {
    let run = run_curie(
        &[
            "--json",
            "example",
            "sre-bot",
            "install",
            "--observability-only",
            "--dry-run",
            "--context",
            "test-ctx",
        ],
        None,
    );
    assert!(run.out.status.success(), "stderr={}", run.stderr());
    assert!(
        run.log.is_empty(),
        "a dry run spawns nothing: {:?}",
        run.log
    );
    let err = run.stderr();
    assert!(
        err.contains("test-ctx") && err.contains("test-cluster"),
        "{err}"
    );
    let plan: serde_json::Value = serde_json::from_slice(&run.out.stdout).expect("JSON plan");
    let lines: Vec<&str> = plan["plan"]
        .as_array()
        .expect("plan lines")
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect();
    for expected in [
        "helm --kube-context test-ctx repo add grafana https://grafana.github.io/helm-charts --force-update",
        "kubectl --context test-ctx rollout status statefulset/tempo --namespace observability --timeout=10m",
    ] {
        assert!(
            lines.contains(&expected),
            "plan must carry the argv {expected:?}: {lines:#?}"
        );
    }
}

fn run_context_case(
    args: &[&str],
    current: Option<&str>,
    state: Option<&serde_json::Value>,
    failure: &str,
) -> Run {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let bin_dir = tmp.path().join("bin");
    fs::create_dir_all(home.join(".kube")).unwrap();
    fs::create_dir_all(&bin_dir).unwrap();
    let kubeconfig = home.join(".kube/config");
    let body = NO_CURRENT_CONTEXT.replace("test-ctx", "pin-ctx");
    let body = body.replace(
        "clusters: []\nusers: []\n",
        "- name: other-ctx\n  context:\n    cluster: other-cluster\n    user: other-user\nclusters: []\nusers: []\n",
    );
    let body = match current {
        Some(name) => format!("current-context: {name}\n{body}"),
        None => body,
    };
    fs::write(&kubeconfig, &body).unwrap();
    if let Some(state) = state {
        fs::create_dir(tmp.path().join(".curie")).unwrap();
        fs::write(tmp.path().join(".curie/last-turn.json"), state.to_string()).unwrap();
    }
    let log = tmp.path().join("calls.log");
    // Reuse the external refusal recorded by the #2864 status fixtures above.
    for program in ["helm", "kubectl"] {
        test_executable::install_in(
            &bin_dir,
            program,
            &format!(
                r#"{LOG_PREFIX}printf '{program}\t%s\t%s\t%s\n' "$cur" "$HELM_KUBECONTEXT" "$*" >> '{}'
if [ -z "$CURIE_TEST_CLUSTER_FAILURE" ]; then exit 0; fi
echo "$CURIE_TEST_CLUSTER_FAILURE" >&2
exit 1
"#,
                log.display(),
            ),
        );
    }
    let mut paths = vec![bin_dir];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    let out = Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(args)
        .arg("--chart")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("../charts/curie"))
        .current_dir(tmp.path())
        .env("PATH", std::env::join_paths(paths).unwrap())
        .env("HOME", &home)
        .env("TMPDIR", tmp.path())
        .env("KUBECONFIG", &kubeconfig)
        .env("CURIE_TEST_ORIGINAL_KUBECONFIG", &kubeconfig)
        .env("CURIE_TEST_CLUSTER_FAILURE", failure)
        .env("NO_COLOR", "1")
        .env_remove("CURIE_API_KEY")
        .env_remove("CURIE_VALKEY_PASSWORD")
        .env_remove("CURIE_MODEL_CREDENTIALS")
        .env_remove("HELM_KUBECONTEXT")
        .output()
        .expect("spawn curie");
    assert_eq!(fs::read_to_string(kubeconfig).unwrap(), body);
    let log = fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .map(|line| {
            let mut fields = line.splitn(4, '\t').map(str::to_string);
            (
                fields.next().unwrap(),
                fields.next().unwrap(),
                fields.next().unwrap(),
                fields.next().unwrap(),
            )
        })
        .collect();
    Run { out, log }
}

fn recorded_context_turn(context: Option<&str>, verb: &str) -> serde_json::Value {
    let mut state = serde_json::json!({
        "verb": verb, "channel": "C0EXAMPLE1", "thread_ts": "9.9",
        "namespace": "recorded-ns", "release": "recorded-release",
        "chart": "charts/curie", "listen_host": null, "timeout_secs": 1,
        "api_url": null, "api_key_env": null,
    });
    if let Some(context) = context {
        state["context"] = context.into();
    }
    state
}

#[test]
fn cluster_up_dry_run_prints_pinned_context() {
    let run = run_context_case(
        &[
            "cluster",
            "up",
            "--context",
            "pin-ctx",
            "--namespace",
            "n",
            "--release",
            "r",
            "--dry-run",
        ],
        None,
        None,
        "unexpected external command",
    );
    assert!(run.out.status.success(), "{}", run.stderr());
    let stdout = String::from_utf8_lossy(&run.out.stdout);
    let helm: Vec<_> = stdout
        .lines()
        .filter(|line| line.starts_with("helm "))
        .collect();
    assert!(!helm.is_empty(), "missing Helm plan: {stdout}");
    for line in helm {
        assert!(
            line.contains("--kube-context pin-ctx"),
            "Helm plan lost pinned context: {line}"
        );
    }
    assert!(
        run.log.is_empty(),
        "dry run spawned commands: {:?}",
        run.log
    );
}

#[test]
fn dry_run_context_kubectl_and_json() {
    let secret_values_file = regex::Regex::new(r"<secret values file: [^>]+>").unwrap();
    let generated_secret = regex::Regex::new(concat!(
        r"((?:postgres\.auth\.password|valkey\.password|",
        r"clickhouse\.auth\.password|rustfs\.auth\.rootPassword|",
        r"langfuse\.(?:salt|encryptionKey|nextauthSecret)|",
        r"api\.(?:apiKey|githubWebhookSecret))=)[0-9a-f]{8}\*{3}",
    ))
    .unwrap();
    let mut plans = Vec::new();
    for json in [false, true] {
        let mut args = vec![
            "cluster",
            "up",
            "--context",
            "pin-ctx",
            "--namespace",
            "n",
            "--release",
            "r",
            "--dry-run",
        ];
        if json {
            args.insert(0, "--json");
        }
        let run = run_context_case(&args, None, None, "unexpected external command");
        assert!(run.out.status.success(), "{}", run.stderr());
        assert!(
            run.log.is_empty(),
            "dry run spawned commands: {:?}",
            run.log
        );
        let lines: Vec<String> = if json {
            let value: serde_json::Value = serde_json::from_slice(&run.out.stdout).unwrap();
            value["plan"]
                .as_array()
                .unwrap()
                .iter()
                .map(|line| line.as_str().unwrap().to_string())
                .collect()
        } else {
            String::from_utf8(run.out.stdout)
                .unwrap()
                .lines()
                .map(str::to_string)
                .collect()
        };
        let mut commands = Vec::new();
        for (program, flag) in [
            ("helm ", "--kube-context pin-ctx"),
            ("kubectl ", "--context pin-ctx"),
        ] {
            let matching: Vec<_> = lines
                .iter()
                .filter(|line| line.starts_with(program))
                .collect();
            assert!(!matching.is_empty(), "missing {program} plan: {lines:?}");
            for line in matching {
                assert!(line.contains(flag), "missing {flag}: {line}");
                commands.push(
                    secret_values_file
                        .replace_all(line, |placeholder: &regex::Captures<'_>| {
                            generated_secret
                                .replace_all(&placeholder[0], "${1}GENERATED***")
                                .into_owned()
                        })
                        .into_owned(),
                );
            }
        }
        plans.push(commands);
    }
    assert_eq!(plans[0], plans[1], "human and JSON commands must agree");
}

#[test]
fn continue_uses_recorded_context_before_secret_discovery() {
    for (current, explicit, recorded, expected) in [
        (None, None, Some("pin-ctx"), "pin-ctx"),
        (Some("other-ctx"), None, Some("pin-ctx"), "pin-ctx"),
        (None, Some("other-ctx"), Some("pin-ctx"), "other-ctx"),
        (None, Some("other-ctx"), Some("unknown-ctx"), "other-ctx"),
        (Some("other-ctx"), None, None, "other-ctx"),
    ] {
        let state = recorded_context_turn(recorded, "cluster");
        let mut args = vec!["cluster", "message", "--continue", "probe"];
        if let Some(context) = explicit {
            args.extend(["--context", context]);
        }
        let run = run_context_case(
            &args,
            current,
            Some(&state),
            "The connection to the server localhost:8080 was refused",
        );
        assert!(!run.out.status.success());
        assert!(
            run.stderr().contains("could not inspect Helm state"),
            "{}",
            run.stderr()
        );
        assert_all_pinned(&run, expected);
        assert!(
            run.log.iter().any(|call| call.3.contains("secret")),
            "must reach credential discovery: {:?}",
            run.log
        );
    }
    for (context, verb, diagnostic) in [
        ("unknown-ctx", "cluster", "unknown-ctx"),
        ("pin-ctx", "local", "last turn was 'local message'"),
    ] {
        let state = recorded_context_turn(Some(context), verb);
        let run = run_context_case(
            &["cluster", "message", "--continue", "probe"],
            None,
            Some(&state),
            "unexpected external command",
        );
        assert!(!run.out.status.success());
        assert!(run.stderr().contains(diagnostic), "{}", run.stderr());
        assert!(
            run.log.is_empty(),
            "refusal must precede cluster children: {:?}",
            run.log
        );
    }
}

#[test]
fn cluster_message_without_context_suggests_context() {
    for explicit_credentials in [false, true] {
        let mut args = vec!["cluster", "message", "probe"];
        if explicit_credentials {
            args.extend([
                "--api-key",
                "fixture-api",
                "--valkey-password",
                "fixture-valkey",
            ]);
        }
        let run = run_context_case(
            &args,
            None,
            None,
            "The connection to the server localhost:8080 was refused",
        );
        let err = run.stderr();
        assert!(!run.out.status.success(), "{err}");
        assert!(err.contains("no Kubernetes context resolved (the kubeconfig has no current-context) and the cluster could not be reached"), "{err}");
        assert!(
            err.contains("rerun with an explicit context: `curie cluster --context <name> status`"),
            "{err}"
        );
        assert!(
            err.contains("pin-ctx") && err.contains("other-ctx"),
            "{err}"
        );
        assert!(
            !run.log.is_empty(),
            "diagnosis must follow a cluster attempt"
        );
    }
}

#[test]
fn unresolved_context_message_controls_preserve_resolved_and_rbac_errors() {
    for explicit_credentials in [false, true] {
        for (context, failure) in [
            (
                Some("pin-ctx"),
                "The connection to the server localhost:8080 was refused",
            ),
            (
                None,
                "Error from server (Forbidden): deployments.apps is forbidden",
            ),
        ] {
            let mut args = vec!["cluster", "message", "probe"];
            if explicit_credentials {
                args.extend([
                    "--api-key",
                    "fixture-api",
                    "--valkey-password",
                    "fixture-valkey",
                ]);
            }
            if let Some(context) = context {
                args.extend(["--context", context]);
            }
            let run = run_context_case(&args, None, None, failure);
            let err = run.stderr();
            assert!(!run.out.status.success(), "{err}");
            assert!(err.contains(failure), "original error lost: {err}");
            assert!(!err.contains("no Kubernetes context resolved"), "{err}");
            assert!(!run.log.is_empty());
        }
    }
}

#[test]
fn unresolved_context_reachable_bootstrap_reaches_port_forward() {
    let run = run_context_case(
        &[
            "cluster",
            "message",
            "probe",
            "--api-key",
            "fixture-api",
            "--valkey-password",
            "fixture-valkey",
        ],
        None,
        None,
        "",
    );
    let err = run.stderr();
    assert!(
        !run.out.status.success(),
        "the fixture ends at port-forward readiness"
    );
    assert!(
        run.log.iter().any(|call| call.3.contains("port-forward")),
        "bootstrap must reach relay setup: {:?}; {err}",
        run.log
    );
    assert!(
        err.contains("kubectl closed stdout before reporting"),
        "{err}"
    );
    assert!(!err.contains("no Kubernetes context resolved"), "{err}");
}
