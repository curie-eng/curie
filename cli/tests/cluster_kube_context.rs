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
//! - `--context <NAME>` is resolved against `kubectl config view -o json` BEFORE any
//!   mutation; an unknown name refuses with zero helm and zero non-config kubectl calls.
//! - every helm/kubectl child sees a KUBECONFIG whose FIRST entry sets
//!   `current-context: <NAME>` and `HELM_KUBECONTEXT=<NAME>` (overriding ambient).
//! - stderr names the context and its cluster.
//! - with no `--context`, the current-context is pinned and named.
//!
//! Env is set per child `Command` (never process env), so tests run in parallel.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output};

fn write_exec(dir: &Path, name: &str, body: &str) {
    let path = dir.join(name);
    fs::write(&path, body).expect("write fake");
    let mut perms = fs::metadata(&path).expect("stat fake").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&path, perms).expect("chmod fake");
}

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
        r#"{LOG_PREFIX}if [ "$1" = config ] && [ "$2" = view ]; then
  cat <<'JSON'
{{"apiVersion":"v1","kind":"Config","current-context":"prod-ctx","contexts":[{{"name":"prod-ctx","context":{{"cluster":"prod-cluster","user":"prod-user"}}}},{{"name":"test-ctx","context":{{"cluster":"test-cluster","user":"test-user"}}}}],"clusters":[{{"name":"prod-cluster","cluster":{{"server":"https://prod:6443"}}}},{{"name":"test-cluster","cluster":{{"server":"https://test:6443"}}}}],"users":[]}}
JSON
  exit 0
fi
printf 'kubectl\t%s\t%s\t%s\n' "$cur" "$HELM_KUBECONTEXT" "$*" >> '{log}'
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
    write_exec(bin_dir, "kubectl", &kubectl);
    write_exec(bin_dir, "helm", &helm);
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
        "apiVersion: v1\nkind: Config\ncurrent-context: prod-ctx\ncontexts: []\nclusters: []\nusers: []\n",
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
