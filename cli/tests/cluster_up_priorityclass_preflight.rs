//! Binary integration contract for the PriorityClass ownership preflight in
//! issue #1568. Every case drives the real `curie cluster up` entrypoint with
//! recording Helm and kubectl executables on PATH.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const TARGET_RELEASE: &str = "target-release";
const TARGET_NAMESPACE: &str = "target-namespace";
const DEFAULT_PLATFORM: &str = "curie-platform";
const DEFAULT_SANDBOX: &str = "curie-sandbox";

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
    upgrade_log: PathBuf,
    query_log: PathBuf,
    controller_query_log: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("temporary directory");
        let bin_dir = temp.path().join("bin");
        fs::create_dir(&bin_dir).expect("create fake binary directory");
        let upgrade_log = temp.path().join("upgrades.log");
        let query_log = temp.path().join("priorityclass-queries.log");
        let controller_query_log = temp.path().join("controller-queries.log");

        write_exec(
            &bin_dir,
            "helm",
            r#"#!/bin/sh
if [ "$1" = "get" ] && [ "$2" = "values" ]; then
    printf '%s\n' 'Error: release: not found' >&2
    exit 1
fi

if [ "$1" = "template" ]; then
    platform_name="curie-platform"
    sandbox_name="curie-sandbox"
    platform_create="true"
    sandbox_create="true"
    show_priorityclass="false"
    show_gvisor_preflight="false"
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --set|--set-string)
                shift
                case "$1" in
                    priorityClasses.platform.name=*) platform_name=${1#*=} ;;
                    priorityClasses.sandbox.name=*) sandbox_name=${1#*=} ;;
                    priorityClasses.platform.create=*) platform_create=${1#*=} ;;
                    priorityClasses.sandbox.create=*) sandbox_create=${1#*=} ;;
                esac
                ;;
            --show-only)
                shift
                if [ "$1" = "templates/priorityclass.yaml" ]; then
                    show_priorityclass="true"
                elif [ "$1" = "templates/preflight-gvisor.yaml" ]; then
                    show_gvisor_preflight="true"
                fi
                ;;
            --show-only=templates/priorityclass.yaml)
                show_priorityclass="true"
                ;;
            --show-only=templates/preflight-gvisor.yaml)
                show_gvisor_preflight="true"
                ;;
        esac
        shift
    done

    if [ "$show_gvisor_preflight" = "true" ]; then
        printf '%s\n' 'Error: could not find template templates/preflight-gvisor.yaml in chart' >&2
        exit 1
    fi

    first="true"
    if [ "$platform_create" = "true" ]; then
        printf '%s\n' \
            'apiVersion: scheduling.k8s.io/v1' \
            'kind: PriorityClass' \
            'metadata:' \
            "  name: $platform_name" \
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
            "  name: $sandbox_name" \
            'value: 100000' \
            'globalDefault: false'
        first="false"
    fi
    if [ "$first" = "true" ] && [ "$show_priorityclass" = "true" ]; then
        printf '%s\n' 'Error: could not find template templates/priorityclass.yaml in chart' >&2
        exit 1
    fi
    exit 0
fi

if [ "$1" = "upgrade" ] && [ "$2" = "--install" ]; then
    printf '%s\n' "$*" >> "$CURIE_TEST_UPGRADE_LOG"
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
        printf '%s\n' "$*" >> "$CURIE_TEST_CONTROLLER_QUERY_LOG"
        case "$CURIE_TEST_CONTROLLER_MODE" in
            absent)
                exit 0
                ;;
            malformed)
                printf '%s\n' '{"metadata":'
                exit 0
                ;;
            incomplete)
                printf '%s\n' '{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"agent-sandbox-controller","labels":{"app.kubernetes.io/managed-by":"Helm"}}}'
                exit 0
                ;;
            failure)
                printf '%s\n' 'Error from server (Forbidden): deployments.apps "agent-sandbox-controller" is forbidden' >&2
                exit 1
                ;;
            same)
                owner_release="$CURIE_TEST_TARGET_RELEASE"
                owner_namespace="$CURIE_TEST_TARGET_NAMESPACE"
                ;;
            foreign)
                owner_release="controller-owner"
                owner_namespace="controller-owner-namespace"
                ;;
            *)
                printf 'unknown controller response mode: %s\n' "$CURIE_TEST_CONTROLLER_MODE" >&2
                exit 64
                ;;
        esac
        printf '{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"agent-sandbox-controller","labels":{"app.kubernetes.io/managed-by":"Helm"},"annotations":{"meta.helm.sh/release-name":"%s","meta.helm.sh/release-namespace":"%s"}}}\n' \
            "$owner_release" "$owner_namespace"
        exit 0
        ;;
esac

if [ "$1" != "get" ] || [ "$2" != "priorityclass" ] || [ -z "$3" ]; then
    printf 'unexpected kubectl invocation: %s\n' "$*" >&2
    exit 64
fi

name="$3"
printf '%s\n' "$name" >> "$CURIE_TEST_QUERY_LOG"
if [ "$name" = "$CURIE_TEST_PLATFORM_NAME" ]; then
    mode="$CURIE_TEST_PLATFORM_MODE"
    foreign_release="platform-owner"
    foreign_namespace="platform-owner-namespace"
elif [ "$name" = "$CURIE_TEST_SANDBOX_NAME" ]; then
    mode="$CURIE_TEST_SANDBOX_MODE"
    foreign_release="sandbox-owner"
    foreign_namespace="sandbox-owner-namespace"
else
    printf 'unexpected PriorityClass query: %s\n' "$name" >&2
    exit 64
fi

case "$mode" in
    absent)
        exit 0
        ;;
    unmanaged)
        printf '{"apiVersion":"scheduling.k8s.io/v1","kind":"PriorityClass","metadata":{"name":"%s"}}\n' \
            "$name"
        exit 0
        ;;
    helm-without-annotations)
        printf '{"apiVersion":"scheduling.k8s.io/v1","kind":"PriorityClass","metadata":{"name":"%s","labels":{"app.kubernetes.io/managed-by":"Helm"}}}\n' \
            "$name"
        exit 0
        ;;
    helm-without-release-namespace)
        printf '{"apiVersion":"scheduling.k8s.io/v1","kind":"PriorityClass","metadata":{"name":"%s","labels":{"app.kubernetes.io/managed-by":"Helm"},"annotations":{"meta.helm.sh/release-name":"%s"}}}\n' \
            "$name" "$foreign_release"
        exit 0
        ;;
    malformed)
        printf '%s\n' '{"metadata":'
        exit 0
        ;;
    failure)
        printf 'Error from server (Forbidden): priorityclasses.scheduling.k8s.io "%s" is forbidden: User "test-user" cannot get resource "priorityclasses" at the cluster scope\n' "$name" >&2
        exit 1
        ;;
    same)
        owner_release="$CURIE_TEST_TARGET_RELEASE"
        owner_namespace="$CURIE_TEST_TARGET_NAMESPACE"
        ;;
    wrong-namespace)
        owner_release="$CURIE_TEST_TARGET_RELEASE"
        owner_namespace="another-namespace"
        ;;
    foreign)
        owner_release="$foreign_release"
        owner_namespace="$foreign_namespace"
        ;;
    *)
        printf 'unknown PriorityClass response mode: %s\n' "$mode" >&2
        exit 64
        ;;
esac

# Issue #1568 records Helm rejecting the release-name annotation. This fixture
# supplies the full Helm ownership tuple: managed-by, release-name, and release-namespace.
printf '{"apiVersion":"scheduling.k8s.io/v1","kind":"PriorityClass","metadata":{"name":"%s","labels":{"app.kubernetes.io/managed-by":"Helm"},"annotations":{"meta.helm.sh/release-name":"%s","meta.helm.sh/release-namespace":"%s"}}}\n' \
    "$name" "$owner_release" "$owner_namespace"
exit 0
"#,
        );

        Self {
            _temp: temp,
            bin_dir,
            upgrade_log,
            query_log,
            controller_query_log,
        }
    }

    fn run(
        &self,
        platform_name: &str,
        platform_mode: &str,
        sandbox_name: &str,
        sandbox_mode: &str,
        controller_mode: &str,
        extra: &[&str],
    ) -> Output {
        let mut paths = vec![self.bin_dir.clone()];
        if let Some(current) = std::env::var_os("PATH") {
            paths.extend(std::env::split_paths(&current));
        }
        let path = std::env::join_paths(paths).expect("join PATH");

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
            "--fake-model",
        ];
        args.extend_from_slice(extra);

        Command::new(bin())
            .args(args)
            .env("PATH", path)
            .env("CI", "1")
            .env("TERM", "dumb")
            .env("NO_COLOR", "1")
            .env("CURIE_TEST_UPGRADE_LOG", &self.upgrade_log)
            .env("CURIE_TEST_QUERY_LOG", &self.query_log)
            .env(
                "CURIE_TEST_CONTROLLER_QUERY_LOG",
                &self.controller_query_log,
            )
            .env("CURIE_TEST_CONTROLLER_MODE", controller_mode)
            .env("CURIE_TEST_TARGET_RELEASE", TARGET_RELEASE)
            .env("CURIE_TEST_TARGET_NAMESPACE", TARGET_NAMESPACE)
            .env("CURIE_TEST_PLATFORM_NAME", platform_name)
            .env("CURIE_TEST_PLATFORM_MODE", platform_mode)
            .env("CURIE_TEST_SANDBOX_NAME", sandbox_name)
            .env("CURIE_TEST_SANDBOX_MODE", sandbox_mode)
            .env_remove("CURIE_CREDENTIALS")
            .env_remove("CURIE_MODEL_CREDENTIALS")
            .env_remove("CURIE_GITHUB_TOKEN")
            .env_remove("CURIE_MODEL")
            .output()
            .expect("run curie cluster up")
    }

    fn upgrade_count(&self) -> usize {
        fs::read_to_string(&self.upgrade_log)
            .unwrap_or_default()
            .lines()
            .count()
    }

    fn queries(&self) -> Vec<String> {
        fs::read_to_string(&self.query_log)
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    fn controller_query_count(&self) -> usize {
        fs::read_to_string(&self.controller_query_log)
            .unwrap_or_default()
            .lines()
            .count()
    }

    fn upgrade_log(&self) -> String {
        fs::read_to_string(&self.upgrade_log).unwrap_or_default()
    }
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn assert_failed_before_upgrade(fixture: &Fixture, output: &Output) -> String {
    let shown = stderr(output);
    assert_eq!(
        output.status.code(),
        Some(1),
        "the ownership preflight must be a runtime failure\nstdout:\n{}\nstderr:\n{shown}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert_eq!(
        fixture.upgrade_count(),
        0,
        "helm upgrade --install must not run after a failed preflight\nstderr:\n{shown}"
    );
    shown
}

fn assert_upgrade_ran_once(fixture: &Fixture, output: &Output) {
    assert!(
        output.status.success(),
        "cluster up failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        stderr(output)
    );
    assert_eq!(
        fixture.upgrade_count(),
        1,
        "helm upgrade --install must run exactly once"
    );
}

fn assert_inference_once(shown: &str, applied_override: &str) {
    assert_eq!(
        shown.matches(applied_override).count(),
        1,
        "the applied override must be disclosed exactly once: {applied_override}\n{shown}"
    );
}

fn assert_read_recovery_hint(shown: &str, class_name: &str) {
    let lower = shown.to_ascii_lowercase();
    assert!(
        shown.contains(class_name),
        "the read error must name the PriorityClass: {shown}"
    );
    assert!(
        lower.contains("run `curie cluster status`"),
        "the read error must tell the operator to run the Curie cluster status command: {shown}"
    );
}

#[test]
fn foreign_owners_for_both_roles_are_reused_with_disclosed_overrides() {
    let fixture = Fixture::new();
    let output = fixture.run(
        "shared-platform",
        "foreign",
        "shared-sandbox",
        "foreign",
        "absent",
        &[
            "--set",
            "priorityClasses.platform.name=shared-platform",
            "--set",
            "priorityClasses.sandbox.name=shared-sandbox",
        ],
    );
    assert_upgrade_ran_once(&fixture, &output);
    let shown = stderr(&output);
    assert_inference_once(&shown, "--set priorityClasses.platform.create=false");
    assert_inference_once(&shown, "--set priorityClasses.sandbox.create=false");
    let upgrade = fixture.upgrade_log();
    assert!(
        upgrade.contains("priorityClasses.platform.create=false")
            && upgrade.contains("priorityClasses.sandbox.create=false"),
        "the applied reuse values must reach Helm:\n{upgrade}"
    );
    assert_eq!(
        fixture.queries(),
        ["shared-platform", "shared-sandbox"],
        "the preflight must inspect both rendered classes before converging"
    );
}

#[test]
fn explicit_priorityclass_creation_contradicting_foreign_ownership_is_rejected() {
    let fixture = Fixture::new();
    let output = fixture.run(
        DEFAULT_PLATFORM,
        "foreign",
        DEFAULT_SANDBOX,
        "absent",
        "absent",
        &["--set", "priorityClasses.platform.create=true"],
    );
    let shown = stderr(&output);

    assert_eq!(
        output.status.code(),
        Some(2),
        "an explicit create value that contradicts observed ownership is a usage error\nstdout:\n{}\nstderr:\n{shown}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert_eq!(
        fixture.upgrade_count(),
        0,
        "Helm must not mutate the release"
    );
    assert!(shown.contains(DEFAULT_PLATFORM), "{shown}");
    assert!(
        shown.contains("priorityClasses.platform.create=true"),
        "{shown}"
    );
    assert!(shown.contains("platform-owner"), "{shown}");
}

#[test]
fn foreign_controller_is_reused_with_one_disclosed_override() {
    let fixture = Fixture::new();
    let output = fixture.run(
        DEFAULT_PLATFORM,
        "absent",
        DEFAULT_SANDBOX,
        "absent",
        "foreign",
        &[],
    );

    assert_upgrade_ran_once(&fixture, &output);
    assert_eq!(fixture.controller_query_count(), 1);
    let shown = stderr(&output);
    assert_inference_once(&shown, "--set agentSandbox.controller.deploy=false");
    assert!(
        fixture
            .upgrade_log()
            .contains("agentSandbox.controller.deploy=false"),
        "the inferred controller reuse value must reach Helm"
    );
}

#[test]
fn explicit_controller_reuse_wins_without_an_inference_line() {
    let fixture = Fixture::new();
    let output = fixture.run(
        DEFAULT_PLATFORM,
        "absent",
        DEFAULT_SANDBOX,
        "absent",
        "foreign",
        &["--set", "agentSandbox.controller.deploy=false"],
    );

    assert_upgrade_ran_once(&fixture, &output);
    assert!(
        !stderr(&output).contains("--set agentSandbox.controller.deploy=false"),
        "an explicit value must not be reported as an inference"
    );
}

#[test]
fn explicit_controller_creation_contradicting_foreign_ownership_is_rejected() {
    let fixture = Fixture::new();
    let output = fixture.run(
        DEFAULT_PLATFORM,
        "absent",
        DEFAULT_SANDBOX,
        "absent",
        "foreign",
        &["--set", "agentSandbox.controller.deploy=true"],
    );
    let shown = stderr(&output);

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout:\n{}\nstderr:\n{shown}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert_eq!(
        fixture.upgrade_count(),
        0,
        "Helm must not mutate the release"
    );
    assert!(shown.contains("agent-sandbox-controller"), "{shown}");
    assert!(
        shown.contains("agentSandbox.controller.deploy=true"),
        "{shown}"
    );
    assert!(shown.contains("controller-owner"), "{shown}");
}

#[test]
fn absent_and_target_owned_controller_do_not_infer_reuse() {
    for mode in ["absent", "same"] {
        let fixture = Fixture::new();
        let output = fixture.run(
            DEFAULT_PLATFORM,
            "absent",
            DEFAULT_SANDBOX,
            "absent",
            mode,
            &[],
        );

        assert_upgrade_ran_once(&fixture, &output);
        assert_eq!(fixture.controller_query_count(), 1, "mode {mode}");
        assert!(
            !stderr(&output).contains("agentSandbox.controller.deploy=false"),
            "mode {mode} must not infer controller reuse"
        );
        assert!(
            !fixture
                .upgrade_log()
                .contains("agentSandbox.controller.deploy=false"),
            "mode {mode} must retain chart controller creation"
        );
    }
}

#[test]
fn unreadable_or_incomplete_controller_ownership_fails_closed() {
    for mode in ["malformed", "incomplete", "failure"] {
        let fixture = Fixture::new();
        let output = fixture.run(
            DEFAULT_PLATFORM,
            "absent",
            DEFAULT_SANDBOX,
            "absent",
            mode,
            &[],
        );
        let shown = assert_failed_before_upgrade(&fixture, &output);

        assert!(
            shown.contains("agent-sandbox-controller"),
            "mode {mode}: {shown}"
        );
        assert!(
            shown.to_ascii_lowercase().contains("cluster status"),
            "mode {mode} needs a named recovery: {shown}"
        );
        assert!(
            !shown.contains("--set agentSandbox.controller.deploy=false"),
            "mode {mode} must not infer from uncertain ownership: {shown}"
        );
    }
}

#[test]
fn unmanaged_platform_class_blocks_with_exact_remediation() {
    let fixture = Fixture::new();
    let output = fixture.run(
        "shared-platform",
        "unmanaged",
        DEFAULT_SANDBOX,
        "absent",
        "absent",
        &["--set", "priorityClasses.platform.name=shared-platform"],
    );
    let shown = assert_failed_before_upgrade(&fixture, &output);

    for expected in [
        "shared-platform",
        "exists without complete Helm ownership metadata",
        "--set priorityClasses.platform.create=false --set priorityClasses.platform.name=shared-platform",
        "--set priorityClasses.platform.name=<different-name>",
    ] {
        assert!(
            shown.contains(expected),
            "the unmanaged platform conflict must contain `{expected}`:\n{shown}"
        );
    }
    assert_eq!(fixture.queries(), ["shared-platform", DEFAULT_SANDBOX]);
}

#[test]
fn helm_labelled_sandbox_class_without_release_annotations_blocks_with_exact_remediation() {
    let fixture = Fixture::new();
    let output = fixture.run(
        DEFAULT_PLATFORM,
        "absent",
        "shared-sandbox",
        "helm-without-annotations",
        "absent",
        &["--set", "priorityClasses.sandbox.name=shared-sandbox"],
    );
    let shown = assert_failed_before_upgrade(&fixture, &output);

    for expected in [
        "shared-sandbox",
        "exists without complete Helm ownership metadata",
        "--set priorityClasses.sandbox.create=false --set priorityClasses.sandbox.name=shared-sandbox",
        "--set priorityClasses.sandbox.name=<different-name>",
    ] {
        assert!(
            shown.contains(expected),
            "the incomplete sandbox ownership conflict must contain `{expected}`:\n{shown}"
        );
    }
    assert_eq!(fixture.queries(), [DEFAULT_PLATFORM, "shared-sandbox"]);
}

#[test]
fn helm_labelled_platform_class_without_release_namespace_blocks_with_exact_remediation() {
    let fixture = Fixture::new();
    let output = fixture.run(
        "shared-platform",
        "helm-without-release-namespace",
        DEFAULT_SANDBOX,
        "absent",
        "absent",
        &["--set", "priorityClasses.platform.name=shared-platform"],
    );
    let shown = assert_failed_before_upgrade(&fixture, &output);

    for expected in [
        "shared-platform",
        "exists without complete Helm ownership metadata",
        "--set priorityClasses.platform.create=false --set priorityClasses.platform.name=shared-platform",
        "--set priorityClasses.platform.name=<different-name>",
    ] {
        assert!(
            shown.contains(expected),
            "the incomplete platform ownership conflict must contain `{expected}`:\n{shown}"
        );
    }
    assert_eq!(fixture.queries(), ["shared-platform", DEFAULT_SANDBOX]);
}

#[test]
fn classes_owned_by_the_target_release_and_namespace_proceed() {
    let fixture = Fixture::new();
    let output = fixture.run(
        DEFAULT_PLATFORM,
        "same",
        DEFAULT_SANDBOX,
        "same",
        "absent",
        &[],
    );

    assert_upgrade_ran_once(&fixture, &output);
    assert_eq!(fixture.queries(), [DEFAULT_PLATFORM, DEFAULT_SANDBOX]);
}

#[test]
fn same_release_name_in_another_namespace_is_reused() {
    let fixture = Fixture::new();
    let output = fixture.run(
        DEFAULT_PLATFORM,
        "wrong-namespace",
        DEFAULT_SANDBOX,
        "absent",
        "absent",
        &[],
    );
    assert_upgrade_ran_once(&fixture, &output);
    assert_inference_once(
        &stderr(&output),
        "--set priorityClasses.platform.create=false",
    );
}

#[test]
fn create_false_skips_the_foreign_class_and_proceeds() {
    let fixture = Fixture::new();
    let output = fixture.run(
        DEFAULT_PLATFORM,
        "foreign",
        DEFAULT_SANDBOX,
        "absent",
        "absent",
        &["--set", "priorityClasses.platform.create=false"],
    );

    assert_upgrade_ran_once(&fixture, &output);
    assert_eq!(
        fixture.queries(),
        [DEFAULT_SANDBOX],
        "a role that the completed value plan does not create must not be queried"
    );
}

#[test]
fn renamed_class_is_discovered_from_the_rendered_chart() {
    let fixture = Fixture::new();
    let output = fixture.run(
        "renamed-platform",
        "absent",
        DEFAULT_SANDBOX,
        "absent",
        "absent",
        &["--set", "priorityClasses.platform.name=renamed-platform"],
    );

    assert_upgrade_ran_once(&fixture, &output);
    assert_eq!(
        fixture.queries(),
        ["renamed-platform", DEFAULT_SANDBOX],
        "the preflight must query the rendered override, not a Rust default"
    );
}

#[test]
fn absent_classes_proceed() {
    let fixture = Fixture::new();
    let output = fixture.run(
        DEFAULT_PLATFORM,
        "absent",
        DEFAULT_SANDBOX,
        "absent",
        "absent",
        &[],
    );

    assert_upgrade_ran_once(&fixture, &output);
    assert_eq!(fixture.queries(), [DEFAULT_PLATFORM, DEFAULT_SANDBOX]);
}

#[test]
fn kubectl_failure_blocks_with_a_recovery_hint() {
    let fixture = Fixture::new();
    let output = fixture.run(
        DEFAULT_PLATFORM,
        "failure",
        DEFAULT_SANDBOX,
        "absent",
        "absent",
        &[],
    );
    let shown = assert_failed_before_upgrade(&fixture, &output);

    assert!(
        shown.contains("Forbidden"),
        "kubectl detail was lost: {shown}"
    );
    assert_read_recovery_hint(&shown, DEFAULT_PLATFORM);
}

#[test]
fn priorityclass_read_failure_json_fix_uses_cluster_status() {
    let fixture = Fixture::new();
    let output = fixture.run(
        DEFAULT_PLATFORM,
        "failure",
        DEFAULT_SANDBOX,
        "absent",
        "absent",
        &["--json"],
    );

    assert_eq!(
        output.status.code(),
        Some(1),
        "a forbidden PriorityClass read remains a runtime failure\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        stderr(&output)
    );
    assert_eq!(
        fixture.upgrade_count(),
        0,
        "helm upgrade --install must not run after a failed preflight"
    );

    let payload: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("--json must emit the error payload: {error}"));
    assert_eq!(
        payload["fix"], "run `curie cluster status`",
        "the machine readable recovery action must be the single Curie status command: {payload}"
    );
    assert!(
        payload["error"]
            .as_str()
            .is_some_and(|error| error.contains(DEFAULT_PLATFORM)),
        "the machine readable error must name the PriorityClass: {payload}"
    );
}

#[test]
fn malformed_successful_json_blocks_with_a_recovery_hint() {
    let fixture = Fixture::new();
    let output = fixture.run(
        DEFAULT_PLATFORM,
        "malformed",
        DEFAULT_SANDBOX,
        "absent",
        "absent",
        &[],
    );
    let shown = assert_failed_before_upgrade(&fixture, &output);

    assert_read_recovery_hint(&shown, DEFAULT_PLATFORM);
    assert!(
        shown.to_ascii_lowercase().contains("json"),
        "a malformed response must be identified as invalid JSON: {shown}"
    );
}
