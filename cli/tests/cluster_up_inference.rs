//! Binary contract for facts inferred by `curie cluster up`.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const TARGET_RELEASE: &str = "target-release";
const TARGET_NAMESPACE: &str = "target-namespace";
const OPENROUTER_CREDENTIAL: &str = "sk-or-v1-PLACEHOLDER";
const ANTHROPIC_CREDENTIAL: &str = "sk-ant-api03-PLACEHOLDER";
const AMBIGUOUS_CREDENTIAL: &str = "sk-MOONSHOT-PLACEHOLDER";
const VALID_RESOLVER: &str = r#"{
  "openrouter.ai": ["1.1.1.1"],
  "api.anthropic.com": ["8.8.8.8"],
  "api.moonshot.ai": ["9.9.9.9"]
}"#;

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
    helm_log: PathBuf,
    kubectl_log: PathBuf,
    upgrade_log: PathBuf,
    values_dir: PathBuf,
    existing_values: String,
}

impl Fixture {
    fn new(existing_values: &str) -> Self {
        let temp = tempfile::tempdir().expect("temporary directory");
        let bin_dir = temp.path().join("bin");
        fs::create_dir(&bin_dir).expect("create fake binary directory");
        let helm_log = temp.path().join("helm.log");
        let kubectl_log = temp.path().join("kubectl.log");
        let upgrade_log = temp.path().join("upgrades.log");
        let values_dir = temp.path().join("helm-values");
        fs::create_dir(&values_dir).expect("create helm-values capture directory");

        write_exec(
            &bin_dir,
            "helm",
            r#"#!/bin/sh
printf '%s\n' "$*" >> "$CURIE_TEST_HELM_LOG"

if [ "$1" = "get" ] && [ "$2" = "values" ]; then
    if [ -z "$CURIE_TEST_EXISTING_VALUES" ]; then
        printf '%s\n' 'Error: release: not found' >&2
        exit 1
    fi
    printf '%s\n' "$CURIE_TEST_EXISTING_VALUES"
    exit 0
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
    n=0
    prev=""
    for arg in "$@"; do
        if [ "$prev" = "-f" ]; then
            n=$((n + 1))
            dest="$CURIE_TEST_VALUES_DIR/values-$n.yaml"
            if [ -f "$arg" ]; then
                cp "$arg" "$dest"
            else
                printf 'missing %s\n' "$arg" > "$dest.missing"
            fi
        fi
        prev=$arg
    done
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
printf '%s\n' "$*" >> "$CURIE_TEST_KUBECTL_LOG"

if [ "$1" = "get" ] && [ "$2" = "namespace" ]; then
    exit 0
fi

case " $* " in
    *" get deployment agent-sandbox-controller "*)
        case " $* " in
            *" -n agent-sandbox-system "*) exit 0 ;;
            *)
                printf 'controller query was not scoped to agent-sandbox-system: %s\n' "$*" >&2
                exit 64
                ;;
        esac
        ;;
esac

printf 'unexpected kubectl invocation: %s\n' "$*" >&2
exit 64
"#,
        );

        Self {
            _temp: temp,
            bin_dir,
            helm_log,
            kubectl_log,
            upgrade_log,
            values_dir,
            existing_values: existing_values.to_string(),
        }
    }

    fn run(
        &self,
        credential_environment: &[(&str, &str)],
        resolver: &str,
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
            // This harness only ever installs with `--dev`, so every non-empty
            // existing-values document it simulates must also record
            // `security.allowDevDefaults=true`. Without it `guard_dev_defaults_flip`
            // (#1145) correctly refuses the run; do not drop the flag to make a test pass.
            "--dev",
            "--no-expose",
        ];
        args.extend_from_slice(extra);

        let mut command = Command::new(bin());
        command
            .args(args)
            .env("PATH", path)
            .env("CI", "1")
            .env("TERM", "dumb")
            .env("NO_COLOR", "1")
            .env("CURIE_TEST_HELM_LOG", &self.helm_log)
            .env("CURIE_TEST_KUBECTL_LOG", &self.kubectl_log)
            .env("CURIE_TEST_UPGRADE_LOG", &self.upgrade_log)
            .env("CURIE_TEST_VALUES_DIR", &self.values_dir)
            .env("CURIE_TEST_EXISTING_VALUES", &self.existing_values)
            .env("CURIE_TEST_PROVIDER_EGRESS_JSON", resolver)
            .env("CURIE_CONFIG_DIR", self._temp.path().join("config"))
            .env_remove("CURIE_CREDENTIALS")
            .env_remove("CURIE_MODEL_CREDENTIALS")
            .env_remove("CURIE_GITHUB_TOKEN")
            .env_remove("CURIE_MODEL");
        for (key, value) in credential_environment {
            command.env(key, value);
        }
        command.output().expect("run curie cluster up")
    }

    fn helm_log(&self) -> String {
        fs::read_to_string(&self.helm_log).unwrap_or_default()
    }

    fn upgrade_log(&self) -> String {
        fs::read_to_string(&self.upgrade_log).unwrap_or_default()
    }

    fn kubectl_log(&self) -> String {
        fs::read_to_string(&self.kubectl_log).unwrap_or_default()
    }

    fn upgrade_count(&self) -> usize {
        self.upgrade_log().lines().count()
    }

    fn captured_values(&self) -> String {
        let mut bodies = Vec::new();
        let mut entries: Vec<PathBuf> = fs::read_dir(&self.values_dir)
            .map(|entries| {
                entries
                    .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                    .collect()
            })
            .unwrap_or_default();
        entries.sort();
        for path in entries {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            let body = fs::read_to_string(&path).unwrap_or_default();
            bodies.push(format!("--- {name} ---\n{body}"));
        }
        bodies.join("\n")
    }
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn all_output(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn assert_success(fixture: &Fixture, output: &Output) {
    assert!(
        output.status.success(),
        "cluster up failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        stderr(output)
    );
    assert_eq!(fixture.upgrade_count(), 1, "Helm must install exactly once");
}

fn assert_inference_once(shown: &str, applied_override: &str) {
    assert_eq!(
        shown.matches(applied_override).count(),
        1,
        "the applied override must be disclosed exactly once: {applied_override}\n{shown}"
    );
}

#[test]
fn recognized_credential_prefixes_infer_provider_egress() {
    for (credential, provider, cidr) in [
        (ANTHROPIC_CREDENTIAL, "anthropic", "8.8.8.8/32"),
        (OPENROUTER_CREDENTIAL, "openrouter", "1.1.1.1/32"),
    ] {
        let fixture = Fixture::new("");
        let output = fixture.run(&[("CURIE_CREDENTIALS", credential)], VALID_RESOLVER, &[]);
        assert_success(&fixture, &output);

        let shown = stderr(&output);
        assert_inference_once(&shown, &format!("--allow-egress-host {provider}"));
        assert!(
            fixture.upgrade_log().contains(cidr),
            "the inferred {provider} route must reach Helm"
        );
        let upgrade = fixture.upgrade_log();
        assert!(
            upgrade.contains("security.networkPolicy.allowedEgress[0].ports[0].protocol=TCP"),
            "the inferred {provider} route must use TCP: {upgrade}"
        );
        assert!(
            upgrade.contains("security.networkPolicy.allowedEgress[0].ports[0].port=443"),
            "the inferred {provider} route must use port 443: {upgrade}"
        );
        assert!(
            !shown.contains("sandbox is sealed"),
            "inferred provider egress must remove the sealed warning: {shown}"
        );
        assert!(
            !all_output(&output).contains(credential),
            "credential leaked"
        );
    }
}

#[test]
fn inferred_provider_refresh_preserves_recorded_web_egress() {
    let fixture = Fixture::new(
        r#"{
          "agentSandbox":{"runner":{"credentials":"sk-or-v1-PLACEHOLDER"}},
          "security":{
            "allowDevDefaults":true,
            "networkPolicy":{"allowedEgress":[{
              "cidr":"203.0.113.0/24",
              "ports":[{"protocol":"TCP","port":443}]
            }]}
          }
        }"#,
    );
    let output = fixture.run(&[], VALID_RESOLVER, &[]);
    assert_success(&fixture, &output);

    let upgrade = fixture.upgrade_log();
    assert!(
        upgrade.contains("security.networkPolicy.allowedEgress[0].cidr=203.0.113.0/24"),
        "the recorded web route must survive the inferred provider refresh: {upgrade}"
    );
    assert!(
        upgrade.contains("security.networkPolicy.allowedEgress[1].cidr=1.1.1.1/32"),
        "the refreshed provider route must use a noncolliding index: {upgrade}"
    );
    assert!(
        upgrade.contains("security.networkPolicy.allowedEgress[1].ports[0].protocol=TCP"),
        "the refreshed provider route must use TCP: {upgrade}"
    );
    assert!(
        upgrade.contains("security.networkPolicy.allowedEgress[1].ports[0].port=443"),
        "the refreshed provider route must use port 443: {upgrade}"
    );
    assert_inference_once(&stderr(&output), "--allow-egress-host openrouter");
}

#[test]
fn repeated_bare_up_does_not_duplicate_the_recorded_inferred_provider_route() {
    let fixture = Fixture::new(
        r#"{
          "agentSandbox":{"runner":{"credentials":"sk-or-v1-PLACEHOLDER"}},
          "security":{
            "allowDevDefaults":true,
            "networkPolicy":{"allowedEgress":[
              {"cidr":"1.1.1.1/32","ports":[{"protocol":"TCP","port":443}]},
              {"cidr":"203.0.113.0/24","ports":[{"protocol":"TCP","port":443}]}
            ]}
          }
        }"#,
    );
    let output = fixture.run(&[], VALID_RESOLVER, &[]);
    assert_success(&fixture, &output);

    let upgrade = fixture.upgrade_log();
    assert_eq!(
        upgrade.matches("1.1.1.1/32").count(),
        1,
        "the recorded provider route must not accumulate on a bare rerun: {upgrade}"
    );
    assert!(
        upgrade.contains("security.networkPolicy.allowedEgress[0].cidr=1.1.1.1/32"),
        "the recorded provider route must retain its index: {upgrade}"
    );
    assert!(
        upgrade.contains("security.networkPolicy.allowedEgress[1].cidr=203.0.113.0/24"),
        "the recorded web route must survive: {upgrade}"
    );
    assert!(
        !upgrade.contains("security.networkPolicy.allowedEgress[2]"),
        "the repeated run must not append another route: {upgrade}"
    );
    assert_inference_once(&stderr(&output), "--allow-egress-host openrouter");
}

#[test]
fn repeated_bare_up_preserves_the_inferred_gvisor_off_posture() {
    // The successful recovery retry from the first install records this value.
    // A second bare up must pass it to Helm before rendering the preflight, or
    // the chart's default `auto` posture will recreate the rejected Job.
    let fixture = Fixture::new(r#"{"security":{"gvisor":{"mode":"off"},"allowDevDefaults":true}}"#);
    let output = fixture.run(&[], VALID_RESOLVER, &[]);
    assert_success(&fixture, &output);

    let upgrade = fixture.upgrade_log();
    assert!(
        upgrade.contains("security.gvisor.mode=off"),
        "the recorded inferred posture must be re-supplied on the second bare up: {upgrade}"
    );
    assert!(
        !stderr(&output).contains("inferred that the cluster has no `gvisor` RuntimeClass"),
        "a rerun with the recorded posture must not announce a fresh inference: {}",
        stderr(&output)
    );
}

#[test]
fn explicit_gvisor_mode_replaces_the_recorded_inferred_posture() {
    let fixture = Fixture::new(r#"{"security":{"gvisor":{"mode":"off"},"allowDevDefaults":true}}"#);
    let output = fixture.run(
        &[],
        VALID_RESOLVER,
        &["--set", "security.gvisor.mode=require"],
    );
    assert_success(&fixture, &output);

    let upgrade = fixture.upgrade_log();
    assert!(
        upgrade.contains("security.gvisor.mode=require"),
        "the explicit operator posture must reach Helm: {upgrade}"
    );
    assert!(
        !upgrade.contains("security.gvisor.mode=off"),
        "the recorded inference must not override an explicit operator posture: {upgrade}"
    );
}

#[test]
fn explicit_provider_list_containing_the_detected_provider_wins_silently() {
    let fixture = Fixture::new("");
    let output = fixture.run(
        &[("CURIE_CREDENTIALS", OPENROUTER_CREDENTIAL)],
        VALID_RESOLVER,
        &[
            "--allow-egress-host",
            "openrouter",
            "--allow-egress-host",
            "anthropic",
        ],
    );
    assert_success(&fixture, &output);

    let shown = stderr(&output);
    assert!(
        !shown.contains("--allow-egress-host openrouter"),
        "an explicit list must not be reported as inferred: {shown}"
    );
    let upgrade = fixture.upgrade_log();
    assert!(upgrade.contains("1.1.1.1/32"), "{upgrade}");
    assert!(upgrade.contains("8.8.8.8/32"), "{upgrade}");
}

#[test]
fn preserved_credential_contradiction_fails_after_the_values_read() {
    let fixture = Fixture::new(
        r#"{
          "agentSandbox":{"runner":{"credentials":"sk-or-v1-PLACEHOLDER"}},
          "security":{"allowDevDefaults":true}
        }"#,
    );
    let output = fixture.run(
        &[],
        "not resolver JSON",
        &["--allow-egress-host", "anthropic"],
    );
    let shown = all_output(&output);

    assert_eq!(output.status.code(), Some(2), "{shown}");
    assert!(shown.contains("openrouter"), "{shown}");
    assert!(shown.contains("--allow-egress-host anthropic"), "{shown}");
    assert!(
        !shown.contains("resolver JSON"),
        "the resolver ran: {shown}"
    );
    assert!(
        !shown.contains("sk-or-v1-PLACEHOLDER"),
        "credential leaked: {shown}"
    );
    assert!(
        fixture.helm_log().contains("get values"),
        "the preserved credential must come from the live values read: {}",
        fixture.helm_log()
    );
    assert_eq!(
        fixture.upgrade_count(),
        0,
        "the contradiction must stop before installation"
    );
}

#[test]
fn provider_contradiction_fails_before_resolver_or_helm_without_secret_bytes() {
    let fixture = Fixture::new("");
    let output = fixture.run(
        &[("CURIE_CREDENTIALS", OPENROUTER_CREDENTIAL)],
        "not resolver JSON",
        &["--allow-egress-host", "anthropic"],
    );
    let shown = all_output(&output);

    assert_eq!(
        output.status.code(),
        Some(2),
        "a detected provider contradiction is a usage error: {shown}"
    );
    assert!(shown.contains("openrouter"), "{shown}");
    assert!(shown.contains("--allow-egress-host anthropic"), "{shown}");
    assert!(
        !shown.contains("resolver JSON"),
        "the resolver ran: {shown}"
    );
    assert!(
        !shown.contains(OPENROUTER_CREDENTIAL),
        "credential leaked: {shown}"
    );
    assert!(
        fixture.helm_log().is_empty(),
        "no Helm read or mutation may precede the contradiction: {}",
        fixture.helm_log()
    );
}

#[test]
fn ambiguous_credentials_stay_sealed_unless_an_explicit_provider_wins() {
    let sealed = Fixture::new("");
    let output = sealed.run(
        &[("CURIE_CREDENTIALS", AMBIGUOUS_CREDENTIAL)],
        VALID_RESOLVER,
        &[],
    );
    assert_success(&sealed, &output);
    let shown = stderr(&output);
    assert!(shown.contains("sandbox is sealed"), "{shown}");
    assert!(!shown.contains("--allow-egress-host anthropic"), "{shown}");
    assert!(!shown.contains("--allow-egress-host openrouter"), "{shown}");
    assert!(!sealed.upgrade_log().contains("allowedEgress"));

    let explicit = Fixture::new("");
    let output = explicit.run(
        &[("CURIE_CREDENTIALS", AMBIGUOUS_CREDENTIAL)],
        VALID_RESOLVER,
        &["--allow-egress-host", "moonshot"],
    );
    assert_success(&explicit, &output);
    let shown = stderr(&output);
    assert!(!shown.contains("sandbox is sealed"), "{shown}");
    assert!(!shown.contains("--allow-egress-host moonshot"), "{shown}");
    assert!(explicit.upgrade_log().contains("9.9.9.9/32"));
}

#[test]
fn inference_uses_the_effective_credential_precedence() {
    let canonical = Fixture::new("");
    let output = canonical.run(
        &[
            ("CURIE_CREDENTIALS", OPENROUTER_CREDENTIAL),
            ("CURIE_MODEL_CREDENTIALS", ANTHROPIC_CREDENTIAL),
        ],
        VALID_RESOLVER,
        &[],
    );
    assert_success(&canonical, &output);
    assert!(canonical.upgrade_log().contains("1.1.1.1/32"));
    assert!(!canonical.upgrade_log().contains("8.8.8.8/32"));

    let final_helm_value = Fixture::new("");
    let explicit_credential = format!("agentSandbox.runner.credentials={ANTHROPIC_CREDENTIAL}");
    let output = final_helm_value.run(
        &[("CURIE_CREDENTIALS", OPENROUTER_CREDENTIAL)],
        VALID_RESOLVER,
        &["--set", explicit_credential.as_str()],
    );
    assert_success(&final_helm_value, &output);
    assert!(final_helm_value.upgrade_log().contains("8.8.8.8/32"));
    assert!(!final_helm_value.upgrade_log().contains("1.1.1.1/32"));

    let preserved = Fixture::new(
        r#"{
          "agentSandbox":{"runner":{"credentials":"sk-or-v1-PLACEHOLDER"}},
          "security":{"allowDevDefaults":true}
        }"#,
    );
    let output = preserved.run(&[], VALID_RESOLVER, &[]);
    assert_success(&preserved, &output);
    assert!(preserved.upgrade_log().contains("1.1.1.1/32"));
    assert_inference_once(&stderr(&output), "--allow-egress-host openrouter");
}

#[test]
fn local_model_without_an_explicit_asset_policy_refuses_before_cluster_access() {
    let fixture = Fixture::new("");
    let output = fixture.run(&[], VALID_RESOLVER, &["--local-model", "qwen3:4b"]);
    let shown = all_output(&output);

    assert_eq!(
        output.status.code(),
        Some(2),
        "an ambiguous local-model asset policy is a usage error: {shown}"
    );
    for recovery in [
        "--set inference.persistence.enabled=true",
        "--set inference.pullModel=false",
    ] {
        assert!(
            shown.contains(recovery),
            "the refusal must include both runnable recovery choices ({recovery}): {shown}"
        );
    }
    assert!(
        fixture.helm_log().is_empty(),
        "the asset-policy refusal must precede every Helm read or mutation: {}",
        fixture.helm_log()
    );
    assert!(
        fixture.kubectl_log().is_empty(),
        "the asset-policy refusal must precede every cluster interaction: {}",
        fixture.kubectl_log()
    );
}

#[test]
fn local_model_accepts_each_explicit_typed_asset_policy() {
    for policy in [
        "inference.persistence.enabled=true",
        "inference.pullModel=false",
    ] {
        let fixture = Fixture::new("");
        let output = fixture.run(
            &[],
            VALID_RESOLVER,
            &["--local-model", "qwen3:4b", "--set", policy],
        );
        assert_success(&fixture, &output);

        let upgrade = fixture.upgrade_log();
        for expected in ["inference.deploy=true", "inference.model=qwen3:4b", policy] {
            assert!(
                upgrade.contains(&format!("--set {expected}")),
                "the accepted local-model policy must stay in Helm's typed lane ({expected}): {upgrade}"
            );
        }
        assert!(
            !upgrade.contains(&format!("--set-string {policy}")),
            "a modeled boolean policy must never be passed as a Helm string: {upgrade}"
        );
    }
}

#[test]
fn string_false_is_not_a_valid_no_pull_recovery() {
    let fixture = Fixture::new("");
    let output = fixture.run(
        &[],
        VALID_RESOLVER,
        &[
            "--local-model",
            "qwen3:4b",
            "--set-string",
            "inference.pullModel=false",
        ],
    );
    let shown = all_output(&output);

    assert_eq!(
        output.status.code(),
        Some(2),
        "a Helm string false is truthy and must not satisfy no-pull: {shown}"
    );
    assert_eq!(fixture.upgrade_count(), 0, "string false must not install");
    assert!(
        fixture.helm_log().is_empty() && fixture.kubectl_log().is_empty(),
        "string false must be rejected before cluster access; helm: {}; kubectl: {}",
        fixture.helm_log(),
        fixture.kubectl_log()
    );
}

/// #1145, through the real consumer path: `curie cluster up --dev` against a
/// release that is NOT already on dev defaults refuses before Helm is ever
/// invoked to mutate anything.
///
/// `AGENTS.md`, "Guards are outcome-tested", requires that a gate's regression
/// test assert the outcome through the real consumer path. Every other
/// `guard_dev_defaults_flip` test calls the pure function directly, so deleting
/// its call site in `run_prepared_up` -- or moving it to AFTER the helm upgrade,
/// which is the damaging half of the same mistake -- leaves them all green.
/// This is the test that fails if the guard is ever disconnected from
/// `cluster up`, and `upgrade_count() == 0` is the assertion that proves it
/// still runs BEFORE any mutation.
///
/// The existing-values document is non-empty and records no
/// `security.allowDevDefaults`, so it is a SEALED release and distinguishable
/// from "no existing release" (which is the supported fresh-install case).
#[test]
fn dev_over_a_sealed_release_is_refused_before_helm_mutates_anything() {
    let fixture = Fixture::new(r#"{"security":{"gvisor":{"mode":"off"}}}"#);
    let output = fixture.run(&[], VALID_RESOLVER, &[]);
    let shown = all_output(&output);

    assert_eq!(
        output.status.code(),
        Some(2),
        "a refused `--dev` is a usage error (exit 2), not a runtime failure: {shown}"
    );
    assert_eq!(
        fixture.upgrade_count(),
        0,
        "the guard must refuse BEFORE any helm mutation: {}",
        fixture.upgrade_log()
    );
    assert!(
        fixture.helm_log().contains("get values"),
        "the refusal must follow the live values read, not precede it: {}",
        fixture.helm_log()
    );

    let refusal = stderr(&output).to_lowercase();
    for token in ["--dev", "refused", "pvc"] {
        assert!(
            refusal.contains(token),
            "the refusal must reach stderr and say why (missing {token:?}): {refusal}"
        );
    }
}

/// #1134 / #1125, through the real consumer path: `curie cluster up --dev`
/// after a previous `cluster comms` (and a recorded sealing key) must
/// re-supply those values on the Helm upgrade.
///
/// The operator sequence is `cluster up --dev`, `cluster comms`, `cluster up
/// --dev`. This fixture is the second `--dev`, with helm already recording the
/// tokens `comms` wrote. Helm 3 reuses prior values only when the upgrade
/// carries no value flags; `--dev` always passes
/// `--set security.allowDevDefaults=true`, so reuse never engages and anything
/// `up` does not re-pass resets to the chart default.
///
/// This is the test that fails if existing-value discovery or comms/sealing
/// preservation is gated behind `!opts.dev` again.
#[test]
fn a_second_dev_upgrade_preserves_recorded_comms_and_sealing_values() {
    const APP_TOKEN: &str = "xapp-EXAMPLE";
    const BOT_TOKEN: &str = "xoxb-EXAMPLE";
    const SEALING_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    let fixture = Fixture::new(&format!(
        r#"{{
          "security":{{"allowDevDefaults":true}},
          "dispatcher":{{"slack":{{"appToken":"{APP_TOKEN}","botToken":"{BOT_TOKEN}"}}}},
          "sealing":{{"privateKey":"{SEALING_KEY}"}}
        }}"#
    ));
    let output = fixture.run(&[], VALID_RESOLVER, &[]);
    assert_success(&fixture, &output);

    let shown = all_output(&output);
    assert!(
        !shown.contains(APP_TOKEN) && !shown.contains(BOT_TOKEN) && !shown.contains(SEALING_KEY),
        "recorded credentials leaked: {shown}"
    );
    assert!(
        shown.contains("preserving") && shown.contains("sealing"),
        "a second --dev upgrade must tell the operator it kept recorded comms/sealing values: {shown}"
    );
    assert!(
        fixture.helm_log().contains("get values"),
        "the preserved tokens must come from the live values read: {}",
        fixture.helm_log()
    );
    let upgrade = fixture.upgrade_log();
    assert!(
        upgrade.contains("security.allowDevDefaults=true"),
        "dev mode must still opt into chart defaults: {upgrade}"
    );
    assert!(
        !upgrade.contains("--reuse-values"),
        "up must remain a full Helm upgrade: {upgrade}"
    );
    assert!(
        !upgrade.contains(APP_TOKEN) && !upgrade.contains(BOT_TOKEN),
        "Slack tokens leaked into helm argv: {upgrade}"
    );

    let values = fixture.captured_values();
    assert!(
        upgrade.contains(" -f "),
        "the second --dev upgrade must pass a values file, not rely on Helm reuse: {upgrade}"
    );
    assert!(
        values.contains(APP_TOKEN),
        "the second --dev upgrade dropped the Slack app token (#1134): values={values} upgrade={upgrade}"
    );
    assert!(
        values.contains(BOT_TOKEN),
        "the second --dev upgrade dropped the Slack bot token (#1134): values={values} upgrade={upgrade}"
    );
    assert!(
        values.contains(SEALING_KEY),
        "the second --dev upgrade dropped the sealing key (#1134): values={values} upgrade={upgrade}"
    );
    assert!(
        !values.contains("apiKey") && !values.contains("postgres"),
        "--dev must not mint generated store secrets: {values}"
    );
}

#[test]
fn a_second_dev_upgrade_stays_disconnected_after_comms_disconnect() {
    let fixture = Fixture::new(
        r#"{
          "security":{"allowDevDefaults":true},
          "dispatcher":{"slack":{"appToken":"","botToken":""}}
        }"#,
    );
    let output = fixture.run(&[], VALID_RESOLVER, &[]);
    assert_success(&fixture, &output);
    let values = fixture.captured_values();
    assert!(
        !values.contains("appToken") && !values.contains("botToken") && !values.contains("xapp-"),
        "empty disconnect values must not be resurrected: {values}"
    );
}

#[test]
fn a_second_dev_upgrade_lets_an_explicit_set_replace_a_recorded_comms_value() {
    let fixture = Fixture::new(
        r#"{
          "security":{"allowDevDefaults":true},
          "dispatcher":{"slack":{"appToken":"xapp-old","botToken":"xoxb-old"}}
        }"#,
    );
    let output = fixture.run(
        &[],
        VALID_RESOLVER,
        &["--set", "dispatcher.slack.botToken=xoxb-new"],
    );
    assert_success(&fixture, &output);
    let upgrade = fixture.upgrade_log();
    let values = fixture.captured_values();
    assert!(
        upgrade.contains("dispatcher.slack.botToken=xoxb-new"),
        "the explicit replacement must reach Helm argv: {upgrade}"
    );
    assert!(
        values.contains("xapp-old"),
        "the untouched app token must still be preserved: values={values} upgrade={upgrade}"
    );
    assert!(
        !values.contains("xoxb-old"),
        "the recorded bot token must not ride alongside the explicit replacement: {values}"
    );
}

#[test]
fn a_fresh_dev_install_does_not_mint_comms_or_sealing_values() {
    let fixture = Fixture::new("");
    let output = fixture.run(&[], VALID_RESOLVER, &[]);
    assert_success(&fixture, &output);
    let values = fixture.captured_values();
    assert!(
        !values.contains("appToken")
            && !values.contains("botToken")
            && !values.contains("privateKey")
            && !values.contains("apiKey"),
        "a fresh --dev install must not invent comms, sealing, or store secrets: {values}"
    );
}
