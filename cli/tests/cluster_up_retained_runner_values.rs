//! #3848: a sealed same-version `curie cluster up` over an existing release
//! must keep the runner environment, every per-agent connector binding, and
//! the recorded model credential reference, even when Curie private storage
//! holds an unrelated saved model credential.
//!
//! Drives the real binary against recording `helm` and `kubectl` shims, then
//! renders the repository chart with real Helm before (the recorded values)
//! and after (exactly what `cluster up` handed `helm upgrade`) and compares the
//! workloads the recorded values feed.

#[path = "support/executable.rs"]
mod test_executable;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::{json, Value};

const TARGET_RELEASE: &str = "acme-release";
const TARGET_NAMESPACE: &str = "acme-namespace";
const SAVED_OPENROUTER_CREDENTIAL: &str = "sk-or-v1-PLACEHOLDER-saved-3848";
const EXPLICIT_ANTHROPIC_CREDENTIAL: &str = "sk-ant-api03-PLACEHOLDER-explicit-3848";
const CONNECTOR_SECRET_A: &str = "PLACEHOLDER-acme-a-grafana-token";
const CONNECTOR_SECRET_B: &str = "PLACEHOLDER-acme-b-github-token";
const NUMERIC_ENV_VALUE: &str = "8080";
/// The installed config schema a same-version release records, so the run is
/// a same-version up rather than a config migration.
const TARGET_SCHEMA_VERSION: &str = "0.9.0";
const RESOLVER: &str = r#"{
  "openrouter.ai": ["1.1.1.1"],
  "api.anthropic.com": ["8.8.8.8"]
}"#;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn chart() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/../charts/curie")
}

/// What a sealed BYO release records in `helm get values`: every generated
/// chart secret and the sealing key (so a sealed rerun generates nothing and
/// the render is deterministic), the connector caller pair by reference, a
/// model credential Secret reference, a runner env list with a numeric
/// string, and connector bindings for two agents.
fn recorded_values() -> Value {
    json!({
        "config": {"schemaVersion": TARGET_SCHEMA_VERSION},
        "postgres": {"auth": {"password": "PLACEHOLDER-postgres-password"}},
        "valkey": {"password": "PLACEHOLDER-valkey-password"},
        "clickhouse": {"auth": {"password": "PLACEHOLDER-clickhouse-password"}},
        "rustfs": {"auth": {"rootPassword": "PLACEHOLDER-rustfs-password"}},
        "langfuse": {
            "salt": "PLACEHOLDER-salt",
            "encryptionKey": "0000000000000000000000000000000000000000000000000000000000000000",
            "nextauthSecret": "PLACEHOLDER-nextauth-secret"
        },
        "api": {
            "apiKey": "PLACEHOLDER-api-key",
            "githubWebhookSecret": "PLACEHOLDER-webhook-secret"
        },
        "sealing": {"privateKey": "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="},
        "connectorCaller": {
            "existingSecret": "acme-connector-caller",
            "signingKeyKey": "signingKey",
            "verifyKeyKey": "verifyKey"
        },
        "security": {"networkPolicy": {"allowedEgress": [{
            "cidr": "1.1.1.1/32",
            "ports": [{"protocol": "TCP", "port": 443}]
        }]}},
        "agentSandbox": {
            "runner": {
                "fakeModel": false,
                "model": "acme/model",
                "credentialsExistingSecret": "byo-model",
                "credentialsExistingSecretKey": "agentCredentials",
                "extraEnv": [
                    {"name": "PORT", "value": NUMERIC_ENV_VALUE},
                    {
                        "name": "FROM_SECRET",
                        "valueFrom": {"secretKeyRef": {"name": "acme-env", "key": "token"}}
                    }
                ]
            },
            "connectorSecrets": {
                "acme-a": {
                    "GRAFANA_TOKEN": CONNECTOR_SECRET_A,
                    "GRAFANA_URL": "http://grafana.example"
                },
                "acme-b": {"GITHUB_PERSONAL_ACCESS_TOKEN": CONNECTOR_SECRET_B}
            }
        }
    })
}

fn install_converged_stub(dir: &Path, name: &str, body: &str) {
    let body = format!(
        "#!/bin/sh\n{}\n{}",
        include_str!("data/converged-installation-read.sh"),
        body.strip_prefix("#!/bin/sh\n").unwrap_or(body)
    );
    test_executable::install_in(dir, name, &body);
}

struct Fixture {
    temp: tempfile::TempDir,
    bin_dir: PathBuf,
    calls_log: PathBuf,
    upgrade_argv_log: PathBuf,
    values_dir: PathBuf,
    existing_values: String,
}

impl Fixture {
    fn new(existing: &Value) -> Self {
        let temp = tempfile::tempdir().expect("temporary directory");
        let bin_dir = temp.path().join("bin");
        fs::create_dir(&bin_dir).expect("create fake binary directory");
        let values_dir = temp.path().join("helm-values");
        fs::create_dir(&values_dir).expect("create values capture directory");

        // Unexpected calls exit 64 with their argv so a failure names the
        // question the binary asked rather than masking it with a plausible
        // answer.
        install_converged_stub(
            &bin_dir,
            "helm",
            r#"#!/bin/sh
printf 'helm %s\n' "$*" >> "$CURIE_TEST_CALLS_LOG"
if [ "$1" = "get" ] && [ "$2" = "values" ]; then
    if [ -n "${CURIE_TEST_FIRST_INSTALL:-}" ]; then
        printf '%s\n' 'Error: release: not found' >&2
        exit 1
    fi
    printf '%s\n' "$CURIE_TEST_EXISTING_VALUES"
    exit 0
fi
if [ "$1" = "template" ]; then
    if [ "$2" = "curie-values-lint" ] && [ -n "${CURIE_TEST_REAL_HELM:-}" ]; then
        "$CURIE_TEST_REAL_HELM" "$@"
        result=$?
        if [ "$result" = 0 ] && [ -n "${CURIE_TEST_MUTATE_VALUES:-}" ]; then
            printf '%s\n' 'worker: {extraEnv: [{name: NUMERIC_STRING, value: MUTATED}]}' > "$CURIE_TEST_MUTATE_VALUES"
        fi
        exit "$result"
    fi
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
fi
if [ "$1" = "upgrade" ] && [ "$2" = "--install" ]; then
    : > "$CURIE_TEST_UPGRADE_ARGV_LOG"
    n=0
    prev=""
    for arg in "$@"; do
        if [ "$prev" = "-f" ]; then
            n=$((n + 1))
            dest="$CURIE_TEST_VALUES_DIR/values-$n.yaml"
            cp "$arg" "$dest" || exit 65
            printf '%s\n' "$dest" >> "$CURIE_TEST_UPGRADE_ARGV_LOG"
        else
            printf '%s\n' "$arg" >> "$CURIE_TEST_UPGRADE_ARGV_LOG"
        fi
        prev=$arg
    done
    exit 0
fi
if [ "$1" = "history" ]; then
    printf '%s\n' 'Error: release: not found' >&2
    exit 1
fi
printf 'unexpected helm invocation: %s\n' "$*" >&2
printf 'UNEXPECTED helm %s\n' "$*" >> "$CURIE_TEST_CALLS_LOG"
exit 64
"#,
        );

        install_converged_stub(
            &bin_dir,
            "kubectl",
            r#"#!/bin/sh
printf 'kubectl %s\n' "$*" >> "$CURIE_TEST_CALLS_LOG"
if [ "$1" = "get" ] && [ "$2" = "namespace" ]; then
    if [ "$3" = "acme-namespace" ]; then
        printf '%s\n' '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"acme-namespace","labels":{"curietech.ai/created-by":"acme-release","curietech.ai/created-in":"acme-namespace"},"uid":"uid-acme-namespace","resourceVersion":"17"}}'
    fi
    exit 0
fi
case " $* " in
    *" get deployment agent-sandbox-controller "*"-n agent-sandbox-system "*) exit 0 ;;
    *" get priorityclass "*) exit 0 ;;
    # The gVisor admission observer watches for FailedCreate events while the
    # upgrade runs; none occurred.
    *" get events -n acme-namespace --field-selector reason=FailedCreate -o json "*|\
    *" get events -n agent-sandbox-system --field-selector reason=FailedCreate -o json "*)
        printf '%s\n' '{"apiVersion":"v1","items":[],"kind":"List","metadata":{"resourceVersion":""}}'
        exit 0
        ;;
esac
if [ "$1" = "get" ] && [ "$2" = "runtimeclass" ]; then
    printf '%s\n' 'Error from server (Forbidden): runtimeclasses.node.k8s.io "gvisor" is forbidden: User "system:serviceaccount:example:example" cannot get resource "runtimeclasses" in API group "node.k8s.io" at the cluster scope' >&2
    exit 1
fi

printf 'unexpected kubectl invocation: %s\n' "$*" >&2
printf 'UNEXPECTED kubectl %s\n' "$*" >> "$CURIE_TEST_CALLS_LOG"
exit 64
"#,
        );

        Self {
            calls_log: temp.path().join("calls.log"),
            upgrade_argv_log: temp.path().join("upgrade-argv.log"),
            temp,
            bin_dir,
            values_dir,
            existing_values: existing.to_string(),
        }
    }

    fn config_dir(&self) -> PathBuf {
        self.temp.path().join("config")
    }

    /// Save a model credential through the public `secrets set` verb, so the
    /// fixture holds exactly the persisted shape a user creates.
    fn save_model_credential(&self, value: &str) {
        let seed = Command::new(bin())
            .args(["secrets", "set", "CURIE_CREDENTIALS", "--from-env", "SEED"])
            .env("CURIE_CONFIG_DIR", self.config_dir())
            .env("SEED", value)
            .env_remove("CURIE_CREDENTIALS")
            .env_remove("CURIE_MODEL_CREDENTIALS")
            .output()
            .expect("run curie secrets set");
        assert!(
            seed.status.success(),
            "seed private storage: {}",
            all_output(&seed)
        );
    }

    fn cluster_up(&self, environment: &[(&str, &str)]) -> Output {
        self.cluster_up_with_args(environment, &[])
    }

    // @spec CLUSTER-VALUES-FILES c1-c4
    fn cluster_up_with_args(&self, environment: &[(&str, &str)], args: &[&str]) -> Output {
        let mut paths = vec![self.bin_dir.clone()];
        if let Some(current) = std::env::var_os("PATH") {
            paths.extend(std::env::split_paths(&current));
        }
        let path = std::env::join_paths(paths).expect("join PATH");
        let mut command = Command::new(bin());
        command
            .args([
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
                "--no-expose",
            ])
            .env("PATH", path)
            .env("CI", "1")
            .env("TERM", "dumb")
            .env("NO_COLOR", "1")
            .env("CURIE_TEST_CALLS_LOG", &self.calls_log)
            .env("CURIE_TEST_UPGRADE_ARGV_LOG", &self.upgrade_argv_log)
            .env("CURIE_TEST_VALUES_DIR", &self.values_dir)
            .env("CURIE_TEST_EXISTING_VALUES", &self.existing_values)
            .env("CURIE_TEST_PROVIDER_EGRESS_JSON", RESOLVER)
            .env("CURIE_CONFIG_DIR", self.config_dir())
            .env_remove("CURIE_CREDENTIALS")
            .env_remove("CURIE_MODEL_CREDENTIALS")
            .env_remove("CURIE_GITHUB_TOKEN")
            .env_remove("CURIE_MODEL")
            .env_remove("CURIE_FAKE_MODEL");
        command.args(args);
        for (key, value) in environment {
            command.env(key, value);
        }
        command.output().expect("run curie cluster up")
    }

    fn calls(&self) -> String {
        fs::read_to_string(&self.calls_log).unwrap_or_default()
    }

    /// The `helm upgrade` argv, with each `-f` operand replaced by the path of
    /// the captured copy (the CLI unlinks the original after Helm exits).
    fn upgrade_argv(&self) -> Vec<String> {
        fs::read_to_string(&self.upgrade_argv_log)
            .unwrap_or_default()
            .lines()
            .map(ToOwned::to_owned)
            .collect()
    }

    fn captured_files(&self) -> Vec<(PathBuf, String)> {
        let argv = self.upgrade_argv();
        argv.windows(2)
            .filter(|pair| pair[0] == "-f")
            .map(|pair| {
                let path = PathBuf::from(&pair[1]);
                let body = fs::read_to_string(&path).expect("read captured values file");
                (path, body)
            })
            .collect()
    }

    fn assert_succeeded(&self, output: &Output) {
        assert!(
            output.status.success(),
            "cluster up failed\n{}\ncalls:\n{}",
            all_output(output),
            self.calls()
        );
        assert!(
            !self.calls().contains("UNEXPECTED"),
            "cluster up made a call the shims do not model:\n{}",
            self.calls()
        );
        assert!(
            !self.upgrade_argv().is_empty(),
            "cluster up never ran helm upgrade:\n{}",
            self.calls()
        );
    }
}

fn all_output(output: &Output) -> String {
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// Helm's merge: maps merge recursively, every other value replaces.
fn merge(into: &mut Value, from: &Value) {
    match (into, from) {
        (Value::Object(into), Value::Object(from)) => {
            for (key, value) in from {
                match into.get_mut(key) {
                    Some(existing) if existing.is_object() && value.is_object() => {
                        merge(existing, value)
                    }
                    _ => {
                        into.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (into, from) => *into = from.clone(),
    }
}

/// The values `helm upgrade` received from files alone, merged in argv order.
fn merged_file_values(files: &[(PathBuf, String)]) -> Value {
    let mut merged = json!({});
    for (path, body) in files {
        let document: Value = serde_norway::from_str(body)
            .unwrap_or_else(|error| panic!("parse {} ({error}): {body}", path.display()));
        merge(&mut merged, &document);
    }
    merged
}

/// Every `--set`/`--set-string`/`--set-json` expression in the upgrade argv,
/// keyed by the dotted path it assigns.
fn set_expressions(argv: &[String]) -> BTreeMap<String, String> {
    argv.windows(2)
        .filter(|pair| matches!(pair[0].as_str(), "--set" | "--set-string" | "--set-json"))
        .filter_map(|pair| {
            pair[1]
                .split_once('=')
                .map(|(key, value)| (key.to_string(), value.to_string()))
        })
        .collect()
}

fn assert_no_set_lane_touches(argv: &[String], family: &str) {
    let sets = set_expressions(argv);
    let touched: Vec<&String> = sets
        .keys()
        .filter(|key| {
            *key == family
                || key.starts_with(&format!("{family}."))
                || key.starts_with(&format!("{family}["))
        })
        .collect();
    assert!(
        touched.is_empty(),
        "{family} must travel in the private values file, not argv: {touched:?}"
    );
}

#[test]
fn sealed_up_keeps_recorded_runner_values_and_ignores_an_unrelated_saved_credential() {
    let recorded = recorded_values();
    let fixture = Fixture::new(&recorded);
    fixture.save_model_credential(SAVED_OPENROUTER_CREDENTIAL);

    let output = fixture.cluster_up(&[]);
    fixture.assert_succeeded(&output);

    let argv = fixture.upgrade_argv();
    let files = fixture.captured_files();

    // a. No credential material or env value on the command line.
    for private in [
        CONNECTOR_SECRET_A,
        CONNECTOR_SECRET_B,
        SAVED_OPENROUTER_CREDENTIAL,
    ] {
        assert!(
            !argv.iter().any(|argument| argument.contains(private)),
            "{private} reached the helm argv: {argv:?}"
        );
    }
    assert!(
        !set_expressions(&argv)
            .values()
            .any(|value| value == NUMERIC_ENV_VALUE),
        "the numeric env value was re-supplied through a typed --set lane: {argv:?}"
    );
    assert_no_set_lane_touches(&argv, "agentSandbox.runner.extraEnv");
    assert_no_set_lane_touches(&argv, "agentSandbox.connectorSecrets");

    // b. The files re-supply the recorded families exactly, the reference is
    //    kept, and the saved credential is nowhere.
    let merged = merged_file_values(&files);
    for pointer in [
        "/agentSandbox/runner/extraEnv",
        "/agentSandbox/connectorSecrets",
    ] {
        assert_eq!(
            merged.pointer(pointer),
            recorded.pointer(pointer),
            "{pointer} must survive a sealed up exactly"
        );
    }
    assert_eq!(
        merged.pointer("/agentSandbox/runner/extraEnv/0/value"),
        Some(&json!(NUMERIC_ENV_VALUE)),
        "the numeric env string must stay a string"
    );
    let sets = set_expressions(&argv);
    for (pointer, dotted) in [
        (
            "/agentSandbox/runner/credentialsExistingSecret",
            "agentSandbox.runner.credentialsExistingSecret",
        ),
        (
            "/agentSandbox/runner/credentialsExistingSecretKey",
            "agentSandbox.runner.credentialsExistingSecretKey",
        ),
        (
            "/connectorCaller/existingSecret",
            "connectorCaller.existingSecret",
        ),
        (
            "/connectorCaller/signingKeyKey",
            "connectorCaller.signingKeyKey",
        ),
        (
            "/connectorCaller/verifyKeyKey",
            "connectorCaller.verifyKeyKey",
        ),
    ] {
        let supplied = sets
            .get(dotted)
            .map(|value| json!(value))
            .or_else(|| merged.pointer(pointer).cloned());
        assert_eq!(
            supplied.as_ref(),
            recorded.pointer(pointer),
            "{dotted} must be re-supplied with its recorded value"
        );
    }
    for (path, body) in &files {
        assert!(
            !body.contains(SAVED_OPENROUTER_CREDENTIAL),
            "the unrelated saved credential reached {}",
            path.display()
        );
    }
    assert!(
        merged.pointer("/agentSandbox/runner/credentials").is_none(),
        "no inline model credential may be installed beside the reference: {merged}"
    );

    // c. The real chart renders the same workloads before and after.
    let Some(helm) = real_helm() else {
        eprintln!("skipping render comparison: helm is not on PATH");
        return;
    };
    let recorded_file = fixture.temp.path().join("recorded-values.json");
    fs::write(&recorded_file, recorded.to_string()).expect("write recorded values");
    let before = render(
        &helm,
        &[
            "-f".to_string(),
            recorded_file.to_string_lossy().into_owned(),
        ],
    );
    let after = render(&helm, &value_arguments(&argv));

    let compared = compared_objects(&before);
    for (kind, name) in [
        ("SandboxTemplate", format!("{TARGET_RELEASE}-curie-runner")),
        (
            "SandboxTemplate",
            format!("{TARGET_RELEASE}-curie-agent-acme-a-runner"),
        ),
        (
            "SandboxTemplate",
            format!("{TARGET_RELEASE}-curie-agent-acme-b-runner"),
        ),
        (
            "Secret",
            format!("{TARGET_RELEASE}-curie-agent-acme-a-connector-secrets"),
        ),
        (
            "Secret",
            format!("{TARGET_RELEASE}-curie-agent-acme-b-connector-secrets"),
        ),
    ] {
        assert!(
            compared.contains_key(&(kind.to_string(), name.clone())),
            "the recorded values must render {kind} {name}; rendered: {:?}",
            compared.keys().collect::<Vec<_>>()
        );
    }
    let after_objects = compared_objects(&after);
    assert_eq!(
        after_objects.keys().collect::<Vec<_>>(),
        compared.keys().collect::<Vec<_>>(),
        "a sealed up changed which runner templates and connector Secrets render"
    );
    for (key, before_object) in &compared {
        assert!(
            after_objects.get(key) == Some(before_object),
            "{key:?} rendered differently after a sealed up\nbefore env: {:?}\nafter env: {:?}",
            env_names(before_object),
            after_objects.get(key).map(env_names)
        );
    }

    let runner = &compared[&(
        "SandboxTemplate".to_string(),
        format!("{TARGET_RELEASE}-curie-runner"),
    )];
    let port = container_env(runner, "PORT").expect("runner renders PORT");
    assert_eq!(
        port["value"],
        json!(NUMERIC_ENV_VALUE),
        "PORT must render as a string"
    );
    let references = model_credential_references(&after);
    assert_eq!(
        references.len(),
        3,
        "every runner template must read a model credential: {references:?}"
    );
    for (template, secret) in &references {
        assert_eq!(
            secret, "byo-model",
            "{template} must still read its model credential from the recorded Secret"
        );
    }
    assert_eq!(
        worker_env(&after, "CURIE_AGENT_CONNECTOR_SECRET_POOLS"),
        Some(json!("acme-a,acme-b")),
    );
    assert_eq!(
        worker_env(&before, "CURIE_AGENT_CONNECTOR_SECRET_POOLS"),
        worker_env(&after, "CURIE_AGENT_CONNECTOR_SECRET_POOLS"),
    );
    assert_eq!(
        worker_secret_refs(&before),
        worker_secret_refs(&after),
        "worker secretKeyRefs (model credential, connector caller pair) changed"
    );
}

/// A release that records its connector caller pair inline keeps exactly that
/// pair through a sealed up, and the pair stays a valid Ed25519 pair: the
/// signing key's public half is the merged verify key.
#[test]
fn sealed_up_keeps_a_valid_recorded_connector_caller_pair() {
    use base64::Engine as _;
    let seed =
        base64::engine::general_purpose::STANDARD.encode(b"acme-3848-caller-pair-seed-00001");
    let verify = curie::connector_caller::verify_key_of(&seed).expect("derive verify key");

    let mut recorded = recorded_values();
    recorded["connectorCaller"] = json!({"signingKey": seed, "verifyKey": verify});
    let fixture = Fixture::new(&recorded);

    let output = fixture.cluster_up(&[]);
    fixture.assert_succeeded(&output);

    let argv = fixture.upgrade_argv();
    let merged = merged_file_values(&fixture.captured_files());
    let sets = set_expressions(&argv);
    let supplied = |pointer: &str, dotted: &str| -> Option<String> {
        sets.get(dotted).cloned().or_else(|| {
            merged
                .pointer(pointer)
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
    };
    let merged_signing = supplied("/connectorCaller/signingKey", "connectorCaller.signingKey")
        .expect("the signing key must be re-supplied");
    let merged_verify = supplied("/connectorCaller/verifyKey", "connectorCaller.verifyKey")
        .expect("the verify key must be re-supplied");

    assert_eq!(
        merged_signing, seed,
        "the recorded signing key must not change"
    );
    assert_eq!(
        merged_verify, verify,
        "the recorded verify key must not change"
    );
    assert_eq!(
        curie::connector_caller::verify_key_of(&merged_signing).expect("merged signing key"),
        merged_verify,
        "the merged pair must be a valid Ed25519 pair"
    );
}

/// Liveness of the explicit path: an explicit `CURIE_CREDENTIALS` is still a
/// change request and replaces the recorded reference, saved credential or not.
#[test]
fn explicit_credential_still_replaces_the_recorded_reference() {
    let recorded = recorded_values();
    let fixture = Fixture::new(&recorded);
    fixture.save_model_credential(SAVED_OPENROUTER_CREDENTIAL);

    // The explicit credential names a different provider than the recorded
    // egress; `cluster up` infers and opens that provider's egress itself.
    let output = fixture.cluster_up(&[("CURIE_CREDENTIALS", EXPLICIT_ANTHROPIC_CREDENTIAL)]);
    fixture.assert_succeeded(&output);

    let argv = fixture.upgrade_argv();
    let files = fixture.captured_files();
    let merged = merged_file_values(&files);
    assert_eq!(
        merged.pointer("/agentSandbox/runner/credentials"),
        Some(&json!(EXPLICIT_ANTHROPIC_CREDENTIAL)),
        "the explicit credential is installed through the private values file"
    );
    let reference = set_expressions(&argv)
        .get("agentSandbox.runner.credentialsExistingSecret")
        .map(|value| json!(value))
        .or_else(|| {
            merged
                .pointer("/agentSandbox/runner/credentialsExistingSecret")
                .cloned()
        });
    assert_eq!(
        reference,
        Some(json!("")),
        "an explicit credential clears the recorded reference"
    );
    for (path, body) in &files {
        assert!(
            !body.contains(SAVED_OPENROUTER_CREDENTIAL),
            "the saved credential reached {}",
            path.display()
        );
    }
    assert!(
        !argv
            .iter()
            .any(|argument| argument.contains(EXPLICIT_ANTHROPIC_CREDENTIAL)),
        "the explicit credential reached argv: {argv:?}"
    );
    // The retained families still survive an explicit credential change.
    assert_eq!(
        merged.pointer("/agentSandbox/connectorSecrets"),
        recorded.pointer("/agentSandbox/connectorSecrets"),
        "connector bindings must survive an explicit model credential change"
    );
}

fn real_helm() -> Option<String> {
    let found = Command::new("sh")
        .args(["-c", "command -v helm"])
        .output()
        .ok()?;
    if !found.status.success() {
        return None;
    }
    let path = String::from_utf8(found.stdout).ok()?.trim().to_string();
    (!path.is_empty()).then_some(path)
}

/// The value-bearing arguments of the captured upgrade, in order, with the
/// `-f` operands pointing at the captured copies.
fn value_arguments(argv: &[String]) -> Vec<String> {
    let mut arguments = Vec::new();
    let mut index = 0;
    while index < argv.len() {
        if matches!(
            argv[index].as_str(),
            "-f" | "--values" | "--set" | "--set-string" | "--set-json" | "--set-file"
        ) {
            let operand = argv
                .get(index + 1)
                .unwrap_or_else(|| panic!("value flag has no operand: {argv:?}"));
            arguments.push(argv[index].clone());
            arguments.push(operand.clone());
            index += 2;
        } else {
            index += 1;
        }
    }
    assert!(!arguments.is_empty(), "upgrade carried no values: {argv:?}");
    arguments
}

fn render(helm: &str, value_arguments: &[String]) -> Vec<Value> {
    let output = Command::new(helm)
        .args([
            "template",
            TARGET_RELEASE,
            chart(),
            "--namespace",
            TARGET_NAMESPACE,
        ])
        .args(value_arguments)
        .output()
        .expect("run helm template");
    assert!(
        output.status.success(),
        "real Helm rejected the values {value_arguments:?}\n{}",
        all_output(&output)
    );
    let rendered = String::from_utf8(output.stdout).expect("rendered chart is UTF 8");
    serde_norway::Deserializer::from_str(&rendered)
        .filter_map(|document| {
            let value = <Value as serde::Deserialize>::deserialize(document)
                .unwrap_or_else(|error| panic!("parse rendered chart ({error})"));
            (!value.is_null()).then_some(value)
        })
        .collect()
}

fn kind_and_name(object: &Value) -> (String, String) {
    (
        object["kind"].as_str().unwrap_or_default().to_string(),
        object
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
    )
}

/// The objects the retained values feed: every SandboxTemplate and every
/// per-agent connector Secret, keyed by kind and name.
fn compared_objects(rendered: &[Value]) -> BTreeMap<(String, String), Value> {
    rendered
        .iter()
        .filter(|object| {
            let (kind, name) = kind_and_name(object);
            kind == "SandboxTemplate" || (kind == "Secret" && name.ends_with("-connector-secrets"))
        })
        .map(|object| (kind_and_name(object), object.clone()))
        .collect()
}

fn containers(object: &Value) -> Vec<&Value> {
    [
        "/spec/podTemplate/spec/containers",
        "/spec/template/spec/containers",
    ]
    .iter()
    .filter_map(|pointer| object.pointer(pointer).and_then(Value::as_array))
    .flatten()
    .collect()
}

fn container_env<'a>(object: &'a Value, name: &str) -> Option<&'a Value> {
    containers(object)
        .into_iter()
        .filter_map(|container| container["env"].as_array())
        .flatten()
        .find(|entry| entry["name"] == name)
}

/// SandboxTemplate name -> the Secret its runner reads `CURIE_CREDENTIALS` from.
fn model_credential_references(rendered: &[Value]) -> BTreeMap<String, String> {
    rendered
        .iter()
        .filter(|object| object["kind"] == "SandboxTemplate")
        .filter_map(|template| {
            let secret = container_env(template, "CURIE_CREDENTIALS")?
                .pointer("/valueFrom/secretKeyRef/name")?
                .as_str()?;
            Some((kind_and_name(template).1, secret.to_string()))
        })
        .collect()
}

fn env_names(object: &Value) -> Vec<String> {
    containers(object)
        .into_iter()
        .filter_map(|container| container["env"].as_array())
        .flatten()
        .filter_map(|entry| entry["name"].as_str().map(str::to_string))
        .collect()
}

fn worker(rendered: &[Value]) -> &Value {
    rendered
        .iter()
        .find(|object| {
            object["kind"] == "Deployment"
                && object
                    .pointer("/metadata/name")
                    .and_then(Value::as_str)
                    .is_some_and(|name| name == format!("{TARGET_RELEASE}-curie-worker"))
        })
        .expect("the chart renders the worker Deployment")
}

fn worker_env(rendered: &[Value], name: &str) -> Option<Value> {
    container_env(worker(rendered), name).map(|entry| entry["value"].clone())
}

fn worker_secret_refs(rendered: &[Value]) -> BTreeMap<String, Value> {
    containers(worker(rendered))
        .into_iter()
        .filter_map(|container| container["env"].as_array())
        .flatten()
        .filter_map(|entry| {
            entry.pointer("/valueFrom/secretKeyRef").map(|reference| {
                (
                    entry["name"].as_str().unwrap_or_default().to_string(),
                    reference.clone(),
                )
            })
        })
        .collect()
}

#[test]
fn ordered_operator_files_reach_real_helm_typed_and_unchanged_after_source_edit() {
    // @spec CLUSTER-VALUES-FILES c1-c4
    let helm = real_helm().expect("real Helm required for ordered values proof");
    let fixture = Fixture::new(&recorded_values());
    let first = fixture.temp.path().join("first.yaml");
    let second = fixture.temp.path().join("second.yaml");
    fs::write(&first, "worker:\n  extraEnv:\n    - name: NUMERIC_STRING\n      value: '1000'\n  adapterCredentials: {}\nagentSandbox:\n  connectorSecrets:\n    acme-a:\n      GRAFANA_TOKEN: PLACEHOLDER-new-file-secret\n").unwrap();
    fs::write(&second, "worker:\n  extraEnv:\n    - name: NUMERIC_STRING\n      value: '8080'\napi:\n  podLabels:\n    example.com/key: 'off'\n").unwrap();
    let out = fixture.cluster_up_with_args(
        &[
            ("CURIE_TEST_REAL_HELM", &helm),
            ("CURIE_TEST_MUTATE_VALUES", second.to_str().unwrap()),
        ],
        &[
            "-f",
            first.to_str().unwrap(),
            "--values-file",
            second.to_str().unwrap(),
        ],
    );
    fixture.assert_succeeded(&out);
    let captured = merged_file_values(&fixture.captured_files());
    assert_eq!(
        captured.pointer("/worker/extraEnv/0/value"),
        Some(&json!("8080"))
    );
    assert_eq!(
        captured.pointer("/worker/adapterCredentials"),
        Some(&json!({}))
    );
    assert_eq!(
        captured.pointer("/api/podLabels/example.com~1key"),
        Some(&json!("off"))
    );
    assert_eq!(
        captured.pointer("/agentSandbox/connectorSecrets/acme-a/GRAFANA_TOKEN"),
        Some(&json!("PLACEHOLDER-new-file-secret"))
    );
    assert!(fs::read_to_string(second).unwrap().contains("MUTATED"));
    assert!(!all_output(&out).contains("PLACEHOLDER-new-file-secret"));
    assert!(!fixture
        .upgrade_argv()
        .iter()
        .any(|arg| arg.contains("PLACEHOLDER-new-file-secret")));
    let rendered = render(&helm, &value_arguments(&fixture.upgrade_argv()));
    assert_eq!(
        worker_env(&rendered, "NUMERIC_STRING").unwrap(),
        json!("8080")
    );
}

#[test]
fn fresh_install_with_files_still_generates_required_store_credentials() {
    // @spec CLUSTER-VALUES-FILES c1-c3
    let helm = real_helm().expect("real Helm required");
    let fixture = Fixture::new(&serde_json::json!({}));
    let file = fixture.temp.path().join("fresh.yaml");
    fs::write(
        &file,
        "security:\n  gvisor:\n    mode: 'off'\nagentSandbox:\n  controller:\n    deploy: false\n",
    )
    .unwrap();
    let output = fixture.cluster_up_with_args(
        &[
            ("CURIE_TEST_REAL_HELM", &helm),
            ("CURIE_TEST_FIRST_INSTALL", "1"),
        ],
        &["--fake-model", "-f", file.to_str().unwrap()],
    );
    fixture.assert_succeeded(&output);
    let values = merged_file_values(&fixture.captured_files());
    for path in ["/postgres/auth/password", "/valkey/password", "/api/apiKey"] {
        assert!(
            values
                .pointer(path)
                .and_then(Value::as_str)
                .is_some_and(|s| s.len() >= 32),
            "missing generated credential: {path}"
        );
    }
}

#[test]
fn dedicated_flags_override_file_identity_and_service_choices() {
    // @spec CLUSTER-VALUES-FILES c1, CLUSTER-VALUES-FILES c3
    let helm = real_helm().expect("real Helm required");
    for clear in [false, true] {
        let fixture = Fixture::new(&serde_json::json!({}));
        let file = fixture.temp.path().join("flag-overrides.yaml");
        fs::write(&file, "security:\n  gvisor:\n    mode: 'off'\napi:\n  githubToken: PLACEHOLDER-file-token\n  githubTokenExistingSecret: file-github\nagentSandbox:\n  controller:\n    deploy: false\n  runner:\n    model: file-model\n    credentialsExistingSecret: file-model-secret\nui:\n  service:\n    type: NodePort\nlangfuse:\n  web:\n    service:\n      type: NodePort\n").unwrap();
        let mut args = vec!["-f", file.to_str().unwrap(), "--model", "flag-model"];
        if clear {
            args.push("--clear-github-token");
        } else {
            args.extend(["--github-token", "PLACEHOLDER-flag-token"]);
        }
        let output = fixture.cluster_up_with_args(
            &[
                ("CURIE_TEST_REAL_HELM", &helm),
                ("CURIE_CREDENTIALS", "sk-ant-api03-PLACEHOLDER-explicit"),
            ],
            &args,
        );
        fixture.assert_succeeded(&output);
        let values = merged_file_values(&fixture.captured_files());
        assert_eq!(
            values.pointer("/api/githubToken").and_then(Value::as_str),
            Some(if clear { "" } else { "PLACEHOLDER-flag-token" })
        );
        assert_eq!(
            values
                .pointer("/api/githubTokenExistingSecret")
                .and_then(Value::as_str),
            Some("")
        );
        assert_eq!(
            values
                .pointer("/agentSandbox/runner/model")
                .and_then(Value::as_str),
            Some("flag-model")
        );
        assert_eq!(
            values
                .pointer("/agentSandbox/runner/credentialsExistingSecret")
                .and_then(Value::as_str),
            Some("")
        );
        assert_eq!(
            values
                .pointer("/agentSandbox/runner/credentials")
                .and_then(Value::as_str),
            Some("sk-ant-api03-PLACEHOLDER-explicit")
        );
        assert_eq!(
            values.pointer("/ui/service/type").and_then(Value::as_str),
            Some("ClusterIP")
        );
        assert_eq!(
            values
                .pointer("/langfuse/web/service/type")
                .and_then(Value::as_str),
            Some("ClusterIP")
        );
        for secret in [
            "PLACEHOLDER-flag-token",
            "sk-ant-api03-PLACEHOLDER-explicit",
        ] {
            assert!(!all_output(&output).contains(secret));
            assert!(!fixture.upgrade_argv().join(" ").contains(secret));
        }
    }
}
