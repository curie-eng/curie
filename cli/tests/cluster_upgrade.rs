//! `curie cluster upgrade` (#2301): one resumable lifecycle with exact
//! convergence and a target-version canary before success.
//!
//! Sibling issues own other slices and are not reimplemented here:
//! - #2299 versioned configuration migrations
//! - #2300 database compatibility windows
//! - #2097 the kind released-install upgrade CI rung
//!
//! DrainPreflight reuse: one worker-reachability check per attempt, resume
//! after it does not repeat it. It is a preflight, not the #2010 drain gate
//! itself (issue #2830); that gate is the chart's pre-upgrade Helm hook Job,
//! observed at Converge.

use curie::ops::{
    run_lifecycle, ClusterUpgradeOutput, CommonOpts, FakeUpgradeHost, LayerClearReason,
    RunnerLayerPlan, UpgradeChart, UpgradeOpts, UpgradePhase,
};
use curie::ui::CliOutput;
use serde_json::{json, Value};

pub(super) const FACTORY_READ_WRAPPER: &str = r#"#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

root = Path(__file__).resolve().parent
tool = Path(sys.argv[0]).name
args = sys.argv[1:]
settings = json.loads((root / 'factory-inputs.json').read_text())

def finish(value=None, error=None):
    log = os.environ.get('CURIE_TEST_CALL_LOG')
    if log:
        with open(log, 'a') as stream:
            stream.write('FACTORY_CALL: ' + json.dumps([tool, *args]) + '\n')
    else:
        with open(root / 'argv.log', 'a') as stream:
            stream.write(json.dumps([tool, *args]) + '\n')
    if error:
        print(error, file=sys.stderr)
        sys.exit(1)
    print(json.dumps(value))
    sys.exit(0)

def flag(name):
    for index, arg in enumerate(args):
        if arg == name and index + 1 < len(args):
            return args[index + 1]
        if arg.startswith(name + '='):
            return arg.split('=', 1)[1]
    return None

if tool == 'helm':
    if args[:1] == ['history']:
        finish(settings['history'])
    if args[:2] == ['get', 'values']:
        revision = flag('--revision') or settings.get('unpinned_revision', '5')
        if '--all' in args and settings.get('computed_error'):
            finish(error=settings['computed_error'])
        section = 'computed' if '--all' in args else 'user'
        finish(settings[section][revision])
    if args[:2] == ['get', 'metadata']:
        revision = flag('--revision') or '5'
        version = '0.12.5' if revision == '4' else '0.13.0'
        finish({'name': 'rel', 'chart': 'curie', 'version': version,
                'appVersion': version, 'revision': int(revision), 'namespace': 'ns',
                'status': 'deployed' if revision == '4' else 'failed'})
    if args[:2] == ['show', 'chart']:
        finish({'apiVersion': 'v2', 'name': 'curie', 'version': '0.13.0', 'appVersion': '0.13.0'})
if tool == 'kubectl' and 'get' in args:
    at = args.index('get')
    resource = args[at + 1] if at + 1 < len(args) else ''
    if resource in ('secret', 'secrets'):
        finish(settings.get('secret'), settings.get('secret_error'))
    if resource in ('configmap', 'configmaps') and at + 2 < len(args) and args[at + 2] == 'acme-factory-ca':
        finish(settings.get('ca'), settings.get('ca_error'))

delegate = root / 'base' / tool
if tool == 'helm' and args and args[0] in ('template', 'upgrade'):
    documents = []
    for index, arg in enumerate(args[:-1]):
        if arg in ('-f', '--values'):
            documents.append(Path(args[index + 1]).read_text())
    log = os.environ.get('CURIE_TEST_CALL_LOG')
    if log:
        with open(log, 'a') as stream:
            stream.write('FACTORY_VALUES: ' + json.dumps({'argv': args, 'documents': documents}) + '\n')
if not delegate.exists():
    finish(error='unexpected resolver command')
os.execv(str(delegate), [str(delegate), *args])
"#;

pub(super) fn factory_read_inputs(user: Value, computed: Value) -> Value {
    let mut older = user.clone();
    older["worker"]["runnerTotalTimeoutSeconds"] = json!(411);
    json!({
        "history": [
            {"revision": 4, "status": "deployed", "chart": "curie-0.12.5", "app_version": "0.12.5", "description": "Upgrade complete"},
            {"revision": 5, "status": "failed", "chart": "curie-0.13.0", "app_version": "0.13.0", "description": "Upgrade failed"}
        ],
        "user": {"4": older, "5": user},
        "computed": {"4": computed.clone(), "5": computed}
    })
}

const FACTORY_RESOLVER_CHILD: &str = "CURIE_TEST_FACTORY_RESOLVER_CHILD";

async fn factory_resolver_in_child(selector: &str) -> bool {
    if std::env::var(FACTORY_RESOLVER_CHILD).as_deref() == Ok(selector) {
        return true;
    }
    let child = {
        let _lock = crate::support::ENV_LOCK.lock().await;
        std::process::Command::new(std::env::current_exe().expect("integration test executable"))
            .args([
                "--exact",
                selector,
                "--nocapture",
                "--test-threads=1",
                "--format=pretty",
            ])
            .env(FACTORY_RESOLVER_CHILD, selector)
            .env("RUST_TEST_THREADS", "1")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn isolated factory resolver case")
    };
    let output = child
        .wait_with_output()
        .expect("wait for isolated resolver case");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "isolated resolver case {selector} failed:\n{stdout}\n{stderr}"
    );
    assert!(
        stdout.contains("running 1 test")
            && stdout.contains("test result: ok. 1 passed; 0 failed;"),
        "the isolated process must execute exactly the selected resolver case:\n{stdout}\n{stderr}"
    );
    false
}

struct FactoryResolverEnvironment(Vec<(&'static str, Option<std::ffi::OsString>)>);

impl FactoryResolverEnvironment {
    fn set(values: &[(&'static str, std::ffi::OsString)]) -> Self {
        assert!(
            std::env::var_os(FACTORY_RESOLVER_CHILD).is_some(),
            "resolver environment changes require an isolated test process"
        );
        let mut previous = Vec::new();
        for (key, value) in values {
            previous.push((*key, std::env::var_os(key)));
            std::env::set_var(key, value);
        }
        Self(previous)
    }
}

impl Drop for FactoryResolverEnvironment {
    fn drop(&mut self) {
        for (key, value) in self.0.iter().rev() {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

struct FactoryResolverFixture {
    dir: tempfile::TempDir,
    inputs: Value,
    pem: String,
}

impl FactoryResolverFixture {
    fn new(user: Value, computed: Value) -> Self {
        use base64::Engine;
        let dir = tempfile::tempdir().expect("resolver fixture directory");
        for tool in ["helm", "kubectl"] {
            crate::support::executable::install(&dir.path().join(tool), FACTORY_READ_WRAPPER);
        }
        let output = std::process::Command::new("openssl")
            .args(["genrsa", "2048"])
            .output()
            .expect("OpenSSL is required for the external GitHub fixture");
        assert!(output.status.success(), "generate fixture App key");
        let pem = String::from_utf8(output.stdout).unwrap();
        let mut inputs = factory_read_inputs(user, computed);
        inputs["secret"] = json!({
            "apiVersion": "v1", "kind": "Secret",
            "metadata": {"name": "acme-factory-app", "namespace": "ns",
                "annotations": {"curie.ai/github-app-id": "90817"}},
            "data": {"signing.pem": base64::engine::general_purpose::STANDARD.encode(pem.as_bytes())}
        });
        let fixture = Self { dir, inputs, pem };
        fixture.save();
        fixture
    }

    fn save(&self) {
        std::fs::write(
            self.dir.path().join("factory-inputs.json"),
            self.inputs.to_string(),
        )
        .unwrap();
    }

    fn environment(&self, first_url: &str, second_url: &str) -> FactoryResolverEnvironment {
        let mut paths = vec![self.dir.path().to_path_buf()];
        paths.extend(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        ));
        FactoryResolverEnvironment::set(&[
            ("PATH", std::env::join_paths(paths).unwrap()),
            ("CURIE_GITHUB_API_URL", first_url.into()),
            ("GITHUB_API_URL", second_url.into()),
        ])
    }

    fn calls(&self) -> Vec<Vec<String>> {
        std::fs::read_to_string(self.dir.path().join("argv.log"))
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn upgrade_dry_run(&self) -> std::process::Output {
        self.upgrade_run(true)
    }

    fn upgrade_run(&self, dry_run: bool) -> std::process::Output {
        let base = self.dir.path().join("base");
        std::fs::create_dir(&base).unwrap();
        for tool in ["helm", "kubectl"] {
            crate::support::executable::install(
                &base.join(tool),
                include_str!("data/upgrade-driver.py"),
            );
        }
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_curie"));
        command.args([
            "--json",
            "cluster",
            "upgrade",
            "--namespace",
            "ns",
            "--release",
            "rel",
            "--to",
            "0.13.0",
            "--chart",
            "charts/curie",
            "--yes",
        ]);
        if dry_run {
            command.arg("--dry-run");
        }
        command
            .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
            .env(
                "PATH",
                crate::support::executable::isolated_fixture_path(self.dir.path()),
            )
            .env("UPGRADE_DRIVER_ROOT", self.dir.path())
            .env("UPGRADE_DRIVER_SCENARIO", "healthy")
            .env("CURIE_GITHUB_API_URL", "http://127.0.0.1:1")
            .env("GITHUB_API_URL", "http://127.0.0.1:2")
            .output()
            .expect("real upgrade CLI")
    }
}

fn resolver_factory_values(origin: &str) -> Value {
    let mut values = super::config_migrate::legacy_factory_values();
    values["api"]["githubAppId"] = json!("90817");
    values["api"]["githubApiUrl"] = json!(origin);
    values["api"]["githubFactoryIntake"] = json!("poll");
    let endpoint = reqwest::Url::parse(origin).unwrap();
    let host = format!(
        "{}:{}",
        endpoint.host_str().unwrap(),
        endpoint.port().unwrap()
    );
    let credential = json!({"type": "github_app", "appId": "90817",
        "existingSecret": "acme-factory-app", "existingSecretKey": "signing.pem"});
    let egress = json!([{"cidr": "127.0.0.1/32", "ports": [{"protocol": "TCP", "port": endpoint.port().unwrap()}]}]);
    values["factory"] = json!({
        "codeHosts": [{"name": "retained-code", "kind": "github", "host": host,
            "apiUrl": origin, "credential": credential.clone(), "egress": egress.clone()}],
        "trackers": [{"name": "retained-issues", "kind": "github", "host": host,
            "apiUrl": origin, "credential": credential, "egress": egress,
            "label": "triage-ready", "mention": "acme-maintainer", "pollIntervalSeconds": 83}],
        "sandboxEgress": {"acme-bot": true}
    });
    values
}

// https://docs.github.com/en/rest/apps/apps#get-the-authenticated-app
// https://docs.github.com/en/rest/apps/apps#create-an-installation-access-token-for-an-app
// https://docs.github.com/en/rest/apps/installations#list-repositories-accessible-to-the-app-installation
// https://docs.github.com/en/rest/repos/repos#get-a-repository
fn factory_resolver_http(failure: &str) -> crate::support::MockServer {
    let failure = failure.to_string();
    crate::support::serve(move |request| {
        let path = request.path.split('?').next().unwrap();
        let body = match (request.method.as_str(), path) {
            ("GET", "/app") if failure == "app-auth" => {
                return crate::support::Response::json(
                    401,
                    r#"{"message":"raw-http-sentinel-4628"}"#,
                );
            }
            ("GET", "/app") => json!({"id": 90817, "slug": "acme-maintainer"}),
            ("GET", "/app/installations") => json!([{"id": 29}]),
            ("POST", "/app/installations/29/access_tokens") => {
                json!({"token": "installation-token-sentinel-4628", "expires_at": "2099-01-01T00:00:00Z"})
            }
            ("GET", "/installation/repositories") => {
                json!({"repositories": [{"id": 73, "full_name": "acme-corp/acme-bot"}]})
            }
            ("GET", "/repos/acme-corp/acme-bot") if failure == "repository" => {
                return crate::support::Response::json(
                    403,
                    r#"{"message":"raw-http-sentinel-4628"}"#,
                );
            }
            ("GET", "/repos/acme-corp/acme-bot") if failure == "repository-json" => {
                json!({"id": "raw-http-sentinel-4628"})
            }
            ("GET", "/repos/acme-corp/acme-bot") => {
                json!({"id": 73, "full_name": "acme-corp/acme-bot"})
            }
            _ => {
                return crate::support::Response::json(
                    404,
                    r#"{"message":"unexpected fixture route"}"#,
                )
            }
        };
        assert!(
            request.header("authorization").is_some(),
            "GitHub reads must authenticate"
        );
        crate::support::Response::json(200, &body.to_string())
    })
}

fn installed_factory_source(
    user: Value,
    computed: Value,
) -> curie::factory_config_migrate::InstalledFactorySource {
    curie::factory_config_migrate::InstalledFactorySource {
        revision: 4,
        installed_chart: "curie-0.12.5".into(),
        user_values: user,
        computed_values: computed,
    }
}

#[tokio::test]
async fn factory_resolver_uses_the_supplied_snapshot_and_ignores_both_url_environment_variables() {
    if !factory_resolver_in_child(
        "cluster_upgrade::factory_resolver_uses_the_supplied_snapshot_and_ignores_both_url_environment_variables",
    ).await {
        return;
    }
    let _lock = crate::support::ENV_LOCK.lock().await;
    let retained = factory_resolver_http("");
    let first = crate::support::serve(|_| crate::support::Response::json(503, "{}"));
    let second = crate::support::serve(|_| crate::support::Response::json(503, "{}"));
    let mut values = resolver_factory_values(&retained.base_url);
    values["api"]["githubStubCaConfigMap"] = json!("legacy-ca-must-not-be-read");
    let fixture = FactoryResolverFixture::new(values.clone(), values.clone());
    let _environment = fixture.environment(&first.base_url, &second.base_url);
    let source = installed_factory_source(values.clone(), values.clone());
    let context = curie::factory_config_migrate::resolve_factory_migration_context(
        &CommonOpts {
            namespace: "ns".into(),
            release: "rel".into(),
            dry_run: true,
        },
        Some(&source),
        &values,
    )
    .await
    .expect("resolve through the retained origin");
    let curie::config_migrate::FactoryMigrationContext::LegacyGitHub {
        effective_legacy,
        repositories,
    } = &context
    else {
        panic!("configured factory requires resolved migration context");
    };
    assert_eq!(repositories.len(), 1);
    assert_eq!(repositories[0].project_id, "73");
    assert_eq!(repositories[0].path, "acme-corp/acme-bot");
    assert!(!effective_legacy.to_string().contains(&fixture.pem));
    assert!(!effective_legacy
        .to_string()
        .contains("installation-token-sentinel-4628"));
    assert!(first.recorded().is_empty());
    assert!(second.recorded().is_empty());
    assert!(retained
        .recorded()
        .iter()
        .any(|request| request.path == "/repos/acme-corp/acme-bot"));
    let calls = fixture.calls();
    assert!(!calls.is_empty(), "read the referenced App Secret");
    assert!(
        calls
            .iter()
            .all(|call| call[0] == "kubectl" && call.iter().any(|arg| arg == "get")),
        "the caller snapshot must not cause Helm rereads or persistent writes: {calls:?}"
    );
    assert!(
        calls
            .iter()
            .any(|call| call.iter().any(|arg| arg == "acme-factory-app")),
        "read the retained Secret reference"
    );
    assert!(calls
        .iter()
        .all(|call| !call.iter().any(|arg| arg == "legacy-ca-must-not-be-read")));
    let outcome =
        curie::config_migrate::migrate_installed_config(values, Some("curie-0.12.5"), &context)
            .unwrap();
    assert_eq!(
        outcome.values["factory"]["bindings"][0]["repos"][0]["projectId"],
        "73"
    );
}

#[tokio::test]
async fn factory_resolver_refuses_secret_decode_pem_auth_and_repository_errors_without_retaining_their_chains(
) {
    use base64::Engine;
    if !factory_resolver_in_child(
        "cluster_upgrade::factory_resolver_refuses_secret_decode_pem_auth_and_repository_errors_without_retaining_their_chains",
    ).await {
        return;
    }
    let _lock = crate::support::ENV_LOCK.lock().await;
    for failure in [
        "secret",
        "decode",
        "pem",
        "app-auth",
        "repository",
        "repository-json",
        "ca-read",
        "ca-pem",
        "ca-key",
    ] {
        let retained = factory_resolver_http(failure);
        let mut values = resolver_factory_values(&retained.base_url);
        if failure.starts_with("ca-") {
            values["codeHostTrust"] = json!({"caBundle": {"configMapRef": {
                "name": "acme-factory-ca", "key": "ca.crt"
            }}});
        }
        let mut fixture = FactoryResolverFixture::new(values.clone(), values.clone());
        match failure {
            "secret" => {
                fixture.inputs["secret_error"] =
                    json!("secret-error-sentinel-4628 app 90817 acme-corp/acme-bot")
            }
            "decode" => {
                fixture.inputs["secret"]["data"]["signing.pem"] =
                    json!("not-base64-secret-sentinel-4628")
            }
            "pem" => {
                fixture.inputs["secret"]["data"]["signing.pem"] =
                    json!(base64::engine::general_purpose::STANDARD
                        .encode("pem-secret-sentinel-4628"))
            }
            "ca-read" => {
                fixture.inputs["ca_error"] =
                    json!("ca-read-sentinel-4628 app 90817 acme-corp/acme-bot")
            }
            "ca-pem" => {
                fixture.inputs["ca"] =
                    json!({"kind": "ConfigMap", "data": {"ca.crt": "ca-pem-sentinel-4628"}})
            }
            "ca-key" => {
                fixture.inputs["ca"] =
                    json!({"kind": "ConfigMap", "data": {"wrong.key": "ca-key-sentinel-4628"}})
            }
            _ => {}
        }
        fixture.save();
        let _environment = fixture.environment("http://127.0.0.1:1", "http://127.0.0.1:2");
        let source = installed_factory_source(values.clone(), values.clone());
        let result = curie::factory_config_migrate::resolve_factory_migration_context(
            &CommonOpts {
                namespace: "ns".into(),
                release: "rel".into(),
                dry_run: true,
            },
            Some(&source),
            &values,
        )
        .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("{failure} must refuse"),
        };
        let message = format!("{error}\n{error:#}\n{error:?}");
        let setting = if failure.starts_with("ca-") {
            "codeHostTrust.caBundle"
        } else if failure.starts_with("repository") {
            "api.githubRepoAllowlist"
        } else {
            "api.githubApp"
        };
        assert!(
            message.contains(setting),
            "{failure} must identify {setting}: {message}"
        );
        for forbidden in [
            "sentinel-4628",
            "90817",
            "acme-corp/acme-bot",
            fixture.pem.lines().nth(1).unwrap(),
        ] {
            assert!(
                !message.contains(forbidden),
                "{failure} error must contain setting names only"
            );
        }
        assert!(fixture
            .calls()
            .iter()
            .all(|call| call[0] == "kubectl" && call.iter().any(|arg| arg == "get")));
        if failure.starts_with("ca-") {
            assert!(
                fixture.calls().iter().any(|call| call
                    .windows(3)
                    .any(|args| args == ["get", "configmap", "acme-factory-ca"])),
                "{failure} must reach the referenced ConfigMap read"
            );
            let reason = match failure {
                "ca-read" => "could not be read",
                "ca-pem" => "PEM certificate",
                "ca-key" => "referenced CA key",
                _ => unreachable!(),
            };
            assert!(
                message.contains(reason),
                "{failure} must refuse at the intended CA boundary: {message}"
            );
            assert!(retained.recorded().is_empty());
        }
        if matches!(failure, "app-auth" | "repository" | "repository-json") {
            assert!(
                !retained.recorded().is_empty(),
                "the refusal must reach the external helper"
            );
        }
    }
}

#[tokio::test]
async fn factory_resolver_computed_defaults_and_supplied_disabled_defaults_do_not_discover_credentials(
) {
    if !factory_resolver_in_child(
        "cluster_upgrade::factory_resolver_computed_defaults_and_supplied_disabled_defaults_do_not_discover_credentials",
    ).await {
        return;
    }
    let _lock = crate::support::ENV_LOCK.lock().await;
    for supplied_disabled in [false, true] {
        let values = if supplied_disabled {
            json!({"config": {"schemaVersion": "0.9.0"}, "api": {"githubFactoryIngressEnabled": false}})
        } else {
            json!({"config": {"schemaVersion": "0.9.0"}})
        };
        let computed = json!({"config": {"schemaVersion": "0.9.0"}, "api": {
            "githubFactoryIngressEnabled": false, "githubFactoryIntake": "poll",
            "githubFactoryPollIntervalSeconds": 45, "githubFactoryLabel": "", "githubFactoryMention": "",
            "githubFactoryCardBaseUrl": "", "githubFactoryCiWaitSeconds": 1200,
            "githubFactoryBases": {}, "githubFactoryPythonCi": {}, "githubFactoryMetadataCi": {}
        }});
        let fixture = FactoryResolverFixture::new(values.clone(), computed.clone());
        let _environment = fixture.environment("http://127.0.0.1:1", "http://127.0.0.1:2");
        let source = installed_factory_source(values.clone(), computed);
        let context = curie::factory_config_migrate::resolve_factory_migration_context(
            &CommonOpts {
                namespace: "ns".into(),
                release: "rel".into(),
                dry_run: true,
            },
            Some(&source),
            &values,
        )
        .await
        .expect("unconfigured factory needs no discovery");
        match context {
            curie::config_migrate::FactoryMigrationContext::NotRequired => {
                assert!(!supplied_disabled)
            }
            curie::config_migrate::FactoryMigrationContext::LegacyGitHub {
                repositories, ..
            } => {
                assert!(supplied_disabled);
                assert!(repositories.is_empty());
            }
        }
        assert!(
            fixture.calls().is_empty(),
            "disabled/default inputs must not read credentials or trust"
        );
    }
}

#[test]
fn factory_upgrade_reads_serving_user_and_computed_values_at_the_same_revision() {
    let user = json!({
        "config": {"schemaVersion": "0.9.0"},
        "connectorCaller": {"existingSecret": "acme-caller-pair"},
        "api": {"githubFactoryIngressEnabled": false},
        "worker": {"runnerTotalTimeoutSeconds": 511}
    });
    let computed = json!({"config": {"schemaVersion": "0.9.0"}, "api": {
        "githubFactoryIngressEnabled": false, "githubFactoryIntake": "poll",
        "githubFactoryPollIntervalSeconds": 45, "githubFactoryLabel": "", "githubFactoryMention": "",
        "githubFactoryCardBaseUrl": "", "githubFactoryCiWaitSeconds": 1200,
        "githubFactoryBases": {}, "githubFactoryPythonCi": {}, "githubFactoryMetadataCi": {}
    }});
    let fixture = FactoryResolverFixture::new(user, computed);
    let output = fixture.upgrade_dry_run();
    assert!(
        output.status.success(),
        "dry upgrade must admit disabled migration: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let calls = fixture.calls();
    let reads: Vec<_> = calls
        .iter()
        .filter(|call| call.starts_with(&["helm".into(), "get".into(), "values".into()]))
        .collect();
    assert!(
        reads
            .iter()
            .any(|call| !call.iter().any(|arg| arg == "--all")),
        "read serving user values"
    );
    assert!(
        reads
            .iter()
            .any(|call| call.iter().any(|arg| arg == "--all")),
        "read matching serving computed values"
    );
    assert!(
        reads
            .iter()
            .all(|call| call.windows(2).any(|pair| pair == ["--revision", "4"])),
        "upgrade must pin every source document to serving revision 4: {reads:?}"
    );
    assert!(
        !calls.iter().any(|call| matches!(
            call.get(1).map(String::as_str),
            Some("upgrade" | "install" | "rollback" | "patch" | "create" | "apply" | "delete")
        )),
        "dry migration must not mutate: {calls:?}"
    );
    let rendered: Vec<_> = std::fs::read_dir(fixture.dir.path())
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("render-values-")
                .then_some(path)
        })
        .collect();
    assert!(!rendered.is_empty(), "validate the actual migrated overlay");
    for path in rendered {
        let values: Value =
            serde_norway::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(values["worker"]["runnerTotalTimeoutSeconds"], 411);
        assert_eq!(values["factory"]["enabled"], false);
        assert!(values["api"].get("githubFactoryIngressEnabled").is_none());
        assert!(
            values["factory"].get("ciWaitSeconds").is_none(),
            "computed defaults must not freeze intent"
        );
    }
}

#[test]
fn upgrade_release_not_found_preserves_supplied_values_in_admission_and_apply() {
    let helm = std::process::Command::new("sh")
        .args(["-c", "command -v helm"])
        .output()
        .expect("locate real Helm for supplied values parsing");
    assert!(helm.status.success(), "real Helm is required");
    let helm = String::from_utf8(helm.stdout).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base");
    std::fs::create_dir(&base).unwrap();
    for tool in [base.join("helm"), dir.path().join("kubectl")] {
        crate::support::executable::install(&tool, include_str!("data/upgrade-driver.py"));
    }
    crate::support::executable::install(
        &dir.path().join("helm"),
        r#"#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

root = Path(os.environ['UPGRADE_DRIVER_ROOT'])
args = sys.argv[1:]
log = root / 'argv.log'
calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
if args[:2] == ['get', 'values'] and not any(call[:2] == ['helm', 'upgrade'] for call in calls):
    with log.open('a') as stream:
        stream.write(json.dumps(['helm', *args]) + '\n')
    print('Error: release: not found', file=sys.stderr)
    sys.exit(1)
delegate = root / 'base' / 'helm'
os.execv(str(delegate), [str(delegate), *args])
"#,
    );
    let supplied = json!({
        "config": {"schemaVersion": "0.9.0"},
        "connectorCaller": {"existingSecret": "acme-caller-pair"},
        "worker": {"deliveryBudgetSeconds": 733,
            "extraEnv": [{"name": "ACME_NUMERIC_STRING", "value": "8080"}]},
        "api": {"podLabels": {"example.com/preserved": "off"}}
    });
    let file = dir.path().join("operator-values.yaml");
    std::fs::write(&file, serde_json::to_vec_pretty(&supplied).unwrap()).unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_curie"))
        .args([
            "--json",
            "cluster",
            "upgrade",
            "--to",
            "0.9.0",
            "--namespace",
            "ns",
            "--release",
            "rel",
            "--chart",
            "charts/curie",
            "--yes",
            "--values-file",
        ])
        .arg(&file)
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
        .env(
            "PATH",
            crate::support::executable::isolated_fixture_path(dir.path()),
        )
        .env("UPGRADE_DRIVER_ROOT", dir.path())
        .env("UPGRADE_DRIVER_SCENARIO", "healthy")
        .env("VALUES_FILES_REAL_HELM", helm.trim())
        .output()
        .expect("real upgrade CLI with supplied values and a positively missing release");
    assert!(
        output.status.success(),
        "supplied files must survive a positively missing retained release: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let calls: Vec<Vec<String>> = std::fs::read_to_string(dir.path().join("argv.log"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let values_read = calls
        .iter()
        .position(|call| call.starts_with(&["helm".into(), "get".into(), "values".into()]))
        .expect("reach the positively missing retained-values read");
    assert!(
        calls[..values_read]
            .iter()
            .any(|call| call.starts_with(&["helm".into(), "history".into()])),
        "exercise release-not-found after history, rather than the no-history branch"
    );
    let upgrades: Vec<_> = calls
        .iter()
        .enumerate()
        .filter(|(_, call)| call.starts_with(&["helm".into(), "upgrade".into()]))
        .collect();
    assert_eq!(upgrades.len(), 1, "one lifecycle Apply");
    assert!(values_read < upgrades[0].0);
    assert!(
        upgrades[0].1.iter().any(|arg| arg == "-f"),
        "Apply must carry the supplied overlay"
    );
    let applied: Value =
        serde_norway::from_str(&std::fs::read_to_string(dir.path().join("values-1.yaml")).unwrap())
            .unwrap();
    for key in ["config", "connectorCaller", "worker", "api"] {
        assert_eq!(applied[key], supplied[key], "preserve supplied {key}");
    }
    let admitted: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("render-values-")
        })
        .collect();
    assert!(
        !admitted.is_empty(),
        "admit the supplied overlay before Apply"
    );
    for path in admitted {
        let values: Value =
            serde_norway::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(
            values, applied,
            "admission and Apply must use the same overlay"
        );
    }
}

#[test]
fn factory_upgrade_resolver_refusal_redacts_cli_output_before_target_render_or_helm_mutation() {
    let peer = factory_resolver_http("");
    let mut values = resolver_factory_values(&peer.base_url);
    values["connectorCaller"] = json!({"existingSecret": "acme-caller-pair"});
    let mut fixture = FactoryResolverFixture::new(values.clone(), values);
    fixture.inputs["secret_error"] =
        json!("secret-read-sentinel-4628 app 90817 acme-corp/acme-bot");
    fixture.save();
    let output = fixture.upgrade_dry_run();
    assert!(
        !output.status.success(),
        "failed credential read must refuse the upgrade"
    );
    let visible = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        visible.contains("api.githubApp"),
        "refusal must name the credential setting: {visible}"
    );
    for forbidden in ["sentinel-4628", "90817", "acme-corp/acme-bot"] {
        assert!(
            !visible.contains(forbidden),
            "CLI output must replace unsafe helper chains"
        );
    }
    let calls = fixture.calls();
    assert!(
        calls
            .iter()
            .any(|call| call[0] == "kubectl" && call.iter().any(|arg| arg == "acme-factory-app")),
        "refusal must reach the retained credential read"
    );
    assert!(calls.iter().all(|call| !matches!(
        call.get(1).map(String::as_str),
        Some("upgrade" | "install" | "rollback" | "patch" | "create" | "apply" | "delete")
    )));
    assert!(
        !calls.iter().any(|call| call[0] == "helm"
            && call.get(1).is_some_and(|arg| arg == "template")
            && !call
                .iter()
                .any(|arg| arg == "--show-only" || arg.starts_with("--show-only="))),
        "refuse before rendering the target overlay"
    );
    assert!(
        peer.recorded().is_empty(),
        "failed credential read precedes external authentication"
    );
}

#[test]
fn factory_upgrade_egress_port_only_connector_refuses_without_changing_helm_history() {
    for dry_run in [true, false] {
        let peer = factory_resolver_http("");
        let mut values = resolver_factory_values(&peer.base_url);
        values.as_object_mut().unwrap().remove("factory");
        values["connectorCaller"] = json!({"existingSecret": "acme-caller-pair"});
        let port = reqwest::Url::parse(&peer.base_url).unwrap().port().unwrap();
        values["agentSandbox"]["connectorEgress"] = json!({"acme-unrelated-4628": [{
            "cidr": "198.51.100.19/32", "ports": [{"protocol": "TCP", "port": port}]
        }]});
        let fixture = FactoryResolverFixture::new(values.clone(), values);
        let retained = std::fs::read(fixture.dir.path().join("factory-inputs.json")).unwrap();
        let read_history = || {
            let output = std::process::Command::new(fixture.dir.path().join("helm"))
                .args(["history", "rel", "-n", "ns", "-o", "json"])
                .output()
                .expect("read recorded Helm history");
            assert!(output.status.success());
            serde_json::from_slice::<Value>(&output.stdout).expect("Helm history JSON")
        };
        let before = read_history();
        let output = fixture.upgrade_run(dry_run);
        assert!(
            !output.status.success(),
            "matching the API port cannot authorize connector egress"
        );
        let visible = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        for setting in [
            "api.githubApiUrl",
            "factory.codeHosts.egress",
            "factory.trackers.egress",
        ] {
            assert!(
                visible.contains(setting),
                "refusal must name {setting}: {visible}"
            );
        }
        for forbidden in [
            peer.base_url.as_str(),
            "198.51.100.19",
            "acme-unrelated-4628",
            "90817",
            "acme-corp/acme-bot",
            "acme-factory-app",
            "installation-token-sentinel-4628",
            fixture.pem.lines().nth(1).unwrap(),
        ] {
            assert!(
                !visible.contains(forbidden),
                "CLI refusal must contain setting names only"
            );
        }
        assert_eq!(
            read_history(),
            before,
            "refusal must preserve serving revision and history"
        );
        assert!(
            std::fs::read(fixture.dir.path().join("factory-inputs.json")).unwrap() == retained,
            "refusal must preserve retained configuration and credentials"
        );
        let calls = fixture.calls();
        assert!(
            !calls.iter().any(|call| call[0] == "helm"
                && matches!(
                    call.get(1).map(String::as_str),
                    Some("upgrade" | "install" | "rollback" | "uninstall")
                )),
            "refusal must occur before any Helm mutation: {calls:?}"
        );
        assert!(
            !calls.iter().any(|call| call[0] == "helm"
                && call.get(1).is_some_and(|arg| arg == "template")
                && !call
                    .iter()
                    .any(|arg| arg == "--show-only" || arg.starts_with("--show-only="))),
            "unproven connector egress must not reach target rendering"
        );
        assert!(
            peer.recorded().iter().all(|request| request.method == "GET"
                || (request.method == "POST"
                    && request.path == "/app/installations/29/access_tokens")),
            "preparation must not write persistent GitHub configuration"
        );
    }
}

fn opts(to: &str) -> UpgradeOpts {
    UpgradeOpts {
        file_values: None,
        common: CommonOpts {
            namespace: "curie".into(),
            release: "curie".into(),
            dry_run: false,
        },
        to: to.into(),
        chart: UpgradeChart::AvailableLocal("charts/curie".into()),
        yes: true,
        forward_only: false,
        allow_stock_layer_clear: false,
        take_over: None,
    }
}

fn dry_opts(to: &str) -> UpgradeOpts {
    let mut o = opts(to);
    o.common.dry_run = true;
    o
}

#[tokio::test]
async fn real_upgrade_refuses_pending_release_before_tool_lookup() {
    let mut pending = opts("0.9.0");
    pending.chart = UpgradeChart::PendingRelease {
        source_url: "https://example.com/curie-0.9.0.tgz".into(),
        cache_path: "/cache/curie/v0.9.0/curie-0.9.0.tgz".into(),
    };

    let error = curie::ops::upgrade(pending)
        .await
        .expect_err("a real upgrade must never accept an unmaterialized release chart");
    assert_eq!(
        format!("{error:#}"),
        "a pending release chart is only valid for a dry run; download the chart before starting a real upgrade"
    );
}

fn output_json(out: &ClusterUpgradeOutput) -> serde_json::Value {
    out.to_json()
}

#[tokio::test]
async fn dry_run_emits_a_redacted_plan_and_does_not_mutate() {
    let secret = "credential-LEAK-2301-secret";
    let mut host = FakeUpgradeHost::installed("0.8.6").with_secret(secret);
    let out = run_lifecycle(dry_opts("0.9.0"), &mut host)
        .await
        .expect("dry-run plan");
    match &out {
        ClusterUpgradeOutput::DryRun(plan) => {
            assert!(
                plan.lines.iter().any(|l| l.contains("plan")),
                "plan must name the inspect/plan phase: {:?}",
                plan.lines
            );
            assert!(
                plan.lines
                    .iter()
                    .all(|l| !l.contains("--reuse-values")
                        && !l.contains("--reset-then-reuse-values")),
                "operator must not choose a Helm merge flag: {:?}",
                plan.lines
            );
            let joined = plan.lines.join("\n");
            assert!(!joined.contains(secret), "plan leaked credential: {joined}");
            assert!(
                joined.contains(&curie::ops::mask_secret(secret)),
                "plan must mask the credential it carried: {joined}"
            );
        }
        other => panic!("dry-run must not mutate: {other:?}"),
    }
    assert_eq!(host.mutate_calls, 0);
    assert_eq!(host.drain_calls, 0);
}

#[tokio::test]
async fn n_to_n_plus_one_succeeds_only_with_converge_and_canary() {
    let mut host = FakeUpgradeHost::installed("0.8.6");
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("upgrade");
    let json = output_json(&out);
    assert_eq!(json["status"], "succeeded");
    assert_eq!(json["phase"], "commit");
    assert_eq!(json["target_version"], "0.9.0");
    assert_eq!(json["from_version"], "0.8.6");
    assert_eq!(json["known_good_version"], "0.9.0");
    assert_eq!(json["convergence"]["exact"], true);
    assert_eq!(json["canary"]["passed"], true);
    assert_eq!(host.drain_calls, 1);
    assert!(host.mutate_calls >= 1);
    assert_eq!(host.current_version(), "0.9.0");
}

#[tokio::test]
async fn fresh_install_to_n_skips_drain_and_commits_known_good() {
    let mut host = FakeUpgradeHost::empty();
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("fresh install");
    let json = output_json(&out);
    assert_eq!(json["status"], "succeeded");
    assert_eq!(json["known_good_version"], "0.9.0");
    assert_eq!(
        host.drain_calls, 0,
        "a first install has nothing in flight; the #2010 hook is pre-upgrade only"
    );
    assert_eq!(host.current_version(), "0.9.0");
}

#[tokio::test]
async fn same_version_rerun_is_idempotent_and_still_proves_canary() {
    let mut host = FakeUpgradeHost::installed("0.9.0").with_known_good("0.9.0");
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("same-version rerun");
    let json = output_json(&out);
    assert_eq!(json["status"], "succeeded");
    assert_eq!(json["unchanged"], true);
    assert_eq!(json["convergence"]["exact"], true);
    assert_eq!(json["canary"]["passed"], true);
    assert_eq!(
        host.mutate_calls, 0,
        "a same-version rerun must not apply a new Helm revision"
    );
}

// #2861: a same-version rerun runs no drain preflight, checkpoint, migrate or
// Helm upgrade. The persisted record and the plan must say so instead of
// listing those phases as completed.
#[tokio::test]
async fn same_version_rerun_records_skipped_phases_not_completed_ones() {
    let mut host = FakeUpgradeHost::installed("0.9.0").with_known_good("0.9.0");
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("same-version rerun");
    let record: serde_json::Value =
        serde_json::from_str(&host.persisted_json()).expect("persisted record");
    assert_eq!(
        record["completed"],
        serde_json::json!(["plan", "validate", "converge", "canary", "commit"]),
        "only phases that executed are completed: {record}"
    );
    assert_eq!(
        record["skipped"],
        serde_json::json!(["drain_preflight", "checkpoint", "migrate", "apply"]),
        "{record}"
    );
    let json = output_json(&out);
    let plan: Vec<String> = serde_json::from_value(json["plan"].clone()).unwrap();
    assert!(
        plan.iter().any(|l| l.contains("0.9.0 is already installed")
            && l.contains("no helm upgrade runs")),
        "the plan must say the apply is skipped: {plan:?}"
    );
    assert!(
        !plan.iter().any(|l| l.starts_with("helm upgrade")
            || l.starts_with("phase drain_preflight:")
            || l.starts_with("phase checkpoint:")
            || l.starts_with("phase migrate:")),
        "a skipped apply must not be planned as a helm upgrade: {plan:?}"
    );
}

// A fresh install has nothing in flight, so DrainPreflight is skipped, not run.
// An interrupted install must still not replay it on resume.
#[tokio::test]
async fn fresh_install_records_drain_preflight_as_skipped_across_resume() {
    let mut host = FakeUpgradeHost::empty().interrupt_after(UpgradePhase::Apply);
    run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect_err("interrupted");
    host.clear_interrupt();
    run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("resume");
    let record: serde_json::Value =
        serde_json::from_str(&host.persisted_json()).expect("persisted record");
    assert_eq!(
        record["skipped"],
        serde_json::json!(["drain_preflight"]),
        "{record}"
    );
    assert!(
        !record["completed"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("drain_preflight")),
        "{record}"
    );
    assert_eq!(
        host.drain_calls, 0,
        "resume must not replay a skipped preflight"
    );
}

// Schema refusal at Validate must not begin mutation. `--forward-only` is a
// clap/LiveHost flag; FakeUpgradeHost keeps a boolean `refuse_schema`.
// The live binary tests in `cluster_upgrade_live.rs` own that AC:
// `pending_contract_refuses_and_names_forward_only` and
// `forward_only_allows_pending_contract_to_reach_helm_upgrade`.
#[tokio::test]
async fn validate_failure_does_not_begin_mutation() {
    let mut host = FakeUpgradeHost::installed("0.8.6").refuse_schema();
    let err = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect_err("schema refuse");
    let msg = format!("{err:#}");
    assert!(
        msg.contains(
            "database/application compatibility check refused the target schema before mutation"
        ),
        "error must name the validate refusal: {msg}"
    );
    assert_eq!(host.mutate_calls, 0);
    assert_eq!(host.drain_calls, 0);
    assert_eq!(host.current_version(), "0.8.6");
}

#[tokio::test]
async fn config_refuse_does_not_begin_mutation() {
    let mut host = FakeUpgradeHost::installed("0.8.6").refuse_config();
    let err = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect_err("config refuse");
    let msg = format!("{err:#}");
    assert!(
        msg.to_lowercase().contains("config") || msg.to_lowercase().contains("validate"),
        "error must name the config refusal: {msg}"
    );
    assert_eq!(host.mutate_calls, 0);
}

#[tokio::test]
async fn sequential_same_target_resume_does_not_redrain() {
    let mut host =
        FakeUpgradeHost::installed("0.8.6").interrupt_after(UpgradePhase::DrainPreflight);
    let err = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect_err("interrupted after drain preflight");
    assert!(format!("{err:#}").contains("interrupted"));
    assert_eq!(host.drain_calls, 1);

    host.clear_interrupt();
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("resume");
    let json = output_json(&out);
    assert_eq!(json["status"], "succeeded");
    assert_eq!(json["resumed"], true);
    assert_eq!(
        host.drain_calls, 1,
        "resume after drain preflight must not repeat it"
    );
}

#[tokio::test]
async fn interruption_at_every_durable_phase_can_resume() {
    for phase in UpgradePhase::ALL {
        let mut host = FakeUpgradeHost::installed("0.8.6").interrupt_after(phase);
        let err = run_lifecycle(opts("0.9.0"), &mut host)
            .await
            .expect_err("interrupted");
        assert!(
            format!("{err:#}").contains("interrupted"),
            "phase {phase:?} must persist then interrupt"
        );
        host.clear_interrupt();
        let out = run_lifecycle(opts("0.9.0"), &mut host)
            .await
            .expect("resume after interrupt");
        let json = output_json(&out);
        assert_eq!(
            json["status"], "succeeded",
            "resume after {phase:?} must complete"
        );
        assert_eq!(json["canary"]["passed"], true);
        assert_eq!(json["convergence"]["exact"], true);
        assert_eq!(json["known_good_version"], "0.9.0");
    }
}

#[tokio::test]
async fn success_is_refused_when_canary_fails() {
    let mut host = FakeUpgradeHost::installed("0.8.6").canary_fails();
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("failed canary is a completed failure payload");
    let json = output_json(&out);
    assert_ne!(json["status"], "succeeded");
    assert_eq!(json["status"], "failed");
    assert_eq!(json["canary"]["passed"], false);
    assert!(json["fail_forward"].is_object());
    assert_eq!(json["known_good_version"], "0.8.6");
}

#[tokio::test]
async fn success_is_refused_when_convergence_is_not_exact() {
    let mut host = FakeUpgradeHost::installed("0.8.6").converge_incomplete();
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("incomplete converge is a failure payload");
    let json = output_json(&out);
    assert_eq!(json["status"], "failed");
    assert_eq!(json["convergence"]["exact"], false);
    assert!(json.get("canary").is_none() || json["canary"].is_null());
}

#[tokio::test]
async fn manifest_mismatch_blocks_known_good_commit() {
    let mut host = FakeUpgradeHost::installed("0.8.6").manifest_mismatch();
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("manifest mismatch");
    let json = output_json(&out);
    assert_eq!(json["status"], "failed");
    assert_eq!(json["convergence"]["manifest_matches"], false);
    assert_eq!(json["known_good_version"], "0.8.6");
}

#[tokio::test]
async fn failure_before_apply_leaves_previous_version_serving() {
    let mut host = FakeUpgradeHost::installed("0.8.6").fail_at(UpgradePhase::Migrate);
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("failed migrate");
    let json = output_json(&out);
    assert_eq!(json["status"], "failed");
    assert_eq!(json["previous_serving"], true);
    assert_eq!(json["known_good_version"], "0.8.6");
    assert_eq!(host.current_version(), "0.8.6");
    assert_eq!(host.mutate_calls, 0);
}

#[tokio::test]
async fn mixed_versions_return_one_fail_forward_path() {
    let mut host = FakeUpgradeHost::installed("0.8.6")
        .fail_at(UpgradePhase::Converge)
        .mixed_versions_on_fail();
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("mixed fail");
    let json = output_json(&out);
    assert_eq!(json["status"], "failed");
    assert_eq!(json["previous_serving"], false);
    let ff = json["fail_forward"].as_object().expect("fail_forward");
    assert!(
        ff["command"]
            .as_str()
            .unwrap_or("")
            .contains("curie cluster upgrade"),
        "one bounded fail-forward command: {ff:?}"
    );
    assert!(
        !ff["command"].as_str().unwrap_or("").contains("helm "),
        "operator must not be sent to a raw Helm command: {ff:?}"
    );
}

#[tokio::test]
async fn persisted_record_is_redacted() {
    let secret = "credential-LEAK-2301-persist";
    let mut host = FakeUpgradeHost::installed("0.8.6").with_secret(secret);
    let _ = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("upgrade");
    let dumped = host.persisted_json();
    assert!(
        !dumped.contains(secret),
        "checkpoint leaked credential: {dumped}"
    );
}

#[tokio::test]
async fn in_flight_drain_refusal_does_not_mutate() {
    let mut host = FakeUpgradeHost::installed("0.8.6").in_flight(&["runs/curie/1-0"]);
    let out = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect("drain preflight refusal");
    let json = output_json(&out);
    assert_eq!(json["status"], "failed");
    assert_eq!(json["phase"], "drain_preflight");
    assert_eq!(json["previous_serving"], true);
    assert_eq!(host.mutate_calls, 0);
    assert_eq!(host.current_version(), "0.8.6");
}

#[tokio::test]
async fn cluster_status_reports_phase_and_known_good() {
    let mut host = FakeUpgradeHost::installed("0.8.6").interrupt_after(UpgradePhase::Checkpoint);
    let _ = run_lifecycle(opts("0.9.0"), &mut host).await;
    let view = host.status_view();
    assert_eq!(view.phase.as_deref(), Some("checkpoint"));
    assert_eq!(view.status, "in_progress");
    assert_eq!(view.known_good_version.as_deref(), Some("0.8.6"));
    assert_eq!(view.target_version.as_deref(), Some("0.9.0"));
}

#[tokio::test]
async fn sequential_different_target_is_refused_without_mutation() {
    let mut host = FakeUpgradeHost::installed("0.8.6").interrupt_after(UpgradePhase::Plan);
    let err = run_lifecycle(opts("0.9.0"), &mut host)
        .await
        .expect_err("first run interrupted, leaving an in-progress checkpoint");
    assert!(format!("{err:#}").contains("interrupted"));
    host.clear_interrupt();

    let err = run_lifecycle(opts("0.9.1"), &mut host)
        .await
        .expect_err("a different target is refused while one is in progress");
    assert!(
        format!("{err:#}").contains("already in progress"),
        "{err:#}"
    );
    assert_eq!(
        host.mutate_calls, 0,
        "the refused run must not have mutated"
    );
}

#[test]
fn upgrade_phase_parse_matches_as_str_and_rejects_unknown() {
    for phase in UpgradePhase::ALL {
        assert_eq!(UpgradePhase::parse(phase.as_str()), Some(phase));
        assert_eq!(
            UpgradePhase::parse(&format!("  {}  ", phase.as_str())),
            Some(phase),
            "parse must trim {}",
            phase.as_str()
        );
    }
    assert_eq!(UpgradePhase::parse("not-a-phase"), None);
    assert_eq!(UpgradePhase::parse(""), None);
}

/// #2862: a dry run exists to be checked before the real run, so a plan that
/// ends in a Validate refusal must fail exactly like the real run does, still
/// without mutating anything.
#[tokio::test]
async fn dry_run_whose_plan_refuses_validation_is_an_error() {
    for (label, host) in [
        (
            "schema",
            FakeUpgradeHost::installed("0.8.6").refuse_schema(),
        ),
        (
            "config",
            FakeUpgradeHost::installed("0.8.6").refuse_config(),
        ),
    ] {
        let mut host = host;
        let err = run_lifecycle(dry_opts("0.9.0"), &mut host)
            .await
            .expect_err(label);
        assert!(
            format!("{err:#}").contains("before mutation"),
            "{label}: the dry-run error must carry the Validate refusal: {err:#}"
        );
        assert_eq!(host.mutate_calls, 0, "{label}");
        assert_eq!(host.drain_calls, 0, "{label}");
        assert_eq!(host.current_version(), "0.8.6", "{label}");
    }
}

/// #2863: the printed apply line carries `--install` and the retained values
/// argument exactly when Apply passes them, with a placeholder for the path.
#[tokio::test]
async fn dry_run_apply_line_carries_install_and_retained_values_only_when_passed() {
    let apply_line = |out: ClusterUpgradeOutput| match out {
        ClusterUpgradeOutput::DryRun(plan) => plan
            .lines
            .into_iter()
            .find(|l| l.starts_with("helm upgrade "))
            .expect("plan has an apply line"),
        other => panic!("dry-run must not mutate: {other:?}"),
    };

    let mut fresh = FakeUpgradeHost::empty().with_retained_values();
    let line = apply_line(run_lifecycle(dry_opts("0.9.0"), &mut fresh).await.unwrap());
    assert_eq!(
        line,
        "helm upgrade curie charts/curie -n curie --wait --timeout 15m --install -f <retained-values>"
    );

    let mut existing = FakeUpgradeHost::installed("0.8.6").with_retained_values();
    let line = apply_line(
        run_lifecycle(dry_opts("0.9.0"), &mut existing)
            .await
            .unwrap(),
    );
    assert_eq!(
        line,
        "helm upgrade curie charts/curie -n curie --wait --timeout 15m -f <retained-values>"
    );

    let mut bare = FakeUpgradeHost::installed("0.8.6");
    let line = apply_line(run_lifecycle(dry_opts("0.9.0"), &mut bare).await.unwrap());
    assert_eq!(
        line,
        "helm upgrade curie charts/curie -n curie --wait --timeout 15m"
    );
}

/// #4332: a same-version known-good rerun skips Apply, so its dry-run plan
/// must not promise to retire any agent's SandboxClaims.
#[tokio::test]
async fn same_version_dry_run_drops_runner_layer_retire_lines() {
    let mut host = FakeUpgradeHost::installed("0.11.0")
        .with_known_good("0.11.0")
        .with_retained_values()
        .with_runner_layer_clears(&["factory", "sre-bot"]);
    let out = run_lifecycle(dry_opts("0.11.0"), &mut host)
        .await
        .expect("dry-run plan");
    let ClusterUpgradeOutput::DryRun(plan) = &out else {
        panic!("dry-run must not mutate: {out:?}");
    };
    assert!(
        plan.lines.iter().any(
            |l| l.contains("0.11.0 is already installed") && l.contains("no helm upgrade runs")
        ),
        "{:?}",
        plan.lines
    );
    assert!(
        plan.lines
            .iter()
            .all(|l| !l.starts_with("retire SandboxClaims ") && !l.starts_with("helm upgrade ")),
        "a skipped Apply retires nothing: {:?}",
        plan.lines
    );
    assert_eq!(host.mutate_calls, 0);
    assert!(host.retired_claims.is_empty());
}

/// #3218: an upgrade that changes the platform runner names every layered
/// agent before it upgrades and clears each one's runner image in the same
/// `helm upgrade`, after the retained values so the clear wins.
#[tokio::test]
async fn dry_run_names_stale_runner_layers_and_clears_them_in_the_apply() {
    let mut host = FakeUpgradeHost::installed("0.10.0")
        .with_retained_values()
        .with_runner_layer_clears(&["factory", "sre-bot"]);
    let out = run_lifecycle(dry_opts("0.11.0"), &mut host)
        .await
        .expect("dry-run plan");
    let ClusterUpgradeOutput::DryRun(plan) = &out else {
        panic!("dry-run must not mutate: {out:?}");
    };
    let apply = plan
        .lines
        .iter()
        .find(|l| l.starts_with("helm upgrade "))
        .expect("plan has an apply line");
    assert_eq!(
        apply,
        "helm upgrade curie charts/curie -n curie --wait --timeout 15m -f <retained-values>"
    );
    assert!(
        !apply.contains("=null"),
        "a null runnerImages override is the render refusal: {apply}"
    );
    let notice = plan
        .lines
        .iter()
        .find(|l| l.starts_with("runner layers:"))
        .expect("plan names the stale runner layers");
    assert!(notice.contains("factory, sre-bot"), "{notice}");
    assert!(notice.contains("WITHOUT their layer"), "{notice}");
    assert!(notice.contains("curie build --plugin-dir"), "{notice}");
    // Claim names are observed after Apply, so the plan describes selection.
    let apply_at = plan.lines.iter().position(|l| l == apply).unwrap();
    assert_eq!(
        &plan.lines[apply_at + 1..apply_at + 3],
        &[
            "retire SandboxClaims of agent factory whose runner is not the planned layer",
            "retire SandboxClaims of agent sre-bot whose runner is not the planned layer",
        ]
    );
    assert!(
        notice.contains("it retires their SandboxClaims whose runner is not the planned layer"),
        "{notice}"
    );
    let json = output_json(&out).to_string();
    assert!(
        !json.contains("=null"),
        "the plan must not pass a null digest: {json}"
    );
    assert!(json.contains("sre-bot"), "{json}");
    assert_eq!(host.mutate_calls, 0);

    let mut untouched = FakeUpgradeHost::installed("0.10.0").with_retained_values();
    let ClusterUpgradeOutput::DryRun(plan) = run_lifecycle(dry_opts("0.11.0"), &mut untouched)
        .await
        .unwrap()
    else {
        panic!("dry-run must not mutate");
    };
    assert!(plan.lines.iter().all(|l| !l.contains("runnerImages")
        && !l.starts_with("runner layers:")
        && !l.contains("SandboxClaims")));
}

/// #3849: a Helm apply error is a terminal failed checkpoint at apply.
/// The previous known-good version stays the one that was serving.
#[tokio::test]
async fn apply_error_records_a_failed_checkpoint_and_keeps_the_previous_release() {
    let mut host = FakeUpgradeHost::installed("0.11.1").apply_error(
        "agentSandbox.runnerImages.acme-bot must be a digest reference, got \"<nil>\"",
    );
    let out = run_lifecycle(opts("0.11.2"), &mut host)
        .await
        .expect("apply failure is a completed failure payload");
    let json = output_json(&out);
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "apply", "{json}");
    assert_eq!(json["previous_serving"], true, "{json}");
    assert_eq!(json["known_good_version"], "0.11.1", "{json}");
    assert_eq!(host.current_version(), "0.11.1");
    assert_eq!(host.mutate_calls, 1, "apply is attempted once");
    let view = host.status_view();
    assert_eq!(view.status, "failed");
    assert_eq!(view.phase.as_deref(), Some("apply"));
    let record: serde_json::Value = serde_json::from_str(&host.persisted_json()).unwrap();
    assert_eq!(record["status"], "failed");
    assert_eq!(record["failed_phase"], "apply");
    assert!(record["completed"]
        .as_array()
        .unwrap()
        .iter()
        .any(|phase| phase == "migrate"));
    assert!(!record["completed"]
        .as_array()
        .unwrap()
        .iter()
        .any(|phase| phase == "apply"));
    assert!(
        json["fail_forward"]["reason"]
            .as_str()
            .unwrap_or("")
            .contains("<nil>"),
        "{json}"
    );
}

/// #3422: a real upgrade that clears layered agents' runner images retires
/// their SandboxClaims after Apply, so live threads leave the old layer the
/// way `cluster deploy` makes them (#3300). An upgrade clearing nothing
/// retires nothing.
#[tokio::test]
async fn upgrade_retires_claims_of_every_cleared_runner_layer_after_apply() {
    let mut host =
        FakeUpgradeHost::installed("0.10.0").with_runner_layer_clears(&["factory", "sre-bot"]);
    let out = run_lifecycle(opts("0.11.0"), &mut host)
        .await
        .expect("upgrade");
    assert_eq!(output_json(&out)["status"], "succeeded", "{out:?}");
    assert_eq!(host.retired_claims, vec!["factory", "sre-bot"]);

    let mut plain = FakeUpgradeHost::installed("0.10.0");
    run_lifecycle(opts("0.11.0"), &mut plain)
        .await
        .expect("upgrade");
    assert!(plain.retired_claims.is_empty());
}

const STOCK_LAYER: &str = "ghcr.io/curie-eng/curie-dark-factory-runner@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const PLATFORM_RUNNER: &str = "ghcr.io/curie-eng/curie-runner@sha256:2222222222222222222222222222222222222222222222222222222222222222";

/// One stock agent rebound to the layer published for `--to` (#4321).
fn stock_rebind_plan() -> RunnerLayerPlan {
    RunnerLayerPlan {
        rebinds: vec![("dark-factory".into(), STOCK_LAYER.into())],
        ..RunnerLayerPlan::default()
    }
}

/// #4321 AC3: the plan lists rebound agents apart from cleared ones, the
/// owner-built line keeps its v0.12.3 text for owner-built agents only, and
/// every changed agent's claims are retired right after Apply.
#[tokio::test]
async fn dry_run_shows_rebinds_and_stock_clears_apart() {
    let mut host = FakeUpgradeHost::installed("0.12.2")
        .with_retained_values()
        .with_runner_layer_plan(RunnerLayerPlan {
            rebinds: vec![("dark-factory".into(), STOCK_LAYER.into())],
            clears: vec![
                ("acme-bot".into(), LayerClearReason::OwnerBuilt),
                (
                    "night-factory".into(),
                    LayerClearReason::StockLayerUnpublished,
                ),
            ],
            kept: Vec::new(),
        });
    let mut opts = dry_opts("0.12.3");
    opts.allow_stock_layer_clear = true;
    let out = run_lifecycle(opts, &mut host).await.expect("dry-run plan");
    let ClusterUpgradeOutput::DryRun(plan) = &out else {
        panic!("dry-run must not mutate: {out:?}");
    };
    let at = |prefix: &str| {
        plan.lines
            .iter()
            .position(|l| l.starts_with(prefix))
            .unwrap_or_else(|| panic!("no line starting {prefix:?}: {:?}", plan.lines))
    };
    let rebound_at = at("runner layers rebound:");
    let owner_at = at("runner layers:");
    let stock_at = at("stock runner layers:");
    assert!(
        rebound_at < owner_at && owner_at < stock_at,
        "rebind line, owner notice, stock notice: {:?}",
        plan.lines
    );
    let rebound = &plan.lines[rebound_at];
    assert!(
        rebound.contains(&format!("dark-factory={STOCK_LAYER}")),
        "{rebound}"
    );
    assert!(
        !rebound.contains("acme-bot") && !rebound.contains("night-factory"),
        "{rebound}"
    );
    let owner = &plan.lines[owner_at];
    assert!(owner.contains("agent(s) acme-bot will stop"), "{owner}");
    assert!(!owner.contains("factory"), "{owner}");
    let stock = &plan.lines[stock_at];
    assert!(stock.contains("night-factory"), "{stock}");
    assert!(stock.contains("curie cluster factory"), "{stock}");
    assert!(!stock.contains("curie build --plugin-dir"), "{stock}");
    assert!(!stock.contains("acme-bot"), "{stock}");

    let apply_at = at("helm upgrade ");
    assert_eq!(
        &plan.lines[apply_at + 1..apply_at + 4],
        &[
            "retire SandboxClaims of agent acme-bot whose runner is not the planned layer",
            "retire SandboxClaims of agent dark-factory whose runner is not the planned layer",
            "retire SandboxClaims of agent night-factory whose runner is not the planned layer",
        ]
    );
    assert_eq!(host.mutate_calls, 0);
    assert!(host.retired_claims.is_empty());

    let json = output_json(&out);
    let lines: Vec<&str> = json["plan"]
        .as_array()
        .expect("--json plan")
        .iter()
        .filter_map(|l| l.as_str())
        .collect();
    for prefix in [
        "runner layers rebound:",
        "runner layers:",
        "stock runner layers:",
    ] {
        assert!(
            lines.iter().any(|l| l.starts_with(prefix)),
            "--json plan lacks {prefix:?}: {json}"
        );
    }
}

/// #4321 AC6: rendered templates that match the plan let the run commit
/// known-good, and a rebound agent's claims are retired after Apply (AC2).
#[tokio::test]
async fn canary_commits_when_templates_match_the_plan() {
    let mut host = FakeUpgradeHost::installed("0.12.2")
        .with_runner_layer_plan(stock_rebind_plan())
        .with_template_images(PLATFORM_RUNNER, &[("dark-factory", STOCK_LAYER)]);
    let out = run_lifecycle(opts("0.12.3"), &mut host)
        .await
        .expect("upgrade");
    let json = output_json(&out);
    assert_eq!(json["status"], "succeeded", "{json}");
    assert_eq!(json["phase"], "commit", "{json}");
    assert_eq!(json["canary"]["passed"], true, "{json}");
    assert_eq!(json["known_good_version"], "0.12.3", "{json}");
    assert_eq!(host.retired_claims, vec!["dark-factory"]);
}

/// #4321 AC6: a template that renders something other than the plan fails
/// the canary, the run does not commit known-good, and the reason names the
/// agent, what the upgrade planned and what the template renders.
#[tokio::test]
async fn canary_fails_and_keeps_known_good_when_a_template_is_off_plan() {
    let mut host = FakeUpgradeHost::installed("0.12.2")
        .with_runner_layer_plan(stock_rebind_plan())
        .with_template_images(PLATFORM_RUNNER, &[("dark-factory", PLATFORM_RUNNER)]);
    let out = run_lifecycle(opts("0.12.3"), &mut host)
        .await
        .expect("failed canary is a completed failure payload");
    let json = output_json(&out);
    assert_eq!(json["canary"]["passed"], false, "{json}");
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "canary", "{json}");
    assert_eq!(json["known_good_version"], "0.12.2", "{json}");
    let reason = json["fail_forward"]["reason"].as_str().unwrap_or("");
    assert!(reason.contains("dark-factory"), "{json}");
    assert!(reason.contains(STOCK_LAYER), "the planned image: {json}");
    assert!(
        reason.contains(PLATFORM_RUNNER),
        "the rendered image: {json}"
    );
    let view = host.status_view();
    assert_eq!(view.status, "failed");
    assert_eq!(view.known_good_version.as_deref(), Some("0.12.2"));
}

const OWNER_LAYER: &str = "ghcr.io/acme/acme-bot-runner@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

/// #4321 review P1: the canary expectations are recorded in the checkpoint
/// when the record is created, and a resumed run checks the recorded ones.
/// After Apply cleared acme-bot, re-planning from the modified values no
/// longer sees acme-bot at all (an empty host plan here), so only the
/// recorded expectation can catch its stale per-agent template.
#[tokio::test]
async fn resumed_canary_uses_the_recorded_expectations_not_the_replanned_ones() {
    let mut host = FakeUpgradeHost::installed("0.12.2")
        .with_runner_layer_clears(&["acme-bot"])
        .with_template_images(PLATFORM_RUNNER, &[("acme-bot", OWNER_LAYER)])
        .interrupt_after(UpgradePhase::Apply);
    let err = run_lifecycle(opts("0.12.3"), &mut host)
        .await
        .expect_err("interrupted after apply");
    assert!(format!("{err:#}").contains("interrupted"), "{err:#}");
    let persisted: serde_json::Value =
        serde_json::from_str(&host.persisted_json()).expect("persisted record");
    assert_eq!(
        persisted["runner_canary"],
        serde_json::json!([["acme-bot", "PlatformRunner"]]),
        "the fresh record carries the plan's canary expectations: {persisted}"
    );

    // The resumed process re-plans from values Apply already changed.
    let mut host = host.with_runner_layer_plan(RunnerLayerPlan::default());
    host.clear_interrupt();
    let out = run_lifecycle(opts("0.12.3"), &mut host)
        .await
        .expect("failed canary is a completed failure payload");
    let json = output_json(&out);
    assert_eq!(json["resumed"], true, "{json}");
    assert_eq!(json["canary"]["passed"], false, "{json}");
    assert_eq!(json["status"], "failed", "{json}");
    assert_eq!(json["phase"], "canary", "{json}");
    assert_eq!(json["known_good_version"], "0.12.2", "{json}");
    let reason = json["fail_forward"]["reason"].as_str().unwrap_or("");
    assert!(reason.contains("acme-bot"), "{json}");
    assert!(reason.contains(OWNER_LAYER), "the stale layer: {json}");
    let persisted: serde_json::Value =
        serde_json::from_str(&host.persisted_json()).expect("persisted record");
    assert_eq!(
        persisted["runner_canary"],
        serde_json::json!([["acme-bot", "PlatformRunner"]]),
        "a resumed run keeps the recorded expectations: {persisted}"
    );
}

/// #4321 review round 2 P2: when a run stops before Apply, the plan Apply
/// will actually execute is the one the resumed process computes. Here the
/// first attempt could only clear acme-bot (say the registry was down), and
/// the resumed one rebinds it to a published layer. The resumed run refreshes
/// and persists the canary expectations from that plan, so the correctly
/// rebound template passes. The post-Apply resume test above is the inverse
/// control: once Apply has run, the recorded expectations win.
#[tokio::test]
async fn resume_before_apply_refreshes_the_canary_expectations_from_the_executed_plan() {
    let mut host = FakeUpgradeHost::installed("0.12.2")
        .with_runner_layer_clears(&["acme-bot"])
        .interrupt_after(UpgradePhase::Checkpoint);
    let err = run_lifecycle(opts("0.12.3"), &mut host)
        .await
        .expect_err("interrupted before apply");
    assert!(format!("{err:#}").contains("interrupted"), "{err:#}");
    let persisted: serde_json::Value =
        serde_json::from_str(&host.persisted_json()).expect("persisted record");
    assert_eq!(
        persisted["runner_canary"],
        serde_json::json!([["acme-bot", "PlatformRunner"]]),
        "the first attempt records its own plan: {persisted}"
    );
    assert!(
        !persisted["completed"]
            .as_array()
            .expect("completed phases")
            .iter()
            .any(|phase| phase == "apply"),
        "Apply is still outstanding: {persisted}"
    );

    let mut host = host
        .with_runner_layer_plan(RunnerLayerPlan {
            rebinds: vec![("acme-bot".into(), STOCK_LAYER.into())],
            ..RunnerLayerPlan::default()
        })
        .with_template_images(PLATFORM_RUNNER, &[("acme-bot", STOCK_LAYER)]);
    host.clear_interrupt();
    let out = run_lifecycle(opts("0.12.3"), &mut host)
        .await
        .expect("upgrade");
    let json = output_json(&out);
    assert_eq!(json["resumed"], true, "{json}");
    assert_eq!(json["status"], "succeeded", "{json}");
    assert_eq!(json["phase"], "commit", "{json}");
    assert_eq!(json["canary"]["passed"], true, "{json}");
    assert_eq!(json["known_good_version"], "0.12.3", "{json}");
    let persisted: serde_json::Value =
        serde_json::from_str(&host.persisted_json()).expect("persisted record");
    assert_eq!(
        persisted["runner_canary"],
        serde_json::json!([["acme-bot", {"Image": STOCK_LAYER}]]),
        "the resumed run persists the expectations of the plan Apply executed: {persisted}"
    );
}

fn render_factory_upgrade_overlay(values: &Value) -> std::process::Output {
    let dir = tempfile::tempdir().expect("private render values directory");
    let file = dir.path().join("values.json");
    std::fs::write(&file, serde_json::to_vec(values).unwrap()).expect("write render overlay");
    std::process::Command::new("helm")
        .args(["template", "curie"])
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/../charts/curie"))
        .arg("-f")
        .arg(file)
        .output()
        .expect("Helm is required for the factory migration render boundary")
}

fn rendered_factory_api(values: &Value) -> Value {
    let output = render_factory_upgrade_overlay(values);
    assert!(
        output.status.success(),
        "migrated overlay must render: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let yaml = String::from_utf8(output.stdout).expect("UTF-8 chart render");
    let api = serde_norway::Deserializer::from_str(&yaml)
        .map(|document| {
            <Value as serde::Deserialize>::deserialize(document).expect("rendered YAML")
        })
        .find(|object| object["kind"] == "Deployment" && object["metadata"]["name"] == "curie-api")
        .expect("rendered API deployment");
    api
}

fn factory_api_env<'a>(api: &'a Value, name: &str) -> &'a Value {
    api["spec"]["template"]["spec"]["containers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|container| container["name"] == "api")
        .expect("API container")["env"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["name"] == name)
        .expect("API environment entry")
}

#[test]
fn factory_upgrade_migrated_overlay_renders_the_preserved_runtime_policy_and_secret_mounts() {
    let values = super::config_migrate::legacy_factory_values();
    let context = super::config_migrate::factory_context(&values);
    let outcome =
        curie::config_migrate::migrate_installed_config(values, Some("curie-0.12.5"), &context)
            .expect("legacy factory migration");
    let api = rendered_factory_api(&outcome.values);
    assert_eq!(factory_api_env(&api, "FACTORY_ENABLED")["value"], "true");
    assert_eq!(factory_api_env(&api, "FACTORY_INTAKE")["value"], "webhook");
    assert_eq!(factory_api_env(&api, "FACTORY_CI_WAIT_S")["value"], "2345");
    assert_eq!(
        factory_api_env(&api, "GITHUB_API_URL")["value"],
        "https://api.github.com"
    );
    assert_eq!(
        factory_api_env(&api, "GITHUB_APP_PRIVATE_KEY")["valueFrom"]["secretKeyRef"],
        json!({"name": "acme-factory-app", "key": "signing.pem"})
    );
    let forges: Value = serde_json::from_str(
        factory_api_env(&api, "CURIE_FORGES")["value"]
            .as_str()
            .unwrap(),
    )
    .expect("actual API factory JSON");
    assert_eq!(forges["card_base_url"], "https://cards.example.com/factory");
    let hosts = forges["code_hosts"].as_array().unwrap();
    let trackers = forges["trackers"].as_array().unwrap();
    let bindings = forges["bindings"].as_array().unwrap();
    assert_eq!(hosts.len(), 1);
    assert_eq!(trackers.len(), 1);
    assert_eq!(bindings.len(), 1);
    assert_eq!(hosts[0]["host"], "github.com");
    assert_eq!(hosts[0]["api_url"], "https://api.github.com");
    assert_eq!(trackers[0]["label"], "triage-ready");
    assert_eq!(trackers[0]["mention"], "acme-maintainer");
    assert_eq!(trackers[0]["poll_interval_s"], 83);
    assert_eq!(bindings[0]["tracker"], trackers[0]["name"]);
    assert_eq!(bindings[0]["tracker_scope_id"], "73");
    let repos = bindings[0]["repos"].as_array().unwrap();
    assert_eq!(repos.len(), 1);
    assert_eq!(repos[0]["code_host"], hosts[0]["name"]);
    assert_eq!(repos[0]["project_id"], "73");
    assert_eq!(repos[0]["path"], "acme-corp/acme-bot");
    assert_eq!(repos[0]["bases"], json!(["release/2026", "main"]));
    assert_eq!(repos[0]["default_base"], "release/2026");
    assert_eq!(
        repos[0]["ci"]["required"],
        json!({
            "key": "required-python", "paths": ["apps", "packages/python"],
            "pending_key_prefix": "python-shard-"
        })
    );
    let keys = repos[0]["ci"]["metadata_rerun_keys"].as_array().unwrap();
    let actual: std::collections::BTreeSet<_> =
        keys.iter().map(|key| key.as_str().unwrap()).collect();
    assert_eq!(
        actual,
        std::collections::BTreeSet::from([
            "body-check",
            "fix-pin",
            "status:approval",
            "status:status:retained-context"
        ])
    );
    assert_eq!(keys.len(), 4);
    let volumes = api["spec"]["template"]["spec"]["volumes"]
        .as_array()
        .unwrap();
    let projected = &volumes
        .iter()
        .find(|volume| volume["name"] == "forge-credentials")
        .expect("projected forge credentials")["projected"]["sources"];
    let sources = projected.as_array().unwrap();
    assert_eq!(sources.len(), 2);
    for (entry, section) in [(&hosts[0], "code_hosts"), (&trackers[0], "trackers")] {
        assert_eq!(entry["credential"]["type"], "github_app");
        assert_eq!(entry["credential"]["app_id"], "17");
        assert!(entry["credential"].get("private_key").is_none());
        let relative_path = format!("{section}-{}/private_key", entry["name"].as_str().unwrap());
        assert_eq!(
            entry["credential"]["private_key_file"],
            format!("/etc/curie/forges/{relative_path}")
        );
        let matching: Vec<_> = sources
            .iter()
            .filter(|source| source["secret"]["items"][0]["path"] == relative_path)
            .collect();
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0]["secret"]["name"], "acme-factory-app");
        assert_eq!(matching[0]["secret"]["items"][0]["key"], "signing.pem");
    }
}

#[test]
fn factory_upgrade_disabled_configuration_stays_disabled_in_the_actual_chart() {
    let mut values = super::config_migrate::legacy_factory_values();
    values["api"]["githubFactoryIngressEnabled"] = json!(false);
    let context = super::config_migrate::factory_context(&values);
    let outcome =
        curie::config_migrate::migrate_installed_config(values, Some("curie-0.12.5"), &context)
            .expect("disabled configured legacy migration");
    let api = rendered_factory_api(&outcome.values);
    assert_eq!(factory_api_env(&api, "FACTORY_ENABLED")["value"], "false");
    let forges: Value = serde_json::from_str(
        factory_api_env(&api, "CURIE_FORGES")["value"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(forges["trackers"][0]["label"], "triage-ready");
    assert_eq!(
        forges["bindings"][0]["repos"][0]["ci"]["required"]["key"],
        "required-python"
    );
}

#[test]
fn factory_upgrade_raw_legacy_values_still_refuse_with_the_stable_guard_prefix() {
    let values = super::config_migrate::legacy_factory_values();
    let output = render_factory_upgrade_overlay(&values);
    assert!(
        !output.status.success(),
        "raw legacy values must remain unsupported"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("api.githubFactoryIngressEnabled is no longer read"),
        "{stderr}"
    );
    for value in [
        "triage-ready",
        "acme-maintainer",
        "acme-corp/acme-bot",
        "acme-factory-app",
    ] {
        assert!(
            !stderr.contains(value),
            "the raw guard must not echo operator values"
        );
    }
}
