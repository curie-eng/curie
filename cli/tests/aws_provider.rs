use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::net::{TcpListener, TcpStream};
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use curie::cluster_secrets::resolve_named_secrets;
use curie::installation::Installation;
use curie::provider::aws::AwsSecretsProvider;
use curie::provider::{PutRequest, SecretMaterial, SecretsProvider, EXPIRY_TAG};
use curie::secrets::{
    get_value, resolve_cluster_secret, save_scoped_value, save_value, ClusterSecretSource,
    SecretScope,
};

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn lock_environment() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct Environment {
    previous: Vec<(String, Option<OsString>)>,
}

impl Environment {
    fn set(values: &[(&str, &OsStr)]) -> Self {
        let mut previous = Vec::with_capacity(values.len());
        for (name, value) in values {
            previous.push(((*name).to_string(), std::env::var_os(name)));
            std::env::set_var(name, value);
        }
        Self { previous }
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        for (name, value) in self.previous.drain(..).rev() {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}

struct Moto {
    child: Child,
    endpoint: String,
}

impl Moto {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve a random moto port");
        let port = listener.local_addr().expect("read moto address").port();
        drop(listener);

        let child = Command::new("uvx")
            .args([
                "--from",
                "moto[server]==5.1.12",
                "moto_server",
                "-H",
                "127.0.0.1",
                "-p",
                &port.to_string(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .process_group(0)
            .spawn()
            .expect("start pinned moto_server through uvx");
        let address = format!("127.0.0.1:{port}");
        let moto = Self {
            child,
            endpoint: format!("http://{address}"),
        };
        let deadline = Instant::now() + Duration::from_secs(60);
        while TcpStream::connect(&address).is_err() {
            assert!(
                Instant::now() < deadline,
                "moto_server did not become ready"
            );
            thread::sleep(Duration::from_millis(50));
        }
        moto
    }
}

impl Drop for Moto {
    fn drop(&mut self) {
        unsafe extern "C" {
            fn kill(pid: i32, signal: i32) -> i32;
        }
        // uvx starts moto_server as a child. Stop the owned process group so
        // neither process survives a completed test or a setup panic.
        unsafe {
            let _ = kill(-(self.child.id() as i32), 15);
        }
        let _ = self.child.wait();
    }
}

fn aws_environment(endpoint: &str, scratch: &Path) -> Environment {
    let credentials = scratch.join("credentials");
    let config = scratch.join("config");
    fs::write(&credentials, "").expect("write empty AWS credentials file");
    fs::write(&config, "").expect("write empty AWS config file");
    Environment::set(&[
        ("AWS_ACCESS_KEY_ID", OsStr::new("test")),
        ("AWS_SECRET_ACCESS_KEY", OsStr::new("test")),
        ("AWS_SESSION_TOKEN", OsStr::new("test")),
        ("AWS_REGION", OsStr::new("us-east-1")),
        ("AWS_DEFAULT_REGION", OsStr::new("us-east-1")),
        ("AWS_ENDPOINT_URL", OsStr::new(endpoint)),
        ("AWS_EC2_METADATA_DISABLED", OsStr::new("true")),
        ("AWS_PAGER", OsStr::new("")),
        ("AWS_SHARED_CREDENTIALS_FILE", credentials.as_os_str()),
        ("AWS_CONFIG_FILE", config.as_os_str()),
    ])
}

#[test]
fn aws_cli_v2_provider_round_trips_every_operation_against_moto() {
    // Command and response shapes follow the AWS CLI v2 Secrets Manager
    // reference: https://docs.aws.amazon.com/cli/latest/reference/secretsmanager/
    let _lock = lock_environment();
    let version = Command::new("aws")
        .arg("--version")
        .output()
        .expect("aws CLI is required for the provider test");
    let version_text = format!(
        "{}{}",
        String::from_utf8_lossy(&version.stdout),
        String::from_utf8_lossy(&version.stderr)
    );
    assert!(
        version.status.success() && version_text.contains("aws-cli/2."),
        "the provider requires aws CLI v2, got {version_text}"
    );

    let moto = Moto::start();
    let scratch = tempfile::tempdir().expect("create AWS test directory");
    let _environment = aws_environment(&moto.endpoint, scratch.path());
    let provider = AwsSecretsProvider::new(
        "us-east-1",
        "curie-aws-secrets-e2e-provider-test",
        "acme-release",
    )
    .expect("construct AWS provider");
    let name = "finance-connector";
    let first_material =
        SecretMaterial::new(r#"{"API_TOKEN":"sensitive-first","CLIENT_ID":"example-client"}"#);

    let first = provider
        .put(&PutRequest {
            name,
            material: &first_material,
            expected_version: None,
        })
        .expect("create provider object");
    assert!(!first.id.is_empty());

    let first_read = provider
        .get(name, Some(&first.id))
        .expect("read the first provider version");
    assert_eq!(first_read.version, first);
    assert_eq!(first_read.material, first_material);
    assert_eq!(first_read.key_names, vec!["API_TOKEN", "CLIENT_ID"]);

    let second_material =
        SecretMaterial::new(r#"{"API_TOKEN":"sensitive-second","CLIENT_ID":"example-client"}"#);
    let second = provider
        .put(&PutRequest {
            name,
            material: &second_material,
            expected_version: Some(&first.id),
        })
        .expect("replace provider object with the current version");
    assert_ne!(second, first);
    let current = provider.get(name, None).expect("read current object");
    assert_eq!(current.version, second);
    assert_eq!(current.material, second_material);

    let metadata = provider.get_metadata(name).expect("read object metadata");
    assert_eq!(metadata.name, name);
    assert_eq!(metadata.version, second);
    assert_eq!(metadata.key_names, vec!["API_TOKEN", "CLIENT_ID"]);

    let listed = provider.list("").expect("list provider objects");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, name);
    assert_eq!(listed[0].version, second);
    assert_eq!(
        provider
            .list("finance")
            .expect("filter provider objects by logical prefix")[0]
            .name,
        name
    );
    assert!(provider
        .list("unrelated")
        .expect("list an empty logical prefix")
        .is_empty());

    let tags = BTreeMap::from([(EXPIRY_TAG.to_string(), "2030-01-02T03:04:05Z".to_string())]);
    let tagged = provider
        .tag(name, &tags, Some(&second.id))
        .expect("tag the current object");
    assert_eq!(tagged, second);
    let tagged_metadata = provider
        .get_metadata(name)
        .expect("read tagged object metadata");
    assert_eq!(tagged_metadata.tags.get(EXPIRY_TAG), tags.get(EXPIRY_TAG));

    let deleted = provider
        .delete(name, Some(&second.id))
        .expect("delete the current object");
    assert_eq!(deleted, second);
    assert!(provider.list("").expect("list after delete").is_empty());

    fs::write(
        scratch.path().join("curie.yaml"),
        "version: 1\ninstall:\n  namespace: acme-system\n  release: acme-release\nsecrets:\n  provider: aws\n  region: us-east-1\n  prefix: curie-aws-secrets-e2e-provider-test\n  role_arn: arn:aws:iam::000000000000:role/curie-sync\n",
    )
    .expect("write provider installation");
    let curie_config = scratch.path().join("curie-config");
    let first_set = Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(["secrets", "set", "logical/KEY", "--from-env", "FIRST_VALUE"])
        .current_dir(scratch.path())
        .env("CURIE_CONFIG_DIR", &curie_config)
        .env("FIRST_VALUE", "first-value")
        .output()
        .expect("run first key provider set");
    assert!(
        first_set.status.success(),
        "first key set failed: {}{}",
        String::from_utf8_lossy(&first_set.stdout),
        String::from_utf8_lossy(&first_set.stderr)
    );
    let second_set = Command::new(env!("CARGO_BIN_EXE_curie"))
        .args([
            "secrets",
            "set",
            "logical/OTHER",
            "--from-env",
            "SECOND_VALUE",
        ])
        .current_dir(scratch.path())
        .env("CURIE_CONFIG_DIR", &curie_config)
        .env("SECOND_VALUE", "second-value")
        .output()
        .expect("run second key provider set");
    assert!(
        second_set.status.success(),
        "second key set failed: {}{}",
        String::from_utf8_lossy(&second_set.stdout),
        String::from_utf8_lossy(&second_set.stderr)
    );
    let merged = provider
        .get("logical", None)
        .expect("read key granular provider object");
    let merged_values: BTreeMap<String, String> =
        serde_json::from_str(merged.material.expose()).expect("parse merged provider object");
    assert_eq!(
        merged_values.get("KEY").map(String::as_str),
        Some("first-value")
    );
    assert_eq!(
        merged_values.get("OTHER").map(String::as_str),
        Some("second-value")
    );
    let removed = Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(["secrets", "rm", "logical"])
        .current_dir(scratch.path())
        .env("CURIE_CONFIG_DIR", &curie_config)
        .output()
        .expect("run provider remove");
    assert!(
        removed.status.success(),
        "provider remove failed: {}{}",
        String::from_utf8_lossy(&removed.stdout),
        String::from_utf8_lossy(&removed.stderr)
    );
    assert!(provider
        .list("")
        .expect("list after provider remove")
        .is_empty());
}

#[cfg(unix)]
#[test]
fn put_keeps_material_out_of_the_aws_child_argv_and_uses_a_private_file() {
    // The file input spelling is the documented AWS CLI v2 binary and string
    // input path: https://docs.aws.amazon.com/cli/latest/userguide/cli-usage-parameters-file.html
    use std::os::unix::fs::PermissionsExt;

    let _lock = lock_environment();
    let scratch = tempfile::tempdir().expect("create fake AWS directory");
    let aws_path = scratch.path().join("aws");
    let argv_log = scratch.path().join("argv.log");
    let body_log = scratch.path().join("body.log");
    let mode_log = scratch.path().join("mode.log");
    fs::write(
        &aws_path,
        r#"#!/bin/sh
set -eu
if [ "${1:-}" = '--version' ]; then
  printf '%s\n' 'aws-cli/2.31.0 Python/3.13.7 Linux/fixture exe/x86_64'
  exit 0
fi
for argument in "$@"; do
  printf '%s\n' "$argument" >> "$AWS_ARGV_LOG"
done
case " $* " in
  *" secretsmanager describe-secret "*)
    printf '%s\n' 'ResourceNotFoundException: fixture object is absent' >&2
    exit 254
    ;;
  *" secretsmanager create-secret "*)
    previous=''
    input=''
    for argument in "$@"; do
      if [ "$previous" = '--cli-input-json' ]; then
        input=${argument#file://}
      fi
      previous=$argument
    done
    [ -n "$input" ]
    /bin/cat "$input" > "$AWS_BODY_LOG"
    /usr/bin/stat -c '%a' "$input" > "$AWS_MODE_LOG"
    printf '%s\n' '{"VersionId":"00000000-0000-4000-8000-000000000001"}'
    ;;
  *)
    printf '%s\n' "unexpected aws invocation: $*" >&2
    exit 64
    ;;
esac
"#,
    )
    .expect("write fake aws executable");
    fs::set_permissions(&aws_path, fs::Permissions::from_mode(0o700))
        .expect("make fake aws executable");

    let _environment = Environment::set(&[
        ("PATH", scratch.path().as_os_str()),
        ("AWS_ARGV_LOG", argv_log.as_os_str()),
        ("AWS_BODY_LOG", body_log.as_os_str()),
        ("AWS_MODE_LOG", mode_log.as_os_str()),
    ]);
    let provider = AwsSecretsProvider::new("us-east-1", "example-prefix", "acme-release")
        .expect("construct AWS provider");
    let sentinel = "never-place-this-value-in-argv";
    let material = SecretMaterial::new(format!(r#"{{"API_TOKEN":"{sentinel}"}}"#));
    provider
        .put(&PutRequest {
            name: "connector",
            material: &material,
            expected_version: None,
        })
        .expect("write through fake aws executable");

    let argv = fs::read_to_string(&argv_log).expect("read captured aws argv");
    assert!(!argv.contains(sentinel), "secret material reached aws argv");
    assert!(argv.contains("--cli-input-json"));
    let body = fs::read_to_string(&body_log).expect("read captured AWS request body");
    assert!(
        body.contains(sentinel),
        "request body did not carry the material"
    );
    assert_eq!(
        fs::read_to_string(&mode_log)
            .expect("read private file mode")
            .trim(),
        "600"
    );
}

#[test]
fn provider_absent_keeps_local_global_scope_and_version_behavior() {
    let _lock = lock_environment();
    let scratch = tempfile::tempdir().expect("create local store directory");
    let empty_path = scratch.path().join("no-tools");
    fs::create_dir(&empty_path).expect("create empty PATH directory");
    let _environment = Environment::set(&[
        ("CURIE_CONFIG_DIR", scratch.path().as_os_str()),
        ("PATH", empty_path.as_os_str()),
    ]);

    let installation = Installation::parse(
        "version: 1\ninstall:\n  namespace: acme-system\n  release: acme-release\n",
    )
    .expect("parse provider absent apply input");
    assert!(
        curie::secrets::provider_for_installation(&installation)
            .expect("provider absent apply preflight")
            .is_none(),
        "provider absent apply must not construct an AWS backend"
    );

    save_value("GLOBAL_TOKEN", "global-value").expect("save global local value");
    assert_eq!(
        get_value("GLOBAL_TOKEN").expect("read global local value"),
        Some("global-value".to_string())
    );
    assert_eq!(
        resolve_named_secrets(&["GLOBAL_TOKEN".to_string()]).expect("resolve a local writer value")
            ["GLOBAL_TOKEN"],
        "global-value"
    );

    let target = SecretScope {
        cluster_identity: "ca:one".into(),
        release: "acme-release".into(),
        namespace: "acme-system".into(),
    };
    assert_eq!(
        save_scoped_value("CLUSTER_TOKEN", &target, "scoped-first", None)
            .expect("create scoped local value"),
        1
    );
    let resolved = resolve_cluster_secret("CLUSTER_TOKEN", &target)
        .expect("resolve matching scope")
        .expect("scoped value exists");
    assert_eq!(resolved.value, "scoped-first");
    assert_eq!(resolved.source, ClusterSecretSource::Scoped { version: 1 });

    let conflict = save_scoped_value("CLUSTER_TOKEN", &target, "scoped-stale", None)
        .expect_err("replacement without a version must conflict")
        .to_string();
    assert!(conflict.contains("version mismatch"));
    assert!(!conflict.contains("scoped-first"));
    assert!(!conflict.contains("scoped-stale"));
    assert_eq!(
        save_scoped_value("CLUSTER_TOKEN", &target, "scoped-second", Some(1))
            .expect("replace with the stored version"),
        2
    );

    let other = SecretScope {
        cluster_identity: "ca:two".into(),
        release: "acme-release".into(),
        namespace: "acme-system".into(),
    };
    let mismatch = resolve_cluster_secret("CLUSTER_TOKEN", &other)
        .expect_err("a different cluster scope must be refused")
        .to_string();
    assert!(mismatch.contains("refusing to inject"));
    assert!(!mismatch.contains("scoped-second"));
}

#[test]
fn environment_resolution_does_not_open_a_broken_local_store() {
    let _lock = lock_environment();
    let scratch = tempfile::tempdir().expect("create local store directory");
    let empty_path = scratch.path().join("no-tools");
    fs::create_dir(&empty_path).expect("create empty PATH directory");
    fs::write(scratch.path().join("credentials.json"), "not json")
        .expect("write broken local store");
    let _environment = Environment::set(&[
        ("CURIE_CONFIG_DIR", scratch.path().as_os_str()),
        ("PATH", empty_path.as_os_str()),
        ("ENV_FIRST_TOKEN", OsStr::new("from-environment")),
    ]);

    let resolved = resolve_named_secrets(&["ENV_FIRST_TOKEN".to_string()])
        .expect("the environment value must short circuit local storage");
    assert_eq!(resolved["ENV_FIRST_TOKEN"], "from-environment");
}
