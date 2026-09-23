//! Installation secrets provider contract.
//!
//! These tests pin the provider neutral types, installation schema, and CLI
//! routing while backend round trips live in `aws_provider.rs`.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Mutex;

use curie::installation::{Installation, ProviderKind};
use curie::provider::{
    not_implemented, InventoryClass, InventoryEntry, ObjectMetadata, ObjectVersion, ProviderError,
    PutRequest, RejectedReason, RotationOwner, SecretMaterial, SecretsProvider, Store,
    StoredObject, UpdatePolicy, EXPIRY_TAG,
};
use serde_json::{json, Value};

const PLANTED: &str = "planted-value-7f3a";

const BARE_INSTALL: &str = "\
version: 1
install:
  namespace: a
  release: a
";

const SECRET_LINES: &str = "  provider: aws
  region: us-east-1
  prefix: tenant/platform
  role_arn: arn:aws:iam::000000000000:role/curie-sync
";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn secrets_yaml(body: &str) -> String {
    format!("{BARE_INSTALL}secrets:\n{body}")
}

fn aws_install() -> String {
    secrets_yaml(SECRET_LINES)
}

fn gov_install() -> String {
    secrets_yaml(
        &SECRET_LINES
            .replace("region: us-east-1", "region: us-gov-west-1")
            .replace(
                "role_arn: arn:aws:iam::000000000000:role/curie-sync",
                "role_arn: arn:aws-us-gov:iam::000000000000:role/curie-sync",
            ),
    )
}

fn raw_output(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn panic_text(output: &Output) -> String {
    raw_output(output).replace(PLANTED, "[redacted]")
}

fn redact(text: &str) -> String {
    text.replace(PLANTED, "[redacted]")
}

struct Isolated {
    _root: tempfile::TempDir,
    config: PathBuf,
    marker: PathBuf,
    path: OsString,
}

fn isolated() -> Isolated {
    let root = tempfile::tempdir().expect("tempdir");
    let bin_dir = root.path().join("bin");
    fs::create_dir(&bin_dir).expect("bin dir");
    let marker = root.path().join("aws-marker");
    let script = format!(
        "#!/bin/sh\nif [ \"${{1:-}}\" = '--version' ]; then\n  printf '%s\\n' 'aws-cli/2.31.0 Python/3.13.7 Linux/fixture exe/x86_64'\n  exit 0\nfi\nprintf x > '{}'\ncase \" $* \" in\n  *\" secretsmanager get-secret-value \"*|*\" secretsmanager describe-secret \"*)\n    printf '%s\\n' 'ResourceNotFoundException: fixture object is absent' >&2\n    exit 254\n    ;;\n  *\" secretsmanager create-secret \"*)\n    printf '%s\\n' '{{\"VersionId\":\"00000000-0000-4000-8000-000000000001\"}}'\n    ;;\n  *\" secretsmanager list-secrets \"*)\n    printf '%s\\n' '{{\"SecretList\":[]}}'\n    ;;\n  *)\n    printf '%s\\n' 'unexpected aws fixture invocation' >&2\n    exit 64\n    ;;\nesac\n",
        marker.display()
    );
    write_exec(&bin_dir, "aws", &script);
    let mut paths = vec![bin_dir];
    if let Some(current) = std::env::var_os("PATH") {
        paths.extend(std::env::split_paths(&current));
    }
    let path = std::env::join_paths(paths).expect("join PATH");
    Isolated {
        config: root.path().to_path_buf(),
        marker,
        path,
        _root: root,
    }
}

fn write_exec(dir: &Path, name: &str, body: &str) {
    let path = dir.join(name);
    fs::write(&path, body).expect("write aws stub");
    let mut permissions = fs::metadata(&path).expect("aws metadata").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).expect("chmod aws stub");
}

fn command(env: &Isolated) -> Command {
    let mut command = Command::new(bin());
    command
        .current_dir(&env.config)
        .env("CURIE_CONFIG_DIR", &env.config)
        .env("HOME", &env.config)
        .env("PATH", &env.path)
        .env("HOLD_VALUE", PLANTED)
        .stdin(Stdio::null());
    command
}

fn run(env: &Isolated, args: &[&str]) -> Output {
    command(env).args(args).output().expect("run curie")
}

fn credentials(env: &Isolated) -> PathBuf {
    env.config.join("credentials.json")
}

fn assert_aws_not_called(env: &Isolated) {
    assert!(!env.marker.exists(), "aws must not be called");
}

fn assert_string_error(output: &Output, needle: &str) {
    let json: Value = serde_json::from_slice(&output.stdout).expect("stdout is JSON");
    assert!(json["error"].is_string(), "error must be a string");
    let error = json["error"].as_str().expect("error string");
    assert!(
        error.contains(needle),
        "error must contain {needle}: {}",
        redact(error)
    );
}

fn assert_field_error(raw: &str, needles: &[&str]) {
    let err = Installation::parse(raw).expect_err("parse must fail");
    let text = format!("{err:#}");
    for needle in needles {
        assert!(
            text.contains(needle),
            "error must contain {needle}: {}",
            redact(&text)
        );
    }
    assert!(!text.contains(PLANTED), "parse error leaked material");
}

fn assert_aws_block(raw: &str, region: &str, role_arn: &str) {
    let parsed = Installation::parse(raw).expect("secrets block must parse");
    let block = parsed.secrets.as_ref().expect("secrets block");
    assert!(matches!(&block.provider, ProviderKind::Aws));
    assert_eq!(block.region, region);
    assert_eq!(block.prefix, "tenant/platform");
    assert_eq!(block.role_arn, role_arn);
    let bare = Installation::parse(BARE_INSTALL).expect("bare file");
    assert_eq!(parsed.helm_sets(), bare.helm_sets());
}

fn inventory(
    class: InventoryClass,
    rotation_owner: RotationOwner,
    update_policy: UpdatePolicy,
    keys: Vec<String>,
) -> InventoryEntry {
    InventoryEntry {
        logical_name: "db".into(),
        class,
        target: "db".into(),
        keys,
        consumers: vec!["api".into()],
        rotation_owner,
        update_policy,
        // cluster keeps every owner valid, including the workload round trip.
        store: Store::Cluster,
        rotated_keys: vec![],
        chart: None,
    }
}

fn assert_round_trip(entry: InventoryEntry, class_tag: &str, owner_tag: &str, policy_tag: &str) {
    let value = serde_json::to_value(&entry).expect("serialize inventory");
    assert_eq!(value["logical_name"], "db");
    assert_eq!(value["class"], class_tag);
    assert_eq!(value["target"], "db");
    assert_eq!(value["keys"], json!(["token"]));
    assert_eq!(value["consumers"], json!(["api"]));
    assert_eq!(value["rotation_owner"], owner_tag);
    assert_eq!(value["update_policy"], policy_tag);
    let decoded: InventoryEntry =
        serde_json::from_value(value.clone()).expect("deserialize inventory");
    let again = serde_json::to_value(&decoded).expect("serialize again");
    assert_eq!(again, value);
    assert!(decoded.validate().is_ok(), "round trip must validate");
}

fn rejects_empty_key(entry: InventoryEntry) -> bool {
    let serde_rejected = match serde_json::to_value(&entry) {
        Ok(value) => serde_json::from_value::<InventoryEntry>(value).is_err(),
        Err(_) => true,
    };
    entry.validate().is_err() || serde_rejected
}

fn good_inventory() -> Value {
    serde_json::to_value(inventory(
        InventoryClass::Stateful,
        RotationOwner::Sm,
        UpdatePolicy::Immutable,
        vec!["token".into()],
    ))
    .expect("serialize inventory")
}

fn must_ok<T>(result: Result<T, ProviderError>, label: &str) -> T {
    match result {
        Ok(value) => value,
        Err(err) => panic!("{label} failed: {}", redact(&err.to_string())),
    }
}

struct LocalProvider {
    objects: Mutex<BTreeMap<String, String>>,
}

fn object_metadata(name: &str) -> ObjectMetadata {
    ObjectMetadata {
        name: name.to_string(),
        version: ObjectVersion { id: "v1".into() },
        tags: BTreeMap::new(),
        key_names: vec!["token".into()],
    }
}

impl SecretsProvider for LocalProvider {
    fn put(&self, request: &PutRequest<'_>) -> Result<ObjectVersion, ProviderError> {
        self.objects.lock().expect("lock").insert(
            request.name.to_string(),
            request.material.expose().to_string(),
        );
        Ok(ObjectVersion { id: "v1".into() })
    }

    fn get(&self, name: &str, _version: Option<&str>) -> Result<StoredObject, ProviderError> {
        let objects = self.objects.lock().expect("lock");
        let Some(bytes) = objects.get(name) else {
            return Err(ProviderError::NotFound {
                name: name.to_string(),
            });
        };
        Ok(StoredObject {
            version: ObjectVersion { id: "v1".into() },
            material: SecretMaterial::new(bytes.clone()),
            key_names: vec!["token".into()],
        })
    }

    fn get_metadata(&self, name: &str) -> Result<ObjectMetadata, ProviderError> {
        let objects = self.objects.lock().expect("lock");
        if !objects.contains_key(name) {
            return Err(ProviderError::NotFound {
                name: name.to_string(),
            });
        }
        Ok(object_metadata(name))
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, ProviderError> {
        let objects = self.objects.lock().expect("lock");
        Ok(objects
            .keys()
            .filter(|name| name.starts_with(prefix))
            .map(|name| object_metadata(name))
            .collect())
    }

    fn tag(
        &self,
        _name: &str,
        _tags: &BTreeMap<String, String>,
        _expected_version: Option<&str>,
    ) -> Result<ObjectVersion, ProviderError> {
        Ok(ObjectVersion { id: "v2".into() })
    }

    fn delete(
        &self,
        _name: &str,
        _expected_version: Option<&str>,
    ) -> Result<ObjectVersion, ProviderError> {
        Ok(ObjectVersion { id: "v1".into() })
    }
}

fn exercise_provider(provider: &dyn SecretsProvider) {
    let material = SecretMaterial::new(PLANTED);
    let request = PutRequest {
        name: "db",
        material: &material,
        expected_version: Some("v0"),
    };
    let stored_version = must_ok(provider.put(&request), "put");
    assert_eq!(stored_version.id, "v1");

    let missing = match provider.get("other", None) {
        Err(err) => err,
        Ok(_) => panic!("unknown name must be not found"),
    };
    let missing_text = missing.to_string();
    assert!(
        matches!(missing, ProviderError::NotFound { ref name } if name == "other"),
        "unknown name is not found"
    );
    assert!(!missing_text.contains(PLANTED), "not found leaked material");

    let stored = must_ok(provider.get("db", Some("v1")), "get");
    assert_eq!(stored.version.id, "v1");
    assert_eq!(stored.material.expose(), PLANTED);
    let rendered = format!("{stored:?}");
    assert!(
        !rendered.contains(PLANTED),
        "stored object debug leaked material"
    );

    let meta = must_ok(provider.get_metadata("db"), "get_metadata");
    assert_eq!(meta.key_names, vec!["token".to_string()]);
    let rows = must_ok(provider.list("db"), "list");
    assert_eq!(rows.len(), 1, "list returns the stored object");
    assert_eq!(rows[0].name, "db");
    assert_eq!(rows[0].key_names, vec!["token".to_string()]);

    let mut tags = BTreeMap::new();
    tags.insert(EXPIRY_TAG.to_string(), "2026-01-01T00:00:00Z".to_string());
    let tagged = must_ok(provider.tag("db", &tags, Some("v1")), "tag");
    assert_eq!(tagged.id, "v2");
    let deleted = must_ok(provider.delete("db", Some("v1")), "delete");
    assert_eq!(deleted.id, "v1");
}

#[test]
fn local_secrets_set_and_list_do_not_call_aws() {
    let env = isolated();
    fs::write(env.config.join("curie.yaml"), BARE_INSTALL).expect("write local install");
    let set = run(
        &env,
        &["secrets", "set", "MODEL_KEY", "--from-env", "HOLD_VALUE"],
    );
    assert!(set.status.success(), "set failed\n{}", panic_text(&set));
    assert!(credentials(&env).is_file(), "local store was not written");
    let body = fs::read_to_string(credentials(&env)).expect("read local store");
    assert!(body.contains("MODEL_KEY"), "local store must name the key");
    assert_aws_not_called(&env);

    let list = run(&env, &["secrets", "list"]);
    assert!(list.status.success(), "list failed\n{}", panic_text(&list));
    let listed = raw_output(&list);
    assert!(
        listed.contains("MODEL_KEY"),
        "list must name the key\n{}",
        panic_text(&list)
    );
    assert!(!listed.contains(PLANTED), "list leaked material");
    assert_aws_not_called(&env);
}

#[test]
fn secrets_set_expires_requires_a_discovered_provider() {
    let env = isolated();
    let output = run(
        &env,
        &[
            "secrets",
            "set",
            "MODEL_KEY",
            "--from-env",
            "HOLD_VALUE",
            "--expires",
            "2026-01-01T00:00:00Z",
            "--json",
        ],
    );
    assert_eq!(
        output.status.code(),
        Some(2),
        "set --expires without a provider must be usage\n{}",
        panic_text(&output)
    );
    assert_string_error(&output, "requires a curie.yaml secrets provider");
    assert_aws_not_called(&env);
    assert!(
        !credentials(&env).exists(),
        "expires must not write the local store"
    );
}

#[test]
fn secrets_set_discovers_the_current_directory_provider() {
    let env = isolated();
    let path = env.config.join("curie.yaml");
    fs::write(&path, aws_install()).expect("write install");
    let output = command(&env)
        .args([
            "secrets",
            "set",
            "logical/MODEL_KEY",
            "--from-env",
            "HOLD_VALUE",
        ])
        .output()
        .expect("run curie");
    assert!(
        output.status.success(),
        "provider set failed\n{}",
        panic_text(&output)
    );
    assert!(env.marker.exists(), "provider set must call aws");
    assert!(
        !raw_output(&output).contains(PLANTED),
        "set leaked material"
    );
    assert!(
        !credentials(&env).exists(),
        "provider set must not write the local store"
    );
}

#[test]
fn secrets_set_file_without_secrets_stays_local() {
    let env = isolated();
    fs::write(env.config.join("curie.yaml"), aws_install()).expect("write discovered install");
    let path = env.config.join("local.yaml");
    fs::write(&path, BARE_INSTALL).expect("write install");
    let output = command(&env)
        .args([
            "secrets",
            "set",
            "MODEL_KEY",
            "--from-env",
            "HOLD_VALUE",
            "--file",
        ])
        .arg(&path)
        .output()
        .expect("run curie");
    assert!(
        output.status.success(),
        "set --file without secrets failed\n{}",
        panic_text(&output)
    );
    assert!(credentials(&env).is_file(), "local store was not written");
    let body = fs::read_to_string(credentials(&env)).expect("read local store");
    assert!(body.contains("MODEL_KEY"), "local store must name the key");
    assert_aws_not_called(&env);
}

#[test]
fn secrets_set_file_with_invalid_yaml_writes_nothing() {
    let env = isolated();
    let path = env.config.join("curie.yaml");
    fs::write(&path, "version: [").expect("write invalid file");
    let output = command(&env)
        .args([
            "secrets",
            "set",
            "MODEL_KEY",
            "--from-env",
            "HOLD_VALUE",
            "--file",
        ])
        .arg(&path)
        .output()
        .expect("run curie");
    assert!(
        !output.status.success(),
        "invalid file must fail\n{}",
        panic_text(&output)
    );
    assert!(
        !credentials(&env).exists(),
        "invalid file must not write the local store"
    );
    assert_aws_not_called(&env);
}

#[test]
fn secrets_list_file_with_a_provider_uses_aws() {
    let env = isolated();
    let path = env.config.join("curie.yaml");
    fs::write(&path, aws_install()).expect("write install");
    let output = command(&env)
        .args(["secrets", "list", "--file"])
        .arg(&path)
        .output()
        .expect("run curie");
    assert!(
        output.status.success(),
        "provider list failed\n{}",
        panic_text(&output)
    );
    assert!(env.marker.exists(), "provider list must call aws");
    assert!(
        !raw_output(&output).contains(PLANTED),
        "list leaked material"
    );
}

#[test]
fn provider_only_verbs_require_a_provider() {
    let env = isolated();
    let provider_invocations: &[(&str, &[&str])] = &[
        ("secrets check", &["secrets", "check"]),
        ("secrets rm", &["secrets", "rm", "logical"]),
    ];
    for (label, args) in provider_invocations {
        let output = run(&env, args);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{label} without a provider must be usage\n{}",
            panic_text(&output)
        );
        assert!(
            raw_output(&output).contains("requires a curie.yaml secrets provider"),
            "{label} must name its provider requirement\n{}",
            panic_text(&output)
        );
        assert_aws_not_called(&env);
    }
}

#[test]
fn secrets_rm_without_a_name_exits_usage() {
    let env = isolated();
    let output = run(&env, &["secrets", "rm"]);
    assert_eq!(
        output.status.code(),
        Some(2),
        "rm without a name must be usage\n{}",
        panic_text(&output)
    );
    assert!(
        !raw_output(&output).contains(PLANTED),
        "usage output leaked material"
    );
    assert_aws_not_called(&env);
}

#[test]
fn minimal_installation_has_no_secrets() {
    let parsed = Installation::parse(BARE_INSTALL).expect("minimal file");
    assert!(parsed.secrets.is_none());
    let again = Installation::parse(BARE_INSTALL).expect("parse again");
    assert_eq!(parsed.helm_sets(), again.helm_sets());
}

#[test]
fn aws_secrets_block_parses_without_changing_helm_sets() {
    assert_aws_block(
        &aws_install(),
        "us-east-1",
        "arn:aws:iam::000000000000:role/curie-sync",
    );
}

#[test]
fn govcloud_region_and_role_parse() {
    assert_aws_block(
        &gov_install(),
        "us-gov-west-1",
        "arn:aws-us-gov:iam::000000000000:role/curie-sync",
    );
}

#[test]
fn a_pasted_provider_value_is_not_echoed() {
    let planted = "sk-example-provider-token";
    let raw = secrets_yaml(&SECRET_LINES.replace("provider: aws", &format!("provider: {planted}")));
    let err = Installation::parse(&raw).expect_err("pasted provider must fail");
    let text = format!("{err:#}");
    assert!(text.contains("secrets.provider"), "{}", redact(&text));
    assert!(!text.contains(planted), "{}", redact(&text));
}

#[test]
fn secrets_block_rejects_invalid_fields() {
    assert_field_error(
        &secrets_yaml(&SECRET_LINES.replace("provider: aws", "provider: vault")),
        &["secrets.provider", "aws"],
    );
    assert_field_error(
        &secrets_yaml(&SECRET_LINES.replace("provider: aws", "provider: AWS")),
        &["secrets.provider", "aws"],
    );
    assert_field_error(
        &secrets_yaml(&format!("{SECRET_LINES}  extra: present\n")),
        &["unknown field"],
    );
    assert_field_error(
        &secrets_yaml(&SECRET_LINES.replace("prefix: tenant/platform", "prefix: \"\"")),
        &["secrets.prefix"],
    );
    assert_field_error(
        &secrets_yaml(&SECRET_LINES.replace("region: us-east-1", "region: sk-example")),
        &["secrets.region"],
    );
    assert_field_error(
        &secrets_yaml(&SECRET_LINES.replace(
            "role_arn: arn:aws:iam::000000000000:role/curie-sync",
            "role_arn: not-an-arn",
        )),
        &["secrets.role_arn"],
    );
    Installation::parse("version: 1\ninstall:\n  namespace: a\n  release: a\nsecrets: {}\n")
        .expect_err("empty secrets block must fail");
}

#[test]
fn inventory_entries_round_trip() {
    assert_round_trip(
        inventory(
            InventoryClass::Stateful,
            RotationOwner::Sm,
            UpdatePolicy::Immutable,
            vec!["token".into()],
        ),
        "stateful",
        "sm",
        "immutable",
    );
    assert_round_trip(
        inventory(
            InventoryClass::External,
            RotationOwner::Workload("api".into()),
            UpdatePolicy::Replace,
            vec!["token".into()],
        ),
        "external",
        "workload:api",
        "replace",
    );
    assert_round_trip(
        inventory(
            InventoryClass::Throwaway,
            RotationOwner::Mint("mail".into()),
            UpdatePolicy::Replace,
            vec!["token".into()],
        ),
        "throwaway",
        "mint:mail",
        "replace",
    );
}

#[test]
fn inventory_entries_reject_bad_rows() {
    let mut value = good_inventory();
    value["class"] = json!("other");
    assert!(serde_json::from_value::<InventoryEntry>(value).is_err());

    let mut value = good_inventory();
    value["update_policy"] = json!("merge");
    assert!(serde_json::from_value::<InventoryEntry>(value).is_err());

    let mut value = good_inventory();
    value["rotation_owner"] = json!("workload:");
    assert!(serde_json::from_value::<InventoryEntry>(value).is_err());

    let mut value = good_inventory();
    value["keys"] = json!([]);
    assert!(serde_json::from_value::<InventoryEntry>(value).is_err());

    let mut value = good_inventory();
    value["extra"] = json!(true);
    assert!(serde_json::from_value::<InventoryEntry>(value).is_err());

    assert!(
        rejects_empty_key(inventory(
            InventoryClass::Stateful,
            RotationOwner::Sm,
            UpdatePolicy::Immutable,
            vec![String::new()],
        )),
        "an empty key must be rejected"
    );
}

#[test]
fn secret_material_redacts_debug_and_display() {
    let material = SecretMaterial::new(PLANTED);
    let debug = format!("{material:?}");
    let display = material.to_string();
    assert!(!debug.contains(PLANTED), "debug leaked material");
    assert!(!display.contains(PLANTED), "display leaked material");
    assert!(debug.contains("redacted"), "debug must say redacted");
    assert!(display.contains("redacted"), "display must say redacted");
    assert_eq!(material.expose(), PLANTED);
}

#[test]
fn provider_errors_omit_material() {
    let rendered = [
        ProviderError::NotFound { name: "db".into() }.to_string(),
        ProviderError::Conflict {
            name: "db".into(),
            expected_version: Some("v1".into()),
            actual_version: Some("v2".into()),
        }
        .to_string(),
        ProviderError::Unavailable {
            name: "db".into(),
            status: 17,
        }
        .to_string(),
        ProviderError::InvalidName { name: "db".into() }.to_string(),
        ProviderError::Rejected {
            name: "db".into(),
            reason: RejectedReason::Immutable,
        }
        .to_string(),
        ProviderError::Rejected {
            name: "db".into(),
            reason: RejectedReason::Expired,
        }
        .to_string(),
    ];
    assert!(
        rendered[1].contains("v1") && rendered[1].contains("v2"),
        "conflict must name versions: {}",
        redact(&rendered[1])
    );
    for text in &rendered {
        assert!(text.contains("db"), "provider error must name the object");
        assert!(!text.contains(PLANTED), "provider error leaked material");
    }
}

#[test]
fn provider_double_redacts_stored_material() {
    let provider = LocalProvider {
        objects: Mutex::new(BTreeMap::new()),
    };
    exercise_provider(&provider);
}

#[test]
fn expiry_tag_and_not_implemented_are_declared() {
    assert_eq!(EXPIRY_TAG, "curie:expires-at");
    let err = not_implemented("curie secrets check").expect_err("must fail");
    let rendered = err.to_string();
    assert!(
        rendered.contains("not implemented yet"),
        "missing not-implemented text: {}",
        redact(&rendered)
    );
}
