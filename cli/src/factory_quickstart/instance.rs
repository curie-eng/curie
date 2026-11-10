//! Forge instance identity, naming, ownership and observation for the
//! non-GitHub quickstarts (#4620, ADR 0197).
//!
//! A code host or tracker instance is its kind and normalized host authority.
//! A recorded entry is found by that identity, never by a fixed name, and is
//! kept as recorded: its name, the Secret and key its credential names, and
//! the release's one `codeHostTrust` CA bundle reference. A new instance gets
//! an entry name derived from its kind and complete API base URL, a binding
//! name from its tracker entry and immutable scope id, and a Secret named for
//! the release and the entry. Objects quickstart creates carry owner labels,
//! and a pre-existing object at a derived name is reused only when it carries
//! them for this release and holds the same bytes.
//!
//! Every refusal here happens before any write. The objects and the Helm
//! revision a run observed are read again before its first write
//! ([`Observation::recheck`]), so a concurrent operator change refuses the
//! run instead of being overwritten. Tokens are compared in memory only and
//! never appear in a message.

use std::path::Path;

use anyhow::Result;
use base64::Engine;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::exit::CliError;
use crate::ops::{plain, run_capture, CommonOpts, OpsCommand};

/// The owner labels a quickstart-created Secret or ConfigMap carries.
pub const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
pub const MANAGED_BY: &str = "curie-quickstart";
pub const RELEASE_LABEL: &str = "curie.dev/release";
/// The annotation Helm puts on an object a release owns.
const HELM_RELEASE_ANNOTATION: &str = "meta.helm.sh/release-name";
/// The data key a new token Secret holds the token under.
pub const TOKEN_KEY: &str = "token";
/// The data key a new CA bundle ConfigMap holds the bundle under, and the
/// chart's default `codeHostTrust.caBundle.configMapRef.key`.
pub const TRUST_KEY: &str = "ca.crt";

const BEGIN: &str = "-----BEGIN CERTIFICATE-----";
const END: &str = "-----END CERTIFICATE-----";

fn instance_usage(message: impl Into<String>, fix: impl Into<String>) -> anyhow::Error {
    CliError::usage(message.into()).with_fix(fix.into()).into()
}

fn instance_failure(message: impl Into<String>, fix: impl Into<String>) -> anyhow::Error {
    CliError::failure(message.into())
        .with_fix(fix.into())
        .into()
}

fn entry_text<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or_default()
}

fn digest_hex8(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .take(4)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// An API base URL as the API compares it: trimmed, without a trailing `/`.
pub fn normalized_api_url(api_url: &str) -> String {
    api_url.trim().trim_end_matches('/').to_string()
}

/// A host authority as the API stores it: trimmed and lowercased.
fn normalized_host(host: &str) -> String {
    host.trim().to_ascii_lowercase()
}

/// The longest config name the API accepts (`_NAME` in
/// apps/api/src/curie_api/forges/config.py).
const CONFIG_NAME_MAX: usize = 64;

/// True for an API config name (`valid_config_name` in
/// apps/api/src/curie_api/forges/config.py): 1 to 64 lowercase letters,
/// digits, `.`, `_` or `-`, starting and ending with a letter or digit.
pub fn valid_config_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    let edge = |c: u8| c.is_ascii_lowercase() || c.is_ascii_digit();
    match (bytes.first(), bytes.last()) {
        (Some(&first), Some(&last)) => {
            bytes.len() <= CONFIG_NAME_MAX
                && edge(first)
                && edge(last)
                && bytes
                    .iter()
                    .all(|&c| edge(c) || matches!(c, b'.' | b'_' | b'-'))
        }
        _ => false,
    }
}

/// The entry name of a new `kind` instance at `api_url`: the kind with `-`
/// for `_`, then 8 hex digits of the SHA-256 of the normalized API base URL.
/// Two servers on one release get two names; one server always gets the
/// same name.
pub fn forge_entry_name(kind: &str, api_url: &str) -> String {
    format!(
        "{}-{}",
        kind.replace('_', "-"),
        digest_hex8(normalized_api_url(api_url).as_bytes())
    )
}

/// The binding name of a new binding of `tracker` on `scope_id`:
/// `<tracker>-<scope id>`, or `<tracker>-<8 hex digits of the scope id>` when
/// that is not a config name. When even that exceeds the API's 64 character
/// limit (a long recorded tracker name), the tracker name is cut to fit and
/// the digest covers the tracker and the scope id, so the name stays distinct
/// per (tracker, scope) and is the same on every run.
pub fn forge_binding_name(tracker: &str, scope_id: &str) -> String {
    let direct = format!("{tracker}-{}", scope_id.to_ascii_lowercase());
    if valid_config_name(&direct) {
        return direct;
    }
    let digested = format!("{tracker}-{}", digest_hex8(scope_id.as_bytes()));
    if valid_config_name(&digested) {
        return digested;
    }
    let digest = digest_hex8(format!("{tracker}\n{scope_id}").as_bytes());
    // A tracker name is a config name: ASCII, led by a letter or digit. The
    // digest supplies the last character.
    let keep = CONFIG_NAME_MAX - digest.len() - 1;
    let prefix: String = tracker.chars().take(keep).collect();
    format!("{prefix}-{digest}")
}

/// The Secret a new entry's credential is written to: `<release>-<entry>-token`.
pub fn forge_secret_name(release: &str, entry: &str) -> String {
    format!("{release}-{entry}-token")
}

/// The CA bundle ConfigMap a release that references none gets.
pub fn code_host_trust_name(release: &str) -> String {
    format!("{release}-code-host-trust")
}

fn recorded_factory_list<'a>(values: &'a Value, list: &str) -> &'a [Value] {
    values
        .pointer(&format!("/factory/{list}"))
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

/// A binding's trackerScopeId as text: a string as is, a number as digits.
pub(crate) fn scope_text(binding: &Value) -> String {
    match binding.get("trackerScopeId") {
        Some(Value::String(scope)) => scope.clone(),
        Some(Value::Number(number)) => match number.as_f64() {
            Some(value) if value.fract() == 0.0 => (value as i64).to_string(),
            _ => number.to_string(),
        },
        _ => String::new(),
    }
}

/// The identity of a `factory.codeHosts`, `factory.trackers` or
/// `factory.bindings` entry: (kind, host) for a forge entry, (tracker, scope
/// id) for a binding.
pub(crate) fn instance_identity(list: &str, entry: &Value) -> (String, String) {
    if list == "bindings" {
        (entry_text(entry, "tracker").to_string(), scope_text(entry))
    } else {
        (
            entry_text(entry, "kind").to_string(),
            normalized_host(entry_text(entry, "host")),
        )
    }
}

/// The recorded `list` entry with `planned`'s identity.
pub(crate) fn recorded_identity<'a>(
    values: &'a Value,
    list: &str,
    planned: &Value,
) -> Option<&'a Value> {
    let wanted = instance_identity(list, planned);
    recorded_factory_list(values, list)
        .iter()
        .find(|entry| instance_identity(list, entry) == wanted)
}

/// The Secret and data key an entry's credential names.
pub(crate) fn credential_ref(entry: &Value) -> (String, String) {
    let credential = entry.get("credential").unwrap_or(&Value::Null);
    let key = entry_text(credential, "existingSecretKey");
    (
        entry_text(credential, "existingSecret").to_string(),
        if key.is_empty() { TOKEN_KEY } else { key }.to_string(),
    )
}

/// Points `entry`'s credential at `secret` and `key`.
pub(crate) fn set_credential_ref(entry: &mut Value, secret: &str, key: &str) {
    entry["credential"]["existingSecret"] = json!(secret);
    entry["credential"]["existingSecretKey"] = json!(key);
}

/// What a planned entry resolved to on the release.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Resolved {
    /// The release records this identity; the entry is kept as recorded.
    Recorded(Value),
    /// A new identity, under its derived name.
    New(Value),
}

impl Resolved {
    pub fn entry(&self) -> &Value {
        match self {
            Self::Recorded(entry) | Self::New(entry) => entry,
        }
    }

    pub fn into_entry(self) -> Value {
        match self {
            Self::Recorded(entry) | Self::New(entry) => entry,
        }
    }

    pub fn recorded(&self) -> Option<&Value> {
        match self {
            Self::Recorded(entry) => Some(entry),
            Self::New(_) => None,
        }
    }
}

/// Resolves a planned code host or tracker entry against the release's
/// recorded values. The recorded entry with the same (kind, host) is kept as
/// recorded when its API base URL is the planned one, and refused when it is
/// another: one host is one instance. A new identity takes its derived name
/// (see [`forge_entry_name`]) and its derived Secret, and is refused when
/// that name already names an entry of another identity. `url_flag` names the
/// flag the URL came from.
pub(crate) fn resolve_instance(
    values: &Value,
    list: &str,
    mut planned: Value,
    url_flag: &str,
    release: &str,
) -> Result<Resolved> {
    let kind = entry_text(&planned, "kind").to_string();
    let host = normalized_host(entry_text(&planned, "host"));
    let planned_api = normalized_api_url(entry_text(&planned, "apiUrl"));
    if let Some(recorded) = recorded_identity(values, list, &planned) {
        let name = entry_text(recorded, "name");
        let recorded_api = normalized_api_url(entry_text(recorded, "apiUrl"));
        if recorded_api != planned_api {
            return Err(instance_usage(
                format!(
                    "release {release} already records factory.{list} entry `{name}` for {kind} host {host} with apiUrl {recorded_api}, and {url_flag} names apiUrl {planned_api} on that host: a conflicting api identity for one host, so the run was refused before any write"
                ),
                format!(
                    "rerun with {url_flag} naming {recorded_api}, or update or remove factory.{list}[{name}] on the release first"
                ),
            ));
        }
        return Ok(Resolved::Recorded(recorded.clone()));
    }
    let name = forge_entry_name(&kind, &planned_api);
    if let Some(occupant) = recorded_factory_list(values, list)
        .iter()
        .find(|entry| entry_text(entry, "name") == name)
    {
        let (other_kind, other_host) = instance_identity(list, occupant);
        return Err(instance_usage(
            format!(
                "factory.{list} name `{name}`, derived for {kind} host {host}, is occupied by an entry of a different identity ({other_kind} host {other_host}) on release {release}; refused before any write"
            ),
            format!("rename or remove factory.{list}[{name}] on the release, then rerun"),
        ));
    }
    planned["name"] = json!(name);
    let secret = forge_secret_name(release, &name);
    set_credential_ref(&mut planned, &secret, TOKEN_KEY);
    Ok(Resolved::New(planned))
}

/// Resolves a planned binding, whose `tracker` is already the resolved
/// tracker entry's name, by its identity (tracker, scope id). The recorded
/// binding is kept as recorded. A new one takes its derived name (see
/// [`forge_binding_name`]), refused when another identity holds it.
pub(crate) fn resolve_binding(
    values: &Value,
    mut planned: Value,
    release: &str,
) -> Result<Resolved> {
    if let Some(recorded) = recorded_identity(values, "bindings", &planned) {
        return Ok(Resolved::Recorded(recorded.clone()));
    }
    let tracker = entry_text(&planned, "tracker").to_string();
    let scope = scope_text(&planned);
    let name = forge_binding_name(&tracker, &scope);
    if let Some(occupant) = recorded_factory_list(values, "bindings")
        .iter()
        .find(|entry| entry_text(entry, "name") == name)
    {
        let (other_tracker, other_scope) = instance_identity("bindings", occupant);
        return Err(instance_usage(
            format!(
                "factory.bindings name `{name}`, derived for tracker {tracker} scope {scope}, is occupied by a binding of a different identity (tracker {other_tracker} scope {other_scope}) on release {release}; refused before any write"
            ),
            format!("rename or remove factory.bindings[{name}] on the release, then rerun"),
        ));
    }
    planned["name"] = json!(name);
    Ok(Resolved::New(planned))
}

/// `list` of the recorded values with `entry` put in: the entry of the same
/// name is replaced when it has `entry`'s identity, or `entry` is appended
/// when no entry has its name. A recorded entry of another identity under
/// that name, or of the same identity under another name, refuses: the
/// release changed after the run resolved its entries.
pub(crate) fn put_entry(recorded: &Value, list: &str, entry: &Value) -> Result<Value> {
    let name = entry_text(entry, "name");
    let wanted = instance_identity(list, entry);
    let mut entries = Vec::new();
    let mut placed = false;
    for kept in recorded_factory_list(recorded, list) {
        let same_name = entry_text(kept, "name") == name;
        let same_identity = instance_identity(list, kept) == wanted;
        match (same_name, same_identity) {
            (true, true) => {
                entries.push(entry.clone());
                placed = true;
            }
            (false, false) => entries.push(kept.clone()),
            _ => {
                return Err(instance_usage(
                    format!(
                        "factory.{list} on the release changed concurrently: entry `{}` no longer matches the entry `{name}` this run resolved, so nothing was written",
                        entry_text(kept, "name")
                    ),
                    "rerun quickstart to plan against the current release",
                ))
            }
        }
    }
    if !placed {
        entries.push(entry.clone());
    }
    Ok(Value::Array(entries))
}

/// How a run uses the Secret an entry's credential names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretUse {
    /// The Secret holds the token already: the recorded one, or one an
    /// earlier run of this release created. It is never rewritten.
    Reuse,
    /// The Secret is absent and is created.
    Create,
}

/// One object a run read before its first write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observed {
    kind: &'static str,
    name: String,
    /// `None` when the object was absent.
    resource_version: Option<String>,
}

/// The objects and the Helm revision a run read before its first write.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Observation {
    namespace: String,
    release: String,
    objects: Vec<Observed>,
    /// The serving Helm revision; `None` when not compared.
    helm_revision: Option<u32>,
}

impl Observation {
    pub(crate) fn new(common: &CommonOpts, helm_revision: Option<u32>) -> Self {
        Self {
            namespace: common.namespace.clone(),
            release: common.release.clone(),
            objects: Vec::new(),
            helm_revision,
        }
    }

    pub(crate) fn push(&mut self, observed: Observed) {
        if !self.objects.contains(&observed) {
            self.objects.push(observed);
        }
    }

    /// Takes the revision this run's own release write (the install's
    /// `cluster up`, the GitHub intake) produced as the one to fence on. The
    /// child that wrote was handed `mark`, which its Helm write records as
    /// the revision's description, so the revisions found here are exactly
    /// the ones that write created. Call it right after that write, having
    /// called [`Self::recheck`] right before it. A child that wrote no
    /// revision leaves the observed one in place; a revision after ours, or
    /// ours no longer serving, refuses as stale before the next write.
    pub(crate) async fn adopt_own_release_write(&mut self, mark: &ReleaseWriteMark) -> Result<()> {
        let common = CommonOpts {
            namespace: self.namespace.clone(),
            release: self.release.clone(),
            dry_run: false,
        };
        let history = release_history(&common).await?;
        self.helm_revision =
            own_write_revision(&history, &self.release, mark.as_str(), self.helm_revision)?;
        Ok(())
    }

    /// Refuses when the serving Helm revision moved since it was observed
    /// (or adopted). Read immediately before a helm upgrade, so a concurrent
    /// operator upgrade is never overwritten by `--reuse-values` of a stale
    /// plan.
    pub(crate) async fn fence_release_revision(&self) -> Result<()> {
        let Some(revision) = self.helm_revision else {
            return Ok(());
        };
        let common = CommonOpts {
            namespace: self.namespace.clone(),
            release: self.release.clone(),
            dry_run: false,
        };
        let now = release_serving_revision(&common).await?;
        if now != Some(revision) {
            return Err(stale_observation(&format!(
                "Helm release {} (revision {revision} when observed, {} now)",
                self.release,
                now.map_or_else(|| "none".to_string(), |r| r.to_string())
            )));
        }
        Ok(())
    }

    /// Reads every observed object and the serving Helm revision again and
    /// refuses when any changed since it was observed.
    pub(crate) async fn recheck(&self) -> Result<()> {
        for observed in &self.objects {
            let now = read_owned_object(&self.namespace, observed.kind, &observed.name)
                .await?
                .and_then(|object| object_resource_version(&object));
            if now != observed.resource_version {
                let shown = |rv: &Option<String>| rv.clone().unwrap_or_else(|| "absent".into());
                return Err(stale_observation(&format!(
                    "{} {} (resourceVersion {} when observed, {} now)",
                    observed.kind,
                    observed.name,
                    shown(&observed.resource_version),
                    shown(&now)
                )));
            }
        }
        self.fence_release_revision().await
    }

    /// The observation as the JSON quickstart hands its GitHub intake child
    /// in [`INTAKE_FENCE_ENV`]: names, resourceVersions and the revision,
    /// never an object's contents.
    pub(crate) fn intake_fence_json(&self) -> String {
        let objects: Vec<Value> = self
            .objects
            .iter()
            .map(|observed| {
                json!({
                    "kind": observed.kind,
                    "name": observed.name,
                    "resourceVersion": observed.resource_version,
                })
            })
            .collect();
        json!({
            "namespace": self.namespace,
            "release": self.release,
            "helmRevision": self.helm_revision,
            "objects": objects,
        })
        .to_string()
    }

    /// The observation a quickstart parent handed this intake run in
    /// [`INTAKE_FENCE_ENV`], `None` when it handed none. A fence that does
    /// not parse, or names another release, refuses before any write.
    pub(crate) fn intake_fence(common: &CommonOpts) -> Result<Option<Self>> {
        match std::env::var(INTAKE_FENCE_ENV) {
            Ok(raw) if !raw.is_empty() => {
                parse_intake_fence(&raw, common)
                    .map(Some)
                    .map_err(|problem| {
                        instance_failure(
                            format!("{INTAKE_FENCE_ENV} {problem}; nothing was written"),
                            format!("unset {INTAKE_FENCE_ENV} and rerun quickstart, which sets it"),
                        )
                    })
            }
            _ => Ok(None),
        }
    }
}

/// The environment variable a quickstart parent hands its observation to
/// the GitHub intake child in, so the child's Secret writes and Helm upgrade
/// are fenced on what the parent observed (#4620).
pub(crate) const INTAKE_FENCE_ENV: &str = "CURIE_QUICKSTART_INTAKE_FENCE";

fn parse_intake_fence(raw: &str, common: &CommonOpts) -> std::result::Result<Observation, String> {
    let fence: Value =
        serde_json::from_str(raw).map_err(|_| "is not a quickstart observation".to_string())?;
    let text = |key: &str| fence.get(key).and_then(Value::as_str).map(str::to_string);
    let (Some(namespace), Some(release)) = (text("namespace"), text("release")) else {
        return Err("names no namespace and release".into());
    };
    if namespace != common.namespace || release != common.release {
        return Err(format!(
            "observed release {release} in namespace {namespace}, not release {} in namespace {}",
            common.release, common.namespace
        ));
    }
    let helm_revision = match fence.get("helmRevision") {
        None | Some(Value::Null) => None,
        Some(revision) => Some(
            revision
                .as_u64()
                .and_then(|revision| u32::try_from(revision).ok())
                .ok_or("carries an unreadable Helm revision")?,
        ),
    };
    let mut observation = Observation {
        namespace,
        release,
        objects: Vec::new(),
        helm_revision,
    };
    for object in fence
        .get("objects")
        .and_then(Value::as_array)
        .ok_or("lists no observed objects")?
    {
        let kind = match object.get("kind").and_then(Value::as_str) {
            Some("Secret") => "Secret",
            Some("ConfigMap") => "ConfigMap",
            _ => return Err("names an object of an unknown kind".into()),
        };
        let name = object
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .ok_or("names an object without a name")?;
        let resource_version = match object.get("resourceVersion") {
            None | Some(Value::Null) => None,
            Some(Value::String(version)) => Some(version.clone()),
            Some(_) => return Err("carries an unreadable resourceVersion".into()),
        };
        observation.push(Observed {
            kind,
            name: name.to_string(),
            resource_version,
        });
    }
    Ok(observation)
}

fn stale_observation(what: &str) -> anyhow::Error {
    instance_usage(
        format!(
            "{what} changed concurrently after quickstart observed it, so its observation is stale and nothing further was written"
        ),
        "review the change, then rerun quickstart to plan against the current state",
    )
}

/// The mark one release write a quickstart child makes records as its Helm
/// revision description (see [`crate::ops::QUICKSTART_RELEASE_WRITE_ENV`]),
/// fresh for every write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReleaseWriteMark(String);

impl ReleaseWriteMark {
    pub(crate) fn fresh() -> Self {
        Self(format!(
            "{}{}",
            crate::ops::QUICKSTART_RELEASE_WRITE_PREFIX,
            uuid::Uuid::new_v4().simple()
        ))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// The revision to fence on after a write marked `mark`: the newest
/// revision whose description is `mark`, which must be both the release's
/// newest revision and its serving one, or `observed` when no revision
/// carries the mark (the write produced none).
fn own_write_revision(
    history: &Value,
    release: &str,
    mark: &str,
    observed: Option<u32>,
) -> Result<Option<u32>> {
    let rows = history.as_array().map_or(&[][..], Vec::as_slice);
    let revision = |row: &Value| {
        row.get("revision")
            .and_then(Value::as_u64)
            .and_then(|revision| u32::try_from(revision).ok())
    };
    let Some(ours) = rows
        .iter()
        .filter(|row| row.get("description").and_then(Value::as_str) == Some(mark))
        .filter_map(revision)
        .max()
    else {
        return Ok(observed);
    };
    let newest = rows.iter().filter_map(revision).max();
    let serving = crate::cluster_secrets::serving_revision(history, release).ok();
    if newest != Some(ours) || serving != Some(ours) {
        let shown =
            |revision: Option<u32>| revision.map_or_else(|| "none".into(), |r| r.to_string());
        return Err(stale_observation(&format!(
            "Helm release {release} (revision {ours} written by this run, newest {} and serving {} now)",
            shown(newest),
            shown(serving)
        )));
    }
    Ok(Some(ours))
}

/// The Helm history of the release as JSON.
async fn release_history(common: &CommonOpts) -> Result<Value> {
    let (ok, stdout, _) = run_capture(&crate::ops::helm_history_cmd(common)).await?;
    if !ok {
        return Err(instance_failure(
            format!(
                "could not read the Helm history of release {}, so its revision could not be compared; nothing further was written",
                common.release
            ),
            "confirm helm can read the release, then rerun",
        ));
    }
    Ok(serde_json::from_str(&stdout).unwrap_or(Value::Null))
}

/// The serving Helm revision of the release, `None` when it has none.
async fn release_serving_revision(common: &CommonOpts) -> Result<Option<u32>> {
    let history = release_history(common).await?;
    Ok(crate::cluster_secrets::serving_revision(&history, &common.release).ok())
}

fn object_resource_version(object: &Value) -> Option<String> {
    object
        .pointer("/metadata/resourceVersion")
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn observed_object(kind: &'static str, name: &str, object: Option<&Value>) -> Observed {
    Observed {
        kind,
        name: name.to_string(),
        resource_version: object.and_then(object_resource_version),
    }
}

/// Reads one Secret or ConfigMap as JSON, `None` when absent. The output
/// stays in memory: a Secret's data is never printed or put in a message.
/// A failed read refuses, since ownership could not be checked.
async fn read_owned_object(namespace: &str, kind: &str, name: &str) -> Result<Option<Value>> {
    let resource = kind.to_ascii_lowercase();
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("get"),
            plain(&resource),
            plain(name),
            plain("-o"),
            plain("json"),
            plain("--ignore-not-found"),
        ],
    );
    let (ok, stdout, _) = run_capture(&cmd).await?;
    if !ok {
        return Err(instance_failure(
            format!(
                "could not read {kind} {name} in namespace {namespace}, so its ownership and contents could not be checked; nothing was written"
            ),
            format!("confirm kubectl can read {resource}s in namespace {namespace}, then rerun"),
        ));
    }
    if stdout.trim().is_empty() {
        return Ok(None);
    }
    serde_json::from_str(&stdout).map(Some).map_err(|_| {
        instance_failure(
            format!("kubectl returned an unreadable {kind} {name}; nothing was written"),
            "rerun; if it persists, check the kubectl version",
        )
    })
}

/// The decoded bytes of `object`'s data `key`, trimmed; `None` when absent.
fn secret_key_bytes(object: &Value, key: &str) -> Option<Vec<u8>> {
    let raw = object.pointer("/data")?.get(key)?.as_str()?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(raw.trim())
        .ok()?;
    Some(decoded.trim_ascii().to_vec())
}

/// Who owns `object` when it is not this release's quickstart: a Helm
/// release, or no owner labels for `release`. `None` when it is ours.
fn foreign_quickstart_owner(object: &Value, release: &str) -> Option<String> {
    let metadata = object.get("metadata").unwrap_or(&Value::Null);
    if let Some(helm) = metadata
        .pointer("/annotations")
        .and_then(|annotations| annotations.get(HELM_RELEASE_ANNOTATION))
        .and_then(Value::as_str)
    {
        return Some(format!("Helm release {helm}"));
    }
    let label = |key: &str| {
        metadata
            .pointer("/labels")
            .and_then(|labels| labels.get(key))
            .and_then(Value::as_str)
    };
    match (label(MANAGED_BY_LABEL), label(RELEASE_LABEL)) {
        (Some(MANAGED_BY), Some(owner)) if owner == release => None,
        (Some(MANAGED_BY), Some(owner)) => Some(format!("the quickstart of release {owner}")),
        (Some(other), _) => Some(format!("{other}, not quickstart")),
        (None, _) => Some("no quickstart owner labels".to_string()),
    }
}

/// Who owns an object a recorded entry or the recorded trust reference names,
/// when that is another release: a Helm release other than `release`, or the
/// quickstart of another release. `None` for this release's objects and for
/// an operator's object that carries no owner marker at all, which stays
/// reusable as recorded.
fn recorded_reference_owner(object: &Value, release: &str) -> Option<String> {
    let metadata = object.get("metadata").unwrap_or(&Value::Null);
    let marker = |section: &str, key: &str| {
        metadata
            .pointer(section)
            .and_then(|map| map.get(key))
            .and_then(Value::as_str)
            .filter(|owner| *owner != release)
    };
    if let Some(helm) = marker("/annotations", HELM_RELEASE_ANNOTATION) {
        return Some(format!(
            "Helm release {helm} (annotation {HELM_RELEASE_ANNOTATION})"
        ));
    }
    marker("/labels", RELEASE_LABEL)
        .map(|owner| format!("release {owner} (label {RELEASE_LABEL}={owner})"))
}

/// Refuses a recorded reference to an object another release owns, before
/// any write: quickstart never shares another release's credential or CA
/// bundle. `what` names the reference for the message.
fn refuse_foreign_reference(
    object: &Value,
    kind: &str,
    name: &str,
    namespace: &str,
    release: &str,
    what: &str,
) -> Result<()> {
    let Some(owner) = recorded_reference_owner(object, release) else {
        return Ok(());
    };
    Err(instance_usage(
        format!(
            "{kind} {name} in namespace {namespace}, {what}, is owned by {owner}, not by release {release}; quickstart never uses a foreign {kind}, so the run was refused before any write and an operator must update or remove that {kind} or the reference before a rerun"
        ),
        format!(
            "update {kind} {name} to drop the other release's owner markers if it belongs to release {release}, or remove the reference to it from release {release} (or remove {kind} {name}), then rerun"
        ),
    ))
}

/// The owner labels for `release`.
fn quickstart_owner_labels(release: &str) -> Value {
    json!({MANAGED_BY_LABEL: MANAGED_BY, RELEASE_LABEL: release})
}

/// Checks the Secret a recorded entry names: it exists, no other release
/// owns it, it holds a non-empty value at `key`, and that value is the token
/// this run was given. A
/// recorded credential is never rewritten: rotation is the operator's update
/// of that Secret.
pub(crate) async fn verify_recorded_secret(
    common: &CommonOpts,
    entry: &str,
    secret: &str,
    key: &str,
    token: &str,
    token_flag: &str,
) -> Result<Observed> {
    let namespace = &common.namespace;
    let Some(object) = read_owned_object(namespace, "Secret", secret).await? else {
        return Err(instance_usage(
            format!(
                "Secret {secret}, which factory entry `{entry}` names for its credential, is missing (not found) in namespace {namespace}; quickstart never recreates a recorded credential, so the run was refused"
            ),
            format!(
                "restore Secret {secret} with the token under key {key}, or update or remove entry `{entry}` on the release, then rerun"
            ),
        ));
    };
    refuse_foreign_reference(
        &object,
        "Secret",
        secret,
        namespace,
        &common.release,
        &format!("which factory entry `{entry}` names for its credential"),
    )?;
    match secret_key_bytes(&object, key) {
        None => Err(instance_usage(
            format!(
                "Secret {secret} has no key {key}, the key factory entry `{entry}` reads its credential from; the run was refused"
            ),
            format!("update Secret {secret} to hold the token under key {key}, then rerun"),
        )),
        Some(bytes) if bytes.is_empty() => Err(instance_usage(
            format!(
                "Secret {secret} key {key}, the credential of factory entry `{entry}`, is empty; the run was refused"
            ),
            format!("update Secret {secret} to hold the token under key {key}, then rerun"),
        )),
        Some(bytes) if bytes != token.as_bytes() => Err(instance_usage(
            format!(
                "Secret {secret} key {key} holds a different token than {token_flag}: this run's credential conflicts with the one factory entry `{entry}` records, and quickstart never rotates a recorded credential, so the run was refused"
            ),
            format!(
                "to rotate, update Secret {secret} key {key} with the new token yourself, then rerun with it; otherwise rerun with the token Secret {secret} holds"
            ),
        )),
        Some(_) => Ok(observed_object("Secret", secret, Some(&object))),
    }
}

/// Plans the Secret a new entry's credential names. Absent, it is created.
/// Present, it is reused only when this release's quickstart owns it and it
/// holds this token, as after an interrupted run; any other object there is
/// refused, naming it.
pub(crate) async fn plan_new_secret(
    namespace: &str,
    release: &str,
    secret: &str,
    key: &str,
    token: &str,
    token_flag: &str,
) -> Result<(SecretUse, Observed)> {
    let object = read_owned_object(namespace, "Secret", secret).await?;
    let Some(found) = &object else {
        return Ok((SecretUse::Create, observed_object("Secret", secret, None)));
    };
    if let Some(owner) = foreign_quickstart_owner(found, release) {
        return Err(instance_usage(
            format!(
                "Secret {secret} in namespace {namespace}, the name this run derives for a new credential, is owned by {owner}, not by the quickstart of release {release}; quickstart never adopts a foreign Secret, so the run was refused and an operator must update or remove that Secret before a rerun"
            ),
            format!(
                "remove Secret {secret}, or update it to carry the labels {MANAGED_BY_LABEL}={MANAGED_BY} and {RELEASE_LABEL}={release} if it is this release's credential, then rerun"
            ),
        ));
    }
    if secret_key_bytes(found, key).as_deref() != Some(token.as_bytes()) {
        return Err(instance_usage(
            format!(
                "Secret {secret} in namespace {namespace}, left by an earlier quickstart of release {release}, holds a different token than {token_flag}; the run was refused rather than overwrite it, so an operator must update or remove that Secret before a rerun"
            ),
            format!(
                "remove Secret {secret}, or update it with the token in {token_flag}, then rerun"
            ),
        ));
    }
    Ok((
        SecretUse::Reuse,
        observed_object("Secret", secret, object.as_ref()),
    ))
}

/// The token Secret a new credential is created as, under `key`, with the
/// owner labels for `release`.
pub(crate) fn token_secret(
    namespace: &str,
    release: &str,
    name: &str,
    key: &str,
    token: &str,
) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {"name": name, "namespace": namespace, "labels": quickstart_owner_labels(release)},
        "data": {key: base64::engine::general_purpose::STANDARD.encode(token.as_bytes())},
    })
}

/// A Secret or ConfigMap write. `Create` fails when the object appeared after
/// it was observed absent; `Replace` carries the observed resourceVersion, so
/// it fails when the object changed. Not `Debug`: a Secret manifest holds a
/// token.
pub struct ObjectWrite {
    replace: bool,
    manifest: Value,
    what: String,
}

impl ObjectWrite {
    pub fn create(manifest: Value) -> Self {
        let what = object_label(&manifest);
        Self {
            replace: false,
            manifest,
            what,
        }
    }

    fn replace(manifest: Value) -> Self {
        let what = object_label(&manifest);
        Self {
            replace: true,
            manifest,
            what,
        }
    }

    /// `Secret <name>` or `ConfigMap <name>`, for messages.
    pub fn what(&self) -> &str {
        &self.what
    }

    /// Writes the object with `kubectl create` or `kubectl replace` over
    /// stdin. kubectl's output is dropped: a failed write can echo the
    /// manifest, which may hold a token.
    pub async fn apply(&self, namespace: &str) -> Result<()> {
        crate::ui::ui().note(&format!("Writing {}", self.what));
        let verb = if self.replace { "replace" } else { "create" };
        let cmd = OpsCommand::new(
            "kubectl",
            vec![
                plain("-n"),
                plain(namespace),
                plain(verb),
                plain("-f"),
                plain("-"),
            ],
        );
        let body = serde_json::to_vec(&self.manifest)?;
        let (ok, _, _) = crate::ops::run_capture_with_stdin(&cmd, &body).await?;
        if !ok {
            return Err(instance_failure(
                format!(
                    "writing {} in namespace {namespace} failed: it may have changed concurrently since quickstart observed it, or access is missing; the forge entries were not written",
                    self.what
                ),
                format!(
                    "check access with `kubectl -n {namespace} auth can-i {verb} {}`, then rerun quickstart to plan against the current state",
                    self.what.split(' ').next().unwrap_or_default().to_ascii_lowercase()
                ),
            ));
        }
        Ok(())
    }
}

fn object_label(manifest: &Value) -> String {
    format!(
        "{} {}",
        entry_text(manifest, "kind"),
        manifest
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or_default()
    )
}

/// The DER certificates of a PEM bundle that holds only CERTIFICATE blocks
/// and whitespace. Anything else refuses with the reason, never the text.
pub(crate) fn parse_pem_strict(pem: &str) -> std::result::Result<Vec<Vec<u8>>, String> {
    let mut certificates = Vec::new();
    let mut rest = pem;
    loop {
        let trimmed = rest.trim_start();
        if trimmed.is_empty() {
            break;
        }
        let Some(body) = trimmed.strip_prefix(BEGIN) else {
            return Err("it holds unexpected text outside a CERTIFICATE block".into());
        };
        let Some((inner, after)) = body.split_once(END) else {
            return Err("a CERTIFICATE block has no END line".into());
        };
        let encoded: String = inner.chars().filter(|c| !c.is_whitespace()).collect();
        let der = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| "a CERTIFICATE block is not valid base64".to_string())?;
        // A DER certificate is a SEQUENCE.
        if der.first() != Some(&0x30) {
            return Err("a CERTIFICATE block does not hold a DER certificate".into());
        }
        if !certificates.contains(&der) {
            certificates.push(der);
        }
        rest = after;
    }
    if certificates.is_empty() {
        return Err("it holds no CERTIFICATE block".into());
    }
    Ok(certificates)
}

/// The DER certificates a recorded bundle holds, skipping anything else.
fn pem_certificates(pem: &str) -> Vec<Vec<u8>> {
    let mut certificates = Vec::new();
    for chunk in pem.split(BEGIN).skip(1) {
        let Some((inner, _)) = chunk.split_once(END) else {
            continue;
        };
        let encoded: String = inner.chars().filter(|c| !c.is_whitespace()).collect();
        if let Ok(der) = base64::engine::general_purpose::STANDARD.decode(encoded) {
            certificates.push(der);
        }
    }
    certificates
}

fn pem_block(der: &[u8]) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(der);
    let mut block = format!("{BEGIN}\n");
    for line in encoded.as_bytes().chunks(64) {
        block.push_str(&String::from_utf8_lossy(line));
        block.push('\n');
    }
    block.push_str(END);
    block.push('\n');
    block
}

/// `existing` with each certificate of `wanted` it lacks appended, compared
/// by DER bytes; `None` when it already holds them all.
pub(crate) fn merge_bundle(existing: &str, wanted: &[Vec<u8>]) -> Option<String> {
    let held = pem_certificates(existing);
    let mut missing: Vec<&Vec<u8>> = Vec::new();
    for der in wanted {
        if !held.contains(der) && !missing.contains(&der) {
            missing.push(der);
        }
    }
    if missing.is_empty() {
        return None;
    }
    let mut merged = existing.trim_end().to_string();
    if !merged.is_empty() {
        merged.push('\n');
    }
    for der in missing {
        merged.push_str(&pem_block(der));
    }
    Some(merged)
}

/// Reads `--ca-file` and refuses anything but a bundle of CERTIFICATE blocks.
/// Runs before any network read, so a malformed bundle writes nothing.
pub(crate) fn read_ca_file(path: &Path) -> Result<Vec<Vec<u8>>> {
    let pem = std::fs::read_to_string(path).map_err(|error| {
        instance_failure(
            format!("could not read --ca-file {}: {error}", path.display()),
            "pass a readable PEM file holding the server's CA certificates",
        )
    })?;
    parse_pem_strict(&pem).map_err(|why| {
        instance_usage(
            format!(
                "--ca-file {} is not a valid PEM certificate bundle: {why}; the run was refused before any write",
                path.display()
            ),
            "pass a file holding only CERTIFICATE PEM blocks",
        )
    })
}

/// The `codeHostTrust.caBundle.configMapRef` a release records.
pub(crate) fn recorded_trust_ref(values: &Value) -> Option<(String, String)> {
    let reference = values.pointer("/codeHostTrust/caBundle/configMapRef")?;
    let name = entry_text(reference, "name");
    if name.is_empty() {
        return None;
    }
    let key = entry_text(reference, "key");
    Some((
        name.to_string(),
        if key.is_empty() { TRUST_KEY } else { key }.to_string(),
    ))
}

/// The CA bundle a release that references none gets: a new owned
/// [`code_host_trust_name`] ConfigMap holding `certificates` under
/// [`TRUST_KEY`].
pub(crate) fn new_trust(common: &CommonOpts, certificates: &[Vec<u8>]) -> ObjectWrite {
    let name = code_host_trust_name(&common.release);
    let manifest = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": common.namespace,
            "labels": quickstart_owner_labels(&common.release),
        },
        "data": {TRUST_KEY: merge_bundle("", certificates).unwrap_or_default()},
    });
    ObjectWrite::create(manifest)
}

/// Plans the merge of `certificates` into the release's one `codeHostTrust`
/// CA bundle, returning the write (`None` when the bundle already holds
/// every certificate) and what was read. A referenced ConfigMap must exist
/// and must not be owned by another release (see [`refuse_foreign_reference`]);
/// its other certificates are kept and the missing ones appended. A release
/// with no reference gets [`code_host_trust_name`], created, or merged into
/// when this release's quickstart owns it.
pub(crate) async fn plan_trust(
    common: &CommonOpts,
    values: &Value,
    certificates: &[Vec<u8>],
) -> Result<(Option<ObjectWrite>, Observed)> {
    let namespace = &common.namespace;
    let release = &common.release;
    let (name, key, set_ref) = match recorded_trust_ref(values) {
        Some((name, key)) => (name, key, false),
        None => (code_host_trust_name(release), TRUST_KEY.to_string(), true),
    };
    let object = read_owned_object(namespace, "ConfigMap", &name).await?;
    let seen = observed_object("ConfigMap", &name, object.as_ref());
    let Some(found) = object else {
        if !set_ref {
            return Err(instance_usage(
                format!(
                    "ConfigMap {name}, the codeHostTrust CA bundle release {release} references, is missing (not found) in namespace {namespace}; quickstart never recreates a referenced trust bundle, so the run was refused"
                ),
                format!(
                    "restore ConfigMap {name} with its certificates under key {key}, or update codeHostTrust.caBundle.configMapRef on the release, then rerun"
                ),
            ));
        }
        return Ok((Some(new_trust(common, certificates)), seen));
    };
    if !set_ref {
        refuse_foreign_reference(
            &found,
            "ConfigMap",
            &name,
            namespace,
            release,
            "the codeHostTrust CA bundle the release references",
        )?;
    }
    if set_ref {
        if let Some(owner) = foreign_quickstart_owner(&found, release) {
            return Err(instance_usage(
                format!(
                    "ConfigMap {name} in namespace {namespace}, the CA bundle name this run derives, is owned by {owner}, not by the quickstart of release {release}; the run was refused, so an operator must update or remove that ConfigMap before a rerun"
                ),
                format!(
                    "remove ConfigMap {name}, or point codeHostTrust.caBundle.configMapRef at it on the release, then rerun"
                ),
            ));
        }
    }
    let existing = found
        .pointer("/data")
        .and_then(|data| data.get(&key))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let write = merge_bundle(existing, certificates).map(|bundle| {
        let mut replacement = found.clone();
        if let Some(metadata) = replacement
            .get_mut("metadata")
            .and_then(Value::as_object_mut)
        {
            metadata.remove("managedFields");
        }
        replacement["data"][&key] = json!(bundle);
        ObjectWrite::replace(replacement)
    });
    Ok((write, seen))
}

/// One credential a run's entries name.
pub(crate) struct Credential<'a> {
    /// The entry that names it, for messages.
    pub entry: &'a str,
    pub secret: String,
    pub key: String,
    /// It is the credential of a recorded entry.
    pub recorded: bool,
}

/// Checks each distinct credential in `credentials` against `token`, adds
/// what it read to `observation`, and returns the Secrets to create.
pub(crate) async fn check_credentials(
    common: &CommonOpts,
    credentials: &[Credential<'_>],
    token: &str,
    token_flag: &str,
    observation: &mut Observation,
) -> Result<Vec<(String, String)>> {
    let mut seen: Vec<(String, String)> = Vec::new();
    let mut create = Vec::new();
    for credential in credentials {
        let pair = (credential.secret.clone(), credential.key.clone());
        if seen.contains(&pair) {
            continue;
        }
        seen.push(pair.clone());
        if credential.recorded {
            observation.push(
                verify_recorded_secret(
                    common,
                    credential.entry,
                    &credential.secret,
                    &credential.key,
                    token,
                    token_flag,
                )
                .await?,
            );
        } else {
            let (use_, read) = plan_new_secret(
                &common.namespace,
                &common.release,
                &credential.secret,
                &credential.key,
                token,
                token_flag,
            )
            .await?;
            observation.push(read);
            if use_ == SecretUse::Create {
                create.push(pair);
            }
        }
    }
    Ok(create)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_intake_fence_carries_the_observation_to_the_child() {
        let common = CommonOpts {
            namespace: "curie".into(),
            release: "acme".into(),
            dry_run: false,
        };
        let mut observation = Observation::new(&common, Some(7));
        observation.push(observed_object("Secret", "acme-jira-token", None));
        observation.push(observed_object(
            "ConfigMap",
            "acme-code-host-trust",
            Some(&json!({"metadata": {"resourceVersion": "41"}})),
        ));
        let raw = observation.intake_fence_json();
        assert_eq!(parse_intake_fence(&raw, &common).unwrap(), observation);
        let other = CommonOpts {
            release: "other".into(),
            ..common.clone()
        };
        assert!(parse_intake_fence(&raw, &other)
            .unwrap_err()
            .contains("release acme"));
        let forged = raw.replace("ConfigMap", "Deployment");
        assert!(parse_intake_fence(&forged, &common).is_err());
        assert!(parse_intake_fence("not json", &common).is_err());
    }

    fn history_row(revision: u32, status: &str, description: &str) -> Value {
        json!({"revision": revision, "status": status, "description": description})
    }

    #[test]
    fn only_the_revision_this_runs_write_produced_is_adopted() {
        let mark = ReleaseWriteMark::fresh();
        let ours = mark.as_str();
        assert!(ours.starts_with(crate::ops::QUICKSTART_RELEASE_WRITE_PREFIX));
        assert_ne!(ReleaseWriteMark::fresh(), mark);
        // Our write produced revision 4, which serves: it is adopted.
        let written = json!([
            history_row(3, "superseded", "Upgrade complete"),
            history_row(4, "deployed", ours),
        ]);
        assert_eq!(
            own_write_revision(&written, "acme", ours, Some(3)).unwrap(),
            Some(4)
        );
        // A failed first attempt and its marked retry: the retry is ours.
        let retried = json!([
            history_row(3, "superseded", "Upgrade complete"),
            history_row(4, "failed", "Upgrade \"acme\" failed: admission"),
            history_row(5, "deployed", ours),
        ]);
        assert_eq!(
            own_write_revision(&retried, "acme", ours, Some(3)).unwrap(),
            Some(5)
        );
        // An operator upgrade right after ours is never adopted.
        let operator = json!([
            history_row(3, "superseded", "Upgrade complete"),
            history_row(4, "superseded", ours),
            history_row(5, "deployed", "Upgrade complete"),
        ]);
        let text = format!(
            "{:#}",
            own_write_revision(&operator, "acme", ours, Some(3)).unwrap_err()
        );
        assert!(
            text.contains("revision 4 written by this run") && text.contains("stale"),
            "{text}"
        );
        // Nor is a failed operator upgrade after ours, which leaves ours serving.
        let failed_after = json!([
            history_row(4, "deployed", ours),
            history_row(5, "failed", "Upgrade \"acme\" failed: x"),
        ]);
        assert!(own_write_revision(&failed_after, "acme", ours, Some(3)).is_err());
        // A write that produced no revision keeps the observed one, even when
        // an operator upgraded meanwhile: the next fence refuses that.
        let unmarked = json!([
            history_row(3, "superseded", "Upgrade complete"),
            history_row(4, "deployed", "Upgrade complete"),
        ]);
        assert_eq!(
            own_write_revision(&unmarked, "acme", ours, Some(3)).unwrap(),
            Some(3)
        );
        assert_eq!(
            own_write_revision(&unmarked, "acme", ours, None).unwrap(),
            None
        );
        // Another run's mark is not ours.
        let other = ReleaseWriteMark::fresh();
        assert_eq!(
            own_write_revision(&written, "acme", other.as_str(), Some(3)).unwrap(),
            Some(3)
        );
    }

    fn fixture_der_certificate(seed: u8) -> Vec<u8> {
        vec![0x30, 0x03, 0x02, 0x01, seed]
    }

    #[test]
    fn names_are_stable_distinct_and_valid() {
        let a = forge_entry_name("gitlab", "https://gitlab.example.test/api/v4");
        assert_eq!(
            a,
            forge_entry_name("gitlab", "https://gitlab.example.test/api/v4/")
        );
        assert_ne!(
            a,
            forge_entry_name("gitlab", "https://gitlab.example.test/other/api/v4")
        );
        assert!(a.starts_with("gitlab-") && a.len() == 15, "{a}");
        let jira = forge_entry_name("jira_cloud", "https://acme.atlassian.net");
        assert!(
            jira.starts_with("jira-cloud-") && valid_config_name(&jira),
            "{jira}"
        );
        assert_eq!(forge_binding_name(&a, "41"), format!("{a}-41"));
        let uuid = "00000000-0000-4000-8000-000000000004";
        assert_eq!(forge_binding_name(&jira, uuid), format!("{jira}-{uuid}"));
        let odd = forge_binding_name(&a, "Group/Sub Project");
        assert!(valid_config_name(&odd) && odd.starts_with(&a), "{odd}");
        assert_eq!(forge_secret_name("acme", &a), format!("acme-{a}-token"));
        assert_eq!(code_host_trust_name("acme"), "acme-code-host-trust");
        for bad in ["", "-a", "a-", "A", "a b", &"a".repeat(65)] {
            assert!(!valid_config_name(bad), "{bad:?}");
        }
    }

    #[test]
    fn a_binding_name_stays_within_the_api_limit_for_a_64_character_tracker() {
        let tracker = format!("t{}", "a".repeat(63));
        assert_eq!(tracker.len(), 64);
        assert!(valid_config_name(&tracker));
        let uuid = "00000000-0000-4000-8000-000000000004";
        for scope in ["41", uuid, "Group/Sub Project", &"9".repeat(200)] {
            let name = forge_binding_name(&tracker, scope);
            assert!(valid_config_name(&name), "{name} ({} chars)", name.len());
            assert_eq!(name.len(), 64, "{name}");
            assert!(name.starts_with(&tracker[..55]), "{name}");
            assert_eq!(name, forge_binding_name(&tracker, scope), "deterministic");
        }
        assert_ne!(
            forge_binding_name(&tracker, "41"),
            forge_binding_name(&tracker, "42"),
            "one name per scope"
        );
        let other = format!("t{}b", "a".repeat(62));
        assert_ne!(
            forge_binding_name(&tracker, "41"),
            forge_binding_name(&other, "41"),
            "two trackers sharing a prefix get two names"
        );
        // Every length of tracker name up to the limit fits.
        for length in 1..=64 {
            let tracker = "a".repeat(length);
            let name = forge_binding_name(&tracker, uuid);
            assert!(valid_config_name(&name), "{length}: {name}");
        }
    }

    #[test]
    fn a_recorded_reference_is_foreign_only_when_another_release_marks_it() {
        let unmarked = json!({"metadata": {"name": "operator-forge-credential"}});
        assert_eq!(recorded_reference_owner(&unmarked, "acme"), None);
        let ours = json!({"metadata": {"labels": quickstart_owner_labels("acme"),
            "annotations": {HELM_RELEASE_ANNOTATION: "acme"}}});
        assert_eq!(recorded_reference_owner(&ours, "acme"), None);
        let helm = json!({"metadata": {"annotations": {HELM_RELEASE_ANNOTATION: "acme-other"}}});
        let owner = recorded_reference_owner(&helm, "acme").unwrap();
        assert!(owner.contains("Helm release acme-other"), "{owner}");
        let labelled = json!({"metadata": {"labels": {RELEASE_LABEL: "acme-other"}}});
        let owner = recorded_reference_owner(&labelled, "acme").unwrap();
        assert!(owner.contains("release acme-other"), "{owner}");
        let error = refuse_foreign_reference(
            &helm,
            "Secret",
            "operator-forge-credential",
            "ns",
            "acme",
            "which factory entry `x` names for its credential",
        )
        .unwrap_err();
        let cli = error.downcast_ref::<CliError>().expect("a CliError");
        let text = format!("{} {}", cli.message, cli.fix.clone().unwrap_or_default());
        for word in [
            "Secret operator-forge-credential",
            "acme-other",
            "update",
            "remove",
        ] {
            assert!(text.contains(word), "{word}: {text}");
        }
    }

    #[test]
    fn identity_normalizes_the_host_and_reads_a_numeric_scope() {
        let entry = json!({"kind": "gitlab", "host": " GitLab.Example.Test:8443 "});
        assert_eq!(
            instance_identity("codeHosts", &entry),
            ("gitlab".into(), "gitlab.example.test:8443".into())
        );
        let binding = json!({"tracker": "t", "trackerScopeId": 41});
        assert_eq!(
            instance_identity("bindings", &binding),
            ("t".into(), "41".into())
        );
    }

    fn planned_gitlab_entry(api: &str) -> Value {
        planned_gitlab_entry_at(api, "127.0.0.1:1")
    }

    fn planned_gitlab_entry_at(api: &str, host: &str) -> Value {
        json!({"kind": "gitlab", "host": host, "apiUrl": api,
               "credential": {"type": "static_token"}})
    }

    #[test]
    fn a_recorded_identity_is_reused_whatever_its_name() {
        let recorded = json!({"name": "operator-code-host", "kind": "gitlab", "host": "127.0.0.1:1",
            "apiUrl": "http://127.0.0.1:1/api/v4/",
            "credential": {"existingSecret": "operator-forge-credential", "existingSecretKey": "operator-key"}});
        let values = json!({"factory": {"codeHosts": [recorded]}});
        let resolved = resolve_instance(
            &values,
            "codeHosts",
            planned_gitlab_entry("http://127.0.0.1:1/api/v4"),
            "--forge-url",
            "acme",
        )
        .unwrap();
        assert_eq!(resolved, Resolved::Recorded(recorded.clone()));
        assert_eq!(
            credential_ref(resolved.entry()),
            ("operator-forge-credential".into(), "operator-key".into())
        );
        let text = format!(
            "{:#}",
            resolve_instance(
                &values,
                "codeHosts",
                planned_gitlab_entry("http://127.0.0.1:1/ctx/api/v4"),
                "--forge-url",
                "acme"
            )
            .unwrap_err()
        );
        assert!(
            text.contains("conflicting api identity") && text.contains("--forge-url"),
            "{text}"
        );
    }

    #[test]
    fn a_new_identity_takes_derived_names_and_refuses_an_occupied_one() {
        let api = "http://127.0.0.1:2/api/v4";
        let resolved = resolve_instance(
            &Value::Null,
            "trackers",
            planned_gitlab_entry_at(api, "127.0.0.1:2"),
            "--forge-url",
            "acme",
        )
        .unwrap();
        let name = forge_entry_name("gitlab", api);
        assert_eq!(entry_text(resolved.entry(), "name"), name);
        assert_eq!(
            credential_ref(resolved.entry()),
            (forge_secret_name("acme", &name), TOKEN_KEY.into())
        );
        assert!(resolved.recorded().is_none());
        let values =
            json!({"factory": {"trackers": [{"name": name, "kind": "jira_cloud", "host": "x"}]}});
        let text = format!(
            "{:#}",
            resolve_instance(
                &values,
                "trackers",
                planned_gitlab_entry_at(api, "127.0.0.1:2"),
                "--forge-url",
                "acme"
            )
            .unwrap_err()
        );
        assert!(text.contains("occupied"), "{text}");
    }

    #[test]
    fn bindings_are_found_by_tracker_and_scope() {
        let recorded = json!({"name": "operator-scope", "tracker": "t", "trackerScopeId": "41"});
        let values = json!({"factory": {"bindings": [recorded]}});
        let found = resolve_binding(
            &values,
            json!({"tracker": "t", "trackerScopeId": "41"}),
            "r",
        )
        .unwrap();
        assert_eq!(found, Resolved::Recorded(recorded));
        let new = resolve_binding(
            &values,
            json!({"tracker": "t", "trackerScopeId": "42"}),
            "r",
        )
        .unwrap();
        assert_eq!(entry_text(new.entry(), "name"), "t-42");
        let occupied = json!({"factory": {"bindings": [{"name": "t-42", "tracker": "u", "trackerScopeId": "43"}]}});
        let text = format!(
            "{:#}",
            resolve_binding(
                &occupied,
                json!({"tracker": "t", "trackerScopeId": "42"}),
                "r"
            )
            .unwrap_err()
        );
        assert!(
            text.contains("binding") && text.contains("occupied"),
            "{text}"
        );
    }

    #[test]
    fn put_entry_replaces_the_same_identity_and_refuses_a_changed_one() {
        let kept = json!({"name": "a", "kind": "gitlab", "host": "h1"});
        let mine = json!({"name": "b", "kind": "gitlab", "host": "h2", "label": "x"});
        let recorded =
            json!({"factory": {"trackers": [kept, {"name": "b", "kind": "gitlab", "host": "h2"}]}});
        assert_eq!(
            put_entry(&recorded, "trackers", &mine).unwrap(),
            json!([kept, mine])
        );
        assert_eq!(
            put_entry(&Value::Null, "trackers", &mine).unwrap(),
            json!([mine])
        );
        let taken =
            json!({"factory": {"trackers": [{"name": "b", "kind": "gitlab", "host": "h9"}]}});
        assert!(put_entry(&taken, "trackers", &mine).is_err());
        let renamed =
            json!({"factory": {"trackers": [{"name": "c", "kind": "gitlab", "host": "h2"}]}});
        assert!(put_entry(&renamed, "trackers", &mine).is_err());
    }

    #[test]
    fn strict_pem_accepts_only_certificate_blocks() {
        let one = pem_block(&fixture_der_certificate(1));
        let two = pem_block(&fixture_der_certificate(2));
        assert_eq!(
            parse_pem_strict(&format!("\n{one}\n{two}")).unwrap(),
            [fixture_der_certificate(1), fixture_der_certificate(2)]
        );
        assert_eq!(
            parse_pem_strict(&format!("{one}{one}")).unwrap(),
            [fixture_der_certificate(1)]
        );
        for bad in [
            format!("{one}\nnot a certificate\n"),
            String::new(),
            "-----BEGIN CERTIFICATE-----\n%%%\n-----END CERTIFICATE-----\n".to_string(),
            "-----BEGIN CERTIFICATE-----\nMIIB\n".to_string(),
            "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n".to_string(),
        ] {
            let why = parse_pem_strict(&bad).unwrap_err();
            assert!(!why.contains("not a certificate\n"), "{why}");
        }
    }

    #[test]
    fn merge_appends_only_missing_certificates() {
        let one = pem_block(&fixture_der_certificate(1));
        assert_eq!(merge_bundle(&one, &[fixture_der_certificate(1)]), None);
        let merged = merge_bundle(
            &one,
            &[
                fixture_der_certificate(2),
                fixture_der_certificate(1),
                fixture_der_certificate(2),
            ],
        )
        .unwrap();
        assert_eq!(
            pem_certificates(&merged),
            [fixture_der_certificate(1), fixture_der_certificate(2)]
        );
        assert!(merged.starts_with(&one));
        assert_eq!(
            merge_bundle("", &[fixture_der_certificate(3)]).unwrap(),
            pem_block(&fixture_der_certificate(3))
        );
    }

    #[test]
    fn ownership_needs_both_labels_for_this_release_and_no_helm_owner() {
        let ours = json!({"metadata": {"labels": quickstart_owner_labels("acme")}});
        assert_eq!(foreign_quickstart_owner(&ours, "acme"), None);
        assert!(foreign_quickstart_owner(&ours, "other").is_some());
        let mut helm = ours.clone();
        helm["metadata"]["annotations"] = json!({HELM_RELEASE_ANNOTATION: "acme-other"});
        assert_eq!(
            foreign_quickstart_owner(&helm, "acme").as_deref(),
            Some("Helm release acme-other")
        );
        assert!(foreign_quickstart_owner(&json!({"metadata": {}}), "acme").is_some());
    }

    #[test]
    fn secret_bytes_decode_trimmed_and_never_panic() {
        let secret = token_secret("ns", "acme", "s", "token", "abc");
        assert_eq!(
            secret_key_bytes(&secret, "token").as_deref(),
            Some(&b"abc"[..])
        );
        assert_eq!(secret_key_bytes(&secret, "other"), None);
        assert!(!secret.to_string().contains("\"abc\""));
        let broken = json!({"data": {"token": "%%%"}});
        assert_eq!(secret_key_bytes(&broken, "token"), None);
    }

    #[test]
    fn recorded_trust_ref_defaults_the_key() {
        let values = json!({"codeHostTrust": {"caBundle": {"configMapRef": {"name": "roots"}}}});
        assert_eq!(
            recorded_trust_ref(&values),
            Some(("roots".into(), TRUST_KEY.into()))
        );
        assert_eq!(recorded_trust_ref(&Value::Null), None);
    }
}
