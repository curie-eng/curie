//! Versioned, local text-skill installation. Bundle policy remains in its release metadata.
use crate::{
    agent_skills::{self, InstallOptions},
    ui::{CliOutput, Ui},
};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

const NAME: &str = "prepare-factory-ticket";
const RELEASE_URL: &str =
    "https://github.com/curie-eng/curie/releases/latest/download/factory-preparation.json";
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseFile {
    pub sha256: String,
    pub content: String,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Release {
    pub schema_version: u32,
    pub preparation_version: String,
    pub contract_versions: Vec<String>,
    pub files: BTreeMap<String, ReleaseFile>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    schema_version: u32,
    preparation_version: String,
    contract_version: String,
    preparation_release: Option<String>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    schema_version: u32,
    source: String,
    release: Release,
    backup: Option<Release>,
    backup_source: Option<String>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PendingUpdate {
    original: Receipt,
    next: Release,
    backup_name: String,
}

#[derive(Serialize)]
pub struct ManagedSkillOutput {
    dry_run: bool,
    status: &'static str,
    current_version: Option<String>,
    available_version: Option<String>,
    preparation_version: Option<String>,
    contract_version: Option<String>,
    directory: PathBuf,
    instructions_path: Option<PathBuf>,
    preferences_path: Option<PathBuf>,
    backup_path: Option<PathBuf>,
}
impl CliOutput for ManagedSkillOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("managed result serializes")
    }
    fn render(&self, ui: &Ui) {
        ui.payload_plain(&format!("{}: {}", self.status, self.directory.display()));
        for (label, value) in [
            ("Current version", &self.current_version),
            ("Available version", &self.available_version),
            ("Preparation version", &self.preparation_version),
            ("Specification contract", &self.contract_version),
        ] {
            if let Some(value) = value {
                ui.payload_plain(&format!("{label}: {value}"));
            }
        }
        if let Some(path) = &self.instructions_path {
            ui.payload_plain(&format!("Instructions: {}", path.display()));
        }
        if let Some(path) = &self.preferences_path {
            ui.payload_plain(&format!("Local preferences: {}", path.display()));
        }
        if let Some(path) = &self.backup_path {
            ui.payload_plain(&format!("Backup: {}", path.display()));
        }
    }
}
fn output(status: &'static str, directory: PathBuf) -> ManagedSkillOutput {
    ManagedSkillOutput {
        dry_run: false,
        status,
        directory,
        current_version: None,
        available_version: None,
        preparation_version: None,
        contract_version: None,
        instructions_path: None,
        preferences_path: None,
        backup_path: None,
    }
}
fn hash(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn safe_version(version: &str) -> bool {
    !version.is_empty()
        && version != "."
        && version != ".."
        && version
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".-_".contains(&b))
}
fn validate(release: &Release) -> Result<()> {
    if release.schema_version != 1
        || !safe_version(&release.preparation_version)
        || release.contract_versions.is_empty()
        || release
            .contract_versions
            .iter()
            .any(|contract| contract.trim().is_empty())
    {
        bail!("unsupported or invalid preparation release metadata");
    }
    let expected = ["SKILL.md", "references/specification.md"];
    if release.files.len() != expected.len()
        || expected.iter().any(|p| !release.files.contains_key(*p))
    {
        bail!("release must contain exactly SKILL.md and references/specification.md");
    }
    for (path, file) in &release.files {
        if hash(file.content.as_bytes()) != file.sha256 {
            bail!("release SHA256 mismatch for {path}");
        }
    }
    Ok(())
}
fn locations(options: &InstallOptions) -> Result<(PathBuf, PathBuf, PathBuf)> {
    let root = agent_skills::selected_root(options)?;
    let payload = root
        .join(options.agent.directory(options.global))
        .join(NAME);
    let agent = serde_json::to_value(options.agent)?
        .as_str()
        .expect("agent string")
        .to_owned();
    let state = root.join(".curie/agent-skills").join(agent).join(NAME);
    agent_skills::check_path(&root, payload.strip_prefix(&root)?)?;
    agent_skills::check_path(&root, state.strip_prefix(&root)?)?;
    for child in [
        "installed.json",
        "pending.json",
        "update.lock",
        "backups",
        "versions",
    ] {
        agent_skills::check_path(&root, state.join(child).strip_prefix(&root)?)?;
    }
    Ok((root, payload, state))
}
fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let mut staging = tempfile::NamedTempFile::new_in(parent)?;
    use std::io::Write;
    staging.write_all(&serde_json::to_vec_pretty(value)?)?;
    staging.as_file().sync_all()?;
    staging
        .persist(path)
        .map_err(|e| e.error)
        .context("publish managed metadata")?;
    Ok(())
}
fn receipt(state: &Path) -> Result<Receipt> {
    let receipt: Receipt=serde_json::from_slice(&fs::read(state.join("installed.json")).context(
        "No managed installation receipt; reinstall the original unedited skill before updating",
    )?)
    .context("read managed installation receipt")?;
    if receipt.schema_version != 1 {
        bail!("unsupported managed installation receipt version");
    }
    Ok(receipt)
}
fn check_payload(payload: &Path, release: &Release) -> Result<()> {
    for (path, file) in &release.files {
        agent_skills::check_path(payload, Path::new(path))?;
        if !fs::symlink_metadata(payload.join(path)).is_ok_and(|metadata| metadata.is_file())
            || fs::read(payload.join(path))
                .map(|b| hash(&b))
                .ok()
                .as_deref()
                != Some(file.sha256.as_str())
        {
            bail!("Local skill customizations detected at {}. No files changed; preserve these edits and reconcile them manually before updating. Automatic merging is disabled.", payload.join(path).display());
        }
    }
    let mut paths = vec![payload.to_path_buf()];
    while let Some(dir) = paths.pop() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_symlink() {
                bail!("refusing symlink in managed skill");
            }
            if entry.file_type()?.is_dir() {
                paths.push(path);
            } else if !entry.file_type()?.is_file() {
                bail!("refusing special file in managed skill");
            }
        }
    }
    Ok(())
}
fn preserve_extras(payload: &Path, staging: &Path, release: &Release) -> Result<()> {
    let mut paths = vec![payload.to_path_buf()];
    while let Some(dir) = paths.pop() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let relative = path.strip_prefix(payload)?;
            if entry.file_type()?.is_symlink() {
                bail!("refusing symlink in managed skill");
            }
            let destination = staging.join(relative);
            if entry.file_type()?.is_dir() {
                fs::create_dir_all(&destination)?;
                paths.push(path);
            } else if !entry.file_type()?.is_file() {
                bail!("refusing special file in managed skill");
            } else if !release
                .files
                .contains_key(&relative.to_string_lossy().replace('\\', "/"))
            {
                fs::create_dir_all(destination.parent().context("extra file parent")?)?;
                fs::copy(path, destination)?;
            }
        }
    }
    Ok(())
}
fn cached(state: &Path, release: &Release) -> PathBuf {
    let digest = hash(&serde_json::to_vec(release).expect("release serializes"));
    state
        .join("versions")
        .join(&release.preparation_version)
        .join(digest)
}
fn cache(state: &Path, release: &Release) -> Result<PathBuf> {
    validate(release)?;
    let dir = cached(state, release);
    agent_skills::check_path(state, dir.strip_prefix(state)?)?;
    if let Some(parent) = dir.parent().filter(|parent| parent.is_dir()) {
        for entry in fs::read_dir(parent)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() && entry.path() != dir {
                bail!("Preparation version {} is already cached with different contents; release versions are immutable",release.preparation_version);
            }
        }
    }
    if dir.exists() {
        check_payload_cache(&dir, release)?;
        return Ok(dir);
    }
    fs::create_dir_all(dir.parent().context("cache parent")?)?;
    let staging = tempfile::Builder::new()
        .prefix(".release-")
        .tempdir_in(dir.parent().context("cache parent")?)?;
    for (path, file) in &release.files {
        let dest = staging.path().join(path);
        fs::create_dir_all(dest.parent().context("file parent")?)?;
        fs::write(dest, &file.content)?;
    }
    fs::write(
        staging.path().join("release.json"),
        serde_json::to_vec_pretty(release)?,
    )?;
    fs::rename(staging.path(), &dir)?;
    Ok(dir)
}
fn check_payload_cache(dir: &Path, release: &Release) -> Result<()> {
    for (path, file) in &release.files {
        agent_skills::check_path(dir, Path::new(path))?;
        if hash(&fs::read(dir.join(path))?) != file.sha256 {
            bail!("cached preparation files were modified; restore the cached release");
        }
    }
    Ok(())
}
fn local_release(source: &Path) -> Result<Release> {
    let release: Release = serde_json::from_slice(&fs::read(source.join("release.json"))?)?;
    validate(&release)?;
    Ok(release)
}
fn trusted_url(url: &reqwest::Url) -> bool {
    url.scheme() == "https"
        && url.username().is_empty()
        && url.password().is_none()
        && url.port().is_none()
        && url.host_str() == Some("github.com")
        && url.path().starts_with("/curie-eng/curie/releases/")
}
async fn fetch(source: Option<&Path>, url: Option<&str>) -> Result<(Release, String)> {
    if let Some(source) = source {
        return Ok((
            local_release(source)?,
            format!("local:{}", source.display()),
        ));
    }
    let url = reqwest::Url::parse(url.unwrap_or(RELEASE_URL))?;
    if url.query().is_some() || url.fragment().is_some() {
        bail!("release URL must not contain query parameters or fragments");
    }
    if !trusted_url(&url) {
        bail!("release URL must use the trusted HTTPS origin https://github.com/curie-eng/curie/releases/");
    }
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            let u = attempt.url();
            if attempt.previous().len() > 5 {
                attempt.error("too many release redirects")
            } else if u.scheme() == "https"
                && (trusted_url(u)
                    || matches!(
                        u.host_str(),
                        Some("release-assets.githubusercontent.com")
                            | Some("objects.githubusercontent.com")
                    ))
            {
                attempt.follow()
            } else {
                attempt.error("untrusted release redirect")
            }
        }))
        .build()?;
    let mut response = client
        .get(url.clone())
        .send()
        .await
        .map_err(reqwest::Error::without_url)?
        .error_for_status()
        .map_err(reqwest::Error::without_url)?;
    if response
        .content_length()
        .is_some_and(|length| length > 2_000_000)
    {
        bail!("preparation release exceeds size limit");
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)?
    {
        if bytes.len() + chunk.len() > 2_000_000 {
            bail!("preparation release exceeds size limit");
        }
        bytes.extend_from_slice(&chunk);
    }
    let release: Release = serde_json::from_slice(&bytes)?;
    validate(&release)?;
    Ok((release, url.to_string()))
}

pub fn embedded(files: &[(&str, &[u8])]) -> Result<Release> {
    #[derive(Deserialize)]
    struct Metadata {
        preparation_version: String,
        contract_versions: Vec<String>,
    }
    let metadata: Metadata = serde_json::from_str(include_str!(
        "../../examples/dark-factory/preparation/release-metadata.json"
    ))?;
    let files = files
        .iter()
        .map(|(path, bytes)| {
            Ok((
                (*path).to_owned(),
                ReleaseFile {
                    sha256: hash(bytes),
                    content: std::str::from_utf8(bytes)?.to_owned(),
                },
            ))
        })
        .collect::<Result<_>>()?;
    let release = Release {
        schema_version: 1,
        preparation_version: metadata.preparation_version,
        contract_versions: metadata.contract_versions,
        files,
    };
    validate(&release)?;
    Ok(release)
}
pub fn record_install(release: Release, options: &InstallOptions) -> Result<()> {
    if options.dry_run {
        return Ok(());
    }
    let (_, _, state) = locations(options)?;
    if state.join("installed.json").exists() {
        return Ok(());
    }
    cache(&state, &release)?;
    write_json(
        &state.join("installed.json"),
        &Receipt {
            schema_version: 1,
            source: "embedded".into(),
            release,
            backup: None,
            backup_source: None,
        },
    )
}
pub fn package(release: Release, out: &Path) -> Result<Box<dyn CliOutput>> {
    validate(&release)?;
    write_json(out, &release)?;
    let files: Vec<_> = release
        .files
        .iter()
        .enumerate()
        .map(|(index, (path, file))| {
            serde_json::json!({
                "SPDXID":format!("SPDXRef-File-{index}"),"fileName":path,
                "checksums":[{"algorithm":"SHA256","checksumValue":file.sha256}],
                "licenseConcluded":"NOASSERTION","copyrightText":"NOASSERTION"
            })
        })
        .collect();
    let mut relationships = vec![
        serde_json::json!({"spdxElementId":"SPDXRef-DOCUMENT","relationshipType":"DESCRIBES","relatedSpdxElement":"SPDXRef-Package"}),
    ];
    relationships.extend((0..files.len()).map(|index| serde_json::json!({"spdxElementId":"SPDXRef-Package","relationshipType":"CONTAINS","relatedSpdxElement":format!("SPDXRef-File-{index}")})));
    let inventory = serde_json::json!({
        "spdxVersion":"SPDX-2.3","dataLicense":"CC0-1.0","SPDXID":"SPDXRef-DOCUMENT",
        "name":"factory-preparation","documentNamespace":format!("https://schemas.curietech.ai/preparation/sbom/{}",hash(&serde_json::to_vec(&release)?)),
        "creationInfo":{"creators":["Tool: curie-factory-prepare-package"],"created":time::OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339)?},
        "packages":[{"SPDXID":"SPDXRef-Package","name":NAME,"versionInfo":release.preparation_version,"downloadLocation":"NOASSERTION","filesAnalyzed":false,"licenseDeclared":"Apache-2.0","copyrightText":"NOASSERTION"}],
        "files":files,"relationships":relationships
    });
    let mut sbom_path = out.as_os_str().to_os_string();
    sbom_path.push(".spdx.json");
    write_json(Path::new(&sbom_path), &inventory)?;
    let mut result = output("packaged", out.to_path_buf());
    result.preparation_version = Some(release.preparation_version);
    Ok(Box::new(result))
}

pub fn preflight_install(options: &InstallOptions) -> Result<()> {
    locations(options)?;
    Ok(())
}

fn recover(payload: &Path, state: &Path, read_only: bool) -> Result<()> {
    let journal = state.join("pending.json");
    if !journal.exists() {
        return Ok(());
    }
    if read_only {
        bail!("An interrupted skill update needs recovery. Run update without --check or --dry-run; the current files were not changed by this check.");
    }
    let pending: PendingUpdate = serde_json::from_slice(&fs::read(&journal)?)?;
    validate(&pending.original.release)?;
    validate(&pending.next)?;
    if !safe_version(&pending.backup_name) || !pending.backup_name.starts_with("backup-") {
        bail!("invalid interrupted update backup");
    }
    let backup = state.join("backups").join(&pending.backup_name);
    agent_skills::check_path(state, backup.strip_prefix(state)?)?;
    let committed = receipt(state).ok().is_some_and(|record| {
        serde_json::to_vec(&record.release).ok() == serde_json::to_vec(&pending.next).ok()
    });
    if committed && check_payload(payload, &pending.next).is_ok() {
        fs::remove_file(journal)?;
        return Ok(());
    }
    if backup.exists() {
        check_payload(&backup, &pending.original.release)?;
        if payload.exists() {
            check_payload(payload, &pending.next)?;
            if extra_files(payload, &pending.next)?
                != extra_files(&backup, &pending.original.release)?
            {
                bail!("Interrupted update contains local customizations outside the managed files. Both copies were preserved at {} and {}; reconcile them manually before retrying.",payload.display(),backup.display());
            }
            fs::remove_dir_all(payload)?;
        }
        fs::rename(&backup, payload).context("recover interrupted skill update")?;
        write_json(&state.join("installed.json"), &pending.original)?;
    } else {
        check_payload(payload, &pending.original.release)?;
    }
    fs::remove_file(journal)?;
    Ok(())
}
fn extra_files(root: &Path, release: &Release) -> Result<BTreeMap<String, String>> {
    let mut extra = BTreeMap::new();
    let mut paths = vec![root.to_path_buf()];
    while let Some(dir) = paths.pop() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = path
                .strip_prefix(root)?
                .to_string_lossy()
                .replace('\\', "/");
            let kind = entry.file_type()?;
            if kind.is_dir() {
                extra.insert(name, "directory".into());
                paths.push(path);
            } else if kind.is_file() {
                if !release.files.contains_key(&name) {
                    extra.insert(name, hash(&fs::read(path)?));
                }
            } else {
                bail!("refusing symlink or special file in interrupted skill update");
            }
        }
    }
    Ok(extra)
}
fn update_lock(state: &Path) -> Result<fs::File> {
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(state.join("update.lock"))?;
    lock.try_lock().map_err(|_| {
        anyhow::anyhow!("Another managed skill update is running; retry after it finishes")
    })?;
    Ok(lock)
}

pub struct UpdateOptions {
    pub install: InstallOptions,
    pub source: Option<PathBuf>,
    pub release_url: Option<String>,
    pub check: bool,
    pub rollback: bool,
}
pub async fn update(options: UpdateOptions) -> Result<Box<dyn CliOutput>> {
    let (root, payload, state) = locations(&options.install)?;
    let mut lock =
        if state.join("pending.json").exists() && !(options.check || options.install.dry_run) {
            Some(update_lock(&state)?)
        } else {
            None
        };
    recover(&payload, &state, options.check || options.install.dry_run)?;
    let original = receipt(&state)?;
    validate(&original.release)?;
    check_payload(&payload, &original.release)?;
    let (next, source) = if options.rollback {
        (
            original
                .backup
                .clone()
                .context("No managed backup is available to roll back")?,
            original
                .backup_source
                .clone()
                .unwrap_or_else(|| original.source.clone()),
        )
    } else {
        match fetch(options.source.as_deref(), options.release_url.as_deref()).await {
            Ok(value) => value,
            Err(error)
                if options.check
                    && options.source.is_none()
                    && error
                        .downcast_ref::<reqwest::Error>()
                        .is_some_and(|error| error.is_connect() || error.is_timeout()) =>
            {
                let mut result = output("offline", payload);
                result.current_version = Some(original.release.preparation_version);
                result.available_version = None;
                let _ = error;
                return Ok(Box::new(result));
            }
            Err(error) => return Err(error),
        }
    };
    validate(&next)?;
    let same = serde_json::to_vec(&next)? == serde_json::to_vec(&original.release)?;
    if !same && next.preparation_version == original.release.preparation_version {
        bail!("Preparation release version was reused with different contents; no files changed");
    }
    let mut result = output(
        if same {
            "current"
        } else if options.check {
            "update-available"
        } else if options.install.dry_run {
            "planned"
        } else if options.rollback {
            "rolled-back"
        } else {
            "updated"
        },
        payload.clone(),
    );
    result.current_version = Some(original.release.preparation_version.clone());
    result.available_version = Some(next.preparation_version.clone());
    result.dry_run = options.install.dry_run;
    if options.check || options.install.dry_run || same {
        return Ok(Box::new(result));
    }
    if lock.is_none() {
        lock = Some(update_lock(&state)?);
    }
    let current = receipt(&state)?;
    if serde_json::to_vec(&current.release)? != serde_json::to_vec(&original.release)? {
        bail!("Installed release changed while checking; rerun the update");
    }
    let _lock = lock;
    cache(&state, &next)?;
    let staging = tempfile::Builder::new()
        .prefix(".curie-update-")
        .tempdir_in(&state)?;
    for (path, file) in &next.files {
        let dest = staging.path().join(path);
        fs::create_dir_all(dest.parent().context("asset parent")?)?;
        fs::write(dest, &file.content)?;
    }
    preserve_extras(&payload, staging.path(), &original.release)?;
    let backups = state.join("backups");
    fs::create_dir_all(&backups)?;
    let backup = tempfile::Builder::new()
        .prefix("backup-")
        .tempdir_in(&backups)?
        .keep();
    fs::remove_dir(&backup)?;
    agent_skills::check_path(&root, payload.strip_prefix(&root)?)?;
    check_payload(&payload, &original.release)?;
    write_json(
        &state.join("pending.json"),
        &PendingUpdate {
            original: original.clone(),
            next: next.clone(),
            backup_name: backup
                .file_name()
                .context("backup name")?
                .to_string_lossy()
                .into_owned(),
        },
    )?;
    if let Err(error) = fs::rename(&payload, &backup) {
        fs::remove_file(state.join("pending.json"))?;
        return Err(error).context("back up installed skill");
    }
    if let Err(error) = fs::rename(staging.path(), &payload) {
        fs::rename(&backup, &payload).context("restore skill after activation failure")?;
        fs::remove_file(state.join("pending.json"))?;
        return Err(error).context("activate skill update");
    }
    let new_receipt = Receipt {
        schema_version: 1,
        source,
        release: next,
        backup: Some(original.release),
        backup_source: Some(original.source),
    };
    if let Err(error) = write_json(&state.join("installed.json"), &new_receipt) {
        fs::remove_dir_all(&payload)?;
        fs::rename(&backup, &payload)?;
        fs::remove_file(state.join("pending.json"))?;
        return Err(error);
    }
    result.backup_path = Some(backup);
    fs::remove_file(state.join("pending.json"))?;
    Ok(Box::new(result))
}

pub struct ResolveOptions {
    pub install: InstallOptions,
    pub repo: PathBuf,
    pub source: Option<PathBuf>,
    pub offline: bool,
}
pub async fn resolve(options: ResolveOptions) -> Result<Box<dyn CliOutput>> {
    let (root, _, state) = locations(&options.install)?;
    let (repo, pin) = read_binding(&options.repo)?;
    let binding = match pin {
        Some(binding) => binding,
        None => {
            let release = receipt(&state)?.release;
            validate(&release)?;
            if release.contract_versions.len() != 1 {
                bail!("Installed release supports multiple specification formats; select contract_version with an optional schema_version 2 repository pin");
            }
            Binding {
                schema_version: 2,
                preparation_version: release.preparation_version,
                contract_version: release.contract_versions[0].clone(),
                preparation_release: None,
            }
        }
    };
    let mut candidates = Vec::new();
    if let Some(source) = options.source {
        let release = local_release(&source)?;
        if release.preparation_version != binding.preparation_version {
            bail!("release preparation version does not match repository pin");
        }
        candidates.push(release);
    }
    let versions = state.join("versions").join(&binding.preparation_version);
    agent_skills::check_path(&root, versions.strip_prefix(&root)?)?;
    if versions.is_dir() {
        for entry in fs::read_dir(&versions)? {
            let entry = entry?;
            agent_skills::check_path(&versions, Path::new(&entry.file_name()))?;
            if entry.file_type()?.is_dir() {
                agent_skills::check_path(
                    &root,
                    entry.path().join("release.json").strip_prefix(&root)?,
                )?;
                let release = local_release(&entry.path())?;
                if cached(&state, &release) != entry.path() {
                    bail!("Cached release metadata changed; restore the original release before continuing");
                }
                candidates.push(release);
            }
        }
    }
    if candidates.is_empty() && !options.offline {
        if let Some(tag) = &binding.preparation_release {
            if !safe_version(tag) {
                bail!("invalid preparation_release locator; use an exact release tag");
            }
            let url = format!("https://github.com/curie-eng/curie/releases/download/{tag}/factory-preparation.json");
            if let Ok((release, _)) = fetch(None, Some(&url)).await {
                candidates.push(release);
            }
        }
    }
    let release=candidates.into_iter().find(|r| r.preparation_version==binding.preparation_version && r.contract_versions.contains(&binding.contract_version)).context("No cached compatible preparation release for repository pin; supply --source DIR or set preparation_release to an exact published release tag. Current installed skill remains unchanged")?;
    let dir = cache(&state, &release)?;
    let mut result = output("resolved", dir.clone());
    result.preparation_version = Some(release.preparation_version);
    result.contract_version = Some(binding.contract_version.clone());
    result.instructions_path = Some(dir.join("SKILL.md"));
    result.preferences_path = Some(repo.join(".curie/factory-preparation.local.json"));
    Ok(Box::new(result))
}

fn read_binding(repo: &Path) -> Result<(PathBuf, Option<Binding>)> {
    let repo = repo
        .canonicalize()
        .context("resolve repository directory")?;
    let config = repo.join(".curie-factory.json");
    let bytes = match fs::read(&config) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok((repo, None)),
        Err(error) => return Err(error).context("read repository preparation pin"),
    };
    const MIGRATION: &str = "Repository preparation pin must use schema_version 2 with preparation_version and contract_version (optional preparation_release). For a legacy deployment binding, copy target.contract_version to contract_version and remove target after review. No file was changed. Remove the optional pin to use the installed release.";
    let binding: Binding = serde_json::from_slice(&bytes).context(MIGRATION)?;
    if binding.schema_version != 2
        || !safe_version(&binding.preparation_version)
        || binding.contract_version.trim().is_empty()
        || binding
            .preparation_release
            .as_ref()
            .is_some_and(|tag| !safe_version(tag))
    {
        bail!(MIGRATION);
    }
    Ok((repo, Some(binding)))
}
