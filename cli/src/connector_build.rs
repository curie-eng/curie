//! The CLI's hand mirrors of the connector declaration and lock (ADR 0113).
//!
//! `packages/plugin-format` owns `connectors.yaml` and `connectors.lock.yaml`,
//! but the CLI reads both before the platform ever sees the bundle: `curie
//! build --plugin-dir` builds what the declaration names and writes the lock,
//! and `curie cluster deploy` preflights that lock. Rust cannot import the
//! Pydantic models, so the shapes are mirrored here and the seam is frozen in
//! `tests/vectors/connector-build-decl.json`, `connector-lock.json`,
//! `connector-fields.json`, `connector-service-dns.json` and
//! `connector-source-digest.json` -- every one of them read by both this
//! crate's tests and the Python suite, so a change made in one language and not
//! the other fails that language.
//!
//! Every struct here carries `#[serde(deny_unknown_fields)]`. A tolerant reader
//! would silently drop a key the bundle author wrote, which is how an operator
//! ends up believing they declared something they did not, and it would leave
//! `curie dev field-parity` green while the two languages had already diverged.
//!
//! `plugin_format.validate_bundle` remains the full gate: it enforces every
//! other rule in `validate_connectors` (names, ports, secret paths,
//! placeholders) and the lock's freshness against the extracted tree. What this
//! module mirrors is what the CLI itself must decide locally -- which form a
//! connector declares, whether its build inputs are inside the bundle, and what
//! the source hashes to.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// The declaration this module reads, named once (its Python twin is
/// `plugin_format.connectors.CONNECTORS_FILE`).
pub const CONNECTORS_FILE: &str = "connectors.yaml";

/// The lock `curie build` writes, named once (its Python twin is
/// `plugin_format.connector_lock.CONNECTOR_LOCK_FILE`).
pub const CONNECTOR_LOCK_FILE: &str = "connectors.lock.yaml";

/// The only lock shape this build understands.
pub const LOCK_VERSION: u32 = 1;

/// Kubernetes DNS label ceiling and the digest suffix a truncated name keeps.
/// Mirrors `connector_render._DNS_LABEL_MAX` / `_DIGEST_LEN`.
const DNS_LABEL_MAX: usize = 63;
const DIGEST_LEN: usize = 8;

// ─── The declaration ─────────────────────────────────────────────────────────

/// `connectors.yaml` itself.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorsFileDecl {
    #[serde(default)]
    pub connectors: BTreeMap<String, ConnectorSpecDecl>,
}

/// One declared connector, a full mirror of `plugin_format.ConnectorSpec`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorSpecDecl {
    // -- hosted form --
    pub image: Option<String>,
    pub build: Option<ConnectorBuildDecl>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default = "default_port")]
    pub port: u32,
    pub unhosted_url: Option<String>,

    // -- remote form --
    pub url: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,

    // -- both --
    #[serde(default)]
    pub secrets: Vec<SecretDecl>,
    /// The env-var name the derived `Authorization: Bearer ${NAME}` header
    /// expands. Optional; a single `secrets:` entry is that name. See
    /// [`bearer_secret_name`].
    #[serde(default)]
    pub bearer_secret: Option<String>,
    #[serde(default)]
    pub sealed_secrets: BTreeMap<String, String>,
    #[serde(default)]
    pub secret_files: BTreeMap<String, String>,
}

/// A declared secret: a bare env var name, or a reference to a Secret someone
/// else provisioned. Both forms exist on the Python side and the CLI only ever
/// carries them through, so this reads either without choosing between them.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum SecretDecl {
    Name(String),
    Ref {
        name: String,
        from_secret: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        key: Option<String>,
    },
}

/// Where a connector's image comes from, when the bundle carries its source.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorBuildDecl {
    pub context: String,
    #[serde(default = "default_dockerfile")]
    pub dockerfile: String,
    #[serde(default)]
    pub platforms: Vec<String>,
}

fn default_port() -> u32 {
    8000
}

fn default_dockerfile() -> String {
    "Dockerfile".to_string()
}

/// Hand-written rather than derived, so an omitted `port` is 8000 whether the
/// value came from a parsed document or from a constructed default. A derived
/// `Default` would say 0, which is not a port and is not what the Python model
/// means by "unset".
impl Default for ConnectorSpecDecl {
    fn default() -> Self {
        Self {
            image: None,
            build: None,
            args: Vec::new(),
            env: BTreeMap::new(),
            port: default_port(),
            unhosted_url: None,
            url: None,
            headers: BTreeMap::new(),
            secrets: Vec::new(),
            bearer_secret: None,
            sealed_secrets: BTreeMap::new(),
            secret_files: BTreeMap::new(),
        }
    }
}

/// Same rule for `dockerfile`: an omitted one is `Dockerfile`, never the empty
/// path a derived `Default` would produce.
impl Default for ConnectorBuildDecl {
    fn default() -> Self {
        Self {
            context: String::new(),
            dockerfile: default_dockerfile(),
            platforms: Vec::new(),
        }
    }
}

// ─── The lock ────────────────────────────────────────────────────────────────

/// `connectors.lock.yaml` itself.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorLockFileDecl {
    pub version: u32,
    #[serde(default)]
    pub connectors: BTreeMap<String, ConnectorLockEntryDecl>,
}

/// The resolved identity of one built connector.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorLockEntryDecl {
    pub image: String,
    pub delivery: Delivery,
    #[serde(default)]
    pub platforms: Vec<String>,
    pub source_digest: String,
}

/// How the built image reaches the tier that runs it. Control bearing: it is
/// what the cluster deploy preflight refuses on, so an unmodelled value is
/// rejected loudly rather than degraded to a default (ADR-0036's rule).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Delivery {
    #[default]
    Registry,
    LocalDaemon,
}

// ─── Field names: the Python/Rust parity corpus ──────────────────────────────

/// The wire field names of one mirror struct.
///
/// Read off a serialized default rather than a hand-typed list, so a field
/// added to the struct shows up here without anyone remembering to update a
/// second copy -- the whole point of `tests/vectors/connector-fields.json` is
/// that neither language can drift alone.
fn field_names<T: Serialize + Default>() -> BTreeSet<String> {
    match serde_json::to_value(T::default()) {
        Ok(serde_json::Value::Object(map)) => map.keys().cloned().collect(),
        _ => BTreeSet::new(),
    }
}

pub fn spec_field_names() -> BTreeSet<String> {
    field_names::<ConnectorSpecDecl>()
}

pub fn build_field_names() -> BTreeSet<String> {
    field_names::<ConnectorBuildDecl>()
}

pub fn lock_file_field_names() -> BTreeSet<String> {
    field_names::<ConnectorLockFileDecl>()
}

pub fn lock_entry_field_names() -> BTreeSet<String> {
    field_names::<ConnectorLockEntryDecl>()
}

// ─── Parsing ─────────────────────────────────────────────────────────────────

/// Parse and check a `connectors.yaml` document.
///
/// Accept/reject parity with the Python validator is the property that matters:
/// a document the CLI accepts and the platform rejects means an operator builds
/// and pushes an image for a bundle that can never deploy, and the reverse
/// means a bundle that deploys through git flow but cannot be built locally.
pub fn parse_connectors(document: &str) -> Result<ConnectorsFileDecl> {
    let file: ConnectorsFileDecl =
        serde_norway::from_str(document).with_context(|| format!("parse {CONNECTORS_FILE}"))?;
    for (name, spec) in &file.connectors {
        check_name(name)?;
        check_spec(name, spec)?;
    }
    Ok(file)
}

/// The connector name cap and shape, mirroring `plugin_format.connectors`
/// (`_NAME_MAX`, `_NAME_RE`): an RFC 1123 label, because the name becomes a
/// Kubernetes object and a DNS label. Change one language and change the other,
/// or the accept/reject parity this module exists to hold is gone.
const CONNECTOR_NAME_MAX: usize = 40;

/// Refuse a name neither the platform nor this CLI can carry.
///
/// Parity with Python is only half of it. The CLI joins the name into a host
/// path of its own (`.curie/connector-secrets/<connector>/`), so an unchecked
/// name is a bundle-authored path component: `../../evil` would put a resolved
/// credential outside the bundle. Checking at load rather than at each use is
/// what makes every downstream join safe by construction.
fn check_name(name: &str) -> Result<()> {
    let alnum = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
    let valid = !name.is_empty()
        && name.len() <= CONNECTOR_NAME_MAX
        && name.chars().all(|c| alnum(c) || c == '-')
        && name.starts_with(alnum)
        && name.ends_with(alnum);
    if !valid {
        bail!(
            "connectors.{name}: a connector name becomes a Kubernetes object, a DNS label and a \
             local directory, so it must be lowercase alphanumeric or dashes, start and end \
             alphanumeric, and be at most {CONNECTOR_NAME_MAX} characters"
        );
    }
    Ok(())
}

/// Parse and check a `connectors.lock.yaml` document.
///
/// The version is checked here because it is the only thing that lets a future
/// shape change be refused by an older reader instead of silently misread, and
/// every entry's image is checked against the delivery it claims because this
/// reader is the last gate before the CLI runs or ships what the lock names
/// (see [`check_lock_entry`]).
pub fn parse_lock(document: &str) -> Result<ConnectorLockFileDecl> {
    let lock: ConnectorLockFileDecl =
        serde_norway::from_str(document).with_context(|| format!("parse {CONNECTOR_LOCK_FILE}"))?;
    if lock.version != LOCK_VERSION {
        bail!(
            "{CONNECTOR_LOCK_FILE}: version {} is not a shape this build understands \
             (expected {LOCK_VERSION})",
            lock.version
        );
    }
    // The lock is a second door onto the same names, and a bring-up reads it
    // whether or not it re-read the declaration.
    for (name, entry) in &lock.connectors {
        check_name(name)?;
        check_lock_entry(name, entry)?;
    }
    Ok(lock)
}

/// The digest suffix both well-formed references carry: `sha256:` plus 64
/// lowercase hex characters. Fixed by the OCI distribution spec's
/// content-addressable digest grammar, not by anything in this repository.
const IMAGE_DIGEST_PREFIX: &str = "sha256:";
const IMAGE_DIGEST_HEX: usize = 64;

/// Does the recorded reference match the delivery it claims?
///
/// The Rust twin of `plugin_format.connector_lock._image_matches_delivery`: a
/// `registry` entry carries `<repo>@sha256:<64 lowercase hex>`, the manifest
/// digest a registry pull resolves, and a `local-daemon` entry carries a bare
/// `sha256:<64 lowercase hex>`, the id `docker image inspect --format {{.Id}}`
/// reports. Nothing else, a tag included: a tag can be repointed at a different
/// artifact after review, which is the failure ADR 0113 exists to close.
fn image_matches_delivery(entry: &ConnectorLockEntryDecl) -> bool {
    match entry.delivery {
        // Split on the FIRST `@`, so a reference carrying a second one leaves it
        // inside the digest half, where it is not hex -- the repository half
        // admits neither an `@` nor whitespace, as Python's `[^@\s]+` does.
        Delivery::Registry => entry.image.split_once('@').is_some_and(|(repo, digest)| {
            !repo.is_empty() && !repo.contains(char::is_whitespace) && is_image_digest(digest)
        }),
        Delivery::LocalDaemon => is_image_digest(&entry.image),
    }
}

fn is_image_digest(reference: &str) -> bool {
    reference
        .strip_prefix(IMAGE_DIGEST_PREFIX)
        .is_some_and(|hex| {
            hex.len() == IMAGE_DIGEST_HEX && hex.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f'))
        })
}

/// Refuse a lock entry whose image is not a digest of its delivery's shape.
///
/// Python refuses this in `apply_lock` rather than in `validate_connector_lock`,
/// because there the two live one call apart. The CLI has no `apply_lock`: it
/// reads the lock and runs or renders what it names, so the refusal belongs at
/// the read. Without it a hand-edited `connectors.lock.yaml` carrying a mutable
/// tag is run by `curie skill up` and shipped by a deploy, which is exactly the
/// identity rule ADR 0113 states -- defeated at the one reader that never
/// asks the platform.
fn check_lock_entry(name: &str, entry: &ConnectorLockEntryDecl) -> Result<()> {
    if image_matches_delivery(entry) {
        return Ok(());
    }
    let (delivery, expected) = match entry.delivery {
        Delivery::Registry => (
            "registry",
            "`<repo>@sha256:` plus 64 lowercase hex characters, the manifest digest a registry \
             pull resolves",
        ),
        Delivery::LocalDaemon => (
            "local-daemon",
            "a bare `sha256:` plus 64 lowercase hex characters, the image id the local daemon \
             reports",
        ),
    };
    bail!(
        "connectors.{name}: {CONNECTOR_LOCK_FILE} records image {:?} for delivery `{delivery}`, \
         which is not that delivery's digest shape -- it must be {expected}. A mutable tag is \
         never run or rendered: it can be repointed at a different artifact after review. \
         Regenerate the lock with `curie build --plugin-dir <dir>`.",
        entry.image
    );
}

/// Read a bundle's `connectors.yaml`, or an empty declaration when it has none.
pub fn load(bundle_dir: &Path) -> Result<ConnectorsFileDecl> {
    let path = bundle_dir.join(CONNECTORS_FILE);
    if !path.is_file() {
        return Ok(ConnectorsFileDecl::default());
    }
    let body =
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    parse_connectors(&body)
}

/// Read a bundle's `connectors.lock.yaml`, or `None` when it has none.
///
/// `None` is the common case and not an error: an ordinary `image:` bundle
/// carries no lock and never will.
pub fn load_lock(bundle_dir: &Path) -> Result<Option<ConnectorLockFileDecl>> {
    let path = bundle_dir.join(CONNECTOR_LOCK_FILE);
    if !path.is_file() {
        return Ok(None);
    }
    let body =
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    parse_lock(&body).map(Some)
}

fn check_spec(name: &str, spec: &ConnectorSpecDecl) -> Result<()> {
    let forms = [
        spec.image.is_some(),
        spec.build.is_some(),
        spec.url.is_some(),
    ];
    let declared = forms.iter().filter(|set| **set).count();
    if declared > 1 {
        bail!(
            "connectors.{name}: set exactly one of `image`, `build` or `url` -- otherwise it \
             is unclear who owns the process"
        );
    }
    if declared == 0 {
        bail!(
            "connectors.{name}: set `image` for Curie to run it, `build` for Curie to build it \
             from source in this bundle, or `url` to point at something already running"
        );
    }
    // Keyed to hosted-ness, not to `image`: a built connector is equally hosted,
    // so a remote-only field must not ride the build form.
    if (spec.image.is_some() || spec.build.is_some()) && !spec.headers.is_empty() {
        bail!(
            "connectors.{name}: `headers` apply to a remote endpoint; configure a hosted \
             connector with `env` and `args` instead"
        );
    }
    if let Some(build) = &spec.build {
        check_build(name, build)?;
    }
    Ok(())
}

fn check_build(name: &str, build: &ConnectorBuildDecl) -> Result<()> {
    if escapes(&build.context) {
        bail!(
            "connectors.{name}: `build.context` is {:?}; it must be a path inside the bundle",
            build.context
        );
    }
    if escapes(&build.dockerfile) {
        bail!(
            "connectors.{name}: `build.dockerfile` is {:?}; it must be a path inside the build \
             context",
            build.dockerfile
        );
    }
    if build.platforms.is_empty() {
        bail!(
            "connectors.{name}: `build.platforms` is empty. A silently single-arch build fails \
             after apply as `no matching manifest`, so the target set is stated, never guessed"
        );
    }
    for platform in &build.platforms {
        if !is_platform(platform) {
            bail!(
                "connectors.{name}: `{platform}` is not an OCI platform. Use `os/arch` or \
                 `os/arch/variant`, such as `linux/amd64` or `linux/arm/v7`"
            );
        }
    }
    Ok(())
}

/// `os/arch`, optionally `os/arch/variant`. Mirrors `connectors._PLATFORM_RE`.
fn is_platform(platform: &str) -> bool {
    let parts: Vec<&str> = platform.split('/').collect();
    let alnum = |s: &&str| {
        !s.is_empty()
            && s.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    };
    match parts.as_slice() {
        [os, arch] => alnum(os) && alnum(arch),
        [os, arch, variant] => {
            alnum(os)
                && alnum(arch)
                && variant.starts_with('v')
                && variant.len() > 1
                && variant[1..].chars().all(|c| c.is_ascii_digit())
        }
        _ => false,
    }
}

/// Is this bundle-relative path empty, absolute, or outside its root?
///
/// Textual, the same check the Python validator performs on a declaration it
/// may be reading without a tree. The resolvers below add what only a
/// filesystem can answer.
fn escapes(path: &str) -> bool {
    if path.trim().is_empty() {
        return true;
    }
    let mut depth: i32 = 0;
    for component in Path::new(path).components() {
        match component {
            Component::RootDir | Component::Prefix(_) => return true,
            Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    return true;
                }
            }
            Component::CurDir => {}
            Component::Normal(_) => depth += 1,
        }
    }
    false
}

// ─── Path resolution ─────────────────────────────────────────────────────────

/// The canonical build context, required to sit inside the bundle.
///
/// ADR 0113 requires a build context constrained to the extracted bundle
/// "without accepting arbitrary host paths". Canonicalization is what decides,
/// so a symlinked directory cannot walk out of the bundle either -- and a
/// symlink is refused rather than dereferenced, the same rule
/// `cli/src/bundle.rs`'s packer applies for the same reason.
pub fn resolve_context(bundle_root: &Path, context: &str) -> Result<PathBuf> {
    if escapes(context) {
        bail!("build context {context:?} must be a path inside the bundle");
    }
    let root = bundle_root
        .canonicalize()
        .with_context(|| format!("resolve bundle root {}", bundle_root.display()))?;
    let candidate = root.join(context);
    refuse_symlink(&candidate)?;
    let resolved = candidate
        .canonicalize()
        .with_context(|| format!("resolve build context {}", candidate.display()))?;
    if !resolved.starts_with(&root) {
        bail!(
            "build context {context:?} resolves to {} which is outside the bundle",
            resolved.display()
        );
    }
    Ok(resolved)
}

/// The canonical Dockerfile, required to sit inside the canonical context.
///
/// The textual check alone is not enough here: a bundle can ship
/// `connectors/x/Dockerfile` as a symlink to a host file, and `curie build`
/// reads that Dockerfile BEFORE the bundle is packed, so the packer's symlink
/// refusal never runs.
pub fn resolve_dockerfile(context_canonical: &Path, dockerfile: &str) -> Result<PathBuf> {
    if escapes(dockerfile) {
        bail!("dockerfile {dockerfile:?} must be a path inside the build context");
    }
    let candidate = context_canonical.join(dockerfile);
    refuse_symlink(&candidate)?;
    let resolved = candidate
        .canonicalize()
        .with_context(|| format!("resolve dockerfile {}", candidate.display()))?;
    if !resolved.starts_with(context_canonical) {
        bail!(
            "dockerfile {dockerfile:?} resolves to {} which is outside the build context",
            resolved.display()
        );
    }
    Ok(resolved)
}

fn refuse_symlink(path: &Path) -> Result<()> {
    if let Ok(meta) = std::fs::symlink_metadata(path) {
        if meta.file_type().is_symlink() {
            bail!(
                "{} is a symlink; Curie refuses to follow one out of the bundle rather than \
                 build whatever it points at",
                path.display()
            );
        }
    }
    Ok(())
}

// ─── object_name / service_dns: the Docker network alias ─────────────────────

/// The Kubernetes object name for a connector, and the Docker network alias the
/// same connector is started under at the skill and local tiers.
///
/// A hand port of `connector_render.object_name`, frozen against
/// `tests/vectors/connector-service-dns.json`, because the runner derives the
/// URL it dials from the PYTHON copy: a mismatch leaves it dialing a name no
/// container owns, which surfaces as a bare connection timeout.
pub fn object_name(release: &str, agent: &str, connector: &str) -> String {
    let base = format!("{release}-{agent}-mcp-{connector}");
    if base.len() <= DNS_LABEL_MAX {
        return base;
    }
    // Truncate WITH a digest of the full name: clipping alone maps two long
    // names sharing a prefix onto one object.
    let digest = hex(Sha256::digest(base.as_bytes()).as_slice());
    let keep = DNS_LABEL_MAX - DIGEST_LEN - 1;
    let head = base[..keep].trim_end_matches('-');
    format!("{head}-{}", &digest[..DIGEST_LEN])
}

/// The in-cluster DNS name of a connector's Service.
pub fn service_dns(release: &str, agent: &str, connector: &str, namespace: &str) -> String {
    format!(
        "{}.{namespace}.svc.cluster.local",
        object_name(release, agent, connector)
    )
}

// ─── source_digest ───────────────────────────────────────────────────────────

/// The content-derived identity of a build input.
///
/// A port of `plugin_format.connector_lock.source_digest_of`, frozen against
/// `tests/vectors/connector-source-digest.json`, whose `comment` states the
/// algorithm in full and why each rule is there. The CLI writes this value into
/// the lock and the platform validator recomputes it from the extracted bundle
/// to refuse a stale one, so two algorithms would make every build look stale on
/// one side of the seam.
///
/// Content plus ONE bit of mode per file, the owner execute bit: the build
/// context tar carries each file's mode and BuildKit keys its cache on it, so an
/// exec-bit flip on an entrypoint is a changed build input.
pub fn source_digest_of(context_dir: &Path, build: &ConnectorBuildDecl) -> Result<String> {
    let rules = dockerignore_rules(context_dir, &build.dockerfile)?;
    let mut included: Vec<String> = Vec::new();
    collect_files(
        context_dir,
        context_dir,
        &rules,
        is_bundle_root_context(&build.context),
        &mut included,
    )?;
    included.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

    let mut stream: Vec<u8> = Vec::new();
    for relative in &included {
        let path = context_dir.join(relative);
        let bytes = std::fs::read(&path).with_context(|| format!("read {relative}"))?;
        stream.extend_from_slice(relative.as_bytes());
        stream.push(0);
        stream.extend_from_slice(hex(Sha256::digest(&bytes).as_slice()).as_bytes());
        stream.push(0);
        stream.push(if is_owner_executable(&path)? {
            b'1'
        } else {
            b'0'
        });
        stream.push(b'\n');
    }
    // The declared build block, so editing `platforms` or `dockerfile` alone
    // still invalidates the lock. Keys sorted, no whitespace, platforms in
    // DECLARED order -- a lane that sorted them would split the two digests.
    let canonical = format!(
        "{{\"context\":{},\"dockerfile\":{},\"platforms\":[{}]}}",
        json_string(&build.context),
        json_string(&build.dockerfile),
        build
            .platforms
            .iter()
            .map(|p| json_string(p))
            .collect::<Vec<_>>()
            .join(",")
    );
    stream.extend_from_slice(b"build\0");
    stream.extend_from_slice(canonical.as_bytes());
    stream.push(b'\n');

    Ok(format!(
        "sha256:{}",
        hex(Sha256::digest(&stream).as_slice())
    ))
}

/// Does the declared `build.context` name the bundle root?
///
/// Decided from the DECLARATION, not the tree: `curie build` writes the
/// generated `connectors.lock.yaml` at the bundle root only, so a
/// `connectors.lock.yaml` is this context's own generated file exactly when the
/// declared context names that root. The Python twin normalizes identically --
/// strip a trailing `/`, then `.` and the empty string are the root.
fn is_bundle_root_context(context: &str) -> bool {
    matches!(context.trim_end_matches('/'), "." | "")
}

/// Is this file executable by its owner? The one bit of mode the digest carries.
///
/// Docker's build context tar carries each file's mode and BuildKit keys its
/// cache on it, so flipping the exec bit on an entrypoint changes the build
/// input while the bytes stay put.
#[cfg(unix)]
fn is_owner_executable(path: &Path) -> Result<bool> {
    use std::os::unix::fs::PermissionsExt;

    let mode = std::fs::metadata(path)
        .with_context(|| format!("stat {}", path.display()))?
        .permissions()
        .mode();
    Ok(mode & 0o100 != 0)
}

/// Off-unix there is no owner execute bit to read, so every file hashes as
/// non-executable -- the same normalization docker itself applies on Windows,
/// where the context tar is written with a fixed mode.
#[cfg(not(unix))]
fn is_owner_executable(_path: &Path) -> Result<bool> {
    Ok(false)
}

fn json_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| String::from("\"\""))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// One `.dockerignore` line: the pattern, and whether it re-includes.
struct IgnoreRule {
    negated: bool,
    pattern: String,
}

/// The context's own `.dockerignore` exclusion set, in file order, or an empty
/// one.
///
/// Docker's file, so honoring it means the digest covers exactly what the
/// daemon receives. `.curieignore` governs bundle packing, a different
/// question, and is not consulted.
///
/// Order is load-bearing: `*` followed by `!Dockerfile` and `!Dockerfile`
/// followed by `*` are different exclusion sets, and the last matching rule is
/// the one that decides. The two trailing rules mirror the docker CLI's own
/// `TrimBuildFilesFromExcludes`, which puts the Dockerfile and `.dockerignore`
/// back into the context however the patterns read.
fn dockerignore_rules(context_dir: &Path, dockerfile: &str) -> Result<Vec<IgnoreRule>> {
    let path = context_dir.join(".dockerignore");
    if !path.is_file() {
        return Ok(Vec::new());
    }
    let body =
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let mut rules: Vec<IgnoreRule> = Vec::new();
    for line in body.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match line.strip_prefix('!') {
            // The `!` is removed and the remainder used verbatim, as Docker
            // does; a bare `!` names nothing and is skipped rather than being
            // read as a re-include of everything.
            Some("") => continue,
            Some(remainder) => rules.push(IgnoreRule {
                negated: true,
                pattern: remainder.to_string(),
            }),
            None => rules.push(IgnoreRule {
                negated: false,
                pattern: line.to_string(),
            }),
        }
    }
    for build_file in [".dockerignore", dockerfile] {
        if is_excluded(&rules, build_file) {
            rules.push(IgnoreRule {
                negated: true,
                pattern: build_file.to_string(),
            });
        }
    }
    Ok(rules)
}

fn collect_files(
    root: &Path,
    dir: &Path,
    rules: &[IgnoreRule],
    exclude_lock: bool,
    out: &mut Vec<String>,
) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        let kind = entry.file_type()?;
        // A symlink is never followed and never hashed; the context resolver
        // refuses one before hashing begins.
        if kind.is_symlink() {
            continue;
        }
        let relative = path
            .strip_prefix(root)
            .map_err(|_| anyhow!("{} is outside {}", path.display(), root.display()))?
            .to_string_lossy()
            .replace('\\', "/");
        if kind.is_dir() {
            // Descended even when the directory itself matches an exclusion: a
            // later `!` can re-include a file beneath it, which is exactly what
            // Docker does once the ignore set carries any exclusion. The
            // per-file check below evaluates the same ancestor paths, so
            // pruning here would only ever LOSE a re-included file.
            collect_files(root, &path, rules, exclude_lock, out)?;
            continue;
        }
        // Only the context's OWN generated lock: `curie build` writes it at the
        // BUNDLE root, so it is skipped only when the declared context IS that
        // root. Under a subdirectory context a `connectors.lock.yaml` at the top
        // is authored input the daemon receives, and so is one nested deeper
        // under any context.
        if exclude_lock && relative == CONNECTOR_LOCK_FILE {
            continue;
        }
        if is_excluded(rules, &relative) {
            continue;
        }
        out.push(relative);
    }
    Ok(())
}

/// Is this path excluded once every rule has had its say, in file order?
///
/// Docker's rule, not "the first exclusion wins": every rule is evaluated and
/// the last one that matches decides, so a later `!` re-includes what an earlier
/// pattern excluded. Each rule is matched segment by segment against the path
/// and each of its ancestor directory paths, so `*` and `?` never cross a `/`
/// and a pattern naming a directory excludes everything beneath it.
fn is_excluded(rules: &[IgnoreRule], relative: &str) -> bool {
    let segments: Vec<&str> = relative.split('/').collect();
    let candidates: Vec<String> = (0..segments.len())
        .map(|i| segments[..=i].join("/"))
        .collect();
    let mut excluded = false;
    for rule in rules {
        if matches_any(&rule.pattern, &candidates) {
            excluded = !rule.negated;
        }
    }
    excluded
}

fn matches_any(pattern: &str, candidates: &[String]) -> bool {
    let parts: Vec<&str> = pattern.split('/').collect();
    for candidate in candidates {
        let against: Vec<&str> = candidate.split('/').collect();
        if parts.len() != against.len() {
            continue;
        }
        if parts
            .iter()
            .zip(against.iter())
            .all(|(p, a)| matches_segment(p, a))
        {
            return true;
        }
    }
    false
}

/// One path segment against one glob segment: `*`, `?` and `[...]` classes,
/// matching Python's `fnmatch.fnmatchcase` on the other side of the seam.
fn matches_segment(pattern: &str, name: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let n: Vec<char> = name.chars().collect();
    glob_match(&p, &n)
}

fn glob_match(pattern: &[char], name: &[char]) -> bool {
    if pattern.is_empty() {
        return name.is_empty();
    }
    match pattern[0] {
        '*' => {
            for split in 0..=name.len() {
                if glob_match(&pattern[1..], &name[split..]) {
                    return true;
                }
            }
            false
        }
        '?' => !name.is_empty() && glob_match(&pattern[1..], &name[1..]),
        '[' => {
            let Some(close) = pattern.iter().position(|c| *c == ']').filter(|i| *i > 1) else {
                // An unterminated class is a literal `[`, as fnmatch reads it.
                return !name.is_empty() && name[0] == '[' && glob_match(&pattern[1..], &name[1..]);
            };
            if name.is_empty() {
                return false;
            }
            let mut class = &pattern[1..close];
            let negated = matches!(class.first(), Some('!'));
            if negated {
                class = &class[1..];
            }
            let hit = class_matches(class, name[0]);
            (hit != negated) && glob_match(&pattern[close + 1..], &name[1..])
        }
        literal => !name.is_empty() && name[0] == literal && glob_match(&pattern[1..], &name[1..]),
    }
}

fn class_matches(class: &[char], candidate: char) -> bool {
    let mut i = 0;
    while i < class.len() {
        if i + 2 < class.len() && class[i + 1] == '-' {
            if class[i] <= candidate && candidate <= class[i + 2] {
                return true;
            }
            i += 3;
            continue;
        }
        if class[i] == candidate {
            return true;
        }
        i += 1;
    }
    false
}

// ─── The build plan: what `curie build --plugin-dir` actually issues ──────────

/// An [`crate::ops::OpsCommand`] whose every token is a plain (non-secret) one.
/// Connector commands carry no credential in argv by construction, which is the
/// property `docker_env` exists to keep.
pub(crate) fn plain_command(program: &str, args: Vec<String>) -> crate::ops::OpsCommand {
    crate::ops::OpsCommand {
        program: program.to_string(),
        args: args.into_iter().map(crate::ops::CmdArg::Plain).collect(),
        env: Vec::new(),
        secret_env: Vec::new(),
    }
}

/// The tag a local-daemon build produces, before the daemon's image id replaces
/// it in the lock. Ephemeral by construction: nothing pulls it, and a rebuild
/// reuses it, which is exactly why the lock records the id instead.
fn local_build_tag(bundle_name: &str, connector: &str) -> String {
    format!("curie-connector-{bundle_name}-{connector}:build")
}

/// How many hex characters of the source digest become the pushed tag. Long
/// enough that two sources cannot collide in practice, short enough to read.
const TAG_DIGEST_LEN: usize = 16;

/// The uid every connector container runs as, mirroring `render_deployment`'s
/// `runAsUser` so a staged credential is readable by the same identity on both
/// tiers.
const CONNECTOR_UID: u32 = 65532;

/// One connector's resolved build: the inputs, the command's target, and the
/// identity the lock will record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorBuildPlan {
    pub connector: String,
    /// Canonical, inside the bundle.
    pub context: PathBuf,
    /// Canonical, inside the context.
    pub dockerfile: PathBuf,
    /// The DECLARED platform set, in declaration order. What the lock records,
    /// and what the registry path builds; the local-daemon path builds
    /// [`ConnectorBuildPlan::host_platform`] instead.
    pub platforms: Vec<String>,
    /// The one platform a local-daemon build can actually produce.
    pub host_platform: String,
    pub delivery: Delivery,
    /// The `-t` tag this build produces.
    pub image_ref: String,
    pub source_digest: String,
    /// Where buildx writes its build result metadata; registry delivery only.
    pub metadata_file: Option<PathBuf>,
}

/// The OCI platform of the machine running the CLI.
///
/// A Linux platform always: `docker build --platform darwin/arm64` would ask
/// the daemon for an image no container runtime can run. The host here means
/// the ARCH of the machine, never its OS.
pub fn host_platform() -> String {
    let arch = match std::env::consts::ARCH {
        "x86_64" | "x86" => "amd64",
        "aarch64" => "arm64",
        "arm" => "arm",
        "powerpc64" => "ppc64le",
        other => other,
    };
    format!("linux/{arch}")
}

/// Resolve everything one connector's build needs, without contacting anything.
///
/// Planning is where a bad declaration fails: a context outside the bundle, a
/// symlinked Dockerfile, a missing Dockerfile. A planner that returned a plan
/// for any of those would hand `docker` a path and surface Docker's error
/// instead of one naming the connector.
pub fn build_plan(
    bundle_root: &Path,
    bundle_name: &str,
    connector: &str,
    spec: &ConnectorSpecDecl,
    registry: Option<&str>,
    host_platform: &str,
    metadata_dir: &Path,
) -> Result<ConnectorBuildPlan> {
    let build = spec.build.as_ref().ok_or_else(|| {
        anyhow!("connectors.{connector}: declares no `build` block, so there is nothing to build")
    })?;
    check_build(connector, build)?;
    let context = resolve_context(bundle_root, &build.context)
        .with_context(|| format!("connectors.{connector}"))?;
    let dockerfile = resolve_dockerfile(&context, &build.dockerfile)
        .with_context(|| format!("connectors.{connector}"))?;
    let source_digest =
        source_digest_of(&context, build).with_context(|| format!("connectors.{connector}"))?;

    let (delivery, image_ref, metadata_file) = match registry {
        Some(registry) => (
            Delivery::Registry,
            registry_image_ref(registry, bundle_name, connector, &source_digest),
            Some(metadata_dir.join(format!("{connector}.metadata.json"))),
        ),
        None => (
            Delivery::LocalDaemon,
            local_build_tag(bundle_name, connector),
            None,
        ),
    };

    Ok(ConnectorBuildPlan {
        connector: connector.to_string(),
        context,
        dockerfile,
        platforms: build.platforms.clone(),
        host_platform: host_platform.to_string(),
        delivery,
        image_ref,
        source_digest,
        metadata_file,
    })
}

/// The `docker` command this plan runs.
///
/// Two delivery paths, two commands. `docker build` cannot emit a multi-arch
/// index, so the local-daemon path builds the host platform only; the declared
/// set stays in the lock so `cluster deploy` can still refuse it.
pub fn build_argv(plan: &ConnectorBuildPlan) -> crate::ops::OpsCommand {
    let mut args: Vec<String> = Vec::new();
    match plan.delivery {
        Delivery::LocalDaemon => {
            args.push("build".into());
            args.push("--platform".into());
            args.push(plan.host_platform.clone());
        }
        Delivery::Registry => {
            args.push("buildx".into());
            args.push("build".into());
            args.push("--platform".into());
            args.push(plan.platforms.join(","));
            args.push("--push".into());
            if let Some(metadata) = &plan.metadata_file {
                args.push("--metadata-file".into());
                args.push(metadata.display().to_string());
            }
        }
    }
    args.push("-f".into());
    args.push(plan.dockerfile.display().to_string());
    args.push("-t".into());
    args.push(plan.image_ref.clone());
    args.push(plan.context.display().to_string());
    plain_command("docker", args)
}

/// Read back the daemon's immutable image id for a tag it just built. The id is
/// what survives a later rebuild reusing the same tag.
pub fn image_inspect_argv(tag: &str) -> crate::ops::OpsCommand {
    plain_command(
        "docker",
        vec![
            "image".into(),
            "inspect".into(),
            "--format".into(),
            "{{.Id}}".into(),
            tag.to_string(),
        ],
    )
}

/// The push target for one connector: a repository per bundle+connector, tagged
/// with a prefix of the source digest.
///
/// Never a mutable name: `:latest` would let a second build silently replace the
/// bytes a first deploy resolved.
pub fn registry_image_ref(
    registry: &str,
    bundle: &str,
    connector: &str,
    source_digest: &str,
) -> String {
    let hex = source_digest
        .strip_prefix("sha256:")
        .unwrap_or(source_digest);
    let tag: String = hex.chars().take(TAG_DIGEST_LEN).collect();
    format!(
        "{}/{bundle}-{connector}:{tag}",
        registry.trim_end_matches('/')
    )
}

/// The index digest buildx reports through `--metadata-file`.
///
/// `containerimage.digest` is the INDEX digest; `containerimage.config.digest`
/// is a single-platform config blob and pulling by it fails on a multi-arch
/// node, so the key is named explicitly rather than pattern-matched.
pub fn digest_from_metadata(raw: &str) -> Result<String> {
    let parsed: serde_json::Value =
        serde_json::from_str(raw).context("parse the buildx metadata file")?;
    parsed
        .get("containerimage.digest")
        .and_then(|d| d.as_str())
        .map(str::to_string)
        .ok_or_else(|| {
            anyhow!(
                "the buildx metadata file names no `containerimage.digest`, so the push did not \
                 produce an image; recording an empty reference would surface much later as \
                 ImagePullBackOff"
            )
        })
}

/// The repository at a digest, with the push tag dropped: a reference exactly
/// one artifact can resolve.
pub fn digest_pinned_ref(image_ref: &str, digest: &str) -> String {
    let repo = match image_ref.split_once('@') {
        Some((repo, _)) => repo,
        None => {
            let tag_start = image_ref
                .rfind(':')
                .filter(|colon| image_ref[*colon..].find('/').is_none());
            match tag_start {
                Some(colon) => &image_ref[..colon],
                None => image_ref,
            }
        }
    };
    format!("{repo}@{digest}")
}

// ─── The lock write ──────────────────────────────────────────────────────────

/// Why this write must not proceed, or `None` when it may.
///
/// The one-directional rule: a local-daemon resolution must not silently
/// replace a registry one, because only the registry lock is deployable to a
/// cluster. The reverse is a promotion and needs no ceremony.
pub fn lock_overwrite_refusal(
    existing: Option<&ConnectorLockFileDecl>,
    next: &ConnectorLockFileDecl,
    force: bool,
) -> Option<String> {
    if force {
        return None;
    }
    let existing = existing?;
    let downgraded: Vec<&str> = next
        .connectors
        .iter()
        .filter(|(name, entry)| {
            entry.delivery == Delivery::LocalDaemon
                && existing
                    .connectors
                    .get(*name)
                    .is_some_and(|prior| prior.delivery == Delivery::Registry)
        })
        .map(|(name, _)| name.as_str())
        .collect();
    if downgraded.is_empty() {
        return None;
    }
    Some(format!(
        "{CONNECTOR_LOCK_FILE} already resolves {} to a pushed registry image, and this build \
         resolved it to the local daemon, which a cluster cannot pull. Re-run with --registry \
         <ref> to keep a pushed image, or with --force to replace the lock deliberately.",
        downgraded.join(", ")
    ))
}

/// Write the lock at the bundle root, atomically, refusing a silent downgrade.
///
/// Byte-stable for an unchanged resolution: nothing here stamps a timestamp and
/// every map is a `BTreeMap`, so "nothing changed" stays visible in
/// `git status` instead of showing a diff on every rebuild.
pub fn write_lock(bundle_root: &Path, lock: &ConnectorLockFileDecl, force: bool) -> Result<()> {
    let existing = load_lock(bundle_root).unwrap_or(None);
    if let Some(refusal) = lock_overwrite_refusal(existing.as_ref(), lock, force) {
        return Err(crate::exit::usage(refusal));
    }
    let path = bundle_root.join(CONNECTOR_LOCK_FILE);
    let body = serde_norway::to_string(lock).context("serialize the connector lock")?;
    let temp = path.with_extension("yaml.tmp");
    std::fs::write(&temp, body).with_context(|| format!("write {}", temp.display()))?;
    std::fs::rename(&temp, &path).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

// ─── The rendered container contract, shared by both local emitters ───────────

/// The deployment identity every derived name is built from.
///
/// Passed in, never re-derived: `--agent`/`--target` can override the
/// manifest's name, and a helper that read `plugin.json` again would alias the
/// connectors under one identity while the runner dialed another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorScope {
    pub release: String,
    pub agent: String,
    pub namespace: String,
}

/// Every name the sandbox might use to reach this connector, in the order
/// `connector_render.host_aliases` emits them.
fn host_aliases(scope: &ConnectorScope, connector: &str, port: u32) -> Vec<String> {
    let short = object_name(&scope.release, &scope.agent, connector);
    let dns = service_dns(&scope.release, &scope.agent, connector, &scope.namespace);
    vec![
        format!("{short}:{port}"),
        format!("{short}.{}:{port}", scope.namespace),
        format!("{dns}:{port}"),
    ]
}

/// What each `${CURIE_*}` placeholder expands to for this connector.
///
/// A port of `connector_render.substitutions`. Every value is one the bundle
/// author cannot know, because all of them derive from the Service name Curie
/// invents.
pub fn connector_substitutions(
    scope: &ConnectorScope,
    connector: &str,
    port: u32,
) -> BTreeMap<String, String> {
    let short = object_name(&scope.release, &scope.agent, connector);
    let dns = service_dns(&scope.release, &scope.agent, connector, &scope.namespace);
    BTreeMap::from([
        (
            "CURIE_ALLOWED_HOSTS".to_string(),
            host_aliases(scope, connector, port).join(","),
        ),
        ("CURIE_CONNECTOR_HOST".to_string(), short),
        ("CURIE_CONNECTOR_PORT".to_string(), port.to_string()),
        (
            "CURIE_CONNECTOR_URL".to_string(),
            format!("http://{dns}:{port}"),
        ),
    ])
}

/// Expand `${CURIE_*}` placeholders in one arg or env value.
pub fn substitute(value: &str, subs: &BTreeMap<String, String>) -> String {
    let mut out = value.to_string();
    for (key, replacement) in subs {
        out = out.replace(&format!("${{{key}}}"), replacement);
    }
    out
}

/// The connector scope the runner needs to derive the same URLs independently.
///
/// All three or none: the boot contract drops a partial set entirely, so
/// emitting two of three silently switches connectors off.
pub fn connector_scope_env(scope: &ConnectorScope) -> Vec<(String, String)> {
    vec![
        (
            curie_aci_protocol::env_keys::CURIE_CONNECTOR_RELEASE.to_string(),
            scope.release.clone(),
        ),
        (
            curie_aci_protocol::env_keys::CURIE_CONNECTOR_AGENT.to_string(),
            scope.agent.clone(),
        ),
        (
            curie_aci_protocol::env_keys::CURIE_CONNECTOR_NAMESPACE.to_string(),
            scope.namespace.clone(),
        ),
    ]
}

// ─── Staged credentials ──────────────────────────────────────────────────────

/// Where resolved connector credentials are staged for a local bring-up.
///
/// Deterministic, not a per-run tempdir: teardown is label-driven and a reap
/// that never inspected a container cannot discover a random path, so the
/// credential would survive every `down`. Under `.curie/`, which the packer
/// excludes and the scaffold gitignores.
pub fn connector_secrets_root(plugin_dir: &Path) -> PathBuf {
    plugin_dir.join(".curie").join("connector-secrets")
}

/// The host path one declared `secret_files` entry is staged at.
///
/// The filename carries a digest of the WHOLE declared path, not just its
/// basename: one connector may declare `/a/token` and `/b/token`, and a
/// basename-only name stages the second over the first. Producer
/// (`stage_secret_file`) and consumers (the `docker run` mounts, the compose
/// overlay) all derive the path here, so the naming is theirs to share.
///
/// The basename is reduced to one path-free component because the declared path
/// is the CONTAINER's: nothing constrains it to a shape the host can hold, and
/// `..` or a drive-letter component would otherwise be joined verbatim.
pub fn staged_secret_path(plugin_dir: &Path, connector: &str, declared_path: &str) -> PathBuf {
    let basename = declared_path
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(declared_path);
    let kept: String = basename
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(*c, '.' | '_' | '-'))
        .collect();
    // `..`, `.` and the empty basename all name a directory rather than a file.
    let stem = if kept.trim_matches('.').is_empty() {
        "secret"
    } else {
        kept.as_str()
    };
    let digest = hex(Sha256::digest(declared_path.as_bytes()).as_slice());
    connector_secrets_root(plugin_dir)
        .join(connector)
        .join(format!("{stem}-{}", &digest[..DIGEST_LEN]))
}

/// Write one resolved credential where the container will find it.
///
/// The parent directories are `0700` because the file mode is not always
/// enough: rootless Docker and Docker Desktop map uids differently, so the
/// `chown` to the container's uid is not always permitted, and where it is not
/// the file falls back to `0444` inside a private parent.
pub fn stage_secret_file(
    plugin_dir: &Path,
    connector: &str,
    declared_path: &str,
    value: &str,
) -> Result<PathBuf> {
    let path = staged_secret_path(plugin_dir, connector, declared_path);
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("staged credential path has no parent"))?;
    std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    private_dir(&connector_secrets_root(plugin_dir))?;
    private_dir(parent)?;
    // Replace, never write over: hardening left any previous staging 0400 (or
    // 0444 where the chown fell back), so a plain write onto it is EACCES and
    // a redeploy or credential rotation would wedge until the tree is removed
    // by hand.
    if path.exists() {
        std::fs::remove_file(&path)
            .with_context(|| format!("replace previously staged {}", path.display()))?;
    }
    std::fs::write(&path, value).with_context(|| format!("write {}", path.display()))?;
    harden_secret_file(&path)?;
    Ok(path)
}

#[cfg(unix)]
fn private_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .with_context(|| format!("restrict {}", path.display()))
}

#[cfg(not(unix))]
fn private_dir(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn harden_secret_file(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o400))
        .with_context(|| format!("restrict {}", path.display()))?;
    // Where the CLI may not hand the file to the container's uid, the file must
    // be world-readable instead -- the 0700 parent is what keeps it private.
    if std::os::unix::fs::chown(path, Some(CONNECTOR_UID), Some(CONNECTOR_UID)).is_err() {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o444))
            .with_context(|| format!("widen {} for the container's uid", path.display()))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn harden_secret_file(_path: &Path) -> Result<()> {
    Ok(())
}

/// Remove a staged credential tree. A no-op when it is already gone, so `down`
/// after `down` does not error.
///
/// Addressed by the root rather than the bundle directory because teardown
/// carries the root it recorded (`ConnectorTeardownStep::WipeSecrets`) while an
/// aborting boot carries only the plugin dir; one implementation serves both.
pub fn wipe_secrets_root(root: &Path) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }
    std::fs::remove_dir_all(root).with_context(|| format!("remove {}", root.display()))
}

/// The same wipe, addressed by the bundle directory a boot staged into.
pub fn wipe_connector_secrets(plugin_dir: &Path) -> Result<()> {
    wipe_secrets_root(&connector_secrets_root(plugin_dir))
}

// ─── The compose overlay (the local tier's emitter) ──────────────────────────

/// The compose network the worker puts a sandboxed runner on. Joined, never
/// created: a network of the overlay's own leaves the runner unable to resolve
/// the alias.
pub const RUNNER_NETWORK: &str = "curie_runner";

/// Where the generated overlay is written: ephemeral state under `.curie/`, so
/// it never enters the uploaded bundle and never perturbs the bundle digest.
pub fn compose_overlay_path(plugin_dir: &Path) -> PathBuf {
    plugin_dir.join(".curie").join("compose.connectors.yaml")
}

/// `docker compose ... up -d --wait` for the generated overlay, joined to the
/// running stack's project so the services land on its network.
///
/// The resolved secret values ride the compose child's environment as masked
/// `secret_env`, which is the ONLY channel that carries them: the overlay on
/// disk holds a `${NAME}` reference, and compose expands it from this
/// environment at parse time. Taking them here rather than at the call site
/// makes an up command that starts the overlay without its credentials
/// unrepresentable.
pub fn compose_up_command(
    overlay: &Path,
    project: &str,
    secret_values: &BTreeMap<String, String>,
) -> crate::ops::OpsCommand {
    plain_command(
        "docker",
        vec![
            "compose".into(),
            "-p".into(),
            project.to_string(),
            "-f".into(),
            overlay.display().to_string(),
            "up".into(),
            "-d".into(),
            "--wait".into(),
        ],
    )
    .with_secret_env(
        secret_values
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
    )
}

/// Whether a declared connector is one Curie runs (as opposed to one already
/// running somewhere else).
fn is_hosted(spec: &ConnectorSpecDecl) -> bool {
    spec.url.is_none() && spec.unhosted_url.is_none()
}

/// The image a hosted connector starts from: the locked digest when the bundle
/// BUILDS it, else the declared `image:`.
///
/// The `build:` check is the whole rule (ADR 0113): the lock records what a
/// declared build resolved to, so it is the identity of a `build:` connector and
/// of nothing else. Consulting it first -- which both emitters used to do --
/// lets an entry outlive the declaration that produced it: a connector switched
/// from `build:` to `image:` keeps running the last source build, and no output
/// anywhere says the declared image was ignored.
///
/// The one rule, called by both emitters, so the skill tier and the local tier
/// cannot answer this differently.
pub fn resolved_image(
    connector: &str,
    spec: &ConnectorSpecDecl,
    lock: &ConnectorLockFileDecl,
) -> Result<String> {
    if spec.build.is_some() {
        if let Some(entry) = lock.connectors.get(connector) {
            return Ok(entry.image.clone());
        }
    }
    spec.image.clone().ok_or_else(|| {
        // Usage (exit 2), not a generic failure: no retry of the same argv
        // clears it, and the fix is the build this bundle never ran.
        crate::exit::usage(format!(
            "connectors.{connector}: declares no `image` and {CONNECTOR_LOCK_FILE} has no entry \
             for it. Run `curie build --plugin-dir <dir>` first."
        ))
    })
}

/// The generated compose overlay: one service per hosted connector, the same
/// rendered contract `ConnectorStartSpec` emits with `docker run`.
///
/// No resolved secret value enters it. The overlay is serialized to a file under
/// `.curie/` that outlives the stack, so a declared secret is written as the
/// `${NAME}` compose reference and the value travels through
/// [`compose_up_command`]'s masked `secret_env` instead -- the same rule
/// `ConnectorStartSpec` follows with its bare `-e NAME`.
pub fn compose_overlay(
    lock: &ConnectorLockFileDecl,
    decl: &ConnectorsFileDecl,
    identity: &ConnectorScope,
    project: &str,
    plugin_dir: &Path,
) -> Result<serde_json::Value> {
    let mut services = serde_json::Map::new();
    for (connector, spec) in &decl.connectors {
        if !is_hosted(spec) {
            continue;
        }
        refuse_out_of_band_secrets(connector, spec)?;
        let image = resolved_image(connector, spec, lock)?;
        let subs = connector_substitutions(identity, connector, spec.port);
        let name = object_name(&identity.release, &identity.agent, connector);
        let alias = service_dns(
            &identity.release,
            &identity.agent,
            connector,
            &identity.namespace,
        );

        let mut environment = serde_json::Map::new();
        for (key, value) in &spec.env {
            environment.insert(
                key.clone(),
                serde_json::Value::String(substitute(value, &subs)),
            );
        }
        for name in declared_secret_names(spec) {
            // Compose has no secretKeyRef, so a declared secret is resolved
            // locally -- but only the REFERENCE is written here. Compose expands
            // `${NAME}` from its own process environment at parse time, so the
            // container receives the literal value while this file, which
            // survives `local down` at 0644, holds nothing but the name.
            // Unconditional: every declared name must be present in the injected
            // environment (`bring_up_local` refuses the bring-up otherwise), or
            // compose warns and expands it to empty.
            let reference = format!("${{{name}}}");
            environment.insert(name, serde_json::Value::String(reference));
        }

        let volumes: Vec<serde_json::Value> = spec
            .secret_files
            .values()
            .map(|declared_path| {
                serde_json::Value::String(format!(
                    "{}:{declared_path}:ro",
                    staged_secret_path(plugin_dir, connector, declared_path).display()
                ))
            })
            .collect();

        let mut service = serde_json::Map::new();
        service.insert("image".into(), serde_json::Value::String(image));
        if !spec.args.is_empty() {
            service.insert(
                "command".into(),
                serde_json::Value::Array(
                    spec.args
                        .iter()
                        .map(|arg| serde_json::Value::String(substitute(arg, &subs)))
                        .collect(),
                ),
            );
        }
        if !environment.is_empty() {
            service.insert("environment".into(), serde_json::Value::Object(environment));
        }
        if !volumes.is_empty() {
            service.insert("volumes".into(), serde_json::Value::Array(volumes));
        }
        let mut attachment = serde_json::Map::new();
        attachment.insert(
            RUNNER_NETWORK.to_string(),
            serde_json::json!({ "aliases": [alias] }),
        );
        service.insert("networks".into(), serde_json::Value::Object(attachment));
        let mut labels = serde_json::Map::new();
        labels.insert(
            "curietech.ai/component".into(),
            serde_json::Value::String("connector".into()),
        );
        labels.insert(
            "curietech.ai/project".into(),
            serde_json::Value::String(project.to_string()),
        );
        // Which agent's connector, and which one: the local tier shares one
        // compose project with the main stack and with every other agent, so
        // these two are the only thing a redeploy can reconcile against
        // (`docker::connectors_to_reap`). Emitted from the same pairs the skill
        // tier's `docker run` labels carry, so one reap selector covers both.
        for (key, value) in crate::docker::connector_identity_labels(&identity.agent, &name) {
            labels.insert(key, serde_json::Value::String(value));
        }
        service.insert("labels".into(), serde_json::Value::Object(labels));
        service.insert("read_only".into(), serde_json::Value::Bool(true));
        service.insert(
            "user".into(),
            serde_json::Value::String(format!("{CONNECTOR_UID}:{CONNECTOR_UID}")),
        );
        service.insert("cap_drop".into(), serde_json::json!(["ALL"]));
        service.insert(
            "security_opt".into(),
            serde_json::json!(["no-new-privileges"]),
        );
        services.insert(name, serde_json::Value::Object(service));
    }

    let mut networks = serde_json::Map::new();
    networks.insert(
        RUNNER_NETWORK.to_string(),
        serde_json::json!({ "external": true, "name": RUNNER_NETWORK }),
    );
    Ok(serde_json::json!({
        "services": services,
        "networks": serde_json::Value::Object(networks),
    }))
}

/// The env-var names a declaration's `secrets:` list asks to be forwarded.
pub fn declared_secret_names(spec: &ConnectorSpecDecl) -> Vec<String> {
    spec.secrets
        .iter()
        .map(|declared| match declared {
            SecretDecl::Name(name) => name.clone(),
            SecretDecl::Ref { name, .. } => name.clone(),
        })
        .collect()
}

/// Every secret NAME this bundle's HOSTED connectors need before bring-up.
///
/// A connector carrying `url:` or `unhosted_url:` is not started here -- its
/// secrets belong to the remote's client config and are expanded by the MCP
/// client, so demanding them would refuse a bundle that works. Both channels a
/// hosted connector resolves are covered: the `secrets:` list and the
/// `secret_files:` keys. Names only; no value ever travels through this seam.
pub fn hosted_secret_names(decl: &ConnectorsFileDecl) -> Vec<String> {
    let mut names = std::collections::BTreeSet::new();
    for spec in decl.connectors.values() {
        if spec.url.is_some() || spec.unhosted_url.is_some() {
            continue;
        }
        names.extend(declared_secret_names(spec));
        names.extend(spec.secret_files.keys().cloned());
    }
    names.into_iter().collect()
}

/// The secret name the derived `Authorization: Bearer ${NAME}` header expands.
///
/// Mirrors `plugin_format.ConnectorSpec.bearer_secret_name`: an explicit
/// `bearer_secret` wins; a single declared secret is that name; two or more
/// without an explicit name is None (the Python validator refuses that
/// document rather than picking `secrets[0]`).
pub fn bearer_secret_name(spec: &ConnectorSpecDecl) -> Option<String> {
    if let Some(name) = &spec.bearer_secret {
        return Some(name.clone());
    }
    let names = declared_secret_names(spec);
    if names.len() == 1 {
        Some(names[0].clone())
    } else {
        None
    }
}

/// The secret NAMES a hosted connector's SANDBOX must also receive (#2503, #2559).
///
/// These are the names the derived `Authorization: Bearer ${NAME}` header on
/// the mounted `.mcp.json` entry expands from: the expansion happens in the
/// sandbox where the MCP client runs, not in the connector pod, so a name that
/// only reaches the pod's `secretKeyRef` ships the literal `${NAME}` and the
/// server answers 401.
///
/// Scope, deliberately narrower than [`hosted_secret_names`]:
/// - A connector Curie itself runs -- one declaring `image:` or `build:` -- is
///   the only one whose credential this box resolves and delivers. A
///   `unhosted_url:` override still points at that same connector's image
///   declaration, so it stays in scope; a pure `url:` remote is the MCP
///   client's own config and is not.
/// - Only the Bearer name ([`bearer_secret_name`]), never every declared
///   secret. Extra names stay on the connector pod.
/// - `SecretDecl::Name` only. A `SecretDecl::Ref` names a Secret someone else
///   provisioned, which ADR-0090 keeps out of the deploy path entirely -- no
///   value for it exists here to bind.
/// - No `secret_files` and no `sealed_secrets`: those are file mounts and
///   out-of-band material, never a sandbox env var a header can expand.
///
/// Sorted and deduped. Names only; no value ever travels through this seam.
pub fn hosted_env_secret_names(decl: &ConnectorsFileDecl) -> Vec<String> {
    let mut names = std::collections::BTreeSet::new();
    for spec in decl.connectors.values() {
        if spec.image.is_none() && spec.build.is_none() {
            continue;
        }
        let Some(bearer) = bearer_secret_name(spec) else {
            continue;
        };
        if spec
            .secrets
            .iter()
            .any(|declared| matches!(declared, SecretDecl::Name(name) if *name == bearer))
        {
            names.insert(bearer);
        }
    }
    names.into_iter().collect()
}

/// The non-`CURIE_`-prefixed names a connector secret must never claim.
///
/// The twin of `_CREDENTIAL_KEYS | _REDIRECT_CAPTURE_KEYS` in
/// `packages/plugin-format/src/plugin_format/reserved_env.py`, which is the
/// policy's authority. This copy exists because the refusal has to fire on THIS
/// box, before a value is read out of the operator's environment or vault and
/// handed to a container the bundle controls, and no Python runs that early.
/// `apps/worker/tests/binding/test_reserved_boot_env_pin.py` is the drift pin
/// that fails CI if the two lists diverge -- the same mechanism that pins the
/// Helm list.
pub const RESERVED_CONNECTOR_SECRET_NAMES: [&str; 8] = [
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "ANTHROPIC_AUTH_TOKEN",
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "NODE_EXTRA_CA_CERTS",
    "ANTHROPIC_CUSTOM_HEADERS",
];

/// Whether `name` is reserved: an enumerated platform credential or
/// redirect/capture key, or anything in the `CURIE_` boot namespace. The prefix
/// rule is the forward-safe half, mirroring `is_reserved_boot_env_name`.
fn is_reserved_secret_name(name: &str) -> bool {
    name.starts_with("CURIE_") || RESERVED_CONNECTOR_SECRET_NAMES.contains(&name)
}

/// Refuse a hosted connector whose declared secret NAME it must not own.
///
/// Scoped to the hosted connectors for the same reason `hosted_secret_names` is:
/// those are the only declarations this box resolves a value for. A connector
/// declaring `ANTHROPIC_API_KEY` would otherwise have the operator's own model
/// credential read out of their environment or vault and injected into a
/// container the bundle chose the image for -- and at the skill and local tiers
/// that happens before the runner validates the bundle, so the plugin-format
/// fence never gets a turn. Names only: no value is read to decide this.
pub fn refuse_reserved_secret_names(decl: &ConnectorsFileDecl) -> Result<()> {
    for (connector, spec) in &decl.connectors {
        if spec.url.is_some() || spec.unhosted_url.is_some() {
            continue;
        }
        for name in declared_secret_names(spec)
            .into_iter()
            .chain(spec.secret_files.keys().cloned())
        {
            crate::secrets::validate_name(&name).map_err(|err| {
                crate::exit::usage(format!(
                    "connectors.{connector}: `{name}` is not a usable secret name: {err}"
                ))
            })?;
            if is_reserved_secret_name(&name) {
                return Err(crate::exit::usage(format!(
                    "connectors.{connector}: `{name}` is a reserved platform boot-env or \
                     model-credential key and cannot be a connector secret. Curie would resolve \
                     the operator's own value for it and hand it to this connector's container. \
                     Rename the variable the connector reads."
                )));
            }
        }
    }
    Ok(())
}

/// The one refusal both tiers issue when declared secrets have no value here.
///
/// Shared so the skill tier and the cluster deploy path cannot drift into two
/// near-identical strings. It names every gap at once rather than one per run,
/// and it names only NAMES -- a value must never reach an error, a log, or argv.
pub fn missing_secrets_error(missing: &[String]) -> anyhow::Error {
    crate::exit::usage(format!(
        "connectors.yaml declares secret(s) with no value available: {}. Export each in the \
         environment, or store it once with `curie secrets set <NAME>`.",
        missing.join(", ")
    ))
}

/// A `from_secret` reference points at a Secret someone else provisioned, and
/// nothing on a laptop corresponds to it. Starting without the credential would
/// give a server that 401s on every call, which reads as a broken connector
/// rather than a missing prerequisite, so bring-up fails closed.
pub fn refuse_out_of_band_secrets(connector: &str, spec: &ConnectorSpecDecl) -> Result<()> {
    for declared in &spec.secrets {
        if let SecretDecl::Ref {
            name, from_secret, ..
        } = declared
        {
            return Err(crate::exit::usage(format!(
                "connectors.{connector}: `{name}` references the out-of-band Secret \
                 `{from_secret}`, which has no local equivalent. Either store the value once \
                 with `curie secrets set {name}` and declare it as a plain `secrets:` entry, \
                 point the connector at something already running with `unhosted_url`, or run \
                 this connector at the cluster tier where the Secret exists."
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod hosted_env_secret_names_tests {
    use super::*;

    fn decl(document: &str) -> ConnectorsFileDecl {
        parse_connectors(document).expect("fixture connectors.yaml should parse")
    }

    #[test]
    fn image_connector_yields_bare_names_only() {
        // #2503: the sandbox must receive exactly the names the derived
        // `Bearer ${NAME}` header expands. A `SecretRef` names a Secret
        // somebody else provisioned, which ADR-0090 keeps out of this path,
        // and a `secret_files` entry is a file mount, never an env var.
        let decl = decl(
            r#"
connectors:
  gh:
    image: ghcr.io/example/gh:1
    secrets:
      - A
      - name: B
        from_secret: s
        key: k
    bearer_secret: A
    secret_files:
      C: /x
"#,
        );
        assert_eq!(hosted_env_secret_names(&decl), vec!["A".to_string()]);
    }

    #[test]
    fn remote_url_connector_is_excluded() {
        // A pure `url:` remote is the MCP client's own config; Curie resolves
        // and delivers no credential for it, so binding its name would put an
        // unrelated value in the sandbox env.
        let decl = decl(
            r#"
connectors:
  remote:
    url: https://mcp.example.com/sse
    secrets:
      - REMOTE_TOKEN
"#,
        );
        assert!(hosted_env_secret_names(&decl).is_empty());
    }

    #[test]
    fn unhosted_url_override_on_an_image_connector_stays_included() {
        // `unhosted_url:` only points at an already-running instance of the
        // SAME declared image, so the credential is still Curie's to deliver.
        let decl = decl(
            r#"
connectors:
  gh:
    image: ghcr.io/example/gh:1
    unhosted_url: http://127.0.0.1:9000/mcp
    secrets:
      - GITHUB_PERSONAL_ACCESS_TOKEN
"#,
        );
        assert_eq!(
            hosted_env_secret_names(&decl),
            vec!["GITHUB_PERSONAL_ACCESS_TOKEN".to_string()]
        );
    }

    #[test]
    fn two_hosted_connectors_sharing_a_name_dedupe_and_sort() {
        // The result keys a bind map, so a duplicate must collapse and the
        // order must be deterministic rather than connector-declaration order.
        let decl = decl(
            r#"
connectors:
  zeta:
    image: ghcr.io/example/zeta:1
    secrets:
      - SHARED
      - ZETA_ONLY
    bearer_secret: SHARED
  alpha:
    build:
      context: ./alpha
      platforms:
        - linux/amd64
    secrets:
      - SHARED
      - ALPHA_ONLY
    bearer_secret: SHARED
"#,
        );
        assert_eq!(hosted_env_secret_names(&decl), vec!["SHARED".to_string()]);
    }

    #[test]
    fn bundle_without_a_connectors_file_binds_nothing() {
        // The overwhelmingly common bundle. It must add no names at all, so
        // the #464 gate and the sandbox bind set are unchanged for it.
        let dir = tempfile::tempdir().expect("tempdir");
        let loaded = load(dir.path()).expect("a bundle with no connectors.yaml loads as empty");
        assert!(hosted_env_secret_names(&loaded).is_empty());
        assert!(hosted_env_secret_names(&ConnectorsFileDecl::default()).is_empty());
    }

    #[test]
    fn multiple_secrets_without_bearer_secret_bind_nothing() {
        // #2559: two bare names and no bearer_secret used to bind both, while
        // only secrets[0] was the Bearer. Bind nothing until the header name
        // is explicit; validate_connectors refuses this document on the
        // Python side.
        let decl = decl(
            r#"
connectors:
  gh:
    image: ghcr.io/example/gh:1
    secrets:
      - POD_ONLY
      - PAT
"#,
        );
        assert!(hosted_env_secret_names(&decl).is_empty());
    }

    #[test]
    fn bearer_secret_binds_only_that_name() {
        // Extra declared names stay on the connector pod (hosted_secret_names
        // / secretKeyRef). The sandbox only receives the header's name.
        let decl = decl(
            r#"
connectors:
  gh:
    image: ghcr.io/example/gh:1
    secrets:
      - POD_ONLY
      - PAT
    bearer_secret: PAT
"#,
        );
        assert_eq!(hosted_env_secret_names(&decl), vec!["PAT".to_string()]);
    }
}
