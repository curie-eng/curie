//! `curie <tier> info`: the resolved bundle plus discovery diagnostics (#1040).
//!
//! The product of this verb is one JSON object describing what the harness
//! resolved from a bundle, plus a `diagnostics` array naming every candidate the
//! pass looked at and did NOT register, where it looked, and why the candidate
//! did not count. An empty inventory and an unreadable one are different facts,
//! and this verb is the surface that keeps them different.
//!
//! ## One discovery pass over a source-agnostic view
//!
//! ```text
//! skill    -> BundleView::from_disk(&Path)                 \
//! local    -> BundleView::from_files(origin, files)         >-- discover() --> InfoReport
//! cluster  -> BundleView::from_files(origin, files)        /
//! ```
//!
//! [`discover`] is pure, synchronous and network-free: it reads nothing but the
//! [`BundleView`] handed to it (no filesystem, no env, no clock, no network).
//! That is what makes cross-tier parity a CHECKED property rather than a claim
//! (`cli/tests/info.rs`): the same bundle read from disk and read as a deployed
//! file list yields the same diagnostics, so a `diagnostics` entry means exactly
//! the same thing at every tier.
//!
//! Everything that is a fact about the INVOCATION rather than about the bundle
//! (the optional `--check-mcp` container probe, shell-env secret satisfaction,
//! `.curie/runner.json` runner state, which deployed tier asked) is layered on
//! by [`run`] after the pass, never inside it.
//!
//! ## A deployed view is a SUBSET of the bundle, and says so
//!
//! The platform serves a stored bundle's text files through an ALLOWLIST
//! (`apps/api`'s `_collect_text_files`): the two manifest locations,
//! `evals/cases.json`, and `skills/**/SKILL.md`. Nothing else is in it, so a
//! deployed view carries no `.mcp.json` no matter what the bundle contains. The
//! pass therefore never reads "absent from the view" as "absent from the
//! bundle" for a path the view could not have carried; it reports the weaker,
//! true claim (the file was never shown to this CLI) and sends the fact
//! unresolved. `artifacts[].exists` means "present in the file view THIS report
//! read", which is why the same row can be `false` here and `true` at `skill`.
//!
//! ## Two sentinels, never an omission and never an empty collection
//!
//! - [`Unavailable`] (`{available:false, reason, where}`) -- the concept has no
//!   meaning at this tier. The inversion cuts both ways: `bundle.deployed`,
//!   `channel` and `comms` are unavailable at `skill`, while `bundle.root`,
//!   `model` and `secrets.declared[].satisfied` are unavailable at the deployed
//!   tiers.
//! - [`Unresolved`] (`{resolved:false, reason}`) -- the concept exists here, but
//!   this bundle's state blocked resolving it. Always paired with a
//!   `diagnostics` entry carrying the machine code.
//!
//! A `null`, a missing key, or a `[]` where a rejection belongs is the exact lie
//! ADR-0041 exists to kill, so no field is ever omitted and no empty array is
//! ever emitted for an unresolved concept.
//!
//! ## A bundle defect is a diagnosis, not an exit code
//!
//! A missing `evals/cases.json`, a `skills/<dir>` with no conforming `SKILL.md`,
//! an unparseable manifest, an `approvalPolicy` the runner would refuse: each is
//! a `diagnostics` entry at exit 0. Only a genuinely unusable invocation (a
//! `--plugin-dir` that does not exist, or a directory holding no plugin
//! manifest at all) is a usage error at exit 2.
//!
//! ## Never print a secret value
//!
//! The payload sits next to declared secret NAMES, MCP `env`/`headers` blocks
//! and model credentials, and at the deployed tiers this CLI now holds a stored
//! bundle's file CONTENTS in memory. The report carries derived facts only:
//! names, counts, booleans and paths. MCP servers collapse to
//! [`commands::DeclaredServer`]'s already-reviewed four fields plus a `load`
//! status, and there is no `content` field anywhere in the contract.
//!
//! The inventory being content-free is only half of it: a `diagnostics` entry
//! describes a bundle DEFECT, and every upstream producer of that text renders
//! the offending value into its own message. So every diagnostic string is
//! built through [`crate::redact`] -- see that module for the rule and the reason.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::Result;
use curie_aci_protocol::env_keys;
use serde::Serialize;

use crate::commands::{self, AgentActionOpts};

/// The `info` report contract version, mirrored by `cli/schema/info.schema.json`'s
/// `$id` (`.../cli/info/v1.json`) and its `index.json` entry.
pub const INFO_REPORT_VERSION: u64 = 1;

/// The family marker, mirroring `CheckReport.check`.
const INFO_FAMILY: &str = "curie";

/// The manifest locations the runner's `load_approval_policy` probes, in order.
/// The same two-element list `commands::MANIFEST_LOCATIONS` names; kept here
/// because the shared pass probes a content view rather than a directory.
const MANIFEST_LOCATIONS: [&str; 2] = [".claude-plugin/plugin.json", "plugin.json"];

/// The bundle-root-relative path of the MCP declaration file.
const MCP_FILE: &str = ".mcp.json";
/// The bundle-root-relative path of the eval suite.
const EVAL_SUITE: &str = "evals/cases.json";
/// The bundle-root-relative skills tree.
const SKILLS_DIR: &str = "skills";

/// Frontmatter keys that LOOK like the tool grant and are silently ignored by
/// the loader. Mirrors `plugin_format`'s `_CONFUSABLE_TOOLS_KEYS`: a bundle
/// carrying one looks correct and behaves wrong, which is the class this verb
/// exists to surface.
const CONFUSABLE_TOOLS_KEYS: &[&str] = &["tools", "allowedTools", "allowed_tools"];

/// The key that actually grants tools.
const ALLOWED_TOOLS_KEY: &str = "allowed-tools";

// ---------------------------------------------------------------------------
// Sentinels
// ---------------------------------------------------------------------------

/// `{available:false, reason, where}` -- the concept does not exist at this
/// tier. `reason` says why it has no meaning here and claims only what is true;
/// `where` names the tier or command that DOES have it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Unavailable {
    /// Always `false`. A discriminator a consumer can branch on without knowing
    /// which field it is reading.
    pub available: bool,
    pub reason: String,
    #[serde(rename = "where")]
    pub where_it_lives: String,
}

impl Unavailable {
    fn new(reason: impl Into<String>, where_it_lives: impl Into<String>) -> Self {
        Unavailable {
            available: false,
            reason: reason.into(),
            where_it_lives: where_it_lives.into(),
        }
    }
}

/// `{resolved:false, reason}` -- the concept exists at this tier, but this
/// bundle's state blocked resolving it. Distinct from [`Unavailable`] on
/// purpose: reading "does not exist here" as "exists but is broken" sends an
/// operator down the wrong path, so the two never share a discriminator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Unresolved {
    /// Always `false`.
    pub resolved: bool,
    pub reason: String,
}

impl Unresolved {
    fn new(reason: impl Into<String>) -> Self {
        Unresolved {
            resolved: false,
            reason: reason.into(),
        }
    }
}

/// A fact that is either known, meaningless at this tier, or unresolvable from
/// this bundle. Untagged, so the JSON is exactly the value or exactly one of the
/// two sentinel objects and nothing wraps it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum Maybe<T> {
    Known(T),
    Unavailable(Unavailable),
    Unresolved(Unresolved),
}

impl<T> Maybe<T> {
    fn unavailable(reason: impl Into<String>, where_it_lives: impl Into<String>) -> Self {
        Maybe::Unavailable(Unavailable::new(reason, where_it_lives))
    }

    fn unresolved(reason: impl Into<String>) -> Self {
        Maybe::Unresolved(Unresolved::new(reason))
    }
}

/// Where the deployed-only facts live, quoted in every skill-tier `where`.
const WHERE_DEPLOYED: &str =
    "`curie local info <agent>` or `curie cluster info <agent>` for a deployed agent";
/// Where the disk-only facts live, quoted in every deployed-tier `where`.
const WHERE_DISK: &str = "`curie skill info --plugin-dir <dir>` against the bundle source";

// ---------------------------------------------------------------------------
// Diagnostics
// ---------------------------------------------------------------------------

/// The COARSE, CLOSED branch axis of a diagnostic. A consumer switches on this
/// and is guaranteed a total match at v1; `code` is the fine, OPEN axis whose
/// prefix always equals its `kind`. Splitting the two is what lets a new
/// rejection reason ship without touching a consumer.
///
/// Variants are declared in the same order their serialized names sort, so the
/// derived `Ord` agrees with the `(kind, candidate, code)` string ordering the
/// payload is emitted in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticKind {
    ApprovalGate,
    Artifact,
    BootEnv,
    Deployed,
    Evals,
    Manifest,
    Mcp,
    Secret,
    Skill,
    State,
}

impl DiagnosticKind {
    /// The serialized name, and the key the payload sorts on.
    pub fn as_str(self) -> &'static str {
        match self {
            DiagnosticKind::ApprovalGate => "approval_gate",
            DiagnosticKind::Artifact => "artifact",
            DiagnosticKind::BootEnv => "boot_env",
            DiagnosticKind::Deployed => "deployed",
            DiagnosticKind::Evals => "evals",
            DiagnosticKind::Manifest => "manifest",
            DiagnosticKind::Mcp => "mcp",
            DiagnosticKind::Secret => "secret",
            DiagnosticKind::Skill => "skill",
            DiagnosticKind::State => "state",
        }
    }
}

/// One candidate the pass looked at and did not register: what it looked for,
/// everywhere it looked, why the candidate did not count, and how to fix it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Diagnostic {
    /// The fine, OPEN axis. Its prefix always equals `kind` (the one documented
    /// exception is the `skills.*` family, which reports on the tree rather than
    /// on one skill and still carries `kind: "skill"`). An unknown `code` is "a
    /// rejection with no specific branch", never a parse failure.
    pub code: String,
    pub kind: DiagnosticKind,
    /// What was rejected, named the way a human would name it.
    pub candidate: String,
    /// The conforming thing that was being looked for.
    pub looked_for: String,
    /// Every path probed, bundle-root-relative so the two populators agree.
    pub looked_in: Vec<String>,
    pub reason: String,
    /// The way forward, or an explicit `null` when there is none. Never absent.
    pub fix: Option<String>,
}

/// Build one diagnostic. Grouped as a builder rather than seven positional
/// arguments so a call site reads as prose and cannot transpose two strings.
struct Diag {
    code: &'static str,
    kind: DiagnosticKind,
    candidate: String,
    looked_for: &'static str,
    looked_in: Vec<String>,
    reason: String,
    fix: Option<String>,
}

impl From<Diag> for Diagnostic {
    fn from(d: Diag) -> Self {
        Diagnostic {
            code: d.code.to_string(),
            kind: d.kind,
            candidate: d.candidate,
            looked_for: d.looked_for.to_string(),
            looked_in: d.looked_in,
            reason: d.reason,
            fix: d.fix,
        }
    }
}

/// Sort by `(kind, candidate, code)` so the payload diffs cleanly and a caller
/// can assert an exact array. `discover.rs` and `bundle.rs` both already sort
/// for the same reason.
fn sort_diagnostics(diagnostics: &mut [Diagnostic]) {
    diagnostics.sort_by(|a, b| {
        (a.kind.as_str(), &a.candidate, &a.code).cmp(&(b.kind.as_str(), &b.candidate, &b.code))
    });
}

// ---------------------------------------------------------------------------
// The report
// ---------------------------------------------------------------------------

/// One registered skill, as the loader would see it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkillRow {
    pub name: String,
    /// The `SKILL.md` that registered it, bundle-root-relative.
    pub path: String,
    pub description: Option<String>,
    pub allowed_tools: Vec<String>,
}

/// One declared MCP server. EXACTLY [`commands::DeclaredServer`]'s reviewed
/// field set plus `load`, and nothing more: `env`, `headers`, `args`, `url` and
/// `command` can each carry a literal token, so none of them has a field here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct McpRow {
    pub name: String,
    /// Where the declaration was read from (`.mcp.json` or the manifest).
    pub source: String,
    /// `stdio`, `http`, `sse`, or `unknown`.
    pub form: String,
    /// True when the server carries a credential block. A boolean, never the
    /// block.
    pub authed: bool,
    pub load: McpLoad,
}

/// Whether the declared server was proven to load. `not_probed` is STATED,
/// never implied by absence: the whole premise of #337 is that a load failure
/// must be visible rather than green-on-fake.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum McpLoad {
    NotProbed,
    Registered,
    RegisteredZeroTools,
    DidNotRegister,
    ProbeFailed,
}

/// One declared connector secret. NAMES only; there is no value field anywhere
/// in this contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SecretRow {
    pub name: String,
    /// A fact about THIS shell at the skill tier, and unavailable at the
    /// deployed tiers, where the platform owns the per-agent binding (ADR-0009).
    pub satisfied: Maybe<bool>,
    pub source: Maybe<String>,
}

/// The declared connector-secret policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SecretsBlock {
    pub declared: Vec<SecretRow>,
}

/// One boot-env key and who writes it. Names come from the generated
/// `curie_aci_protocol::env_keys` constants, never string literals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BootEnvRow {
    pub name: String,
    pub set_by_this_tier: Maybe<bool>,
    /// Always unavailable: this CLI cannot read a running container's
    /// environment at any tier.
    pub value_present: Maybe<bool>,
    pub note: Option<String>,
}

/// One armed approval gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GateRow {
    pub gate: String,
    pub route: String,
}

/// The bundle's own eval suite. Never a different bundle's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EvalsInfo {
    pub path: String,
    pub suite_name: String,
    pub case_count: u64,
}

/// The deployed agent's Slack channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChannelInfo {
    pub id: String,
}

/// Whether the deployed agent is wired to a real workspace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CommsInfo {
    pub connected: bool,
    pub detail: String,
}

/// The model credential the CLI would forward, BY NAME.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CredentialInfo {
    /// The env var NAME, never its value.
    pub name: Option<String>,
    /// `shell_env`, `curie_vault`, `env_file`, or `none`.
    pub source: String,
}

/// What a `skill up` from THIS shell would resolve, plus whatever runner is
/// actually recorded for the bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModelInfo {
    pub mode: String,
    pub model_id: Option<String>,
    pub base_url_override: bool,
    pub credential: CredentialInfo,
    pub recorded_runner: Maybe<RecordedRunner>,
    pub note: String,
}

/// The runner `skill up` recorded in `.curie/runner.json`. No secret and no
/// model id: `RunnerState` records neither.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecordedRunner {
    pub container_name: String,
    pub image: String,
    pub base_url: String,
    pub fake_model: bool,
    pub plugin_dir: String,
}

/// The deployed artifact this report describes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeployedInfo {
    pub agent: String,
    pub agent_id: String,
    pub version_id: String,
    pub version_label: String,
    pub environment: String,
    pub bundle_sha256: Option<String>,
}

/// The bundle's identity and where it was read from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BundleInfo {
    pub name: Option<String>,
    pub version: Option<String>,
    /// `disk` or `deployed_bundle`.
    pub source: String,
    /// The filesystem root, real at `skill` and unavailable at the deployed
    /// tiers (a stored bundle has no directory on this machine).
    pub root: Maybe<String>,
    pub manifest_path: Option<String>,
    pub manifest_location: Option<String>,
    pub deployed: Maybe<DeployedInfo>,
}

/// One expected file of the bundle shape, present or not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactRow {
    pub kind: String,
    pub path: String,
    pub exists: bool,
}

/// The whole product of `curie <tier> info`. Every field is `pub` so a test can
/// build one by hand, and `Serialize` is the only projection: `InfoOutput`'s
/// `to_json` delegates wholesale rather than hand-picking fields, which is what
/// keeps this family out of `cli/api-mirrors.json`'s `emits` array.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InfoReport {
    /// The family marker, literal `"curie"`.
    pub info: String,
    pub version: u64,
    /// `skill`, `local` or `cluster`. Unresolved from a bare [`discover`] over a
    /// deployed file view, which genuinely cannot tell `local` from `cluster`;
    /// [`run`] sets it from the invocation.
    pub tier: Maybe<String>,
    pub bundle: Maybe<BundleInfo>,
    pub skills: Maybe<Vec<SkillRow>>,
    pub mcp_servers: Maybe<Vec<McpRow>>,
    pub secrets: Maybe<SecretsBlock>,
    pub boot_env: Vec<BootEnvRow>,
    pub approval_gates: Maybe<Vec<GateRow>>,
    pub evals: Maybe<EvalsInfo>,
    pub channel: Maybe<ChannelInfo>,
    pub comms: Maybe<CommsInfo>,
    pub model: Maybe<ModelInfo>,
    pub artifacts: Vec<ArtifactRow>,
    /// Every candidate looked at and rejected. An EMPTY array is honest: zero
    /// rejections is a real answer, unlike an empty `mcp_servers` on an
    /// unreadable declaration.
    pub diagnostics: Vec<Diagnostic>,
}

// ---------------------------------------------------------------------------
// The view
// ---------------------------------------------------------------------------

/// Where a [`BundleView`]'s bytes came from. The one thing [`discover`] knows
/// about the tier, and the source of every inverted sentinel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BundleOrigin {
    /// A bundle directory on this machine.
    Disk { root: PathBuf },
    /// A version's stored files, read back over the platform API.
    Deployed {
        agent: String,
        agent_id: String,
        version_id: String,
        version_label: String,
        environment: String,
        bundle_sha256: Option<String>,
        channel: String,
    },
}

impl BundleOrigin {
    fn is_deployed(&self) -> bool {
        matches!(self, BundleOrigin::Deployed { .. })
    }
}

/// A source-agnostic `path -> content` view of a bundle's files. Keys are
/// normalized to bundle-root-relative, forward-slashed form on BOTH populators,
/// so `./skills/x/SKILL.md` from a stored bundle and `skills/x/SKILL.md` from a
/// disk walk land on the same key. Without that, the cross-source parity
/// property degenerates into a path-format assertion.
#[derive(Debug, Clone)]
pub struct BundleView {
    origin: BundleOrigin,
    files: BTreeMap<String, String>,
    /// Paths that are symlinks on disk. Always empty for a deployed view:
    /// `bundle::pack_tar_gz` refuses to pack a symlink rather than dereference
    /// it, so a stored bundle contains none by construction.
    symlinks: BTreeSet<String>,
    /// Why this bundle's `.curieignore` could not be applied, if it could not.
    /// A bundle-CONTENT defect, so it rides the view to [`discover`] and comes
    /// out as a diagnostic at exit 0 rather than failing the populator: a
    /// caller who must first learn WHAT is wrong cannot be handed an error
    /// instead of the report. Always `None` on a deployed view, which was
    /// packed through a clean ignore file by construction.
    ignore_defect: Option<crate::bundle::IgnoreDefect>,
}

impl BundleView {
    /// Populate from a bundle directory.
    ///
    /// The two exit-2 cases live here and nowhere else: a `--plugin-dir` that
    /// cannot be resolved, and a directory holding no plugin manifest at either
    /// accepted location. A directory with no manifest is not an incomplete
    /// bundle, it is not a bundle, matching `commands::check`'s treatment.
    pub fn from_disk(root: &Path) -> Result<BundleView> {
        let requested = root.display().to_string();
        let root = root.canonicalize().map_err(|err| {
            anyhow::Error::from(
                crate::exit::CliError::usage(format!("plugin dir not found: {requested}: {err}"))
                    .with_fix(
                        "point --plugin-dir at an existing bundle directory; `info` inspects a \
                     bundle DIRECTORY at the skill tier",
                    ),
            )
        })?;
        if !root.is_dir() {
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(format!("plugin dir is not a directory: {requested}"))
                    .with_fix("point --plugin-dir at a bundle directory, not a file"),
            ));
        }
        // The packer's own exclusion set, not a copy of one of its inputs: the
        // built-in names PLUS whatever this bundle's `.curieignore` declares.
        // Reading the ignore file here is what keeps the disk view describing
        // the same bundle `pack_tar_gz` would ship. A `.curieignore` the packer
        // would refuse comes back as a defect rather than an error, and is
        // reported as a diagnostic below; the walk falls back to the built-in
        // names, exactly what the packer applies before reading any ignore file.
        let (exclusions, ignore_defect) = crate::bundle::Exclusions::resolve(&root)?;
        let mut files = BTreeMap::new();
        let mut symlinks = BTreeSet::new();
        walk_disk(&root, &root, &exclusions, &mut files, &mut symlinks)?;
        if !MANIFEST_LOCATIONS
            .iter()
            .any(|loc| files.contains_key(*loc))
        {
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(crate::scaffold::no_manifest_message(&root)).with_fix(
                    "run `curie init <name>` to scaffold a new bundle, or \
                     `curie init --adopt <dir>` to adopt an existing directory",
                ),
            ));
        }
        Ok(BundleView {
            origin: BundleOrigin::Disk { root },
            files,
            symlinks,
            ignore_defect,
        })
    }

    /// Populate from a deployed bundle's stored files: exactly the
    /// `(path, content)` shape `crate::api::BundleFile` yields.
    ///
    /// Never fails: an empty or manifest-less file set is a platform-side gap
    /// reported as a `deployed.*` diagnosis at exit 0, NOT the skill tier's
    /// exit-2 no-manifest path, which is a filesystem-only outcome and would
    /// misreport a platform gap as a bad `--plugin-dir`.
    pub fn from_files(origin: BundleOrigin, files: Vec<(String, String)>) -> BundleView {
        let mut map = BTreeMap::new();
        for (path, content) in files {
            let key = normalize_path(&path);
            if key.is_empty() || excluded(&key) {
                continue;
            }
            map.insert(key, content);
        }
        BundleView {
            origin,
            files: map,
            symlinks: BTreeSet::new(),
            ignore_defect: None,
        }
    }

    /// Where this view's bytes came from.
    pub fn origin(&self) -> &BundleOrigin {
        &self.origin
    }

    /// The content at a bundle-root-relative path, if the view carries it.
    pub fn file(&self, path: &str) -> Option<&str> {
        self.files.get(path).map(String::as_str)
    }

    /// Every path in the view, sorted.
    pub fn paths(&self) -> impl Iterator<Item = &str> {
        self.files.keys().map(String::as_str)
    }
}

/// Normalize one path to a bundle-root-relative, forward-slashed view key.
fn normalize_path(raw: &str) -> String {
    let mut s = raw.replace('\\', "/");
    loop {
        let trimmed = s.trim_start_matches('/').to_string();
        if let Some(rest) = trimmed.strip_prefix("./") {
            s = rest.to_string();
        } else {
            s = trimmed;
            break;
        }
    }
    s
}

/// Would the packer have dropped `key`? A deployed view has no directory to
/// read a `.curieignore` from, and its bytes were already packed through one,
/// so only the built-in names remain to re-assert. Asks
/// `bundle::Exclusions` rather than mirroring its name list.
fn excluded(key: &str) -> bool {
    crate::bundle::Exclusions::builtin().excludes_any_ancestor(Path::new(key))
}

fn walk_disk(
    root: &Path,
    dir: &Path,
    exclusions: &crate::bundle::Exclusions,
    files: &mut BTreeMap<String, String>,
    symlinks: &mut BTreeSet<String>,
) -> Result<()> {
    let entries = std::fs::read_dir(dir)
        .map_err(|err| anyhow::anyhow!("reading {}: {err}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|err| anyhow::anyhow!("reading {}: {err}", dir.display()))?;
        let path = entry.path();
        let Ok(rel) = path.strip_prefix(root) else {
            continue;
        };
        // Ordered exactly as `bundle::append_dir` orders it: exclusion runs
        // before the symlink check, so an excluded entry that is itself a link
        // is skipped rather than recorded. Matching on `rel` (not the bare
        // name) is what lets a `.curieignore` path pattern land.
        if exclusions.is_excluded(rel) {
            continue;
        }
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        let key = normalize_path(&rel.to_string_lossy());
        if kind.is_symlink() {
            // Recorded, never dereferenced: `pack_tar_gz` refuses to pack it, so
            // a symlinked file deploys as nothing.
            symlinks.insert(key);
            continue;
        }
        if kind.is_dir() {
            walk_disk(root, &path, exclusions, files, symlinks)?;
        } else if let Ok(content) = std::fs::read_to_string(&path) {
            // A non-UTF-8 file is skipped exactly as the stored-bundle read
            // does; the deployed `BundleFile.content` is a String.
            files.insert(key, content);
        }
    }
    Ok(())
}

/// Report a `.curieignore` the packer would refuse.
///
/// Not a sentinel case: every row this report carries is still a true statement
/// about the directory on disk, so blanking the inventory to `unresolved` would
/// withhold facts the pass genuinely resolved. What is NOT true of such a bundle
/// is that it corresponds to anything the platform could store, and that is
/// exactly what this diagnostic says. It names the defect and its location only;
/// no byte of the ignore file reaches the payload (see [`crate::redact`]).
fn ignore_file_diagnostic(defect: crate::bundle::IgnoreDefect) -> Diagnostic {
    match defect {
        crate::bundle::IgnoreDefect::Symlink => Diag {
            code: "artifact.symlink",
            kind: DiagnosticKind::Artifact,
            candidate: crate::bundle::IGNORE_FILE.to_string(),
            looked_for: "a regular .curieignore file at the bundle root, declaring this bundle's \
                 own packing exclusions",
            looked_in: vec![crate::bundle::IGNORE_FILE.to_string()],
            reason: "this .curieignore is a symlink, and `bundle::pack_tar_gz` refuses to pack a \
                 bundle whose ignore file is a link out of the bundle root, so the inventory \
                 below was built from the built-in exclusions alone and describes a directory \
                 the platform cannot store"
                .to_string(),
            fix: Some(
                "replace the symlink with a regular .curieignore file inside the bundle, or \
                 delete it"
                    .to_string(),
            ),
        }
        .into(),
    }
}

// ---------------------------------------------------------------------------
// The pass
// ---------------------------------------------------------------------------

/// The one discovery pass. Pure, synchronous and network-free: it reads nothing
/// but `view`. Every tier runs THIS function, which is what makes a
/// `diagnostics` entry mean the same thing everywhere.
pub fn discover(view: &BundleView) -> InfoReport {
    let mut diagnostics: Vec<Diagnostic> = Vec::new();

    if let Some(defect) = view.ignore_defect {
        diagnostics.push(ignore_file_diagnostic(defect));
    }

    let manifest = resolve_manifest(view, &mut diagnostics);
    let bundle = build_bundle(view, manifest.as_ref(), &mut diagnostics);

    let skills = discover_skills(view, &mut diagnostics);
    let mcp_servers = discover_mcp(view, manifest.as_ref(), &mut diagnostics);
    let secrets = discover_secrets(view, manifest.as_ref(), &mut diagnostics);
    let approval_gates = discover_gates(manifest.as_ref(), &mut diagnostics);
    let evals = discover_evals(view, &mut diagnostics);

    // The `boot_env` rows are a SUBSET of the declared contract at every tier,
    // so the boundary is stated on every report rather than left to be inferred
    // from an absent row.
    diagnostics.push(boot_env_scope_diagnostic());

    let mut report = InfoReport {
        info: INFO_FAMILY.to_string(),
        version: INFO_REPORT_VERSION,
        tier: tier_for(view.origin()),
        bundle,
        skills,
        mcp_servers,
        secrets,
        boot_env: boot_env_rows(view.origin()),
        approval_gates,
        evals,
        channel: channel_for(view.origin()),
        comms: comms_for(view.origin()),
        model: model_for(view.origin()),
        artifacts: artifact_rows(view),
        diagnostics,
    };
    sort_diagnostics(&mut report.diagnostics);
    report
}

/// The manifest a pass resolved: its location, its raw body, and its parsed
/// value when the body was valid JSON.
struct ResolvedManifest {
    location: &'static str,
    body: String,
    value: Option<serde_json::Value>,
}

fn resolve_manifest(
    view: &BundleView,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<ResolvedManifest> {
    let (location, body) = MANIFEST_LOCATIONS
        .iter()
        .find_map(|loc| view.file(loc).map(|body| (*loc, body.to_string())))?;

    if location != MANIFEST_LOCATIONS[0] {
        diagnostics.push(
            Diag {
                code: "manifest.location_fallback",
                kind: DiagnosticKind::Manifest,
                candidate: MANIFEST_LOCATIONS[0].to_string(),
                looked_for: "the preferred manifest location",
                looked_in: MANIFEST_LOCATIONS
                    .iter()
                    .map(|l| (*l).to_string())
                    .collect(),
                reason: format!(
                    "the preferred location {} was probed first and not found; the manifest was \
                     read from the accepted fallback {location}",
                    MANIFEST_LOCATIONS[0]
                ),
                fix: Some(format!(
                    "move the manifest to {} so it sits where the plugin format documents it",
                    MANIFEST_LOCATIONS[0]
                )),
            }
            .into(),
        );
    }

    let value = match serde_json::from_str::<serde_json::Value>(&body) {
        Ok(value) => Some(value),
        Err(err) => {
            diagnostics.push(
                Diag {
                    code: "manifest.invalid_json",
                    kind: DiagnosticKind::Manifest,
                    candidate: location.to_string(),
                    looked_for: "a plugin manifest that parses as JSON",
                    looked_in: vec![location.to_string()],
                    reason: format!(
                        "{location} is present but is not valid JSON ({}); the runner's \
                         manifest read fails on it, so every fact the manifest is the sole \
                         source of is unresolved rather than empty",
                        crate::redact::json_syntax(&err)
                    ),
                    fix: Some(format!("fix the JSON syntax in {location}")),
                }
                .into(),
            );
            None
        }
    };

    if let Some(parsed) = value.as_ref() {
        let name_ok = parsed
            .get("name")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|n| !n.trim().is_empty());
        if !name_ok {
            diagnostics.push(
                Diag {
                    code: "manifest.name_invalid",
                    kind: DiagnosticKind::Manifest,
                    candidate: location.to_string(),
                    looked_for: "a non-empty string `name`",
                    looked_in: vec![location.to_string()],
                    reason: "the manifest declares no usable `name`; the runner's manifest parse \
                         requires one and refuses the bundle without it"
                        .to_string(),
                    fix: Some("add a non-empty string `name` to the manifest".to_string()),
                }
                .into(),
            );
        }
    }

    Some(ResolvedManifest {
        location,
        body,
        value,
    })
}

fn build_bundle(
    view: &BundleView,
    manifest: Option<&ResolvedManifest>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Maybe<BundleInfo> {
    let Some(manifest) = manifest else {
        // Only reachable from a deployed view: `from_disk` refuses a directory
        // with no manifest before a pass ever runs.
        if view.origin().is_deployed() {
            diagnostics.push(
                Diag {
                    code: "deployed.bundle_unreadable",
                    kind: DiagnosticKind::Deployed,
                    candidate: "the in-force version's stored files".to_string(),
                    looked_for: "a plugin manifest in the stored bundle",
                    looked_in: MANIFEST_LOCATIONS
                        .iter()
                        .map(|l| (*l).to_string())
                        .collect(),
                    reason: "the in-force version's stored file set carries no plugin manifest at \
                         either accepted location, so the deployed bundle's identity cannot be \
                         resolved; this is a platform-side gap, not a defect in a local directory"
                        .to_string(),
                    fix: Some(
                        "re-run `curie <tier> deploy` from the bundle source so the version's \
                         stored files carry its manifest"
                            .to_string(),
                    ),
                }
                .into(),
            );
        }
        return Maybe::unresolved(
            "no plugin manifest was readable in this bundle, so its name, version and \
             manifest location are unknown",
        );
    };

    let name = manifest
        .value
        .as_ref()
        .and_then(|v| v.get("name"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let version = manifest
        .value
        .as_ref()
        .and_then(|v| v.get("version"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);

    let (source, root, deployed) = match view.origin() {
        BundleOrigin::Disk { root } => (
            "disk",
            Maybe::Known(root.display().to_string()),
            Maybe::unavailable(commands::VERSIONS_REASON, WHERE_DEPLOYED),
        ),
        BundleOrigin::Deployed {
            agent,
            agent_id,
            version_id,
            version_label,
            environment,
            bundle_sha256,
            ..
        } => (
            "deployed_bundle",
            Maybe::unavailable(
                "a deployed bundle is stored in the platform as files and has no directory on \
                 this machine",
                WHERE_DISK,
            ),
            Maybe::Known(DeployedInfo {
                agent: agent.clone(),
                agent_id: agent_id.clone(),
                version_id: version_id.clone(),
                version_label: version_label.clone(),
                environment: environment.clone(),
                bundle_sha256: bundle_sha256.clone(),
            }),
        ),
    };

    Maybe::Known(BundleInfo {
        name,
        version,
        source: source.to_string(),
        root,
        manifest_path: Some(manifest.location.to_string()),
        manifest_location: Some(manifest.location.to_string()),
        deployed,
    })
}

// --- skills ---------------------------------------------------------------

fn discover_skills(view: &BundleView, diagnostics: &mut Vec<Diagnostic>) -> Maybe<Vec<SkillRow>> {
    let prefix = format!("{SKILLS_DIR}/");
    let under: Vec<&str> = view
        .paths()
        .filter(|p| p.starts_with(prefix.as_str()))
        .collect();

    // No second clause on `paths()`: a view key is only ever a FILE, from either
    // populator, so "is `skills` itself a key" is dead by construction.
    if under.is_empty() {
        diagnostics.push(
            Diag {
                code: "skills.dir_absent",
                kind: DiagnosticKind::Skill,
                candidate: SKILLS_DIR.to_string(),
                looked_for: "a skills/ tree carrying at least one SKILL.md",
                looked_in: vec![SKILLS_DIR.to_string()],
                reason: "this bundle carries no skills/ tree holding any FILE, so it registers no \
                     skill; the plugin-format validator returns silently in this case, which \
                     makes this the only surface that says so. An entirely empty skills/<dir>/ \
                     is invisible to this pass from a disk walk and from a stored bundle alike \
                     -- neither records directory entries, only files -- so this says nothing \
                     about empty directories either way"
                    .to_string(),
                fix: Some(
                    "add skills/<name>/SKILL.md, or run `curie init <name>` to scaffold one"
                        .to_string(),
                ),
            }
            .into(),
        );
        return Maybe::Known(Vec::new());
    }

    // Immediate children of skills/ are the candidates a reader thinks of as
    // "a skill". Deeper directories are a registered skill's own supporting
    // files (references/, scripts/), so rejecting them would bury the real
    // finding in noise.
    let mut candidates: BTreeSet<&str> = BTreeSet::new();
    for path in &under {
        let rest = &path[prefix.len()..];
        if let Some((first, _)) = rest.split_once('/') {
            candidates.insert(first);
        }
    }

    let mut rows: Vec<SkillRow> = Vec::new();
    for candidate in candidates {
        let dir = format!("{prefix}{candidate}");
        // `plugin_format._validate_skills` uses rglob("SKILL.md"), so a nested
        // skills/a/b/SKILL.md registers exactly as a flat one does.
        let mut found: Vec<&str> = under
            .iter()
            .copied()
            .filter(|p| p.starts_with(&format!("{dir}/")) && p.ends_with("/SKILL.md"))
            .collect();
        found.sort_unstable();

        if found.is_empty() {
            diagnostics.push(
                Diag {
                    code: "skill.no_skill_md",
                    kind: DiagnosticKind::Skill,
                    candidate: dir.clone(),
                    looked_for: "SKILL.md",
                    looked_in: vec![format!("{dir}/SKILL.md")],
                    reason: format!(
                        "{dir} carries no file named SKILL.md at any depth, so the loader \
                         registers nothing from it; the directory is looked at and rejected \
                         rather than silently dropped from the inventory"
                    ),
                    fix: Some(format!(
                        "rename the skill file to {dir}/SKILL.md (the name is case- and \
                         spelling-exact)"
                    )),
                }
                .into(),
            );
            continue;
        }

        for path in found {
            if view.symlinks.contains(path) {
                diagnostics.push(
                    Diag {
                        code: "skill.symlink",
                        kind: DiagnosticKind::Skill,
                        candidate: path.to_string(),
                        looked_for: "a regular SKILL.md file",
                        looked_in: vec![path.to_string()],
                        reason: "this SKILL.md is a symlink, and `bundle::pack_tar_gz` refuses to \
                             pack a symlink rather than dereference it, so the skill deploys as \
                             nothing while looking correct here"
                            .to_string(),
                        fix: Some(
                            "replace the symlink with the real file inside the bundle".to_string(),
                        ),
                    }
                    .into(),
                );
                continue;
            }
            let body = view.file(path).unwrap_or_default();
            match parse_frontmatter(body) {
                FrontmatterOutcome::Missing => diagnostics.push(
                    Diag {
                        code: "skill.frontmatter_missing",
                        kind: DiagnosticKind::Skill,
                        candidate: dir.clone(),
                        looked_for: "a leading `---` YAML frontmatter block",
                        looked_in: vec![path.to_string()],
                        reason: format!(
                            "{path} opens with no `---` frontmatter block, so the loader reads \
                             no name, description or tool grant from it and the skill does not \
                             register"
                        ),
                        fix: Some(
                            "add a frontmatter block declaring `name:`, `description:` and \
                             `allowed-tools:`"
                                .to_string(),
                        ),
                    }
                    .into(),
                ),
                FrontmatterOutcome::Invalid(detail) => diagnostics.push(
                    Diag {
                        code: "skill.frontmatter_invalid",
                        kind: DiagnosticKind::Skill,
                        candidate: dir.clone(),
                        looked_for: "a closed frontmatter block declaring `name:`",
                        looked_in: vec![path.to_string()],
                        reason: format!(
                            "{path} has a frontmatter block this pass cannot use: {detail}"
                        ),
                        fix: Some(
                            "close the `---` block and declare a non-empty `name:`".to_string(),
                        ),
                    }
                    .into(),
                ),
                FrontmatterOutcome::Parsed(fm) => {
                    for key in &fm.confusable_keys {
                        diagnostics.push(
                            Diag {
                                code: "skill.tools_confusable",
                                kind: DiagnosticKind::Skill,
                                candidate: path.to_string(),
                                looked_for: ALLOWED_TOOLS_KEY,
                                looked_in: vec![path.to_string()],
                                reason: format!(
                                    "the frontmatter declares `{key}:`, which looks like the tool \
                                     grant and is silently ignored; only `{ALLOWED_TOOLS_KEY}:` \
                                     grants tools, so this skill registers with a different tool \
                                     set than it appears to ask for"
                                ),
                                fix: Some(format!("rename `{key}:` to `{ALLOWED_TOOLS_KEY}:`")),
                            }
                            .into(),
                        );
                    }
                    rows.push(SkillRow {
                        name: fm.name,
                        path: path.to_string(),
                        description: fm.description,
                        allowed_tools: fm.allowed_tools,
                    });
                }
            }
        }
    }

    if rows.is_empty() && !under.is_empty() && !under.iter().any(|p| p.ends_with("/SKILL.md")) {
        diagnostics.push(
            Diag {
                code: "skills.empty",
                kind: DiagnosticKind::Skill,
                candidate: SKILLS_DIR.to_string(),
                looked_for: "at least one SKILL.md anywhere under skills/",
                looked_in: vec![format!("{prefix}**/SKILL.md")],
                reason:
                    "the skills/ tree exists but carries no SKILL.md at any depth, so the bundle \
                     registers no skill at all"
                        .to_string(),
                fix: Some("add skills/<name>/SKILL.md with a `---` frontmatter block".to_string()),
            }
            .into(),
        );
    }

    rows.sort_by(|a, b| a.path.cmp(&b.path));
    Maybe::Known(rows)
}

/// What a `SKILL.md`'s frontmatter yielded.
enum FrontmatterOutcome {
    /// No leading `---` block at all.
    Missing,
    /// A block that opens but cannot be used, with the detail.
    Invalid(String),
    Parsed(Frontmatter),
}

struct Frontmatter {
    name: String,
    description: Option<String>,
    allowed_tools: Vec<String>,
    confusable_keys: Vec<String>,
}

/// Read a `SKILL.md`'s leading frontmatter block. A deliberately small reader
/// over the flat `key: value` / `  - item` shape the scaffold emits and the
/// loader accepts: it never needs a YAML engine, and it reports what it could
/// not use rather than guessing.
fn parse_frontmatter(body: &str) -> FrontmatterOutcome {
    let mut lines = body.lines();
    match lines.next() {
        Some(first) if first.trim_end() == "---" => {}
        _ => return FrontmatterOutcome::Missing,
    }

    let mut closed = false;
    let mut current_key: Option<String> = None;
    let mut scalars: BTreeMap<String, String> = BTreeMap::new();
    let mut lists: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut key_order: Vec<String> = Vec::new();

    for line in lines {
        if line.trim_end() == "---" {
            closed = true;
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(item) = trimmed.strip_prefix("- ") {
            if let Some(key) = current_key.as_ref() {
                lists
                    .entry(key.clone())
                    .or_default()
                    .push(unquote(item.trim()).to_string());
            }
            continue;
        }
        let Some((key, value)) = trimmed.split_once(':') else {
            continue;
        };
        let key = key.trim().to_string();
        let value = value.trim();
        if !key_order.contains(&key) {
            key_order.push(key.clone());
        }
        if value.is_empty() {
            current_key = Some(key.clone());
            lists.entry(key).or_default();
        } else {
            current_key = None;
            // An inline flow list (`allowed-tools: [A, B]`) is the other shape
            // the loader accepts.
            if let Some(inner) = value.strip_prefix('[').and_then(|v| v.strip_suffix(']')) {
                lists.insert(
                    key,
                    inner
                        .split(',')
                        .map(|item| unquote(item.trim()).to_string())
                        .filter(|item| !item.is_empty())
                        .collect(),
                );
            } else {
                scalars.insert(key, unquote(value).to_string());
            }
        }
    }

    if !closed {
        return FrontmatterOutcome::Invalid(
            "the `---` block opens but is never closed".to_string(),
        );
    }
    let Some(name) = scalars.get("name").filter(|n| !n.trim().is_empty()) else {
        return FrontmatterOutcome::Invalid("the block declares no non-empty `name:`".to_string());
    };

    let confusable_keys: Vec<String> = key_order
        .iter()
        .filter(|k| CONFUSABLE_TOOLS_KEYS.contains(&k.as_str()))
        .cloned()
        .collect();

    FrontmatterOutcome::Parsed(Frontmatter {
        name: name.clone(),
        description: scalars.get("description").cloned(),
        allowed_tools: lists.get(ALLOWED_TOOLS_KEY).cloned().unwrap_or_default(),
        confusable_keys,
    })
}

fn unquote(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .or_else(|| value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
        .unwrap_or(value)
}

// --- mcp ------------------------------------------------------------------

fn discover_mcp(
    view: &BundleView,
    manifest: Option<&ResolvedManifest>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Maybe<Vec<McpRow>> {
    // `.mcp.json` first: it is the file the loader reads, and a manifest
    // declaration is the fallback shape.
    if let Some(body) = view.file(MCP_FILE) {
        let value: serde_json::Value = match serde_json::from_str(body) {
            Ok(value) => value,
            Err(err) => {
                diagnostics.push(
                    Diag {
                        code: "mcp.invalid_json",
                        kind: DiagnosticKind::Mcp,
                        candidate: MCP_FILE.to_string(),
                        looked_for: "an MCP declaration that parses as JSON",
                        looked_in: vec![MCP_FILE.to_string()],
                        reason: format!(
                            "{MCP_FILE} is present but is not valid JSON ({}); the declared \
                             servers cannot be resolved, which is NOT the same fact as declaring \
                             none",
                            crate::redact::json_syntax(&err)
                        ),
                        fix: Some(format!("fix the JSON syntax in {MCP_FILE}")),
                    }
                    .into(),
                );
                return Maybe::unresolved(format!(
                    "{MCP_FILE} is present but is not valid JSON, so the declared servers are \
                     unknown"
                ));
            }
        };
        return mcp_from_declaration(&value, MCP_FILE, MCP_FILE, diagnostics);
    }

    if let Some(value) = manifest.and_then(|m| m.value.as_ref()) {
        if value.get("mcpServers").is_some() {
            let location = manifest
                .map(|m| m.location)
                .unwrap_or(MANIFEST_LOCATIONS[0]);
            return mcp_from_declaration(value, "manifest", location, diagnostics);
        }
    }

    // A manifest that is present but did not parse is the same unknown
    // `discover_secrets` already reports: it is the only remaining source of an
    // `mcpServers` declaration, and it could not be read. Answering `[]` here
    // would assert the manifest names no server, which nothing established.
    if let Some(unparsed) = manifest.filter(|m| m.value.is_none()) {
        let reason = format!(
            "there is no readable {MCP_FILE} in this bundle view, so {} is the only remaining \
             source of an `mcpServers` declaration, and it is not valid JSON. Whether this \
             bundle declares any server is unknown, which is NOT the same fact as declaring none",
            unparsed.location
        );
        diagnostics.push(
            Diag {
                code: "mcp.manifest_unreadable",
                kind: DiagnosticKind::Mcp,
                candidate: unparsed.location.to_string(),
                looked_for: "an `mcpServers` declaration in the only remaining source",
                looked_in: vec![
                    MCP_FILE.to_string(),
                    format!("{}#/mcpServers", unparsed.location),
                ],
                reason: reason.clone(),
                fix: Some(format!("fix the JSON syntax in {}", unparsed.location)),
            }
            .into(),
        );
        return Maybe::unresolved(reason);
    }

    // A deployed view is an allowlist that has never carried `.mcp.json` (see
    // the module doc). "Not in the view" is all this CLI was shown, so it says
    // exactly that rather than `mcp.no_declaration`'s "there is no .mcp.json".
    if view.origin().is_deployed() {
        let reason = format!(
            "a deployed bundle is read through the platform's stored-TEXT-FILE view, which \
             carries the plugin manifest, {EVAL_SUITE} and {SKILLS_DIR}/**/SKILL.md and nothing \
             else, so {MCP_FILE} was never shown to this CLI whether or not the bundle ships \
             one. No `mcpServers` fallback was readable from the plugin manifest either, so \
             whether this bundle declares any server is unknown here. That is weaker than the \
             skill tier's `mcp.declared_none`, which is a file that WAS read"
        );
        diagnostics.push(
            Diag {
                code: "mcp.not_in_bundle_view",
                kind: DiagnosticKind::Mcp,
                candidate: MCP_FILE.to_string(),
                looked_for: "an mcpServers declaration",
                looked_in: vec![
                    format!("the deployed bundle's stored text files (no {MCP_FILE} entry)"),
                    format!("{}#/mcpServers", MANIFEST_LOCATIONS[0]),
                ],
                reason: reason.clone(),
                fix: Some(format!(
                    "run `curie skill info --plugin-dir <dir>` against the bundle source to read \
                     {MCP_FILE}, or declare the servers under the manifest's `mcpServers`, which \
                     the deployed view DOES carry"
                )),
            }
            .into(),
        );
        return Maybe::unresolved(reason);
    }

    diagnostics.push(
        Diag {
            code: "mcp.no_declaration",
            kind: DiagnosticKind::Mcp,
            candidate: MCP_FILE.to_string(),
            looked_for: "an mcpServers declaration",
            looked_in: vec![
                MCP_FILE.to_string(),
                format!("{}#/mcpServers", MANIFEST_LOCATIONS[0]),
            ],
            reason: "no MCP declaration was found at all: this bundle directory carries no \
                 .mcp.json and its manifest, which WAS read, names no `mcpServers`. The empty \
                 server list below means \"nothing was declared anywhere\", not \"a declaration \
                 was read and named none\" -- and not the deployed tiers' weaker \"the file was \
                 never shown to this CLI\""
                .to_string(),
            fix: Some(format!(
                "add {MCP_FILE} with an `mcpServers` object if this bundle should load MCP servers"
            )),
        }
        .into(),
    );
    Maybe::Known(Vec::new())
}

fn mcp_from_declaration(
    value: &serde_json::Value,
    source: &str,
    location: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> Maybe<Vec<McpRow>> {
    let declared = value.get("mcpServers");

    if declared.is_some_and(serde_json::Value::is_string) {
        diagnostics.push(
            Diag {
                code: "mcp.declared_pointer",
                kind: DiagnosticKind::Mcp,
                candidate: location.to_string(),
                looked_for: "an `mcpServers` OBJECT keyed by server name",
                looked_in: vec![format!("{location}#/mcpServers")],
                // The declared string itself stays out of the payload: it is
                // bundle-authored text in a field whose contract is "names,
                // counts, booleans and paths", and a URL written here carries
                // its own userinfo. `looked_in` already names the location.
                reason: "`mcpServers` is declared as a path string rather than an object; the \
                         loader ignores it and the servers never register, while the bundle \
                         looks correct"
                    .to_string(),
                fix: Some(
                    "inline the servers as an `mcpServers` object keyed by server name".to_string(),
                ),
            }
            .into(),
        );
        return Maybe::Known(Vec::new());
    }

    let Some(map) = declared.and_then(serde_json::Value::as_object) else {
        diagnostics.push(
            Diag {
                code: "mcp.declared_none",
                kind: DiagnosticKind::Mcp,
                candidate: location.to_string(),
                looked_for: "an `mcpServers` object naming at least one server",
                looked_in: vec![format!("{location}#/mcpServers")],
                reason: format!(
                    "{location} was read and declares no usable `mcpServers` object, so this \
                     bundle declares zero servers. That is a different fact from no declaration \
                     being found at all"
                ),
                fix: None,
            }
            .into(),
        );
        return Maybe::Known(Vec::new());
    };

    if map.is_empty() {
        diagnostics.push(
            Diag {
                code: "mcp.declared_none",
                kind: DiagnosticKind::Mcp,
                candidate: location.to_string(),
                looked_for: "an `mcpServers` object naming at least one server",
                looked_in: vec![format!("{location}#/mcpServers")],
                reason: format!(
                    "{location} was read and its `mcpServers` object is empty, so this bundle \
                     declares zero servers. That is a different fact from no declaration being \
                     found at all, and the empty list below must not be read as the latter"
                ),
                fix: None,
            }
            .into(),
        );
        return Maybe::Known(Vec::new());
    }

    let mut rows: Vec<McpRow> = Vec::new();
    for (name, spec) in map {
        let form = if spec.get("command").is_some() {
            "stdio"
        } else if let Some(kind) = spec.get("type").and_then(serde_json::Value::as_str) {
            match kind {
                "sse" => "sse",
                "http" | "streamable-http" => "http",
                _ => "unknown",
            }
        } else if spec.get("url").is_some() {
            "http"
        } else {
            "unknown"
        };
        // A boolean, never the block: `env` and `headers` can each carry a
        // literal token, so only their PRESENCE travels.
        let authed = ["env", "headers"].iter().any(|key| {
            spec.get(*key)
                .and_then(serde_json::Value::as_object)
                .is_some_and(|o| !o.is_empty())
        });
        rows.push(McpRow {
            name: name.clone(),
            source: source.to_string(),
            form: form.to_string(),
            authed,
            load: McpLoad::NotProbed,
        });
        diagnostics.push(
            Diag {
                code: "mcp.not_probed",
                kind: DiagnosticKind::Mcp,
                candidate: name.clone(),
                looked_for: "proof that the declared server actually loads",
                looked_in: vec![format!("{location}#/mcpServers/{name}")],
                reason: "`info` is static by default: it declines to boot a runner container, so \
                     whether this server registers its tools is NOT known. \"Not probed\" is \
                     stated rather than implied by a missing verdict"
                    .to_string(),
                fix: Some(
                    "run `curie skill info --plugin-dir <dir> --check-mcp` (or `curie skill \
                     check`) to probe it; needs Docker"
                        .to_string(),
                ),
            }
            .into(),
        );
    }
    rows.sort_by(|a, b| a.name.cmp(&b.name));
    Maybe::Known(rows)
}

// --- secrets --------------------------------------------------------------

fn discover_secrets(
    view: &BundleView,
    manifest: Option<&ResolvedManifest>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Maybe<SecretsBlock> {
    let Some(value) = manifest.and_then(|m| m.value.as_ref()) else {
        // Declared secret NAMES have exactly one source. Reporting `declared: []`
        // here would read as "this bundle declares no secrets", which is the lie.
        return Maybe::unresolved(
            "the declared connector-secret NAMES come only from the plugin manifest, which \
             could not be parsed, so whether this bundle declares any is unknown",
        );
    };

    // The identical two-line read `scaffold::read_declared_secrets` makes, applied
    // to the view's manifest value rather than a path so both tiers share it.
    let names: Vec<String> = value
        .get("secrets")
        .and_then(serde_json::Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();

    let deployed = view.origin().is_deployed();
    let declared = names
        .into_iter()
        .map(|name| {
            if deployed {
                SecretRow {
                    satisfied: Maybe::unavailable(
                        "satisfaction is a fact about THIS shell, not about the deployed \
                         sandbox; the platform owns the per-agent secret binding (ADR-0009)",
                        WHERE_DISK,
                    ),
                    source: Maybe::unavailable(
                        "the binding source for a deployed agent is platform state, not this \
                         machine's shell or vault",
                        WHERE_DISK,
                    ),
                    name,
                }
            } else {
                SecretRow {
                    // Filled in by `run`, which is where reading this shell
                    // belongs; the pass itself never touches the environment.
                    satisfied: Maybe::unresolved(
                        "whether this shell satisfies the secret is layered on by the skill \
                         tier's invocation, not by the bundle pass",
                    ),
                    source: Maybe::unresolved(
                        "the satisfying source is layered on by the skill tier's invocation",
                    ),
                    name,
                }
            }
        })
        .collect::<Vec<_>>();

    let _ = diagnostics;
    Maybe::Known(SecretsBlock { declared })
}

// --- approval gates -------------------------------------------------------

fn discover_gates(
    manifest: Option<&ResolvedManifest>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Maybe<Vec<GateRow>> {
    let Some(manifest) = manifest else {
        return Maybe::unresolved(
            "the armed approval gates come only from the plugin manifest, which was not \
             readable in this bundle",
        );
    };
    if manifest.value.is_none() {
        // The manifest.invalid_json diagnostic already names the file; a second
        // diagnostic for the same defect would be noise.
        return Maybe::unresolved(
            "the armed approval gates come only from the plugin manifest, which is present but \
             is not valid JSON",
        );
    }

    // The fail-closed parse the runner itself performs (#520), reused rather
    // than forked. Its Err is a USAGE error for `skill approvals`; here it is a
    // diagnosis, because a bundle defect is never this verb's exit code.
    //
    // Forwarding that error's text is safe only because its schema half is
    // redacted at the source (`commands::validate_against_plugin_format_schema`
    // renders locations, not the failing instance); the alternative was to
    // rebuild the same prose here and let the two drift.
    match commands::parse_manifest_gates(&manifest.body, manifest.location) {
        Ok(gates) => Maybe::Known(
            gates
                .into_iter()
                .map(|(gate, route)| GateRow { gate, route })
                .collect(),
        ),
        Err(err) => {
            let reason = format!("{err}");
            diagnostics.push(
                Diag {
                    code: "approval_gate.manifest_invalid",
                    kind: DiagnosticKind::ApprovalGate,
                    candidate: manifest.location.to_string(),
                    looked_for: "an `approvalPolicy` the runner would arm exactly as declared",
                    looked_in: vec![format!("{}#/approvalPolicy", manifest.location)],
                    reason: reason.clone(),
                    fix: Some(
                        "fix the reported `approvalPolicy` defect; the runner arms ZERO gates \
                         for a manifest it rejects, including any well-formed ones"
                            .to_string(),
                    ),
                }
                .into(),
            );
            Maybe::unresolved(reason)
        }
    }
}

// --- evals ----------------------------------------------------------------

fn discover_evals(view: &BundleView, diagnostics: &mut Vec<Diagnostic>) -> Maybe<EvalsInfo> {
    let Some(body) = view.file(EVAL_SUITE) else {
        diagnostics.push(
            Diag {
                code: "evals.file_absent",
                kind: DiagnosticKind::Evals,
                candidate: EVAL_SUITE.to_string(),
                looked_for: "the bundle's own eval suite",
                looked_in: vec![EVAL_SUITE.to_string()],
                reason: format!(
                    "{EVAL_SUITE} is not present in this bundle, so it ships no eval suite. \
                     Reporting a zero case count would read as \"the suite exists and is empty\""
                ),
                fix: Some(
                    "add evals/cases.json, or run `curie init <name>` to scaffold a seed suite"
                        .to_string(),
                ),
            }
            .into(),
        );
        return Maybe::unresolved(format!(
            "{EVAL_SUITE} is absent from this bundle, so there is no suite to describe"
        ));
    };

    let value: serde_json::Value = match serde_json::from_str(body) {
        Ok(value) => value,
        Err(err) => {
            return evals_invalid(
                diagnostics,
                format!(
                    "{EVAL_SUITE} is not valid JSON ({})",
                    crate::redact::json_syntax(&err)
                ),
            )
        }
    };
    if value.is_array() {
        let reason = format!(
            "{EVAL_SUITE} is in the retired eval-case format (a top-level array of \
             [{{name, input, expect_contains}}]). The format is now a suite object: \
             {{\"name\": \"...\", \"cases\": [{{\"id\": \"...\", \"input\": \"...\", \
             \"grader\": {{\"kind\": \"contains\", \"expected\": \"...\"}}}}]}}"
        );
        diagnostics.push(
            Diag {
                code: "evals.retired_format",
                kind: DiagnosticKind::Evals,
                candidate: EVAL_SUITE.to_string(),
                looked_for: "a suite OBJECT with `name` and `cases`",
                looked_in: vec![EVAL_SUITE.to_string()],
                reason: reason.clone(),
                fix: Some("rewrite the file to the suite-object form".to_string()),
            }
            .into(),
        );
        return Maybe::unresolved(reason);
    }

    // Borrowed rather than moved so the shape locator below can read the same
    // parsed value. A TYPED serde failure is the one deserialization in this
    // module whose message quotes its input (`invalid type: string "<token>",
    // expected a sequence`), so it is answered by a locator, never forwarded.
    let suite: crate::evals::EvalSuite = match serde::Deserialize::deserialize(&value) {
        Ok(suite) => suite,
        Err(_) => {
            return evals_invalid(
                diagnostics,
                format!(
                    "{EVAL_SUITE} parses as JSON but does not match the eval-suite shape: {}",
                    eval_shape_defect(&value)
                ),
            )
        }
    };
    // The identical content-based validation `load_suite` delegates to, so both
    // tiers apply one suite rule rather than two.
    if crate::evals::validate_suite(&suite.name, &suite.cases).is_err() {
        return evals_invalid(diagnostics, eval_grader_defect(&suite));
    }

    Maybe::Known(EvalsInfo {
        path: EVAL_SUITE.to_string(),
        suite_name: suite.name,
        case_count: suite.cases.len() as u64,
    })
}

/// Say WHERE a parsed suite departs from the eval-suite shape, and what TYPE
/// sits there, never what the value was.
///
/// `EvalSuite`'s own deserialization stays the authority on whether the suite is
/// valid; this only locates the defect it found. It does that by re-running the
/// SAME types over narrower slices rather than by hand-mirroring their fields,
/// so it cannot drift into disagreeing with them -- at worst it stops localizing
/// and falls through to the generic tail.
fn eval_shape_defect(value: &serde_json::Value) -> String {
    let Some(object) = value.as_object() else {
        return format!(
            "the suite is {} rather than an object with `name` and `cases`",
            crate::redact::json_type(value)
        );
    };
    match object.get("name") {
        None => return "`name` is missing".to_string(),
        Some(name) if !name.is_string() => {
            return format!(
                "`name` is {} rather than a string",
                crate::redact::json_type(name)
            )
        }
        Some(_) => {}
    }
    let Some(cases) = object.get("cases") else {
        return "`cases` is missing".to_string();
    };
    let Some(cases) = cases.as_array() else {
        return format!(
            "`cases` is {} rather than an array of case objects",
            crate::redact::json_type(cases)
        );
    };
    for (index, case) in cases.iter().enumerate() {
        if <crate::evals::EvalCase as serde::Deserialize>::deserialize(case).is_err() {
            return format!(
                "`cases[{index}]` does not match the eval-case shape (`id` and `input` are \
                 strings, and `grader` is an object with a declared `kind` and a string \
                 `expected`)"
            );
        }
    }
    "the suite object does not match the eval-suite shape".to_string()
}

/// Say WHICH case carries a grader the local runner refuses, without printing
/// the grader itself: `validate_suite`'s message interpolates the raw
/// `grader.expected` pattern read straight out of the bundle. Re-running that
/// same function per case is what locates the offender, so the rule stays in one
/// place and this cannot disagree with it.
fn eval_grader_defect(suite: &crate::evals::EvalSuite) -> String {
    if suite.cases.is_empty() {
        return format!(
            "{EVAL_SUITE} declares zero eval cases, so the local grader has nothing to run"
        );
    }
    match suite.cases.iter().position(|case| {
        crate::evals::validate_suite(&suite.name, std::slice::from_ref(case)).is_err()
    }) {
        Some(index) => format!(
            "{EVAL_SUITE} declares a grader at `cases[{index}]` the local runner refuses: an \
             invalid regex pattern, or a `tool_called` grader with an empty tool name. The \
             pattern is not reproduced here; `curie skill eval` reports it in full on the error \
             channel"
        ),
        None => format!("{EVAL_SUITE} does not pass eval-suite validation"),
    }
}

fn evals_invalid(diagnostics: &mut Vec<Diagnostic>, reason: String) -> Maybe<EvalsInfo> {
    diagnostics.push(
        Diag {
            code: "evals.invalid",
            kind: DiagnosticKind::Evals,
            candidate: EVAL_SUITE.to_string(),
            looked_for: "a suite the local grader would load",
            looked_in: vec![EVAL_SUITE.to_string()],
            reason: reason.clone(),
            fix: Some(
                "fix the reported suite defect; `curie skill eval` refuses the same file"
                    .to_string(),
            ),
        }
        .into(),
    );
    Maybe::unresolved(reason)
}

// --- tier-shaped blocks ---------------------------------------------------

fn tier_for(origin: &BundleOrigin) -> Maybe<String> {
    match origin {
        BundleOrigin::Disk { .. } => Maybe::Known("skill".to_string()),
        // A stored file view genuinely does not name which deployed tier asked
        // for it; `run` sets this from the invocation.
        BundleOrigin::Deployed { .. } => Maybe::unresolved(
            "a deployed file view does not name which deployed tier requested it; \
             `curie local info` / `curie cluster info` set this",
        ),
    }
}

fn channel_for(origin: &BundleOrigin) -> Maybe<ChannelInfo> {
    match origin {
        BundleOrigin::Disk { .. } => Maybe::unavailable(
            "`skill message` posts a synthetic event straight to the runner's ACI surface with \
             the default local user, so this tier binds no channel",
            WHERE_DEPLOYED,
        ),
        BundleOrigin::Deployed { channel, .. } => Maybe::Known(ChannelInfo {
            id: channel.clone(),
        }),
    }
}

fn comms_for(origin: &BundleOrigin) -> Maybe<CommsInfo> {
    match origin {
        BundleOrigin::Disk { .. } => Maybe::unavailable(
            "there is no dispatcher at this tier: `skill message` bypasses the worker and \
             Valkey entirely, so there is no comms wiring to report on",
            WHERE_DEPLOYED,
        ),
        BundleOrigin::Deployed { channel, .. } => {
            let stub = channel == crate::api::DEFAULT_SLACK_CHANNEL;
            Maybe::Known(CommsInfo {
                connected: !channel.is_empty() && !stub,
                detail: if channel.is_empty() {
                    "the agent is bound to no channel, so no reply can be routed".to_string()
                } else if stub {
                    format!(
                        "the agent is bound to the local-dev stub channel {channel}, which is \
                         what `<tier> message` drives; no real workspace is wired"
                    )
                } else {
                    format!("the agent is bound to channel {channel}")
                },
            })
        }
    }
}

fn model_for(origin: &BundleOrigin) -> Maybe<ModelInfo> {
    match origin {
        // Resolved from THIS shell's env and `.curie/runner.json`, neither of
        // which the bundle pass may read; `run` layers it on.
        BundleOrigin::Disk { .. } => Maybe::unresolved(
            "the model a run would resolve comes from this shell's environment and \
             `.curie/runner.json`, which the bundle pass does not read; `curie skill info` \
             layers it on",
        ),
        BundleOrigin::Deployed { .. } => Maybe::unavailable(
            "a deployed agent's model is resolved by the platform from chart and worker \
             configuration, not from this shell's environment or a local runner record",
            WHERE_DISK,
        ),
    }
}

/// Who writes a boot-env key at the `skill` tier, mirroring the ONE producer:
/// `docker::StartSpec::run_args`.
enum BootEnvProducer {
    /// `skill up` writes it on every invocation (`run_args`'s unconditional
    /// block).
    Always,
    /// `run_args` writes it only inside a conditional whose input is an
    /// invocation FLAG. The pure pass cannot know a future invocation's flags,
    /// so it answers `unresolved` and [`layer_boot_env`] fills it from the SAME
    /// resolution the `model` block reports -- which is what stops the two
    /// blocks contradicting each other in one payload.
    PerInvocation(&'static str),
    /// `skill up` never writes it, whatever the flags.
    Never(&'static str),
}

/// The boot-env keys this report describes, with who writes each. Names come
/// from the generated `env_keys` constants so a renamed key cannot drift.
///
/// The set is deliberately the keys the `skill up` boot path DECIDES, not the
/// whole declared `BootEnv` contract; the rest are provisioned by the platform
/// and are named as out of scope by [`boot_env_scope_diagnostic`] rather than
/// silently omitted.
fn boot_env_rows(origin: &BundleOrigin) -> Vec<BootEnvRow> {
    let deployed = origin.is_deployed();
    BOOT_ENV_TABLE
        .into_iter()
        .map(|(name, producer)| {
            let (set_by_this_tier, note) = match producer {
                _ if deployed => (
                    Maybe::unavailable(
                        "which producer writes a boot-env key at the deployed tiers is owned by \
                         the worker and the chart (ADR-0049), and cannot be read from a stored \
                         bundle",
                        "the chart values and the worker's sandbox configuration",
                    ),
                    None,
                ),
                BootEnvProducer::Always => (Maybe::Known(true), None),
                BootEnvProducer::PerInvocation(note) => (
                    Maybe::unresolved(format!(
                        "whether `curie skill up` writes this key depends on the flags that \
                         invocation passes, which the bundle pass does not know: {note}"
                    )),
                    Some(note.to_string()),
                ),
                BootEnvProducer::Never(note) => (Maybe::Known(false), Some(note.to_string())),
            };
            BootEnvRow {
                name: name.to_string(),
                set_by_this_tier,
                value_present: Maybe::unavailable(
                    "this CLI cannot read a running container's environment at any tier, so \
                     whether the key holds a value is not observable from here",
                    "the runner container's own environment",
                ),
                note,
            }
        })
        .collect()
}

/// The table itself, a `const` so [`layer_boot_env`] resolves a row's PRODUCER
/// from the one declaration rather than re-spelling the conditional key names.
const BOOT_ENV_TABLE: [(&str, BootEnvProducer); 11] = [
    (env_keys::CURIE_PLUGIN_DIR, BootEnvProducer::Always),
    (env_keys::CURIE_SESSION_ID, BootEnvProducer::Always),
    (env_keys::CURIE_SANDBOX_ID, BootEnvProducer::Always),
    (env_keys::CURIE_BUDGET, BootEnvProducer::Always),
    (
        env_keys::CURIE_FAKE_MODEL,
        BootEnvProducer::PerInvocation(
            "`skill up` writes this only under `--fake-model` (and not when `--local-model` \
                 overrides it); it is never read from this shell",
        ),
    ),
    (
        env_keys::CURIE_MODEL,
        BootEnvProducer::PerInvocation(
            "`skill up` writes this only under `--model <id>`, which has no environment \
                 default; a plain run leaves the SDK default",
        ),
    ),
    (
        env_keys::ANTHROPIC_BASE_URL,
        BootEnvProducer::PerInvocation(
            "`skill up` writes this only under `--local-model`, which points the runner at \
                 the Ollama container it started",
        ),
    ),
    (
        env_keys::OTEL_EXPORTER_OTLP_ENDPOINT,
        BootEnvProducer::PerInvocation(
            "`skill up` writes this only under `--otel-endpoint <url>`; without it the \
                 runner exports no traces",
        ),
    ),
    (
        env_keys::CURIE_APPROVAL_REQUIRED_TOOLS,
        BootEnvProducer::Never(
            "a plain `curie skill up` does not forward this: the container gets it only when \
                 the invocation adds `--secret CURIE_APPROVAL_REQUIRED_TOOLS` with the value in \
                 this shell or the vault, so it is an override the bundle cannot declare",
        ),
    ),
    (
        env_keys::CURIE_MEMORY_REF,
        BootEnvProducer::Never(commands::MEMORY_REASON),
    ),
    (
        env_keys::CURIE_HISTORY_REF,
        BootEnvProducer::Never(
            "`skill up` provisions no history namespace: the runner it boots keeps a turn's \
                 history in process and nothing persists it",
        ),
    ),
];

/// State the BOUNDARY of the `boot_env` block rather than letting a declared key
/// be absent from it. The frozen `BootEnv` contract carries keys this CLI has no
/// say over (the state/history/memory tokens and URLs, the connector-secret key
/// list, the turn caps, the runner port, the remaining `OTEL_*` pair), all
/// provisioned platform-side; reporting only the ones `skill up` decides is
/// fine, reporting them with no statement that the list is a subset is the
/// verb's own lie-class one level down.
fn boot_env_scope_diagnostic() -> Diagnostic {
    Diag {
        code: "boot_env.rows_scoped",
        kind: DiagnosticKind::BootEnv,
        candidate: "the declared BootEnv keys outside this report's rows".to_string(),
        looked_for: "a producer this CLI can answer for",
        looked_in: vec![
            "the boot environment `curie skill up` writes".to_string(),
            "the chart values and the worker's sandbox configuration".to_string(),
        ],
        reason: "the `boot_env` rows below are the keys the `curie skill up` boot path decides, \
                 not the whole frozen BootEnv contract. Every other declared key (the state, \
                 history and memory tokens and URLs, the connector-secret key list, the turn and \
                 history caps, the runner port and the remaining OTEL_* pair) is provisioned \
                 platform-side and is absent from the rows because this CLI cannot answer for \
                 it, NOT because it is undeclared"
            .to_string(),
        fix: None,
    }
    .into()
}

/// The expected files of the bundle shape, present or not.
///
/// `exists` means "present in the FILE VIEW this report read", not "present in
/// the bundle": a deployed view is served through an allowlist that has never
/// carried `.mcp.json` (see the module doc), so `mcp_declaration.exists: false`
/// there is a fact about the view. Each row's absence already has a dedicated
/// `code` elsewhere in the pass, so no row emits an `artifact.*` diagnostic of
/// its own.
fn artifact_rows(view: &BundleView) -> Vec<ArtifactRow> {
    let manifest_path = MANIFEST_LOCATIONS
        .iter()
        .find(|loc| view.file(loc).is_some())
        .copied()
        .unwrap_or(MANIFEST_LOCATIONS[0]);
    let has_skills = view.paths().any(|p| p.starts_with("skills/"));

    vec![
        ArtifactRow {
            kind: "manifest".to_string(),
            path: manifest_path.to_string(),
            exists: view.file(manifest_path).is_some(),
        },
        ArtifactRow {
            kind: "mcp_declaration".to_string(),
            path: MCP_FILE.to_string(),
            exists: view.file(MCP_FILE).is_some(),
        },
        ArtifactRow {
            kind: "eval_suite".to_string(),
            path: EVAL_SUITE.to_string(),
            exists: view.file(EVAL_SUITE).is_some(),
        },
        ArtifactRow {
            kind: "skills_dir".to_string(),
            path: SKILLS_DIR.to_string(),
            exists: has_skills,
        },
    ]
}

// ---------------------------------------------------------------------------
// The verb
// ---------------------------------------------------------------------------

/// Which tier asked. Carried into the report so the payload names the tier the
/// operator actually invoked, rather than a guess from the data source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Skill,
    Local,
    Cluster,
}

impl Tier {
    fn as_str(self) -> &'static str {
        match self {
            Tier::Skill => "skill",
            Tier::Local => "local",
            Tier::Cluster => "cluster",
        }
    }
}

/// What `info` should inspect.
///
/// Deliberately NOT `Debug`: `AgentActionOpts` carries the API key, and a
/// derived `Debug` would be a value-print vector one `dbg!` away.
pub enum InfoTarget {
    /// A bundle directory on this machine, plus the runner image and timeout the
    /// optional `--check-mcp` probe would use.
    Skill {
        plugin_dir: PathBuf,
        image: String,
        timeout_s: u64,
    },
    /// A deployed agent's in-force bundle, read back over the platform API.
    Deployed { tier: Tier, opts: AgentActionOpts },
}

/// The whole invocation.
pub struct InfoOpts {
    pub target: InfoTarget,
    pub check_mcp: bool,
}

/// The result of `curie <tier> info`: the uniform dry-run plan, or the report.
///
/// The report variant is much larger than the plan variant, and deliberately
/// not boxed: this enum is constructed once per invocation and the contract
/// tests build `InfoOutput::Report(discover(&view))` directly, so a `Box` would
/// buy nothing but an indirection at every call site.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum InfoOutput {
    DryRun(crate::ui::DryRunPlan),
    Report(InfoReport),
}

impl crate::ui::CliOutput for InfoOutput {
    /// Delegates WHOLESALE to `serde_json::to_value` over a `Serialize` value.
    /// A delegating `to_json` cannot drop a field by hand-picking one, which is
    /// why this family needs no `emits` entry in `cli/api-mirrors.json`. Do not
    /// replace this with a `json!` literal.
    fn to_json(&self) -> serde_json::Value {
        match self {
            InfoOutput::DryRun(plan) => plan.to_json(),
            InfoOutput::Report(report) => {
                serde_json::to_value(report).unwrap_or_else(|_| serde_json::json!({}))
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            InfoOutput::DryRun(plan) => plan.render(ui),
            InfoOutput::Report(report) => render_report(report, ui),
        }
    }
}

fn render_report(report: &InfoReport, ui: &crate::ui::Ui) {
    if let Maybe::Known(tier) = &report.tier {
        ui.kv("tier", tier);
    }
    match &report.bundle {
        Maybe::Known(bundle) => ui.kv(
            "bundle",
            &format!(
                "{} {}",
                bundle.name.as_deref().unwrap_or("<unnamed>"),
                bundle.version.as_deref().unwrap_or("<no version>")
            ),
        ),
        Maybe::Unavailable(u) => ui.kv("bundle", &format!("unavailable: {}", u.reason)),
        Maybe::Unresolved(u) => ui.kv("bundle", &format!("unresolved: {}", u.reason)),
    }
    match &report.skills {
        Maybe::Known(skills) => {
            ui.kv("skills", &format!("{}", skills.len()));
            for skill in skills {
                ui.payload_plain(&format!("skill: {} ({})", skill.name, skill.path));
            }
        }
        Maybe::Unavailable(u) => ui.kv("skills", &format!("unavailable: {}", u.reason)),
        Maybe::Unresolved(u) => ui.kv("skills", &format!("unresolved: {}", u.reason)),
    }
    match &report.mcp_servers {
        Maybe::Known(servers) => {
            ui.kv("mcp_servers", &format!("{}", servers.len()));
            for server in servers {
                ui.payload_plain(&format!(
                    "mcp: {} ({}, authed: {}, load: {})",
                    server.name,
                    server.form,
                    server.authed,
                    load_label(server.load)
                ));
            }
        }
        Maybe::Unavailable(u) => ui.kv("mcp_servers", &format!("unavailable: {}", u.reason)),
        Maybe::Unresolved(u) => ui.kv("mcp_servers", &format!("unresolved: {}", u.reason)),
    }
    match &report.evals {
        Maybe::Known(evals) => ui.kv(
            "evals",
            &format!("{} case(s) in {}", evals.case_count, evals.path),
        ),
        Maybe::Unavailable(u) => ui.kv("evals", &format!("unavailable: {}", u.reason)),
        Maybe::Unresolved(u) => ui.kv("evals", &format!("unresolved: {}", u.reason)),
    }
    ui.kv("diagnostics", &format!("{}", report.diagnostics.len()));
    for diag in &report.diagnostics {
        ui.payload_plain(&format!(
            "{}: {} -- {}",
            diag.code, diag.candidate, diag.reason
        ));
    }
}

fn load_label(load: McpLoad) -> &'static str {
    match load {
        McpLoad::NotProbed => "not_probed",
        McpLoad::Registered => "registered",
        McpLoad::RegisteredZeroTools => "registered_zero_tools",
        McpLoad::DidNotRegister => "did_not_register",
        McpLoad::ProbeFailed => "probe_failed",
    }
}

/// `curie <tier> info`: populate the view for the requested tier, run the one
/// discovery pass, then layer on the facts that belong to the INVOCATION rather
/// than to the bundle.
pub async fn run(opts: InfoOpts) -> Result<InfoOutput> {
    match opts.target {
        InfoTarget::Skill {
            plugin_dir,
            image,
            timeout_s,
        } => run_skill(&plugin_dir, image, timeout_s, opts.check_mcp).await,
        InfoTarget::Deployed { tier, opts: agent } => run_deployed(tier, agent).await,
    }
}

async fn run_skill(
    plugin_dir: &Path,
    image: String,
    timeout_s: u64,
    check_mcp: bool,
) -> Result<InfoOutput> {
    let view = BundleView::from_disk(plugin_dir)?;
    let root = match view.origin() {
        BundleOrigin::Disk { root } => root.clone(),
        BundleOrigin::Deployed { .. } => unreachable!("from_disk yields a Disk origin"),
    };
    let mut report = discover(&view);
    report.tier = Maybe::Known(Tier::Skill.as_str().to_string());

    layer_shell_secrets(&mut report);
    // ONE fact, two consumers: this is a PLAIN invocation, so the `model` block
    // and the conditional `boot_env` rows both answer for a run that passes no
    // flags. Deciding that separately in each is what let the two contradict
    // each other in a single payload.
    let boot = plain_skill_up_boot();
    layer_model(&mut report, &root, &boot);
    layer_boot_env(&mut report);
    layer_memory_note(&mut report);
    if check_mcp {
        probe_mcp(&mut report, &root, image, timeout_s).await;
    }

    sort_diagnostics(&mut report.diagnostics);
    Ok(InfoOutput::Report(report))
}

/// Resolve declared secret satisfaction against THIS shell and this machine's
/// vault. A fact about the invocation, so it never belongs in the pure pass.
fn layer_shell_secrets(report: &mut InfoReport) {
    let Maybe::Known(secrets) = &mut report.secrets else {
        return;
    };
    let mut diagnostics: Vec<Diagnostic> = Vec::new();
    for (index, row) in secrets.declared.iter_mut().enumerate() {
        if crate::secrets::validate_name(&row.name).is_err() {
            // Positional, not quoted. This branch fires precisely when a
            // `secrets[]` entry is not shaped like a name -- i.e. exactly when
            // an author has pasted a VALUE there -- so reprinting the entry to
            // explain the rejection would republish the thing it rejected.
            let reason = format!(
                "the declared name at `secrets[{index}]` is not a usable environment-variable \
                 name: it must be non-empty and match ^[A-Z_][A-Z0-9_]*$. Authoritative name \
                 validation, including the reserved boot-env list, runs server-side in \
                 `plugin_format.validate_bundle` at deploy"
            );
            diagnostics.push(
                Diag {
                    code: "secret.name_invalid",
                    kind: DiagnosticKind::Secret,
                    candidate: format!("secrets[{index}]"),
                    looked_for: "a declared secret name of the form ^[A-Z_][A-Z0-9_]*$",
                    looked_in: vec![format!("{}#/secrets", MANIFEST_LOCATIONS[0])],
                    reason: reason.clone(),
                    fix: Some("rename the declared secret to an upper-snake-case name".to_string()),
                }
                .into(),
            );
            row.satisfied = Maybe::unresolved(reason.clone());
            row.source = Maybe::unresolved(reason);
            continue;
        }

        match credential_location(&row.name) {
            CredentialLocation::ShellEnv => {
                row.satisfied = Maybe::Known(true);
                row.source = Maybe::Known("shell_env".to_string());
            }
            CredentialLocation::Vault => {
                row.satisfied = Maybe::Known(true);
                row.source = Maybe::Known("curie_vault".to_string());
            }
            CredentialLocation::Absent => {
                row.satisfied = Maybe::Known(false);
                row.source = Maybe::Known("none".to_string());
                diagnostics.push(
                    Diag {
                        code: "secret.unsatisfied",
                        kind: DiagnosticKind::Secret,
                        candidate: row.name.clone(),
                        looked_for: "an exported value or a saved vault entry",
                        looked_in: vec![
                            "this shell's environment".to_string(),
                            "the Curie private credential store".to_string(),
                        ],
                        reason: format!(
                            "the bundle declares {} but nothing in this shell or the local vault \
                             satisfies it, so `curie skill up --secret {}` would forward nothing",
                            row.name, row.name
                        ),
                        fix: Some(format!(
                            "export {} in this shell, or run `curie secrets set {}`",
                            row.name, row.name
                        )),
                    }
                    .into(),
                );
            }
            CredentialLocation::Unreadable(cause) => {
                // Claiming "unsatisfied" when the vault is merely locked sends an
                // operator down the wrong path, so this is unresolved, not false.
                let reason = format!(
                    "the local credential store could not be read ({cause}), so whether it \
                     satisfies {} is unknown",
                    row.name
                );
                diagnostics.push(
                    Diag {
                        code: "secret.vault_unreadable",
                        kind: DiagnosticKind::Secret,
                        candidate: row.name.clone(),
                        looked_for: "a readable local credential index",
                        looked_in: vec!["the Curie private credential store".to_string()],
                        reason: reason.clone(),
                        fix: Some(
                            "unlock the OS keychain (or fix the credential index) and re-run"
                                .to_string(),
                        ),
                    }
                    .into(),
                );
                row.satisfied = Maybe::unresolved(reason.clone());
                row.source = Maybe::unresolved(reason);
            }
        }
    }
    report.diagnostics.extend(diagnostics);
}

/// Where a credential NAME is satisfied from, in the frozen order: this shell
/// first, then the local vault.
///
/// One predicate for the three callers that ask it -- the declared-secret rows,
/// the ambient probe [`plain_skill_up_boot`] hands `select_passthrough_env`, and
/// the `model` block's `credential.source`. Each maps the answer into its own
/// payload wording, so no string moves here; what is shared is the ORDER and the
/// empty-string-is-absent rule, which three hand-written copies of the predicate
/// were free to disagree about.
enum CredentialLocation {
    /// Exported in this shell with a non-empty value.
    ShellEnv,
    /// Saved in the Curie private credential store.
    Vault,
    /// Neither.
    Absent,
    /// The store could not be read, so absence cannot be claimed. Carries the
    /// rendered cause for the one caller that reports it.
    Unreadable(String),
}

fn credential_location(name: &str) -> CredentialLocation {
    // The frozen empty-string-is-absent rule (#540): `NAME=""` forwards nothing
    // usable, so reporting it satisfied would call a bundle ready while the
    // sandbox gets nothing.
    if commands::env_credential_present(name) {
        return CredentialLocation::ShellEnv;
    }
    match crate::secrets::is_saved(name) {
        Ok(true) => CredentialLocation::Vault,
        Ok(false) => CredentialLocation::Absent,
        Err(err) => CredentialLocation::Unreadable(format!("{err:#}")),
    }
}

/// What a PLAIN `curie skill up` from this shell resolves: the one resolution
/// that feeds both the `model` block and the conditional `boot_env` rows.
///
/// "Plain" is load-bearing and is stated in the payload's own `note`. Fake mode,
/// the model id and the base-URL override are properties of an invocation's
/// FLAGS (`--fake-model`, `--model`, `--local-model`, `--otel-endpoint`), none of
/// which has an environment default (`cli/src/main.rs`). Reading them back out
/// of this shell's `CURIE_FAKE_MODEL` / `CURIE_MODEL` / `ANTHROPIC_BASE_URL` --
/// which is what an earlier draft did -- is a category error: those are keys the
/// CLI WRITES INTO the container (`docker::StartSpec::run_args`), never keys it
/// reads to decide, so an exported `CURIE_FAKE_MODEL=0` reported `fake_model`
/// for a run that would have gone live.
///
/// So the only fields here are the ones a plain run genuinely RESOLVES. Fake
/// mode, the model id, the base-URL override and the OTEL endpoint are false or
/// null by construction for every plain run, and carrying them as fields made
/// three downstream branches look conditional while never taking their other
/// arm.
struct PlainSkillUpBoot {
    /// A BYO credential blob is present (in this shell or the vault), whatever
    /// its value.
    byo: Option<String>,
    /// The one credential NAME `select_passthrough_env` would forward.
    credential_name: Option<String>,
    /// `shell_env`, `curie_vault`, `env_file`, or `none`.
    credential_source: &'static str,
}

/// What `commands::up` derives for a no-flag run:
/// `fake_model = opts.local_model.is_none() && opts.fake_model`, and
/// `base_url_override = model_base_url.is_some()` (set only by `--local-model`).
/// A plain run passes neither flag.
const PLAIN_FAKE_MODEL: bool = false;
const PLAIN_BASE_URL_OVERRIDE: bool = false;

fn plain_skill_up_boot() -> PlainSkillUpBoot {
    // `commands::up` resolves a credential from the shell OR the vault (it
    // extends `docker_env` with `load_model_credentials_from_secret_store`
    // before calling `ambient_present_for`). Consulting only the shell reported
    // `unauthenticated` for a bundle that boots live off `curie secrets set`,
    // and made the `curie_vault` arm below unreachable. `--env-file` is the
    // third source and is invocation-only, so it is named in the note instead.
    let ambient_present = |name: &str| {
        matches!(
            credential_location(name),
            CredentialLocation::ShellEnv | CredentialLocation::Vault
        )
    };
    let byo = std::env::var(env_keys::CURIE_CREDENTIALS)
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| {
            crate::secrets::is_saved(env_keys::CURIE_CREDENTIALS)
                .unwrap_or(false)
                .then(|| "stored".to_string())
        });
    // The frozen #495 forwarding rule, called rather than forked. It returns
    // NAMES; no value ever reaches this report.
    let names = commands::select_passthrough_env(
        PLAIN_FAKE_MODEL,
        PLAIN_BASE_URL_OVERRIDE,
        byo.as_deref(),
        &ambient_present,
    );
    let credential_name = names.first().cloned();
    let credential_source = match credential_name.as_deref() {
        None => "none",
        Some(name) => match credential_location(name) {
            CredentialLocation::ShellEnv => "shell_env",
            CredentialLocation::Vault => "curie_vault",
            // An unreadable store is reported as `none` here on purpose: this
            // block answers "what would a plain run forward", and the row-level
            // `secret.vault_unreadable` diagnostic carries the uncertainty.
            CredentialLocation::Absent | CredentialLocation::Unreadable(_) => "none",
        },
    };
    PlainSkillUpBoot {
        byo,
        credential_name,
        credential_source,
    }
}

/// What a plain `skill up` from THIS shell would resolve, plus whatever runner
/// is actually recorded for the bundle.
fn layer_model(report: &mut InfoReport, root: &Path, boot: &PlainSkillUpBoot) {
    // No `fake_model` arm: fake mode is turned on by `--fake-model` alone, and
    // this block answers for a run that passes no flags. The mode a fake run
    // resolves is not unreported, it is simply not this block's question.
    let mode = if boot.byo.is_some() {
        "byo_credential"
    } else if boot.credential_name.is_some() {
        "ambient_sdk_credential"
    } else {
        "unauthenticated"
    };

    let (recorded_runner, state_diag) = recorded_runner(root);
    if let Some(diag) = state_diag {
        report.diagnostics.push(diag);
    }

    report.model = Maybe::Known(ModelInfo {
        mode: mode.to_string(),
        // Both are properties of `--model` and `--local-model`, which a plain
        // run does not pass; the `note` below states that boundary in the
        // payload itself.
        model_id: None,
        base_url_override: PLAIN_BASE_URL_OVERRIDE,
        credential: CredentialInfo {
            name: boot.credential_name.clone(),
            source: boot.credential_source.to_string(),
        },
        recorded_runner,
        note: "these are the values a PLAIN `curie skill up` FROM THIS SHELL would resolve -- no \
               `--fake-model`, `--model`, `--local-model`, `--otel-endpoint` or `--env-file` -- \
               and not a running fact. Those flags are the only thing that turns on fake mode, a \
               model id or a base-URL override, so this block cannot answer for an invocation \
               that passes them; `recorded_runner.fake_model` is the recorded fact for the \
               runner that IS booted. A null model id means the SDK default. The credential is \
               resolved from this shell and the local vault, in the frozen forwarding order"
            .to_string(),
    });
}

/// Fill the `boot_env` rows whose producer is conditional from the SAME fact
/// `layer_model` reports -- that this run passes no flags -- so
/// `model.base_url_override: false` beside `ANTHROPIC_BASE_URL:
/// set_by_this_tier: true`, two fields of one object disagreeing about one fact,
/// cannot happen.
///
/// The row is selected by its PRODUCER, read back out of [`BOOT_ENV_TABLE`],
/// never by re-listing the conditional key names here. `PerInvocation` means
/// "written only under a flag" by definition, so a fifth such key added to the
/// table is answered the moment it is declared; a second list would have left it
/// shipping `unresolved` forever with nothing to notice.
fn layer_boot_env(report: &mut InfoReport) {
    for row in &mut report.boot_env {
        let producer = BOOT_ENV_TABLE
            .iter()
            .find(|(name, _)| *name == row.name)
            .map(|(_, producer)| producer);
        if matches!(producer, Some(BootEnvProducer::PerInvocation(_))) {
            row.set_by_this_tier = Maybe::Known(false);
        }
    }
}

fn recorded_runner(root: &Path) -> (Maybe<RecordedRunner>, Option<Diagnostic>) {
    match crate::state::load(root) {
        Ok(Some(state)) => {
            let recorded = PathBuf::from(&state.plugin_dir);
            let foreign = recorded
                .canonicalize()
                .map(|p| p != root)
                .unwrap_or(recorded != root);
            let diag = if foreign {
                Some(
                    Diag {
                        code: "state.foreign_runner",
                        kind: DiagnosticKind::State,
                        candidate: format!(
                            "{}/{}",
                            crate::state::STATE_DIR,
                            crate::state::STATE_FILE
                        ),
                        looked_for: "a runner record naming THIS bundle directory",
                        looked_in: vec![format!(
                            "{}/{}",
                            crate::state::STATE_DIR,
                            crate::state::STATE_FILE
                        )],
                        reason: format!(
                            "the recorded runner was started for {:?}, not for {:?}, so its \
                             container is serving a different bundle than the one described here",
                            state.plugin_dir,
                            root.display().to_string()
                        ),
                        fix: Some(
                            "run `curie skill down` then `curie skill up` from this bundle \
                             directory"
                                .to_string(),
                        ),
                    }
                    .into(),
                )
            } else {
                None
            };
            (
                Maybe::Known(RecordedRunner {
                    container_name: state.container_name,
                    image: state.image,
                    base_url: state.base_url,
                    fake_model: state.fake_model,
                    plugin_dir: state.plugin_dir,
                }),
                diag,
            )
        }
        Ok(None) => (
            Maybe::unavailable(
                "no runner is recorded for this bundle: `.curie/runner.json` does not exist, so \
                 nothing is running these bytes",
                "`curie skill up`, which writes the record",
            ),
            Some(
                Diag {
                    code: "state.runner_absent",
                    kind: DiagnosticKind::State,
                    candidate: format!("{}/{}", crate::state::STATE_DIR, crate::state::STATE_FILE),
                    looked_for: "a recorded local runner for this bundle",
                    looked_in: vec![format!(
                        "{}/{}",
                        crate::state::STATE_DIR,
                        crate::state::STATE_FILE
                    )],
                    reason: "no runner state file exists in this bundle, so no local runner is \
                         recorded and every model fact above is a would-resolve rather than a \
                         running one"
                        .to_string(),
                    fix: Some("run `curie skill up` to start one".to_string()),
                }
                .into(),
            ),
        ),
        Err(err) => {
            let reason = format!("the runner state file exists but could not be read ({err:#})");
            (
                Maybe::unresolved(reason.clone()),
                Some(
                    Diag {
                        code: "state.unreadable",
                        kind: DiagnosticKind::State,
                        candidate: format!(
                            "{}/{}",
                            crate::state::STATE_DIR,
                            crate::state::STATE_FILE
                        ),
                        looked_for: "a parseable runner state file",
                        looked_in: vec![format!(
                            "{}/{}",
                            crate::state::STATE_DIR,
                            crate::state::STATE_FILE
                        )],
                        reason,
                        fix: Some(
                            "run `curie skill down` to clear the stale record, then \
                             `curie skill up`"
                                .to_string(),
                        ),
                    }
                    .into(),
                ),
            )
        }
    }
}

/// The skill tier configures no memory namespace. Reuses `MEMORY_REASON`
/// VERBATIM so this answer and `skill memory`'s cannot drift.
fn layer_memory_note(report: &mut InfoReport) {
    report.diagnostics.push(
        Diag {
            code: "boot_env.not_set_at_this_tier",
            kind: DiagnosticKind::BootEnv,
            candidate: env_keys::CURIE_MEMORY_REF.to_string(),
            looked_for: "a memory namespace the booted runner would address",
            looked_in: vec![format!(
                "the boot environment `curie skill up` writes ({})",
                env_keys::CURIE_MEMORY_REF
            )],
            reason: commands::MEMORY_REASON.to_string(),
            fix: Some(
                "use `curie local info <agent>` or `curie cluster info <agent>` for a deployed \
                 agent, which has a memory namespace"
                    .to_string(),
            ),
        }
        .into(),
    );
}

/// Probe MCP load through the identical container path `skill check` runs.
///
/// Never propagates a failure: the probe failing is a fact about the
/// environment (no Docker daemon, a red verdict), not a verdict on this
/// command, and an inventory verb must not exit non-zero because Docker is
/// down. `check_outcome` is deliberately never called.
async fn probe_mcp(report: &mut InfoReport, root: &Path, image: String, timeout_s: u64) {
    let Maybe::Known(rows) = &mut report.mcp_servers else {
        return;
    };
    if rows.is_empty() {
        report.diagnostics.push(
            Diag {
                code: "mcp.not_probed",
                kind: DiagnosticKind::Mcp,
                candidate: MCP_FILE.to_string(),
                looked_for: "at least one declared server to probe",
                looked_in: vec![MCP_FILE.to_string()],
                reason: "--check-mcp was requested but this bundle declares no MCP server, so no \
                     container was booted; probing nothing would cost tens of seconds and \
                     answer nothing"
                    .to_string(),
                fix: None,
            }
            .into(),
        );
        return;
    }

    // The static pass already recorded "not probed" for each server; a real
    // verdict replaces it rather than sitting alongside it.
    report.diagnostics.retain(|d| d.code != "mcp.not_probed");

    let probe = commands::run_check_report(root.to_path_buf(), image, timeout_s).await;
    let mut extra: Vec<Diagnostic> = Vec::new();
    match probe {
        Err(_) => {
            // The probe error carries the container's whole stdout and stderr,
            // and that container has just executed this bundle's own MCP
            // servers -- their argv, their env and whatever they print. It is
            // real diagnostic material, so it stays where an unpasteable error
            // channel already holds it rather than being folded into an exit-0
            // report.
            let reason = "the MCP load probe could not be run: the runner container returned no \
                          usable check report. Its stdout and stderr are the bundle's own MCP \
                          servers talking, so they are not reproduced here; `curie skill check` \
                          reports them in full on the error channel"
                .to_string();
            for row in rows.iter_mut() {
                row.load = McpLoad::ProbeFailed;
                extra.push(
                    Diag {
                        code: "mcp.probe_failed",
                        kind: DiagnosticKind::Mcp,
                        candidate: row.name.clone(),
                        looked_for: "a load verdict from an offline runner container",
                        looked_in: vec!["the local Docker daemon".to_string()],
                        reason: reason.clone(),
                        fix: Some(
                            "start Docker (and build the runner image with `curie build`), then \
                             re-run with --check-mcp"
                                .to_string(),
                        ),
                    }
                    .into(),
                );
            }
        }
        Ok(check) => {
            for row in rows.iter_mut() {
                let matched = check.matches.iter().find(|m| m.declared == row.name);
                match matched {
                    Some(m) if m.connected && m.tool_count > 0 => row.load = McpLoad::Registered,
                    Some(m) if m.connected => {
                        row.load = McpLoad::RegisteredZeroTools;
                        extra.push(
                            Diag {
                                code: "mcp.registered_zero_tools",
                                kind: DiagnosticKind::Mcp,
                                candidate: row.name.clone(),
                                looked_for: "at least one tool from the registered server",
                                looked_in: vec![MCP_FILE.to_string()],
                                reason: format!(
                                    "{} registered but exposed zero tools, so the agent gains \
                                     nothing from it while the bundle looks correct",
                                    row.name
                                ),
                                fix: Some(
                                    "check the server's command/args, and forward its credential \
                                     with `curie skill up --secret <NAME>` if it needs one"
                                        .to_string(),
                                ),
                            }
                            .into(),
                        );
                    }
                    _ => {
                        row.load = McpLoad::DidNotRegister;
                        extra.push(
                            Diag {
                                code: "mcp.did_not_register",
                                kind: DiagnosticKind::Mcp,
                                candidate: row.name.clone(),
                                looked_for: "the declared server in the runner's registered set",
                                looked_in: vec![MCP_FILE.to_string()],
                                reason: format!(
                                    "{} is declared but never registered in the offline runner, \
                                     so its tools are silently absent at run time (the exact \
                                     green-on-fake class #337 exists to make visible)",
                                    row.name
                                ),
                                fix: Some(
                                    "read `curie skill check`'s printed reasons: fix the \
                                     server's command/args, forward its credential, or raise \
                                     --timeout"
                                        .to_string(),
                                ),
                            }
                            .into(),
                        );
                    }
                }
            }
            if check.verdict == "invalid_bundle" {
                for reason in &check.reasons {
                    extra.push(
                        Diag {
                            code: "mcp.probe_failed",
                            kind: DiagnosticKind::Mcp,
                            candidate: "the bundle's structure".to_string(),
                            looked_for: "a bundle the runner's check accepts",
                            looked_in: vec![MANIFEST_LOCATIONS[0].to_string()],
                            reason: format!("the runner refused to probe this bundle: {reason}"),
                            fix: Some(
                                "fix the reported bundle-structure error and re-run".to_string(),
                            ),
                        }
                        .into(),
                    );
                }
            }
        }
    }
    report.diagnostics.extend(extra);
}

async fn run_deployed(tier: Tier, opts: AgentActionOpts) -> Result<InfoOutput> {
    if opts.dry_run {
        return Ok(InfoOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![
                format!(
                    "GET {}/agents  (would resolve agent {:?} to <id>)",
                    opts.api_url, opts.agent
                ),
                format!("GET {}/agents/<id>/deployments", opts.api_url),
                format!("GET {}/agents/<id>/versions", opts.api_url),
                format!(
                    "GET {}/agents/<id>/versions/<vid>/files  (the in-force bundle's stored files)",
                    opts.api_url
                ),
            ],
        }));
    }

    let client = crate::api::ApiClient::new(&opts.api_url, &opts.api_key)?;
    // A nonexistent agent is not a bundle diagnosis: `find_agent`'s existing
    // error (and its exit class) stands unchanged.
    let agent = client.find_agent(&opts.agent).await?;
    let deployments = match client.list_deployments(&agent.id).await {
        Ok(d) => d,
        Err(_) => {
            return Ok(InfoOutput::Report(deployed_gap(
                tier,
                &agent,
                "deployed.bundle_unreadable",
                // Status and endpoint, never the response body: `expect_ok`
                // appends the server's text verbatim, and this report is meant
                // to be pasteable. `looked_in` already names the endpoint.
                "listing the agent's deployments over the platform API did not succeed. The API \
                 response is not reproduced here; every other verb reports it in full on the \
                 error channel"
                    .to_string(),
                "check the API is reachable and the key is authorized, then re-run",
            )));
        }
    };

    // Prod outranks dev, then most recent: `select_in_force_deployment` is
    // called rather than reimplemented.
    let Some(deployment) = commands::select_in_force_deployment(&deployments) else {
        return Ok(InfoOutput::Report(deployed_gap(
            tier,
            &agent,
            "deployed.no_active_deployment",
            "this agent has no active deployment, so nothing is running its bundle and there is \
             no in-force version to inspect. That is a real answer, not a failure"
                .to_string(),
            "run `curie <tier> deploy` to put a version in force",
        )));
    };
    let Some(version_id) = deployment.version_id.clone() else {
        return Ok(InfoOutput::Report(deployed_gap(
            tier,
            &agent,
            "deployed.bundle_unreadable",
            format!(
                "the in-force deployment {} reports no version id, so its bundle cannot be \
                 addressed",
                deployment.id
            ),
            "re-run `curie <tier> deploy` so the deployment names a version",
        )));
    };
    // Two independent reads of the same agent: neither takes anything the other
    // produces, so they go out together rather than one round trip after the
    // other. The error handling below is unchanged, including the early return.
    let (files, versions) = tokio::join!(
        client.bundle_files(&agent.id, &version_id),
        client.list_versions(&agent.id),
    );
    let files = match files {
        Ok(files) => files,
        Err(_) => {
            return Ok(InfoOutput::Report(deployed_gap(
                tier,
                &agent,
                "deployed.bundle_unreadable",
                // As above: the API response body is server-controlled text and
                // stays on the error channel.
                "fetching the deployed bundle's stored files over the platform API did not \
                 succeed. The API response is not reproduced here; every other verb reports it \
                 in full on the error channel"
                    .to_string(),
                "check the API is reachable and the key is authorized, then re-run",
            )));
        }
    };

    // Best effort: a version listing that fails costs the label and the content
    // hash, never the pass.
    let version = versions
        .unwrap_or_default()
        .into_iter()
        .find(|v| v.id == version_id);
    let (version_label, bundle_sha256) = match version {
        Some(v) => (v.version_label, v.bundle_sha256),
        None => (version_id.clone(), None),
    };

    let origin = BundleOrigin::Deployed {
        agent: agent.name.clone(),
        agent_id: agent.id.clone(),
        version_id,
        version_label,
        environment: deployment.environment.clone(),
        bundle_sha256,
        channel: agent.slack_channel.clone(),
    };
    let view = BundleView::from_files(
        origin,
        files.into_iter().map(|f| (f.path, f.content)).collect(),
    );
    let mut report = discover(&view);
    report.tier = Maybe::Known(tier.as_str().to_string());
    sort_diagnostics(&mut report.diagnostics);
    Ok(InfoOutput::Report(report))
}

/// A deployed report for a bundle that could not be reached at all. Exit 0 with
/// a `deployed.*` code: the distinction "the manifest declares nothing" vs "the
/// lookup did not complete" (#607) is reproduced here, and this is deliberately
/// NOT the skill tier's exit-2 no-manifest path, which would misreport a
/// platform-side gap as a bad `--plugin-dir`.
///
/// The pass is deliberately NOT run for a gap. Running `discover` over an empty
/// file set would answer from files that were never fetched -- `skills: []`,
/// `mcp_servers: []`, `evals` absent, every `artifacts[].exists: false` -- which
/// is #607's distinction undone one level down, and worse than the disk case:
/// an agent polling right after `deploy` would read a clean-looking empty
/// inventory instead of "nothing is in force yet". Every bundle-derived fact is
/// therefore `unresolved` with the gap's own reason, and `artifacts` carries no
/// rows at all rather than rows asserting a path was absent from a bundle this
/// CLI never read.
fn deployed_gap(
    tier: Tier,
    agent: &crate::api::Agent,
    code: &'static str,
    reason: String,
    fix: &str,
) -> InfoReport {
    let origin = BundleOrigin::Deployed {
        agent: agent.name.clone(),
        agent_id: agent.id.clone(),
        version_id: String::new(),
        version_label: String::new(),
        environment: String::new(),
        bundle_sha256: None,
        channel: agent.slack_channel.clone(),
    };
    let mut report = InfoReport {
        info: INFO_FAMILY.to_string(),
        version: INFO_REPORT_VERSION,
        tier: Maybe::Known(tier.as_str().to_string()),
        bundle: Maybe::unresolved(reason.clone()),
        skills: Maybe::unresolved(reason.clone()),
        mcp_servers: Maybe::unresolved(reason.clone()),
        secrets: Maybe::unresolved(reason.clone()),
        boot_env: boot_env_rows(&origin),
        approval_gates: Maybe::unresolved(reason.clone()),
        evals: Maybe::unresolved(reason.clone()),
        channel: channel_for(&origin),
        comms: comms_for(&origin),
        model: model_for(&origin),
        artifacts: Vec::new(),
        diagnostics: vec![boot_env_scope_diagnostic()],
    };
    report.diagnostics.push(
        Diag {
            code,
            kind: DiagnosticKind::Deployed,
            candidate: agent.name.clone(),
            looked_for: "the in-force version's stored bundle files",
            looked_in: vec![
                format!("GET /agents/{}/deployments", agent.id),
                format!("GET /agents/{}/versions/<vid>/files", agent.id),
            ],
            reason,
            fix: Some(fix.to_string()),
        }
        .into(),
    );
    sort_diagnostics(&mut report.diagnostics);
    report
}

// ---------------------------------------------------------------------------
// Unit tests for the pure classifiers
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_path_collapses_dot_slash_and_backslashes() {
        assert_eq!(normalize_path("./skills/x/SKILL.md"), "skills/x/SKILL.md");
        assert_eq!(normalize_path("skills\\x\\SKILL.md"), "skills/x/SKILL.md");
        assert_eq!(normalize_path("/skills/x/SKILL.md"), "skills/x/SKILL.md");
        assert_eq!(normalize_path(".//skills/x"), "skills/x");
    }

    #[test]
    fn frontmatter_reports_missing_invalid_and_parsed_distinctly() {
        assert!(matches!(
            parse_frontmatter("# Plain\n\nno block\n"),
            FrontmatterOutcome::Missing
        ));
        assert!(matches!(
            parse_frontmatter("---\nname: x\n"),
            FrontmatterOutcome::Invalid(_)
        ));
        let parsed = parse_frontmatter(
            "---\nname: sample\ndescription: A sample.\nallowed-tools:\n  - WebSearch\n---\n",
        );
        match parsed {
            FrontmatterOutcome::Parsed(fm) => {
                assert_eq!(fm.name, "sample");
                assert_eq!(fm.allowed_tools, vec!["WebSearch".to_string()]);
                assert!(fm.confusable_keys.is_empty());
            }
            _ => panic!("a well-formed block must parse"),
        }
    }

    #[test]
    fn a_confusable_tools_key_is_recorded_without_unregistering_the_skill() {
        let parsed = parse_frontmatter("---\nname: x\ntools:\n  - WebSearch\n---\n");
        match parsed {
            FrontmatterOutcome::Parsed(fm) => {
                assert_eq!(fm.confusable_keys, vec!["tools".to_string()]);
                // The confusable key grants nothing, which is the whole point.
                assert!(fm.allowed_tools.is_empty());
            }
            _ => panic!("a confusable key must not stop the skill registering"),
        }
    }

    #[test]
    fn the_two_sentinels_never_share_a_discriminator() {
        let unavailable = serde_json::to_value(Maybe::<bool>::unavailable("r", "w")).unwrap();
        let unresolved = serde_json::to_value(Maybe::<bool>::unresolved("r")).unwrap();
        assert!(unavailable.get("resolved").is_none());
        assert!(unresolved.get("available").is_none());
        assert_eq!(unavailable["available"], serde_json::json!(false));
        assert_eq!(unresolved["resolved"], serde_json::json!(false));
    }

    #[test]
    fn excluded_matches_any_segment() {
        assert!(excluded(".curie/runner.json"));
        assert!(excluded("skills/x/__pycache__/y.pyc"));
        assert!(!excluded("skills/x/SKILL.md"));
    }
}
