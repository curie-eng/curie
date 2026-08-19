//! Package a plugin bundle directory into a tar.gz archive for upload.
//!
//! The platform API (B2) accepts tar.gz/tar/zip, extracts, and validates via
//! the frozen plugin-format package. Workstation state (`.curie/`, `.git/`,
//! virtualenvs, `node_modules`, tool caches) never belongs in an immutable
//! bundle, so it is excluded here, and a bundle can declare its own exclusions
//! in a root `.curieignore` (see [`Exclusions::load`]).

use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{bail, Context, Result};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use sha2::{Digest, Sha256};

/// Name of the optional per-bundle ignore file, read from the bundle root.
const IGNORE_FILE: &str = ".curieignore";

/// Entry names never packed, matched against any file or directory with that
/// name at any depth.
const EXCLUDED_NAMES: &[&str] = &[
    IGNORE_FILE,
    ".curie",
    ".git",
    ".venv",
    "venv",
    "node_modules",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
];

/// Entries the archive skips: the built-in names plus whatever the bundle's
/// `.curieignore` declares.
struct Exclusions {
    /// Bare names, matched against any entry at any depth.
    names: Vec<String>,
    /// Bundle-root-relative paths, matched against the exact entry.
    paths: Vec<PathBuf>,
}

impl Exclusions {
    /// Read `.curieignore` from the bundle root, if present.
    ///
    /// One pattern per line, with `#` comments and blank lines skipped and
    /// surrounding whitespace plus a trailing `/` stripped. A pattern with no
    /// `/` matches any entry with that name at any depth, the same shape as
    /// the built-in defaults; a pattern containing `/` is a
    /// bundle-root-relative path matching that exact entry and its subtree.
    /// There is no glob support, so an odd pattern simply never matches, and a
    /// pattern that could reach outside the bundle is dropped. The ignore file
    /// is stat'd without following links, so a symlinked `.curieignore` is
    /// refused rather than read from outside the bundle root.
    fn load(root: &Path) -> Result<Self> {
        let mut exclusions = Self {
            names: EXCLUDED_NAMES.iter().map(|n| (*n).to_string()).collect(),
            paths: Vec::new(),
        };
        let ignore_path = root.join(IGNORE_FILE);
        match std::fs::symlink_metadata(&ignore_path) {
            Ok(meta) if meta.file_type().is_symlink() => bail!(
                "symlinks are not supported in plugin bundles: {} (the {} file must be a regular file inside the bundle)",
                ignore_path.display(),
                IGNORE_FILE
            ),
            Ok(meta) if meta.is_file() => {
                let text = std::fs::read_to_string(&ignore_path)
                    .with_context(|| format!("reading {}", ignore_path.display()))?;
                exclusions.extend_from(&text);
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err).with_context(|| format!("stat {}", ignore_path.display())),
        }
        Ok(exclusions)
    }

    fn extend_from(&mut self, text: &str) {
        for line in text.lines() {
            let pattern = line.trim().trim_end_matches('/');
            if pattern.is_empty() || pattern.starts_with('#') {
                continue;
            }
            let candidate = Path::new(pattern);
            // Anything that is not a plain relative component (a root, a drive
            // prefix, `.`, `..`) could reach outside the bundle, so drop it.
            if candidate
                .components()
                .any(|c| !matches!(c, Component::Normal(_)))
            {
                continue;
            }
            if pattern.contains('/') {
                self.paths.push(candidate.to_path_buf());
            } else {
                self.names.push(pattern.to_string());
            }
        }
    }

    fn is_excluded(&self, rel: &Path) -> bool {
        let name = rel.file_name().unwrap_or_default();
        self.names.iter().any(|n| name == std::ffi::OsStr::new(n))
            || self.paths.iter().any(|p| rel == p)
    }
}

fn append_dir(
    builder: &mut tar::Builder<GzEncoder<Vec<u8>>>,
    root: &Path,
    dir: &Path,
    exclusions: &Exclusions,
) -> Result<()> {
    // Sorted for deterministic archives: same tree, same byte layout order.
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .collect::<std::io::Result<_>>()?;
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let path = entry.path();
        let rel = path.strip_prefix(root).expect("entry is under root");
        // Exclusion runs before the symlink check and applies to every entry
        // type, so an excluded dir that is itself a link is skipped, not an
        // error. Everything that survives still hits the guard below.
        if exclusions.is_excluded(rel) {
            continue;
        }
        // file_type() does not follow symlinks: a link inside the bundle would
        // otherwise be dereferenced by tar and upload host files from outside
        // the plugin root (e.g. a link into ~/.ssh). Refuse loudly instead.
        let file_type = entry
            .file_type()
            .with_context(|| format!("stat {}", path.display()))?;
        if file_type.is_symlink() {
            bail!(
                "symlinks are not supported in plugin bundles: {} (if this is workstation state, exclude it by adding a line to a root {} file)",
                path.display(),
                IGNORE_FILE
            );
        }
        if file_type.is_dir() {
            append_dir(builder, root, &path, exclusions)?;
        } else if file_type.is_file() {
            builder
                .append_path_with_name(&path, rel)
                .with_context(|| format!("archiving {}", path.display()))?;
        }
    }
    Ok(())
}

/// Tar-gzip the bundle at `dir`, returning the archive bytes.
pub fn pack_tar_gz(dir: &Path) -> Result<Vec<u8>> {
    if !dir.is_dir() {
        bail!("bundle path is not a directory: {}", dir.display());
    }
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut builder = tar::Builder::new(encoder);
    let exclusions = Exclusions::load(dir)?;
    append_dir(&mut builder, dir, dir, &exclusions)?;
    let encoder = builder.into_inner().context("finalizing tar archive")?;
    encoder.finish().context("finalizing gzip stream")
}

/// A materialized, content-addressed copy of a bundle source tree (#1087).
///
/// The skill tier runs one of these rather than the editable source, matching
/// what the local and cluster tiers already do: they fetch and extract a stored
/// bundle, so a host edit cannot reach a running container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleSnapshot {
    /// sha256 of the [`pack_tar_gz`] bytes for the source tree, lowercase hex.
    ///
    /// That is the same archive `local deploy` / `cluster deploy` upload, and
    /// `apps/api/src/curie_api/deploy.py` records `sha256` of exactly those
    /// bytes -- the identical computation over the identical archive, so the
    /// same packed bytes always yield the same digest here and at the API.
    /// It identifies an ARCHIVE, not a source tree: [`pack_tar_gz`] embeds
    /// per-file mtime, uid and gid, so the same content checked out fresh on
    /// another machine packs to different bytes and a different digest. Do not
    /// read this as a stable per-source identifier across machines or clones.
    pub digest: String,
    /// The directory holding the extracted archive, mounted at `/plugin`.
    pub dir: PathBuf,
}

/// Directory under the bundle's state dir holding every materialized snapshot.
/// The single definition the layout and the containment guard both derive from.
pub const SNAPSHOT_SUBDIR: &str = "snapshots";

/// Where every snapshot of the bundle at `source` lives:
/// `<source>/.curie/snapshots/`.
///
/// Snapshots sit under the bundle's own state dir because that is already where
/// this tier keeps per-bundle state, it is already gitignored by the `init`
/// scaffold, and `.curie` is already in [`EXCLUDED_NAMES`] -- so a snapshot can
/// never feed back into a later pack.
pub fn snapshot_root(source: &Path) -> PathBuf {
    source.join(crate::state::STATE_DIR).join(SNAPSHOT_SUBDIR)
}

/// Pack `source`, then extract the archive into a directory under
/// [`snapshot_root`]: `dir_name` when one is given, the packed digest otherwise.
///
/// `dir_name` is what keeps an ephemeral snapshot off the canonical
/// `<digest>/` path; the digest is still computed and reported either way.
///
/// `dir_name` must be a single ordinary path component: non-empty, no path
/// separator, and not `.` or `..`. That contract is enforced, not assumed,
/// because the destination is handed straight to `remove_dir_all` one line
/// later. An empty name would resolve the destination to [`snapshot_root`]
/// itself and wipe every snapshot under it, including the canonical one a live
/// `skill up` runner has mounted at `/plugin`; a separator or `..` component
/// could resolve outside the root entirely.
///
/// Packing happens before anything is written, so a bundle the packer refuses
/// (a symlink, an unreadable entry) leaves nothing materialized. An existing
/// destination is removed rather than extracted over, which makes a partial
/// extract from a crashed run self-heal and a re-snapshot of unchanged source
/// idempotent.
fn snapshot_into(source: &Path, dir_name: Option<&str>) -> Result<BundleSnapshot> {
    if let Some(name) = dir_name {
        let mut components = Path::new(name).components();
        let single_plain =
            matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none();
        if !single_plain {
            bail!(
                "refusing to snapshot into '{name}': a snapshot directory name must be a single path component, with no separator and no '.' or '..'"
            );
        }
    }
    let bytes = pack_tar_gz(source)?;
    let digest: String = Sha256::digest(&bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let dest = snapshot_root(source).join(dir_name.unwrap_or(&digest));
    if dest.exists() {
        std::fs::remove_dir_all(&dest)
            .with_context(|| format!("clearing the bundle snapshot at {}", dest.display()))?;
    }
    std::fs::create_dir_all(&dest)
        .with_context(|| format!("creating the bundle snapshot at {}", dest.display()))?;
    tar::Archive::new(GzDecoder::new(&bytes[..]))
        .unpack(&dest)
        .with_context(|| format!("extracting the bundle snapshot into {}", dest.display()))?;
    Ok(BundleSnapshot { digest, dir: dest })
}

/// Materialize the canonical snapshot of `source` at
/// `<source>/.curie/snapshots/<digest>/`.
///
/// Content-addressed, and owned by a recorded runner: it lives until that
/// runner is torn down. Unchanged source re-snapshots onto the same directory.
pub fn snapshot(source: &Path) -> Result<BundleSnapshot> {
    snapshot_into(source, None)
}

/// Materialize a throwaway snapshot of `source` at
/// `<source>/.curie/snapshots/sweep-<secs>-<pid>-<seq>/`.
///
/// Owned by one `eval --model` sweep or one `curie scenario` run, and deleted
/// when that sweep or run ends. The directory name is unique per call by
/// construction and is deliberately NOT the canonical `<digest>/` path: a sweep
/// or a scenario on unchanged source would otherwise resolve onto the very
/// directory a live `skill up` runner has mounted at `/plugin`, and its own
/// cleanup would delete it out from under a healthy container. Do not
/// "simplify" this back onto the digest path.
pub fn snapshot_ephemeral(source: &Path) -> Result<BundleSnapshot> {
    // Seconds plus pid read well in a directory listing but collide for two
    // sweeps in the same second in the same process, so the counter is what
    // actually carries uniqueness within a process.
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let name = format!(
        "sweep-{secs}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    snapshot_into(source, Some(&name))
}

/// Remove a snapshot directory recorded for the bundle at `source`.
///
/// `recorded_dir` is read back out of `.curie/runner.json`, so it is only ever
/// removed after it is proved to lie under [`snapshot_root`]. Both sides are
/// canonicalized before the containment check: production records an absolute
/// path but tears down with a relative source (`Path::new(".")`), and
/// canonicalizing resolves that mismatch as well as `..` traversal and a
/// recorded path that is a symlink out of the root.
///
/// A directory that is already gone is not an error -- there is nothing to
/// remove. A missing snapshot root is: no path can be contained in a root that
/// does not exist, so the removal fails closed rather than deleting anything.
pub fn remove_snapshot(recorded_dir: &Path, source: &Path) -> Result<()> {
    // Before canonicalizing: `canonicalize` errors on a path that does not
    // exist, which would turn "already gone" into a failed teardown.
    if !recorded_dir.exists() {
        return Ok(());
    }
    let root = snapshot_root(source);
    let canonical_root = root.canonicalize().with_context(|| {
        format!(
            "refusing to remove '{}': the bundle snapshot root {} does not exist",
            recorded_dir.display(),
            root.display()
        )
    })?;
    let canonical_recorded = recorded_dir.canonicalize().with_context(|| {
        format!(
            "resolving the bundle snapshot at {}",
            recorded_dir.display()
        )
    })?;
    if !canonical_recorded.starts_with(&canonical_root) {
        bail!(
            "refusing to remove '{}': it is not inside the bundle snapshot root {}",
            canonical_recorded.display(),
            canonical_root.display()
        );
    }
    std::fs::remove_dir_all(&canonical_recorded).with_context(|| {
        format!(
            "removing the bundle snapshot at {}",
            canonical_recorded.display()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;

    fn entry_names(bytes: &[u8]) -> Vec<String> {
        let mut archive = tar::Archive::new(GzDecoder::new(bytes));
        archive
            .entries()
            .unwrap()
            .map(|e| e.unwrap().path().unwrap().to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn packs_the_bundle_and_excludes_workstation_state() {
        let dir = tempfile::tempdir().unwrap();
        crate::scaffold::scaffold(dir.path(), "deal-desk").unwrap();
        std::fs::create_dir_all(dir.path().join(".curie")).unwrap();
        std::fs::write(dir.path().join(".curie/runner.json"), "{}").unwrap();
        std::fs::create_dir_all(dir.path().join(".git")).unwrap();
        std::fs::write(dir.path().join(".git/HEAD"), "ref").unwrap();

        let names = entry_names(&pack_tar_gz(dir.path()).unwrap());
        assert!(names.contains(&".claude-plugin/plugin.json".to_string()));
        assert!(names.contains(&"skills/deal-desk/SKILL.md".to_string()));
        assert!(names.contains(&".mcp.json".to_string()));
        assert!(!names.iter().any(|n| n.starts_with(".curie")));
        assert!(!names.iter().any(|n| n.starts_with(".git/")));
    }

    #[test]
    fn refuses_a_missing_directory() {
        assert!(pack_tar_gz(Path::new("/nonexistent/bundle")).is_err());
    }

    #[test]
    fn excludes_virtualenvs_caches_and_vendored_dependencies() {
        let dir = tempfile::tempdir().unwrap();
        crate::scaffold::scaffold(dir.path(), "deal-desk").unwrap();
        for excluded in [
            "venv",
            "node_modules",
            "__pycache__",
            ".mypy_cache",
            ".pytest_cache",
        ] {
            std::fs::create_dir_all(dir.path().join("skills/deal-desk").join(excluded)).unwrap();
            std::fs::write(
                dir.path()
                    .join("skills/deal-desk")
                    .join(excluded)
                    .join("junk"),
                "junk",
            )
            .unwrap();
        }

        let names = entry_names(&pack_tar_gz(dir.path()).unwrap());
        assert!(names.contains(&"skills/deal-desk/SKILL.md".to_string()));
        assert!(
            !names.iter().any(|n| n.contains("junk")),
            "excluded trees leaked: {names:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn excludes_a_virtualenv_whose_entries_are_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        crate::scaffold::scaffold(dir.path(), "deal-desk").unwrap();
        let host = tempfile::tempdir().unwrap();
        std::fs::write(host.path().join("python"), "binary").unwrap();
        std::fs::create_dir_all(dir.path().join(".venv/bin")).unwrap();
        std::os::unix::fs::symlink(
            host.path().join("python"),
            dir.path().join(".venv/bin/python"),
        )
        .unwrap();

        let names = entry_names(&pack_tar_gz(dir.path()).unwrap());
        assert!(names.contains(&"skills/deal-desk/SKILL.md".to_string()));
        assert!(
            !names.iter().any(|n| n.starts_with(".venv")),
            "virtualenv leaked: {names:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn excludes_a_virtualenv_that_is_itself_a_symlink() {
        let dir = tempfile::tempdir().unwrap();
        crate::scaffold::scaffold(dir.path(), "deal-desk").unwrap();
        let host = tempfile::tempdir().unwrap();
        std::fs::write(host.path().join("id_rsa"), "private").unwrap();
        std::os::unix::fs::symlink(host.path(), dir.path().join(".venv")).unwrap();

        let names = entry_names(&pack_tar_gz(dir.path()).unwrap());
        assert!(!names.iter().any(|n| n.contains("id_rsa")), "{names:?}");
    }

    #[cfg(unix)]
    #[test]
    fn refuses_a_symlink_in_a_normal_subdirectory() {
        let dir = tempfile::tempdir().unwrap();
        crate::scaffold::scaffold(dir.path(), "deal-desk").unwrap();
        let secret = tempfile::tempdir().unwrap();
        std::fs::write(secret.path().join("id_rsa"), "private").unwrap();
        std::os::unix::fs::symlink(
            secret.path().join("id_rsa"),
            dir.path().join("skills/deal-desk/key"),
        )
        .unwrap();

        let err = pack_tar_gz(dir.path()).unwrap_err();
        assert!(
            err.to_string().contains("symlinks are not supported"),
            "{err}"
        );
    }

    #[test]
    fn honors_the_curieignore_file_and_omits_it_from_the_archive() {
        let dir = tempfile::tempdir().unwrap();
        crate::scaffold::scaffold(dir.path(), "deal-desk").unwrap();
        std::fs::create_dir_all(dir.path().join("skills/deal-desk/fixtures")).unwrap();
        std::fs::write(dir.path().join("skills/deal-desk/fixtures/big.bin"), "x").unwrap();
        std::fs::create_dir_all(dir.path().join("docs")).unwrap();
        std::fs::write(dir.path().join("docs/notes.md"), "notes").unwrap();
        std::fs::write(dir.path().join("docs/keep.md"), "keep").unwrap();
        std::fs::write(
            dir.path().join(".curieignore"),
            "# a comment\n\n  fixtures/  \ndocs/notes.md\n",
        )
        .unwrap();

        let names = entry_names(&pack_tar_gz(dir.path()).unwrap());
        assert!(names.contains(&"docs/keep.md".to_string()), "{names:?}");
        assert!(!names.contains(&"docs/notes.md".to_string()), "{names:?}");
        assert!(
            !names.iter().any(|n| n.contains("fixtures")),
            "bare-name pattern did not match at depth: {names:?}"
        );
        assert!(!names.contains(&".curieignore".to_string()), "{names:?}");
    }

    #[test]
    fn ignores_curieignore_patterns_that_reach_outside_the_bundle() {
        let dir = tempfile::tempdir().unwrap();
        crate::scaffold::scaffold(dir.path(), "deal-desk").unwrap();
        std::fs::create_dir_all(dir.path().join("docs")).unwrap();
        std::fs::write(dir.path().join("docs/keep.md"), "keep").unwrap();
        std::fs::write(
            dir.path().join(".curieignore"),
            "/etc/passwd\n../outside\n../../docs/keep.md\n./docs/keep.md\ndocs/notes.md\n",
        )
        .unwrap();

        // Assert on the parsed exclusions, not just the archive: an escaping
        // pattern can never match a bundle-relative entry anyway, so an
        // archive-only assertion passes even with the guard removed.
        let exclusions = Exclusions::load(dir.path()).unwrap();
        assert_eq!(
            exclusions.paths,
            vec![PathBuf::from("docs/notes.md")],
            "escaping patterns were not dropped: {:?}",
            exclusions.paths
        );
        assert!(
            !exclusions.names.iter().any(|n| n.contains("passwd")),
            "{:?}",
            exclusions.names
        );

        let names = entry_names(&pack_tar_gz(dir.path()).unwrap());
        assert!(names.contains(&"docs/keep.md".to_string()), "{names:?}");
        assert!(names.contains(&".mcp.json".to_string()), "{names:?}");
        assert!(!names.iter().any(|n| n.contains("passwd")), "{names:?}");
    }

    #[cfg(unix)]
    #[test]
    fn refuses_an_curieignore_that_is_a_symlink() {
        let dir = tempfile::tempdir().unwrap();
        crate::scaffold::scaffold(dir.path(), "deal-desk").unwrap();
        let host = tempfile::tempdir().unwrap();
        std::fs::write(host.path().join("credentials"), "secret\n").unwrap();
        std::os::unix::fs::symlink(
            host.path().join("credentials"),
            dir.path().join(".curieignore"),
        )
        .unwrap();

        let err = pack_tar_gz(dir.path()).unwrap_err();
        assert!(
            err.to_string().contains("symlinks are not supported"),
            "{err}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn refuses_symlinks_instead_of_dereferencing_them() {
        let dir = tempfile::tempdir().unwrap();
        crate::scaffold::scaffold(dir.path(), "deal-desk").unwrap();
        let secret = tempfile::tempdir().unwrap();
        std::fs::write(secret.path().join("id_rsa"), "private").unwrap();
        std::os::unix::fs::symlink(secret.path(), dir.path().join("skills/data")).unwrap();

        let err = pack_tar_gz(dir.path()).unwrap_err();
        assert!(
            err.to_string().contains("symlinks are not supported"),
            "{err}"
        );
    }

    // ───────────────────────────────────────────────────────────────────────
    // #1087: the skill tier executes an immutable, content-addressed snapshot
    // rather than the editable source tree. Every assertion below terminates in
    // a filesystem outcome -- bytes read back from under the snapshot dir, a
    // directory that does or does not exist -- never in a struct field alone.
    // ───────────────────────────────────────────────────────────────────────

    /// The digest the deploy path computes for the same bytes:
    /// `hashlib.sha256(data).hexdigest()` over `pack_tar_gz` output
    /// (`apps/api/src/curie_api/deploy.py`). Computed independently here so the
    /// parity claim is proved by execution rather than asserted in a comment.
    fn sha256_hex(bytes: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        Sha256::digest(bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    fn read(path: &Path) -> String {
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
    }

    /// A real scaffolded bundle plus one nested file whose bytes the snapshot
    /// tests read back. The scaffold is the shape `curie init` produces, so no
    /// fixture here can drift from what a user actually snapshots.
    fn bundle(note: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        crate::scaffold::scaffold(dir.path(), "deal-desk").unwrap();
        std::fs::create_dir_all(dir.path().join("docs")).unwrap();
        std::fs::write(dir.path().join("docs/notes.md"), note).unwrap();
        dir
    }

    /// Serializes the one test that changes the PROCESS working directory
    /// against itself and any future sibling, the same discipline
    /// `cli/tests/chat_enqueue.rs` uses for its cwd-sensitive case.
    static CWD_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Restores the process working directory when dropped, so a panicking
    /// assertion cannot strand the rest of the binary in a temp directory.
    struct CwdGuard(PathBuf);

    impl Drop for CwdGuard {
        fn drop(&mut self) {
            let _ = std::env::set_current_dir(&self.0);
        }
    }

    #[test]
    fn snapshot_digest_equals_sha256_of_the_packed_archive() {
        let dir = bundle("original");
        // The SAME function `deploy` packs with, executed here: the skill tier's
        // digest is byte-identical to the one the API records for this source.
        let expected = sha256_hex(&pack_tar_gz(dir.path()).unwrap());

        let snap = snapshot(dir.path()).unwrap();

        assert_eq!(
            snap.digest, expected,
            "the recorded digest must be sha256 of the pack_tar_gz bytes"
        );
        assert_eq!(
            snap.dir,
            snapshot_root(dir.path()).join(&expected),
            "the canonical snapshot is content-addressed by that digest"
        );
        assert!(snap.dir.is_dir(), "the snapshot dir must be materialized");
    }

    #[test]
    fn snapshot_materializes_the_packed_contents_not_the_live_tree() {
        let dir = bundle("original");
        let skill = dir.path().join("skills/deal-desk/SKILL.md");
        let packed_skill = read(&skill);

        let snap = snapshot(dir.path()).unwrap();

        // Edit the source AFTER snapshotting: a snapshot that symlinked,
        // hard-linked or bind-mounted the source would now read back the edit.
        std::fs::write(dir.path().join("docs/notes.md"), "edited after boot").unwrap();
        std::fs::write(&skill, "edited after boot").unwrap();

        assert_eq!(
            read(&snap.dir.join("docs/notes.md")),
            "original",
            "the mounted snapshot must still hold the bytes packed at boot"
        );
        assert_eq!(
            read(&snap.dir.join("skills/deal-desk/SKILL.md")),
            packed_skill,
            "a source edit must not reach the snapshot the runner has mounted"
        );
    }

    #[test]
    fn snapshot_excludes_the_state_dir_so_snapshots_never_nest() {
        let dir = bundle("original");
        std::fs::create_dir_all(dir.path().join(".curie")).unwrap();
        std::fs::write(dir.path().join(".curie/runner.json"), "{}").unwrap();

        let snap = snapshot(dir.path()).unwrap();

        assert!(
            !snap.dir.join(".curie").exists(),
            "the state dir must never be packed into a snapshot"
        );
        assert!(
            !snap.dir.join(".curie/snapshots").exists(),
            "a snapshot inside a snapshot is the feedback loop this closes"
        );
        assert!(snap.dir.join("docs/notes.md").is_file());
    }

    #[test]
    fn snapshot_honors_a_curieignore_entry() {
        let dir = bundle("keep me");
        let with_notes = snapshot(dir.path()).unwrap();
        assert!(with_notes.dir.join("docs/notes.md").is_file());

        std::fs::write(dir.path().join(".curieignore"), "docs/notes.md\n").unwrap();
        let without_notes = snapshot(dir.path()).unwrap();

        assert!(
            !without_notes.dir.join("docs/notes.md").exists(),
            "an ignored entry must not be materialized into the snapshot"
        );
        assert_ne!(
            without_notes.digest, with_notes.digest,
            "changing the packing directives changes the artifact, so the digest moves"
        );
    }

    #[test]
    fn snapshot_digest_is_stable_across_repeated_runs() {
        let dir = bundle("original");

        let first = snapshot(dir.path()).unwrap();
        let second = snapshot(dir.path()).unwrap();

        assert_eq!(first.digest, second.digest);
        assert_eq!(first.dir, second.dir);
        assert_eq!(
            read(&second.dir.join("docs/notes.md")),
            "original",
            "re-snapshotting an untouched tree must leave a complete snapshot behind"
        );
    }

    #[test]
    fn snapshot_digest_changes_when_source_content_changes() {
        let dir = bundle("original");
        let first = snapshot(dir.path()).unwrap();

        std::fs::write(dir.path().join("docs/notes.md"), "changed content").unwrap();
        let second = snapshot(dir.path()).unwrap();

        assert_ne!(
            first.digest, second.digest,
            "changed source content must package as a new digest"
        );
        assert_ne!(first.dir, second.dir);
        assert_eq!(read(&first.dir.join("docs/notes.md")), "original");
        assert_eq!(read(&second.dir.join("docs/notes.md")), "changed content");
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_refuses_a_bundle_containing_a_symlink() {
        let dir = bundle("original");
        let secret = tempfile::tempdir().unwrap();
        std::fs::write(secret.path().join("id_rsa"), "private").unwrap();
        let link = dir.path().join("skills/deal-desk/key");
        std::os::unix::fs::symlink(secret.path().join("id_rsa"), &link).unwrap();

        let err = snapshot(dir.path()).unwrap_err();

        let message = format!("{err:#}");
        assert!(
            message.contains("symlinks are not supported"),
            "the packer's own refusal must reach the skill path: {message}"
        );
        assert!(
            message.contains(&link.display().to_string()),
            "the error must name the offending path: {message}"
        );
        assert!(
            message.contains(".curieignore"),
            "the error must point at the remedy: {message}"
        );
        assert!(
            !snapshot_root(dir.path()).exists(),
            "a refused bundle must leave nothing materialized"
        );
    }

    #[test]
    fn snapshot_recreates_a_partially_extracted_directory() {
        let dir = bundle("original");
        let expected = sha256_hex(&pack_tar_gz(dir.path()).unwrap());
        // What a run killed between create_dir_all and unpack leaves behind.
        let dest = snapshot_root(dir.path()).join(&expected);
        std::fs::create_dir_all(&dest).unwrap();
        std::fs::write(dest.join("junk-from-a-crashed-run"), "junk").unwrap();

        let snap = snapshot(dir.path()).unwrap();

        assert_eq!(snap.dir, dest);
        assert!(
            !dest.join("junk-from-a-crashed-run").exists(),
            "a partial extract must be removed, not extracted over"
        );
        assert_eq!(read(&dest.join("docs/notes.md")), "original");
    }

    #[test]
    fn snapshot_ephemeral_never_resolves_to_the_canonical_digest_path() {
        let dir = bundle("original");
        let canonical = snapshot(dir.path()).unwrap();
        let sweep = snapshot_ephemeral(dir.path()).unwrap();
        let other_sweep = snapshot_ephemeral(dir.path()).unwrap();

        let name = sweep
            .dir
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert_ne!(
            sweep.dir, canonical.dir,
            "a sweep must never own the canonical digest directory"
        );
        assert_ne!(
            name, sweep.digest,
            "the ephemeral directory must not be named by the digest"
        );
        assert!(
            name.starts_with("sweep-"),
            "unexpected sweep dir name: {name}"
        );
        assert_ne!(
            sweep.dir, other_sweep.dir,
            "two sweeps must get two directories, uniqueness by construction"
        );
        assert!(sweep.dir.starts_with(snapshot_root(dir.path())));
        assert!(other_sweep.dir.starts_with(snapshot_root(dir.path())));
        assert_eq!(
            sweep.digest, canonical.digest,
            "same source, same content digest, different directory"
        );
        assert_eq!(read(&sweep.dir.join("docs/notes.md")), "original");

        // The hazard, closed: the sweep's own cleanup must not be able to pull
        // /plugin out from under a live `skill up` runner mounting the
        // canonical snapshot of the same unchanged source.
        remove_snapshot(&sweep.dir, dir.path()).unwrap();
        assert!(!sweep.dir.exists(), "the sweep removes what it created");
        assert!(
            canonical.dir.is_dir(),
            "the recorded runner's mount must survive a sweep's cleanup"
        );
        assert_eq!(read(&canonical.dir.join("docs/notes.md")), "original");
    }

    #[test]
    fn snapshot_into_refuses_a_name_that_is_not_a_single_path_component() {
        let dir = bundle("original");
        // A live canonical snapshot first: it is exactly what a running
        // `skill up` runner has mounted at /plugin, and it is what an unguarded
        // `remove_dir_all` of a bad destination would take out.
        let canonical = snapshot(dir.path()).unwrap();
        let root = snapshot_root(dir.path());

        for name in ["", ".", "..", "sweep/../..", "a/b", "../outside"] {
            let err = snapshot_into(dir.path(), Some(name)).unwrap_err();
            assert!(
                format!("{err:#}").contains("a single path component"),
                "unexpected refusal for {name:?}: {err:#}"
            );
            // The outcome that matters: nothing was deleted.
            assert!(
                root.is_dir(),
                "the snapshot root must survive the refused name {name:?}"
            );
            assert!(
                canonical.dir.is_dir(),
                "the live runner's mount must survive the refused name {name:?}"
            );
            assert_eq!(
                read(&canonical.dir.join("docs/notes.md")),
                "original",
                "nothing under the snapshot root may be deleted for {name:?}"
            );
        }
    }

    #[test]
    fn snapshot_into_accepts_a_plain_single_component_name() {
        let dir = bundle("original");

        let snap = snapshot_into(dir.path(), Some("sweep-1-2-3")).unwrap();

        assert_eq!(snap.dir, snapshot_root(dir.path()).join("sweep-1-2-3"));
        assert_eq!(read(&snap.dir.join("docs/notes.md")), "original");
    }

    #[test]
    fn remove_snapshot_refuses_a_path_outside_the_bundle_snapshot_root() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("bundle");
        std::fs::create_dir_all(&source).unwrap();
        crate::scaffold::scaffold(&source, "deal-desk").unwrap();
        // A real snapshot first, so the root exists and only the containment
        // check can be what refuses below.
        snapshot(&source).unwrap();

        let outside = root.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("precious.txt"), "do not delete me").unwrap();

        let traversal = snapshot_root(&source)
            .join("..")
            .join("..")
            .join("..")
            .join("outside");
        let mut recorded = vec![outside.clone(), traversal];

        #[cfg(unix)]
        {
            // A recorded path that LOOKS contained but links out of the root.
            let link = snapshot_root(&source).join("linked-outside");
            std::os::unix::fs::symlink(&outside, &link).unwrap();
            recorded.push(link);
        }

        for path in recorded {
            assert!(
                remove_snapshot(&path, &source).is_err(),
                "a recorded path outside the snapshot root must be refused: {}",
                path.display()
            );
            assert!(
                outside.is_dir(),
                "the refused directory must still exist: {}",
                path.display()
            );
            assert_eq!(
                read(&outside.join("precious.txt")),
                "do not delete me",
                "nothing under the refused path may be deleted"
            );
        }
    }

    #[test]
    fn remove_snapshot_accepts_a_relative_source_with_an_absolute_recorded_dir() {
        // The production call shape: `skill up` canonicalizes the bundle dir and
        // records an ABSOLUTE snapshot path in runner.json, while `stop` hands
        // `remove_snapshot` the RELATIVE `Path::new(".")`. A guard that compares
        // the raw values refuses every real `skill down`, warns, and leaks the
        // snapshot forever while every other test here stays green.
        let dir = bundle("original");
        let source = dir.path().canonicalize().unwrap();
        let snap = snapshot(&source).unwrap();
        let recorded = PathBuf::from(snap.dir.display().to_string());
        assert!(recorded.is_absolute(), "{}", recorded.display());
        assert!(recorded.is_dir());

        let result = {
            let _lock = CWD_LOCK.lock().unwrap_or_else(|poison| poison.into_inner());
            let _cwd = CwdGuard(std::env::current_dir().unwrap());
            std::env::set_current_dir(&source).unwrap();
            remove_snapshot(&recorded, Path::new("."))
        };

        assert!(
            result.is_ok(),
            "a relative source against an absolute recorded dir is the real teardown: {result:?}"
        );
        assert!(
            !recorded.exists(),
            "the snapshot must actually be gone, not merely reported Ok"
        );
    }

    #[test]
    fn remove_snapshot_tolerates_an_already_absent_directory() {
        let dir = bundle("original");
        let snap = snapshot(dir.path()).unwrap();
        remove_snapshot(&snap.dir, dir.path()).unwrap();
        assert!(!snap.dir.exists());

        // Tearing down the same record twice, and a path that was never written:
        // nothing to remove is not a failure. An implementation that
        // canonicalizes before checking existence errors on both.
        remove_snapshot(&snap.dir, dir.path()).unwrap();
        remove_snapshot(
            &snapshot_root(dir.path()).join("never-materialized"),
            dir.path(),
        )
        .unwrap();
    }

    #[test]
    fn remove_snapshot_refuses_when_the_snapshot_root_does_not_exist() {
        let dir = bundle("original");
        assert!(
            !snapshot_root(dir.path()).exists(),
            "this bundle has never snapshotted"
        );
        // An existing directory to point the record at, owned by another bundle.
        let other = bundle("elsewhere");
        let snap = snapshot(other.path()).unwrap();

        assert!(
            remove_snapshot(&snap.dir, dir.path()).is_err(),
            "with no snapshot root, no path can be contained in it: fail closed"
        );
        assert!(
            snap.dir.is_dir(),
            "the refused directory must still exist and still hold its contents"
        );
        assert_eq!(read(&snap.dir.join("docs/notes.md")), "elsewhere");
    }
}
