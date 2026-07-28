//! Package a plugin bundle directory into a tar.gz archive for upload.
//!
//! The platform API (B2) accepts tar.gz/tar/zip, extracts, and validates via
//! the frozen plugin-format package. Workstation state (`.curie/`, `.git/`,
//! virtualenvs, `node_modules`, tool caches) never belongs in an immutable
//! bundle, so it is excluded here, and a bundle can declare its own exclusions
//! in a root `.curieignore` (see [`Exclusions::load`]).

use std::path::{Component, Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{bail, Context, Result};
use flate2::write::GzEncoder;
use flate2::Compression;

/// Name of the optional per-bundle ignore file, read from the bundle root.
/// `pub(crate)` so `info` names it in a diagnostic without re-spelling it.
pub(crate) const IGNORE_FILE: &str = ".curieignore";

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

/// Why a bundle's `.curieignore` could not be applied.
///
/// A bundle-CONTENT defect, so it is returned rather than raised: `pack_tar_gz`
/// refuses such a bundle, while `info` reports it and still describes the
/// directory. Both need the same fact, only one of them wants to stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IgnoreDefect {
    /// The ignore file is a symlink, so reading it would follow a link that may
    /// leave the bundle root.
    Symlink,
}

/// Entries the archive skips: the built-in names plus whatever the bundle's
/// `.curieignore` declares.
///
/// `pub` on purpose: `info`'s disk walk (`crate::info::BundleView`) and the
/// integration suite's deployed-view helper must both describe the SAME file set
/// `pack_tar_gz` ships, so they ask this type "would the packer include this
/// entry?" rather than mirroring [`EXCLUDED_NAMES`]. A mirrored denylist drifts,
/// and the mirror already had: it carried the built-in names but not the
/// bundle's `.curieignore`, so the disk view and the deployed view described
/// different bundles.
pub struct Exclusions {
    /// Bare names, matched against any entry at any depth.
    names: Vec<String>,
    /// Bundle-root-relative paths, matched against the exact entry.
    paths: Vec<PathBuf>,
}

impl Exclusions {
    /// The built-in names alone, with no bundle `.curieignore` applied.
    ///
    /// For a caller holding a bundle's files but no directory to read an ignore
    /// file from (a stored bundle read back over the platform API). Such a
    /// bundle was already packed through [`Self::load`], so its `.curieignore`
    /// exclusions are baked into the file list; only the built-in names remain
    /// worth re-asserting.
    pub fn builtin() -> &'static Self {
        static BUILTIN: OnceLock<Exclusions> = OnceLock::new();
        BUILTIN.get_or_init(|| Self {
            names: EXCLUDED_NAMES.iter().map(|n| (*n).to_string()).collect(),
            paths: Vec::new(),
        })
    }

    /// Read `.curieignore` from the bundle root, if present, reporting a
    /// defective ignore file rather than deciding what to do about it.
    ///
    /// One pattern per line, with `#` comments and blank lines skipped and
    /// surrounding whitespace plus a trailing `/` stripped. A pattern with no
    /// `/` matches any entry with that name at any depth, the same shape as
    /// the built-in defaults; a pattern containing `/` is a
    /// bundle-root-relative path matching that exact entry and its subtree.
    /// There is no glob support, so an odd pattern simply never matches, and a
    /// pattern that could reach outside the bundle is dropped. The ignore file
    /// is stat'd without following links, so a symlinked `.curieignore` is
    /// never read from outside the bundle root.
    ///
    /// The returned [`IgnoreDefect`] is the ONE stat, answered once for both
    /// callers with different jobs: [`Self::load`] turns it into the packer's
    /// refusal, while `info` turns it into a diagnostic and reports the
    /// directory anyway. Neither restates the check.
    pub(crate) fn resolve(root: &Path) -> Result<(Self, Option<IgnoreDefect>)> {
        let mut exclusions = Self {
            names: Self::builtin().names.clone(),
            paths: Vec::new(),
        };
        let ignore_path = root.join(IGNORE_FILE);
        match std::fs::symlink_metadata(&ignore_path) {
            Ok(meta) if meta.file_type().is_symlink() => {
                return Ok((exclusions, Some(IgnoreDefect::Symlink)))
            }
            Ok(meta) if meta.is_file() => {
                let text = std::fs::read_to_string(&ignore_path)
                    .with_context(|| format!("reading {}", ignore_path.display()))?;
                exclusions.extend_from(&text);
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err).with_context(|| format!("stat {}", ignore_path.display())),
        }
        Ok((exclusions, None))
    }

    /// [`Self::resolve`] under the packer's policy: a defective `.curieignore`
    /// is a refusal, because packing on despite it would ship a file set the
    /// bundle author did not ask for.
    fn load(root: &Path) -> Result<Self> {
        match Self::resolve(root)? {
            (exclusions, None) => Ok(exclusions),
            (_, Some(IgnoreDefect::Symlink)) => bail!(
                "symlinks are not supported in plugin bundles: {} (the {} file must be a regular file inside the bundle)",
                root.join(IGNORE_FILE).display(),
                IGNORE_FILE
            ),
        }
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

    /// Would a recursive walk prune `rel` at `rel` itself?
    ///
    /// Callers that walk recursively (this module's [`append_dir`], `info`'s
    /// disk walk) only need this: excluding a directory prunes its whole
    /// subtree by never descending.
    pub fn is_excluded(&self, rel: &Path) -> bool {
        let name = rel.file_name().unwrap_or_default();
        self.names.iter().any(|n| name == std::ffi::OsStr::new(n))
            || self.paths.iter().any(|p| rel == p)
    }

    /// Would a recursive walk have pruned `rel` at `rel` or at any ancestor?
    ///
    /// For a caller holding a FLAT list of bundle-root-relative paths rather
    /// than a tree to walk. It never descends, so it must ask about every
    /// ancestor to reach the answer [`is_excluded`](Self::is_excluded) reaches
    /// by pruning.
    pub(crate) fn excludes_any_ancestor(&self, rel: &Path) -> bool {
        rel.ancestors().any(|ancestor| self.is_excluded(ancestor))
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
}
