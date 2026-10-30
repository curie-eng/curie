//! Offline installation of bundled text skills into local coding agents.
//! Workflow instructions and assets remain owned by the calling bundle.

use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result};
use clap::ValueEnum;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::ui::{CliOutput, DryRunPlan, Ui};

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentClient {
    Codex,
    ClaudeCode,
    Copilot,
    Gemini,
    Cursor,
}

impl AgentClient {
    pub(crate) fn directory(self, global: bool) -> &'static str {
        match self {
            Self::Codex => ".agents/skills",
            Self::ClaudeCode => ".claude/skills",
            Self::Copilot if global => ".copilot/skills",
            Self::Copilot => ".github/skills",
            Self::Gemini => ".gemini/skills",
            Self::Cursor => ".cursor/skills",
        }
    }

    fn guidance(self, name: &str) -> String {
        match self {
            Self::Codex => format!("Use ${name} followed by your request or link. If absent, restart the Codex session."),
            Self::ClaudeCode => format!("Use /{name} followed by your request or link. If absent, start a new Claude Code session."),
            Self::Copilot => format!("Ask a skill-capable Copilot agent to use {name}. In Copilot CLI, run /skills reload; in an IDE, refresh the agent session if absent."),
            Self::Gemini => format!("Run /skills reload in Gemini CLI, then ask it to use {name}. Gemini CLI requests activation consent."),
            Self::Cursor => format!("Ask Cursor Agent to use {name}. If absent, refresh the agent session and check its skill settings."),
        }
    }
}

#[derive(Clone)]
pub struct InstallOptions {
    pub agent: AgentClient,
    pub global: bool,
    pub dir: Option<PathBuf>,
    pub dry_run: bool,
}

#[derive(Serialize)]
struct AgentSkillInstallOutput {
    agent: AgentClient,
    skill: String,
    scope: &'static str,
    directory: PathBuf,
    status: &'static str,
    guidance: String,
}

impl CliOutput for AgentSkillInstallOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("installation result serializes")
    }

    fn render(&self, ui: &Ui) {
        ui.payload_plain(&format!("{}: {}", self.status, self.directory.display()));
        ui.payload_plain(&self.guidance);
    }
}

pub struct SkillFiles<'a> {
    pub name: &'a str,
    pub files: Vec<(&'a str, &'a [u8])>,
    /// SHA-256 of exact previously shipped directory trees, including paths.
    pub previous_versions: &'a [&'a str],
}

struct AgentSkillsInstallOutput {
    skills: Vec<Box<dyn CliOutput>>,
}

impl CliOutput for AgentSkillsInstallOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({"skills": self.skills.iter().map(|skill| skill.to_json()).collect::<Vec<_>>()})
    }

    fn render(&self, ui: &Ui) {
        for skill in &self.skills {
            skill.render(ui);
        }
    }
}

/// Check every destination before installing any skill. Existing customized
/// skills are never overwritten; unrelated skills remain outside this operation.
/// I/O failures can leave earlier folders installed, so rerunning accepts them.
pub fn install_set(
    skills: &[SkillFiles<'_>],
    options: InstallOptions,
) -> Result<Box<dyn CliOutput>> {
    let mut preview = options.clone();
    preview.dry_run = true;
    let mut lines = Vec::new();
    for skill in skills {
        let plan = install(skill, preview.clone())?;
        let value = plan.to_json();
        lines.extend(
            value["plan"]
                .as_array()
                .expect("dry run steps")
                .iter()
                .map(|line| line.as_str().expect("dry run line").to_owned()),
        );
    }
    if options.dry_run {
        return Ok(Box::new(DryRunPlan { lines }));
    }
    let installed = skills
        .iter()
        .map(|skill| install(skill, options.clone()))
        .collect::<Result<Vec<_>>>()?;
    Ok(Box::new(AgentSkillsInstallOutput { skills: installed }))
}

fn refusal(path: &Path) -> anyhow::Error {
    crate::exit::usage(format!(
        "skill installation conflicts with {}. Review and move the existing skill aside, then rerun; no existing files were overwritten",
        path.display()
    ))
}

/// Check only below the explicitly selected root. The root itself is resolved
/// once (e.g. macOS /tmp); client directories and payloads must not be symlinks.
pub(crate) fn check_path(root: &Path, relative: &Path) -> Result<()> {
    let mut path = root.to_path_buf();
    for part in relative.components() {
        if !matches!(part, Component::Normal(_)) {
            return Err(crate::exit::usage(
                "skill paths must be relative normal components",
            ));
        }
        path.push(part);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => return Err(refusal(&path)),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).with_context(|| format!("inspect {}", path.display())),
        }
    }
    Ok(())
}

pub(crate) fn selected_root(options: &InstallOptions) -> Result<PathBuf> {
    if options.global && options.dir.is_some() {
        return Err(crate::exit::usage("--global and --dir cannot be combined"));
    }
    let root = if options.global {
        let key = if cfg!(windows) { "USERPROFILE" } else { "HOME" };
        let home = std::env::var_os(key)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                crate::exit::usage(format!("{key} must name your home directory for --global"))
            })?;
        PathBuf::from(home)
    } else if let Some(dir) = &options.dir {
        dir.clone()
    } else {
        let cwd = std::env::current_dir().context("read current directory")?;
        cwd.ancestors()
            .find(|parent| parent.join(".git").exists())
            .unwrap_or(&cwd)
            .to_path_buf()
    };
    let root = root.canonicalize().with_context(|| {
        format!(
            "installation root {} must be an existing directory",
            root.display()
        )
    })?;
    if !root.is_dir() {
        return Err(crate::exit::usage("installation root must be a directory"));
    }
    Ok(root)
}

/// Publish a complete embedded skill, accept an identical existing copy, or
/// replace a tree whose digest is in the trusted previous-version catalog.
pub fn install(skill: &SkillFiles<'_>, options: InstallOptions) -> Result<Box<dyn CliOutput>> {
    let SkillFiles {
        name,
        files,
        previous_versions,
    } = skill;
    if Path::new(name).components().count() != 1 || name.is_empty() {
        return Err(crate::exit::usage("skill name must be one path component"));
    }
    let root = selected_root(&options)?;
    let relative = Path::new(options.agent.directory(options.global)).join(name);
    check_path(&root, &relative)?;
    let directory = root.join(&relative);
    let existing = directory
        .try_exists()
        .context("inspect skill destination")?;
    if existing && !directory.is_dir() {
        return Err(refusal(&directory));
    }
    let mut identical = existing;
    for (file, bytes) in files {
        if file.is_empty() {
            return Err(crate::exit::usage("skill asset path must not be empty"));
        }
        check_path(&root, &relative.join(file))?;
        if existing && fs::read(directory.join(file)).as_deref().ok() != Some(*bytes) {
            identical = false;
        }
    }
    let upgrade = if existing && !identical {
        let digest = directory_digest(&directory)?;
        if !previous_versions.contains(&digest.as_str()) {
            return Err(refusal(&directory));
        }
        true
    } else {
        false
    };
    let guidance = options.agent.guidance(name);
    if options.dry_run {
        return Ok(Box::new(DryRunPlan {
            lines: vec![
                format!(
                    "{} {}",
                    if upgrade {
                        "Upgrade unmodified skill at"
                    } else if existing {
                        "Keep identical skill at"
                    } else {
                        "Install embedded skill at"
                    },
                    directory.display()
                ),
                guidance,
            ],
        }));
    }
    if !existing || upgrade {
        let parent = directory.parent().expect("skill has a parent");
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        check_path(&root, &relative)?;
        let staging = tempfile::Builder::new()
            .prefix(".curie-skill-")
            .tempdir_in(parent)?;
        for (file, bytes) in files {
            let path = staging.path().join(file);
            fs::create_dir_all(path.parent().expect("asset has a parent"))?;
            fs::write(&path, bytes).with_context(|| format!("stage skill asset {file}"))?;
        }
        // Do not merge into a destination another installation created while
        // staging. Rename publishes all assets together on the same filesystem.
        check_path(&root, &relative)?;
        if upgrade {
            // Recheck immediately before moving the old tree so intervening
            // edits detected before the move prevent replacement.
            let digest = directory_digest(&directory)?;
            if !previous_versions.contains(&digest.as_str()) {
                return Err(refusal(&directory));
            }
            let backup = tempfile::Builder::new()
                .prefix(".curie-skill-backup-")
                .tempdir_in(parent)?;
            let old = backup.path().join("previous");
            fs::rename(&directory, &old).context("stage previous skill")?;
            if let Err(error) = fs::rename(staging.path(), &directory) {
                if let Err(restore_error) = fs::rename(&old, &directory) {
                    let retained = backup.keep();
                    return Err(anyhow::anyhow!(
                        "publish upgraded skill failed: {error}; restore failed: {restore_error}; previous skill retained at {}",
                        retained.display()
                    ));
                }
                return Err(error).context("publish upgraded skill");
            }
        } else {
            if directory.try_exists()? {
                return Err(refusal(&directory));
            }
            fs::rename(staging.path(), &directory).context("publish staged skill")?;
        }
    }
    Ok(Box::new(AgentSkillInstallOutput {
        agent: options.agent,
        skill: (*name).to_owned(),
        scope: if options.global { "user" } else { "repository" },
        directory,
        status: if upgrade {
            "upgraded"
        } else if existing {
            "unchanged"
        } else {
            "installed"
        },
        guidance,
    }))
}

/// Digest an exact tree. File names, directory names, file bytes, and types
/// all contribute; symlinks and non-file entries fail closed.
fn directory_digest(root: &Path) -> Result<String> {
    fn visit(
        root: &Path,
        directory: &Path,
        entries: &mut Vec<(String, bool, Vec<u8>)>,
    ) -> Result<()> {
        for entry in
            fs::read_dir(directory).with_context(|| format!("read {}", directory.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            let relative = path
                .strip_prefix(root)?
                .to_str()
                .ok_or_else(|| refusal(&path))?
                .replace(std::path::MAIN_SEPARATOR, "/");
            let kind = entry.file_type()?;
            if kind.is_dir() {
                entries.push((relative, true, Vec::new()));
                visit(root, &path, entries)?;
            } else if kind.is_file() {
                entries.push((relative, false, fs::read(&path)?));
            } else {
                return Err(refusal(&path));
            }
        }
        Ok(())
    }
    let mut entries = Vec::new();
    visit(root, root, &mut entries)?;
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let mut hash = Sha256::new();
    for (path, directory, bytes) in entries {
        hash.update(if directory { b"D" } else { b"F" });
        hash.update((path.len() as u64).to_be_bytes());
        hash.update(path.as_bytes());
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
    }
    Ok(hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(root: &Path, dry_run: bool) -> InstallOptions {
        InstallOptions {
            agent: AgentClient::Codex,
            global: false,
            dir: Some(root.to_path_buf()),
            dry_run,
        }
    }

    #[test]
    fn known_previous_tree_upgrades_and_then_is_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join(".agents/skills/example-skill");
        fs::create_dir_all(directory.join("references")).unwrap();
        fs::write(directory.join("SKILL.md"), b"old").unwrap();
        fs::write(directory.join("references/help.md"), b"help").unwrap();
        let digest = directory_digest(&directory).unwrap();
        let versions = [digest.as_str()];
        let skill = SkillFiles {
            name: "example-skill",
            files: vec![("SKILL.md", b"new"), ("references/help.md", b"help")],
            previous_versions: &versions,
        };
        assert!(install(&skill, options(root.path(), true))
            .unwrap()
            .to_json()["plan"][0]
            .as_str()
            .unwrap()
            .contains("Upgrade"));
        assert_eq!(fs::read(directory.join("SKILL.md")).unwrap(), b"old");
        assert_eq!(
            install(&skill, options(root.path(), false))
                .unwrap()
                .to_json()["status"],
            "upgraded"
        );
        assert_eq!(fs::read(directory.join("SKILL.md")).unwrap(), b"new");
        assert_eq!(
            install(&skill, options(root.path(), false))
                .unwrap()
                .to_json()["status"],
            "unchanged"
        );
    }

    #[test]
    fn changed_or_extra_previous_tree_is_never_upgraded() {
        for change in ["edit", "extra-file", "extra-directory", "missing-file"] {
            let root = tempfile::tempdir().unwrap();
            let directory = root.path().join(".agents/skills/example-skill");
            fs::create_dir_all(directory.join("references")).unwrap();
            fs::write(directory.join("SKILL.md"), b"old").unwrap();
            fs::write(directory.join("references/help.md"), b"help").unwrap();
            let digest = directory_digest(&directory).unwrap();
            match change {
                "edit" => fs::write(directory.join("SKILL.md"), b"local edit").unwrap(),
                "extra-file" => fs::write(directory.join("notes.md"), b"local note").unwrap(),
                "extra-directory" => fs::create_dir(directory.join("local")).unwrap(),
                "missing-file" => fs::remove_file(directory.join("references/help.md")).unwrap(),
                _ => unreachable!(),
            }
            let versions = [digest.as_str()];
            let skill = SkillFiles {
                name: "example-skill",
                files: vec![("SKILL.md", b"new"), ("references/help.md", b"help")],
                previous_versions: &versions,
            };
            assert!(
                install(&skill, options(root.path(), true)).is_err(),
                "{change}"
            );
            assert!(
                install(&skill, options(root.path(), false)).is_err(),
                "{change}"
            );
            assert_ne!(fs::read(directory.join("SKILL.md")).unwrap(), b"new");
        }
    }

    #[test]
    fn set_conflict_prevents_a_known_upgrade() {
        let root = tempfile::tempdir().unwrap();
        let first = root.path().join(".agents/skills/first");
        let second = root.path().join(".agents/skills/second");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        fs::write(first.join("SKILL.md"), b"old first").unwrap();
        fs::write(second.join("SKILL.md"), b"local second").unwrap();
        let first_digest = directory_digest(&first).unwrap();
        let versions = [first_digest.as_str()];
        let skills = [
            SkillFiles {
                name: "first",
                files: vec![("SKILL.md", b"new first")],
                previous_versions: &versions,
            },
            SkillFiles {
                name: "second",
                files: vec![("SKILL.md", b"new second")],
                previous_versions: &[],
            },
        ];
        assert!(install_set(&skills, options(root.path(), false)).is_err());
        assert_eq!(fs::read(first.join("SKILL.md")).unwrap(), b"old first");
        assert_eq!(fs::read(second.join("SKILL.md")).unwrap(), b"local second");
    }
}
