//! Offline installation of bundled text skills into local coding agents.
//! Workflow instructions and assets remain owned by the calling bundle.

use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result};
use clap::ValueEnum;
use serde::Serialize;

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

pub type SkillFiles<'a> = (&'a str, Vec<(&'a str, &'a [u8])>);

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
/// Each new folder is published atomically. I/O failures can leave earlier
/// folders installed, so rerunning accepts those identical copies.
pub fn install_set(
    skills: &[SkillFiles<'_>],
    options: InstallOptions,
) -> Result<Box<dyn CliOutput>> {
    let mut preview = options.clone();
    preview.dry_run = true;
    let mut lines = Vec::new();
    for (name, files) in skills {
        let plan = install(name, files, preview.clone())?;
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
        .map(|(name, files)| install(name, files, options.clone()))
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

/// Publish a complete embedded skill, or accept an identical existing copy.
/// A changed or incomplete existing skill is refused, including on dry runs.
pub fn install(
    name: &str,
    files: &[(&str, &[u8])],
    options: InstallOptions,
) -> Result<Box<dyn CliOutput>> {
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
    for (file, bytes) in files {
        if file.is_empty() {
            return Err(crate::exit::usage("skill asset path must not be empty"));
        }
        check_path(&root, &relative.join(file))?;
        if existing && fs::read(directory.join(file)).as_deref().ok() != Some(*bytes) {
            return Err(refusal(&directory.join(file)));
        }
    }
    let guidance = options.agent.guidance(name);
    if options.dry_run {
        return Ok(Box::new(DryRunPlan {
            lines: vec![
                format!(
                    "{} {}",
                    if existing {
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
    if !existing {
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
        if directory.try_exists()? {
            return Err(refusal(&directory));
        }
        fs::rename(staging.path(), &directory).context("publish staged skill")?;
    }
    Ok(Box::new(AgentSkillInstallOutput {
        agent: options.agent,
        skill: name.to_owned(),
        scope: if options.global { "user" } else { "repository" },
        directory,
        status: if existing { "unchanged" } else { "installed" },
        guidance,
    }))
}
