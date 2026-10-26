//! `curie <local|cluster> skills <agent> <verb>`: the operator surface over an
//! agent's learned skills (ADR 0247). A skill ref (id or slug) goes to the API
//! verbatim; the API resolves it, deleted skills included, so `revert` also
//! undoes a `delete`. Every write is a compare-and-swap on the head revision
//! this command reads first.

use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::api::{ApiClient, LearnedSkill, LearnedSkillRevision};
use crate::api_requests::LearnedSkillRevert;
use crate::ui::{CliOutput, Ui};

/// The verb a `skills` invocation runs, as the dispatcher hands it over.
pub enum SkillsVerb {
    List,
    Show {
        skill: String,
    },
    Diff {
        skill: String,
        from: Option<u32>,
        to: Option<u32>,
    },
    History {
        skill: String,
    },
    Revert {
        skill: String,
        to: Option<u32>,
    },
    Delete {
        skill: String,
    },
    Export {
        out: PathBuf,
    },
}

/// The resolved connection and target agent of a `skills` verb.
pub struct SkillsOpts {
    pub api_url: String,
    pub api_key: String,
    /// "local" or "cluster". A cluster tunnel is loopback too, so it must not
    /// trigger local key discovery.
    pub tier: &'static str,
    pub agent: String,
}

impl SkillsVerb {
    /// Refuse a ref that is not an id or slug shape before any request, so it
    /// can never add a path segment or query to the URL it is spliced into.
    pub fn validate(&self) -> Result<()> {
        let skill = match self {
            SkillsVerb::Show { skill }
            | SkillsVerb::Diff { skill, .. }
            | SkillsVerb::History { skill }
            | SkillsVerb::Revert { skill, .. }
            | SkillsVerb::Delete { skill } => skill,
            SkillsVerb::List | SkillsVerb::Export { .. } => return Ok(()),
        };
        if is_ref(skill) {
            Ok(())
        } else {
            Err(crate::exit::usage(format!(
                "skill must be an id or a slug (letters, digits, `-`), got {skill:?}"
            )))
        }
    }
}

fn is_ref(raw: &str) -> bool {
    !raw.is_empty() && raw.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

/// The generated `SKILL.md`, byte-identical to the runner's
/// (`learned_skills.skill_markdown`): frontmatter is name plus a JSON-quoted
/// description, never authored.
pub fn skill_markdown(slug: &str, description: &str, body: &str) -> String {
    let description = serde_json::Value::from(description);
    format!("---\nname: {slug}\ndescription: {description}\n---\n\n{body}")
}

/// A line diff of `old` against `new` as one full-context unified hunk.
/// Common prefix and suffix are trimmed before the LCS table, so the
/// quadratic part covers only the changed middle.
pub fn unified_diff(old_name: &str, new_name: &str, old: &str, new: &str) -> String {
    let header = format!("--- {old_name}\n+++ {new_name}\n");
    if old == new {
        return header;
    }
    // Lines keep their terminators, so a line-ending-only change is a change.
    let a: Vec<&str> = old.split_inclusive('\n').collect();
    let b: Vec<&str> = new.split_inclusive('\n').collect();
    let render = |out: &mut String, sign: char, line: &str| {
        out.push(sign);
        out.push_str(line);
        if !line.ends_with('\n') {
            out.push_str("\n\\ No newline at end of file\n");
        }
    };
    let pre = a.iter().zip(&b).take_while(|(x, y)| x == y).count();
    let suf = a[pre..]
        .iter()
        .rev()
        .zip(b[pre..].iter().rev())
        .take_while(|(x, y)| x == y)
        .count();
    let (ma, mb) = (&a[pre..a.len() - suf], &b[pre..b.len() - suf]);
    let mut lcs = vec![vec![0u32; mb.len() + 1]; ma.len() + 1];
    for i in (0..ma.len()).rev() {
        for j in (0..mb.len()).rev() {
            lcs[i][j] = if ma[i] == mb[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }
    let mut out = format!("{header}@@ -1,{} +1,{} @@\n", a.len(), b.len());
    a[..pre].iter().for_each(|l| render(&mut out, ' ', l));
    let (mut i, mut j) = (0, 0);
    while i < ma.len() || j < mb.len() {
        if i < ma.len() && j < mb.len() && ma[i] == mb[j] {
            render(&mut out, ' ', ma[i]);
            (i, j) = (i + 1, j + 1);
        } else if j < mb.len() && (i == ma.len() || lcs[i][j + 1] >= lcs[i + 1][j]) {
            render(&mut out, '+', mb[j]);
            j += 1;
        } else {
            render(&mut out, '-', ma[i]);
            i += 1;
        }
    }
    a[a.len() - suf..]
        .iter()
        .for_each(|l| render(&mut out, ' ', l));
    out
}

/// `skills list`: under `--json`, an array of skills.
pub struct SkillListOutput {
    pub agent: String,
    pub enabled: bool,
    pub skills: Vec<LearnedSkill>,
}

impl CliOutput for SkillListOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(&self.skills).unwrap_or_default()
    }

    fn render(&self, ui: &Ui) {
        if !self.enabled {
            ui.payload(&format!("learned skills are off for {}", self.agent));
        }
        if self.skills.is_empty() {
            ui.payload("no learned skills");
            return;
        }
        let rows: Vec<Vec<String>> = self
            .skills
            .iter()
            .map(|s| {
                vec![
                    s.slug.clone(),
                    s.revision.to_string(),
                    s.author.clone(),
                    s.updated_at.clone(),
                    s.description.clone(),
                ]
            })
            .collect();
        let table = crate::ui::table(
            &["SLUG", "REV", "AUTHOR", "UPDATED", "DESCRIPTION"],
            &rows,
            &[1],
        );
        ui.payload_plain(table.trim_end());
    }
}

/// `skills show`, `revert` and `delete`: the skill as the API now holds it.
pub struct SkillOutput {
    /// `None` for `show`; the past-tense verb for a write.
    pub changed: Option<&'static str>,
    pub skill: LearnedSkill,
}

impl CliOutput for SkillOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(&self.skill).unwrap_or_default()
    }

    fn render(&self, ui: &Ui) {
        let s = &self.skill;
        if let Some(verb) = self.changed {
            ui.payload(&format!(
                "{verb} {}, now at revision {}",
                s.slug, s.revision
            ));
            return;
        }
        let state = if s.deleted { " (deleted)" } else { "" };
        ui.payload_plain(&format!(
            "{} revision {}{state} by {} at {}\n{}\n\n{}",
            s.slug, s.revision, s.author, s.updated_at, s.description, s.body
        ));
    }
}

/// `skills history`: every revision, oldest first.
pub struct SkillHistoryOutput {
    pub skill: String,
    pub revisions: Vec<LearnedSkillRevision>,
}

impl CliOutput for SkillHistoryOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({ "skill": self.skill, "revisions": self.revisions })
    }

    fn render(&self, ui: &Ui) {
        let rows: Vec<Vec<String>> = self
            .revisions
            .iter()
            .map(|r| {
                vec![
                    r.revision.to_string(),
                    r.action.clone(),
                    r.author.clone(),
                    r.source.clone(),
                    r.created_at.clone(),
                ]
            })
            .collect();
        let table = crate::ui::table(
            &["REV", "ACTION", "AUTHOR", "SOURCE", "CREATED"],
            &rows,
            &[0],
        );
        ui.payload_plain(table.trim_end());
    }
}

/// `skills diff`: the unified diff of two revisions' SKILL.md.
pub struct SkillDiffOutput {
    pub skill: String,
    pub from: u32,
    pub to: u32,
    pub diff: String,
}

impl CliOutput for SkillDiffOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "skill": self.skill, "from": self.from, "to": self.to, "diff": self.diff,
        })
    }

    fn render(&self, ui: &Ui) {
        ui.payload_plain(self.diff.trim_end());
    }
}

/// `skills export`: the files written.
pub struct SkillExportOutput {
    pub files: Vec<PathBuf>,
}

impl CliOutput for SkillExportOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({ "files": self.files })
    }

    fn render(&self, ui: &Ui) {
        for file in &self.files {
            ui.payload_plain(&file.display().to_string());
        }
        ui.payload(&format!("exported {} learned skills", self.files.len()));
    }
}

fn revision(revs: &[LearnedSkillRevision], n: u32) -> Result<&LearnedSkillRevision> {
    revs.iter()
        .find(|r| r.revision == n)
        .ok_or_else(|| crate::exit::usage(format!("no revision {n}")))
}

/// `<tier> skills <agent> <verb>`.
pub async fn learned_skills(opts: SkillsOpts, verb: SkillsVerb) -> Result<Box<dyn CliOutput>> {
    verb.validate()?;
    let client = if opts.tier == "cluster" {
        ApiClient::with_resolved_key(&opts.api_url, &opts.api_key)?
    } else {
        ApiClient::new(&opts.api_url, &opts.api_key)?
    };
    let agent = client.find_agent(&opts.agent).await?;
    let id = agent.id.as_str();
    Ok(match verb {
        SkillsVerb::List => {
            let list = client.list_learned_skills(id).await?;
            Box::new(SkillListOutput {
                agent: agent.name.clone(),
                enabled: list.enabled,
                skills: list.skills,
            })
        }
        SkillsVerb::Show { skill } => Box::new(SkillOutput {
            changed: None,
            skill: client.get_learned_skill(id, &skill).await?,
        }),
        SkillsVerb::History { skill } => Box::new(SkillHistoryOutput {
            revisions: client.learned_skill_revisions(id, &skill).await?,
            skill,
        }),
        SkillsVerb::Diff { skill, from, to } => {
            let revs = client.learned_skill_revisions(id, &skill).await?;
            let head = revs.last().map_or(0, |r| r.revision);
            let to = to.unwrap_or(head);
            let from = from.unwrap_or(to.saturating_sub(1));
            let (a, b) = (revision(&revs, from)?, revision(&revs, to)?);
            let md = |r: &LearnedSkillRevision| skill_markdown(&r.slug, &r.description, &r.body);
            let diff = unified_diff(
                &format!("{}@{from}", a.slug),
                &format!("{}@{to}", b.slug),
                &md(a),
                &md(b),
            );
            Box::new(SkillDiffOutput {
                skill,
                from,
                to,
                diff,
            })
        }
        SkillsVerb::Revert { skill, to } => {
            // Mutate by the id read here, not the typed slug, so a skill deleted
            // and recreated under the slug in between cannot be hit instead.
            let head = client.get_learned_skill(id, &skill).await?;
            let body = LearnedSkillRevert {
                expected_revision: head.revision,
                to_revision: to,
            };
            Box::new(SkillOutput {
                changed: Some("reverted"),
                skill: client.revert_learned_skill(id, &head.id, &body).await?,
            })
        }
        SkillsVerb::Delete { skill } => {
            let head = client.get_learned_skill(id, &skill).await?;
            Box::new(SkillOutput {
                changed: Some("deleted"),
                skill: client
                    .delete_learned_skill(id, &head.id, head.revision)
                    .await?,
            })
        }
        SkillsVerb::Export { out } => {
            let mut files = Vec::new();
            for s in client.list_learned_skills(id).await?.skills {
                // The API enforces the slug pattern; checked again here because
                // the slug becomes a directory name.
                if !is_ref(&s.slug) {
                    anyhow::bail!("refusing to export skill with unsafe slug {:?}", s.slug);
                }
                let dir = out.join("skills").join(&s.slug);
                std::fs::create_dir_all(&dir)
                    .with_context(|| format!("creating {}", dir.display()))?;
                let file = dir.join("SKILL.md");
                std::fs::write(&file, skill_markdown(&s.slug, &s.description, &s.body))
                    .with_context(|| format!("writing {}", file.display()))?;
                files.push(file);
            }
            Box::new(SkillExportOutput { files })
        }
    })
}
