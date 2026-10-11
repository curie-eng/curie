//! Read-only toolchain discovery before Helm. This reports the fixed runner's
//! limits; repository declarations never change its image or install tools.

use std::collections::BTreeMap;

use anyhow::Result;
use regex::Regex;

use crate::exit::CliError;
use crate::factory_app::{GithubApi, InstalledApp, RepositoryContent};

pub const SKIPPED_NOTE: &str = "toolchain inference skipped: no --app-id";
pub const PLAN_NOTE: &str = "infer repository toolchains before Helm from App-authenticated workflows and manifests; warn about tools or versions absent from the fixed runner";
const MANIFESTS: &[&str] = &[
    "pyproject.toml",
    "package.json",
    "Cargo.toml",
    "go.mod",
    "pom.xml",
    "Gemfile",
    ".tool-versions",
    ".nvmrc",
    ".node-version",
    ".python-version",
    ".ruby-version",
    "rust-toolchain",
    "rust-toolchain.toml",
];

#[derive(Debug, Clone)]
struct Requirement {
    tool: &'static str,
    version: String,
    file: String,
    minimum: bool,
}

#[derive(Debug, Default)]
pub struct Report {
    requirements: Vec<Requirement>,
}

impl Report {
    fn add(&mut self, tool: &'static str, version: &str, file: &str, minimum: bool) {
        let version = version.trim();
        let version = version
            .strip_prefix("==")
            .filter(|pin| number_parts(pin).is_some())
            .unwrap_or(version);
        let version = if version.is_empty() {
            "unspecified"
        } else {
            version
        };
        if !self
            .requirements
            .iter()
            .any(|r| r.tool == tool && r.version == version && r.file == file)
        {
            self.requirements.push(Requirement {
                tool,
                version: version.into(),
                file: file.into(),
                minimum,
            });
        }
    }

    pub fn notes(&self) -> Vec<String> {
        if self.requirements.is_empty() {
            return vec!["no toolchain signals found in repository workflows or manifests".into()];
        }
        self.requirements
            .iter()
            .map(|r| {
                format!(
                    "inferred toolchain: {} {} from {}",
                    r.tool, r.version, r.file
                )
            })
            .collect()
    }

    pub fn warnings(&self) -> Vec<String> {
        self.requirements.iter().filter(|r| !supported(r)).map(|r| format!(
            "fixed factory runner lacks the required {} {} from {}; runner toolchains are Python 3.13, Node 22.23, and Rust 1.95",
            r.tool, r.version, r.file
        )).collect()
    }
}

fn number_parts(raw: &str) -> Option<Vec<u32>> {
    let raw = raw.trim().trim_start_matches('v');
    if !Regex::new(r"^\d+(?:\.\d+){0,2}(?:\.[x*])?$")
        .expect("static exact version regex")
        .is_match(raw)
    {
        return None;
    }
    let mut out = Vec::new();
    for part in raw.split('.') {
        if part == "x" || part == "*" {
            break;
        }
        let digits: String = part.chars().take_while(|c| c.is_ascii_digit()).collect();
        if digits.is_empty() {
            return None;
        }
        out.push(digits.parse().ok()?);
    }
    (!out.is_empty()).then_some(out)
}

fn supported(r: &Requirement) -> bool {
    let runner = match r.tool {
        "Python" => [3, 13],
        "Node" => [22, 23],
        "Rust" => [1, 95],
        _ => return false,
    };
    if r.version == "unspecified" {
        return true;
    }
    if r.minimum {
        return number_parts(&r.version).is_some_and(|v| [v[0], *v.get(1).unwrap_or(&0)] <= runner);
    }
    // An exact toolchain pin must match the runner's minor. Patches do not
    // change support. Node's conventional bare major or 22.x admits that minor.
    if let Some(v) = number_parts(&r.version) {
        return v[0] == runner[0] && v.get(1).is_none_or(|minor| *minor == runner[1]);
    }
    if r.version.contains("${{") {
        return false;
    }
    let token =
        Regex::new(r"(>=|<=|==|!=|>|<|=|\^|~=?|)?\s*v?(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:\.([x*]))?")
            .expect("static version regex");
    r.version.split("||").any(|alternative| {
        let mut seen = false;
        let mut end = 0;
        for c in token.captures_iter(alternative) {
            let matched = c.get(0).expect("version capture");
            if !alternative[end..matched.start()]
                .chars()
                .all(|ch| ch.is_whitespace() || ch == ',')
            {
                return false;
            }
            end = matched.end();
            seen = true;
            let wanted = [
                c[2].parse::<u32>().unwrap_or(u32::MAX),
                c.get(3).and_then(|m| m.as_str().parse().ok()).unwrap_or(0),
            ];
            let op = c.get(1).map(|m| m.as_str()).unwrap_or("");
            let matches = match op {
                ">=" => runner >= wanted,
                ">" => runner > wanted,
                "<=" => runner <= wanted,
                "<" => runner < wanted,
                "!=" => runner != wanted,
                "^" => runner >= wanted && runner[0] == wanted[0],
                "~" => {
                    runner >= wanted
                        && runner[0] == wanted[0]
                        && (c.get(3).is_none() || runner[1] == wanted[1])
                }
                "~=" => {
                    runner >= wanted
                        && runner[0] == wanted[0]
                        && (c.get(4).is_none() || runner[1] == wanted[1])
                }
                _ => runner[0] == wanted[0] && (c.get(3).is_none() || runner[1] == wanted[1]),
            };
            if !matches {
                return false;
            }
        }
        seen && alternative[end..]
            .chars()
            .all(|ch| ch.is_whitespace() || ch == ',')
    })
}

fn quoted_assignment(text: &str, key: &str) -> Option<String> {
    let pattern = format!(r#"(?m)^\s*{}\s*=\s*["']([^"']+)["']"#, regex::escape(key));
    Regex::new(&pattern)
        .ok()?
        .captures(text)
        .map(|c| c[1].to_string())
}

fn scalar(value: &serde_norway::Value) -> Option<String> {
    match value {
        serde_norway::Value::String(v) => Some(v.clone()),
        serde_norway::Value::Number(v) => Some(v.to_string()),
        _ => None,
    }
}

fn workflow_values(raw: &serde_norway::Value, job: &serde_norway::Value) -> Vec<String> {
    let Some(text) = scalar(raw) else {
        return Vec::new();
    };
    let expression =
        Regex::new(r"^\$\{\{\s*matrix\.([\w-]+)\s*\}\}$").expect("static matrix regex");
    if let Some(c) = expression.captures(&text) {
        let matrix = &job["strategy"]["matrix"];
        let mut values: Vec<String> = matrix[&c[1]]
            .as_sequence()
            .into_iter()
            .flatten()
            .filter_map(scalar)
            .collect();
        for entry in matrix["include"].as_sequence().into_iter().flatten() {
            if let Some(value) = scalar(&entry[&c[1]]) {
                values.push(value);
            }
        }
        if !values.is_empty() {
            return values;
        }
    }
    vec![text]
}

fn workflow(
    text: &str,
    path: &str,
    files: &BTreeMap<String, String>,
    report: &mut Report,
) -> Result<()> {
    let document: serde_norway::Value = serde_norway::from_str(text)
        .map_err(|_| CliError::failure(format!("cannot parse repository workflow {path}")))?;
    let jobs = document["jobs"].as_mapping().ok_or_else(|| {
        CliError::failure(format!(
            "repository workflow {path} carries no jobs mapping"
        ))
    })?;
    for job in jobs.values() {
        for step in job["steps"].as_sequence().into_iter().flatten() {
            let Some(uses) = step["uses"].as_str() else {
                continue;
            };
            let action = uses.split('@').next().unwrap_or(uses).to_ascii_lowercase();
            let (tool, version_key, file_key) = match action.as_str() {
                "actions/setup-python" => ("Python", "python-version", Some("python-version-file")),
                "actions/setup-node" => ("Node", "node-version", Some("node-version-file")),
                "actions/setup-go" => ("Go", "go-version", Some("go-version-file")),
                "actions/setup-java" => ("Java", "java-version", None),
                "ruby/setup-ruby" => ("Ruby", "ruby-version", None),
                "dtolnay/rust-toolchain" => ("Rust", "toolchain", None),
                "actions-rs/toolchain" => ("Rust", "toolchain", None),
                "actions/setup-dotnet" => (".NET", "dotnet-version", None),
                "actions/setup-gradle" | "gradle/actions/setup-gradle" => {
                    ("Gradle", "gradle-version", None)
                }
                _ => continue,
            };
            let versions = workflow_values(&step["with"][version_key], job);
            if !versions.is_empty() {
                for version in versions {
                    report.add(tool, &version, path, false);
                }
            } else if let Some(version_file) = file_key.and_then(|key| step["with"][key].as_str()) {
                if let Some(content) = files.get(version_file) {
                    if !MANIFESTS.contains(&version_file) {
                        report.add(tool, content.trim(), version_file, false);
                    }
                } else {
                    report.add(
                        tool,
                        &format!("unknown (version file {version_file})"),
                        path,
                        false,
                    );
                }
            } else if action == "dtolnay/rust-toolchain" {
                report.add(
                    tool,
                    uses.split_once('@')
                        .map(|(_, tag)| tag)
                        .unwrap_or("unspecified"),
                    path,
                    false,
                );
            } else {
                report.add(tool, "unspecified", path, false);
            }
        }
    }
    Ok(())
}

pub fn infer_files(files: &BTreeMap<String, String>) -> Result<Report> {
    let go_version =
        Regex::new(r"(?m)^\s*(?:go|toolchain)\s+(?:go)?([\d.]+)").expect("static Go regex");
    let java_version = Regex::new(
        r"<(?:maven\.compiler\.(?:release|source|target)|java\.version)\s*>\s*([^<]+)\s*</",
    )
    .expect("static Java regex");
    let ruby_version = Regex::new(r#"(?m)^\s*ruby\s+["']([^"']+)["']"#).expect("static Ruby regex");
    let mut report = Report::default();
    for (path, text) in files {
        match path.as_str() {
            "pyproject.toml" => {
                let version = quoted_assignment(text, "requires-python")
                    .or_else(|| quoted_assignment(text, "python"));
                report.add(
                    "Python",
                    version.as_deref().unwrap_or("unspecified"),
                    path,
                    false,
                );
            }
            "package.json" => {
                let body: serde_json::Value = serde_json::from_str(text).map_err(|_| {
                    CliError::failure(format!("cannot parse repository manifest {path}"))
                })?;
                if !body.is_object() {
                    return Err(CliError::failure(format!(
                        "repository manifest {path} must be an object"
                    ))
                    .into());
                }
                report.add(
                    "Node",
                    body.pointer("/engines/node")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unspecified"),
                    path,
                    false,
                );
            }
            "Cargo.toml" => report.add(
                "Rust",
                quoted_assignment(text, "rust-version")
                    .as_deref()
                    .unwrap_or("unspecified"),
                path,
                true,
            ),
            "go.mod" => {
                let mut versions = go_version.captures_iter(text).peekable();
                if versions.peek().is_none() {
                    report.add("Go", "unspecified", path, false);
                }
                for c in versions {
                    report.add("Go", &c[1], path, false);
                }
            }
            "pom.xml" => {
                let mut versions = java_version.captures_iter(text).peekable();
                if versions.peek().is_none() {
                    report.add("Java", "unspecified", path, false);
                }
                for c in versions {
                    report.add("Java", &c[1], path, false);
                }
            }
            "Gemfile" => {
                report.add(
                    "Ruby",
                    ruby_version
                        .captures(text)
                        .map(|c| c[1].to_string())
                        .as_deref()
                        .unwrap_or("unspecified"),
                    path,
                    false,
                );
            }
            ".python-version" => report.add("Python", text.trim(), path, false),
            ".nvmrc" | ".node-version" => report.add("Node", text.trim(), path, false),
            ".ruby-version" => report.add("Ruby", text.trim(), path, false),
            "rust-toolchain" => report.add("Rust", text.trim(), path, false),
            "rust-toolchain.toml" => report.add(
                "Rust",
                quoted_assignment(text, "channel")
                    .as_deref()
                    .unwrap_or("unknown"),
                path,
                false,
            ),
            ".tool-versions" => {
                for line in text.lines() {
                    let mut parts = line.split('#').next().unwrap_or("").split_whitespace();
                    let Some(name) = parts.next() else {
                        continue;
                    };
                    let tool = match name {
                        "python" => "Python",
                        "nodejs" | "node" => "Node",
                        "rust" => "Rust",
                        "golang" | "go" => "Go",
                        "java" => "Java",
                        "ruby" => "Ruby",
                        _ => continue,
                    };
                    for version in parts {
                        report.add(tool, version, path, false);
                    }
                }
            }
            _ if path.starts_with(".github/workflows/")
                && (path.ends_with(".yml") || path.ends_with(".yaml")) =>
            {
                workflow(text, path, files, &mut report)?
            }
            _ => {}
        }
    }
    Ok(report)
}

async fn read_file(
    api: &GithubApi,
    app: &InstalledApp,
    repo: &str,
    path: &str,
    commit: &str,
    files: &mut BTreeMap<String, String>,
) -> Result<()> {
    match api.contents(app, repo, path, commit).await? {
        RepositoryContent::File(text) => {
            files.insert(path.into(), text);
            Ok(())
        }
        _ => Err(CliError::failure(format!(
            "repository toolchain file {path} could not be read at its default branch commit"
        ))
        .into()),
    }
}

pub async fn infer_repository(api: &GithubApi, app: &InstalledApp, repo: &str) -> Result<Report> {
    let commit = api.default_branch_commit(app, repo).await?;
    let mut files = BTreeMap::new();
    match api.contents(app, repo, "", &commit).await? {
        RepositoryContent::Directory(entries) => {
            for entry in entries {
                if entry.kind == "file" && MANIFESTS.contains(&entry.path.as_str()) {
                    read_file(api, app, repo, &entry.path, &commit, &mut files).await?;
                }
            }
        }
        _ => {
            return Err(CliError::failure(
                "GitHub did not return the repository root contents directory",
            )
            .into())
        }
    }
    match api
        .contents(app, repo, ".github/workflows", &commit)
        .await?
    {
        RepositoryContent::Missing => {}
        RepositoryContent::Directory(entries) => {
            for entry in entries {
                if entry.kind == "file"
                    && entry.path.starts_with(".github/workflows/")
                    && (entry.path.ends_with(".yml") || entry.path.ends_with(".yaml"))
                {
                    read_file(api, app, repo, &entry.path, &commit, &mut files).await?;
                }
            }
        }
        _ => {
            return Err(
                CliError::failure("GitHub did not return the workflow contents directory").into(),
            )
        }
    }
    // Setup actions can name a conventional or repository-specific version
    // file. Discover the path from the action input rather than guessing it.
    let mut version_files = Vec::new();
    for (path, text) in &files {
        if !path.starts_with(".github/workflows/") {
            continue;
        }
        let document: serde_norway::Value = serde_norway::from_str(text)
            .map_err(|_| CliError::failure(format!("cannot parse repository workflow {path}")))?;
        if let Some(jobs) = document["jobs"].as_mapping() {
            for job in jobs.values() {
                for step in job["steps"].as_sequence().into_iter().flatten() {
                    for key in [
                        "python-version-file",
                        "node-version-file",
                        "go-version-file",
                    ] {
                        if let Some(file) = step["with"][key].as_str() {
                            if !file.contains("${{")
                                && !file.starts_with('/')
                                && !file.split('/').any(|part| part == "..")
                                && !files.contains_key(file)
                                && !version_files.iter().any(|found| found == file)
                            {
                                version_files.push(file.to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    for path in version_files {
        match api.contents(app, repo, &path, &commit).await? {
            // A missing action-declared version file leaves the workflow's
            // requirement unknown. Its diagnostic names the file and warns;
            // files discovered in directory listings remain mandatory reads.
            RepositoryContent::Missing => {}
            RepositoryContent::File(text) => {
                files.insert(path, text);
            }
            RepositoryContent::Directory(_) => {
                return Err(CliError::failure(format!(
                    "repository toolchain version file {path} is a directory"
                ))
                .into())
            }
        }
    }
    infer_files(&files)
}

/// Expand wildcard allowlists to the installed repositories, then report only
/// selected repositories. No content is printed, and diagnostics use stderr.
pub async fn announce(api: &GithubApi, app: &InstalledApp, allowlist: &[String]) -> Result<()> {
    for repo in crate::factory_app::label_targets(allowlist, &app.repos) {
        let report = infer_repository(api, app, &repo).await?;
        for note in report.notes() {
            crate::ui::ui().note(&format!("{repo}: {note}"));
        }
        for warning in report.warnings() {
            crate::ui::ui().warn(&format!("{repo}: {warning}"));
        }
    }
    Ok(())
}

pub struct AppPreflight {
    pub api: GithubApi,
    pub app: InstalledApp,
    pub pem: String,
    pub publication: crate::factory_app::PublicationIdentity,
}

pub async fn preflight(
    app_id: &str,
    key: &std::path::Path,
    requested: &[String],
) -> Result<AppPreflight> {
    let pem = crate::factory_app::read_private_key(key)?;
    let api = GithubApi::new()?;
    let app = crate::factory_app::inspect_app(&api, app_id, &pem).await?;
    let repos = crate::factory_app::resolve_allowlist(requested, &app)?;
    announce(&api, &app, &repos).await?;
    let publication = api.bot_publication_identity(&app.slug).await?;
    Ok(AppPreflight {
        api,
        app,
        pem,
        publication,
    })
}
