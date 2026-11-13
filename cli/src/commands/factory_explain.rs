//! `curie factory explain`: read a factory pull request's why-record from the
//! forge with the caller's own login. It never reads the platform key.

use std::collections::BTreeMap;
use std::process::Command;

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;

use crate::exit::CliError;
use crate::ui::{CliOutput, Ui};

const NONE_LINE: &str = "None: the change follows directly from the acceptance criteria.";
const MAX_ENTRIES: usize = 7;
const MAX_PAGES: usize = 10;
const SUMMARY_CHARS: usize = 1000;
const REVIEW_CHARS: usize = 2000;
/// Nonblank lines in the Decisions section, not counting the breaks between them.
const MAX_SECTION_CHARACTERS: usize = 7000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct DecisionEntry {
    pub n: u64,
    pub choice: String,
    pub rejected: String,
    pub reason: String,
    pub source: String,
    pub evidence: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DecisionsParse {
    Recorded(Vec<DecisionEntry>),
    None,
    NotRecorded,
    Unparsed(Vec<String>),
}

#[derive(Clone, Debug)]
struct IssueRef {
    owner: String,
    repo: String,
    number: u64,
}

#[derive(Serialize)]
struct DecisionsOut {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    entries: Option<Vec<DecisionEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lines: Option<Vec<String>>,
}

#[derive(Serialize)]
struct VerificationOut {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    checked: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unchecked: Option<u64>,
}

#[derive(Serialize)]
struct ForgeReview {
    reviewer: String,
    state: String,
}

#[derive(Serialize)]
struct ChecksOut {
    passing: u64,
    failing: u64,
    pending: u64,
    failing_names: Vec<String>,
}

#[derive(Serialize)]
struct PullOut {
    number: u64,
    url: String,
    title: String,
    state: String,
    head_sha: String,
    created_at: String,
    summary: String,
    decisions: DecisionsOut,
    verification: VerificationOut,
    review_section: String,
    forge_reviews: Vec<ForgeReview>,
    checks: ChecksOut,
}

/// One JSON object for `curie factory explain`.
pub struct FactoryExplainOutput {
    issue: String,
    pull_requests: Vec<PullOut>,
    message: Option<String>,
}

#[derive(Serialize)]
struct ExplainJson<'a> {
    issue: &'a str,
    pull_requests: &'a [PullOut],
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<&'a str>,
}

impl CliOutput for FactoryExplainOutput {
    fn to_json(&self) -> Value {
        serde_json::to_value(ExplainJson {
            issue: &self.issue,
            pull_requests: &self.pull_requests,
            message: self.message.as_deref(),
        })
        .unwrap_or_else(|_| Value::Object(Default::default()))
    }

    fn render(&self, ui: &Ui) {
        ui.payload_plain(&format!("issue {}", self.issue));
        if let Some(message) = &self.message {
            ui.payload_plain(message);
        }
        for pull in &self.pull_requests {
            render_pull(ui, pull);
        }
    }
}

fn render_pull(ui: &Ui, pull: &PullOut) {
    ui.payload_plain(&format!("pull request {}", pull.number));
    ui.payload_plain(&format!("url {}", pull.url));
    ui.payload_plain(&format!("title {}", pull.title));
    ui.payload_plain(&format!("state {}", pull.state));
    ui.payload_plain(&format!("head {}", pull.head_sha));
    ui.payload_plain(&format!("created {}", pull.created_at));
    ui.payload_plain("summary");
    for line in pull.summary.lines() {
        ui.payload_plain(line);
    }
    ui.payload_plain(&format!("decisions {}", pull.decisions.status));
    if let Some(entries) = &pull.decisions.entries {
        for entry in entries {
            ui.payload_plain(&format!("{}. {}", entry.n, entry.choice));
            ui.payload_plain(&format!("rejected {}", entry.rejected));
            ui.payload_plain(&format!("reason {}", entry.reason));
            ui.payload_plain(&format!("source {}", entry.source));
            ui.payload_plain(&format!("evidence {}", entry.evidence));
        }
    }
    if let Some(lines) = &pull.decisions.lines {
        for line in lines {
            ui.payload_plain(line);
        }
    }
    match &pull.verification.checked {
        Some(checked) => ui.payload_plain(&format!(
            "verification checked {checked} unchecked {}",
            pull.verification.unchecked.unwrap_or(0)
        )),
        None => ui.payload_plain("verification not recorded"),
    }
    ui.payload_plain(&format!("reviews {}", pull.review_section));
    for review in &pull.forge_reviews {
        ui.payload_plain(&format!("review {} {}", review.reviewer, review.state));
    }
    let checks = &pull.checks;
    ui.payload_plain(&format!(
        "checks passing {} failing {} pending {}",
        checks.passing, checks.failing, checks.pending
    ));
    for name in &checks.failing_names {
        ui.payload_plain(&format!("failing {name}"));
    }
}

/// Read the factory pull requests that publish `reference`.
pub async fn explain(reference: String, api_url: String) -> Result<FactoryExplainOutput> {
    let issue = parse_reference(&reference)?;
    let api_url = api_url.trim().trim_end_matches('/').to_string();
    let host = gh_hostname(&api_url)?;
    let token = forge_token(&host)?;
    let client = github_client()?;
    let requested_path = format!("/repos/{}/{}", issue.owner, issue.repo);
    let repo = get_object(&client, &api_url, &requested_path, &token).await?;
    let canonical_owner = repo
        .pointer("/owner/login")
        .and_then(Value::as_str)
        .filter(|value| ident(value))
        .unwrap_or(issue.owner.as_str());
    let canonical_repo = repo
        .get("name")
        .and_then(Value::as_str)
        .filter(|value| ident(value))
        .unwrap_or(issue.repo.as_str());
    let repo_path = format!("/repos/{canonical_owner}/{canonical_repo}");
    let repo_id = repo
        .get("id")
        .and_then(Value::as_u64)
        .ok_or_else(|| failure("GitHub repository response has no id"))?;
    let html_url = repo
        .get("html_url")
        .and_then(Value::as_str)
        .ok_or_else(|| failure("GitHub repository response has no html_url"))?;
    let marker_host = reqwest::Url::parse(html_url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .ok_or_else(|| failure("GitHub repository html_url has no host"))?;
    let issue_url = format!("{repo_path}/issues/{}", issue.number);
    let issue_doc = get_object(&client, &api_url, &issue_url, &token).await?;
    if issue_doc
        .get("pull_request")
        .is_some_and(|value| !value.is_null())
    {
        return Err(usage(
            "the reference is a pull request; pass the issue it closes",
        ));
    }
    let marker = format!(
        "<!-- curie-factory-issue kind=github host={marker_host} scope={repo_id} issue={} -->",
        issue.number
    );
    let mut candidates = timeline_candidates(
        &client,
        &api_url,
        &format!("{issue_url}/timeline?per_page=100"),
        &token,
        canonical_owner,
        canonical_repo,
    )
    .await?;
    candidates.sort_by(|left, right| {
        right
            .created
            .cmp(&left.created)
            .then(right.number.cmp(&left.number))
    });
    candidates.truncate(20);
    let mut pulls = Vec::new();
    for candidate in candidates {
        let pr = get_object(
            &client,
            &api_url,
            &format!("{repo_path}/pulls/{}", candidate.number),
            &token,
        )
        .await?;
        let body = pr.get("body").and_then(Value::as_str).unwrap_or("");
        if !body.lines().any(|line| line.trim() == marker) {
            continue;
        }
        pulls.push(pull_record(&client, &api_url, &repo_path, &token, &pr).await?);
    }
    pulls.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then(right.number.cmp(&left.number))
    });
    let message = if pulls.is_empty() {
        Some("no factory pull request".to_string())
    } else {
        None
    };
    Ok(FactoryExplainOutput {
        issue: format!("{}/{}#{}", issue.owner, issue.repo, issue.number),
        pull_requests: pulls,
        message,
    })
}

struct Candidate {
    number: u64,
    created: String,
}

async fn timeline_candidates(
    client: &reqwest::Client,
    api_url: &str,
    path: &str,
    token: &str,
    owner: &str,
    repo: &str,
) -> Result<Vec<Candidate>> {
    let events = get_pages(client, api_url, path, token, PageShape::Array).await?;
    let mut best: BTreeMap<u64, String> = BTreeMap::new();
    for event in events {
        let Some((number, created)) = cross_referenced_pull(&event, owner, repo) else {
            continue;
        };
        let slot = best.entry(number).or_default();
        if created > *slot {
            *slot = created;
        }
    }
    Ok(best
        .into_iter()
        .map(|(number, created)| Candidate { number, created })
        .collect())
}

fn cross_referenced_pull(event: &Value, owner: &str, repo: &str) -> Option<(u64, String)> {
    if event.get("event").and_then(Value::as_str) != Some("cross-referenced") {
        return None;
    }
    let source = event.get("source")?.get("issue")?;
    let url = source.get("pull_request")?.get("url")?.as_str()?;
    let needle = format!("/repos/{owner}/{repo}/pulls/").to_ascii_lowercase();
    let lowered = url.to_ascii_lowercase();
    let rest = lowered.split(&needle).nth(1)?;
    let number = rest.split(['/', '?', '#']).next()?.parse().ok()?;
    if number == 0 {
        return None;
    }
    let created = source
        .get("created_at")
        .and_then(Value::as_str)
        .or_else(|| event.get("created_at").and_then(Value::as_str))
        .unwrap_or("")
        .to_string();
    Some((number, created))
}

async fn pull_record(
    client: &reqwest::Client,
    api_url: &str,
    repo_path: &str,
    token: &str,
    pr: &Value,
) -> Result<PullOut> {
    let number = pr
        .get("number")
        .and_then(Value::as_u64)
        .ok_or_else(|| failure("GitHub pull request response has no number"))?;
    let sha = pr
        .pointer("/head/sha")
        .and_then(Value::as_str)
        .ok_or_else(|| failure("GitHub pull request response has no head sha"))?;
    let body = pr.get("body").and_then(Value::as_str).unwrap_or("");
    let reviews = get_pages(
        client,
        api_url,
        &format!("{repo_path}/pulls/{number}/reviews?per_page=100"),
        token,
        PageShape::Array,
    )
    .await?;
    let check_runs = get_pages(
        client,
        api_url,
        &format!("{repo_path}/commits/{sha}/check-runs?per_page=100"),
        token,
        PageShape::CheckRuns,
    )
    .await?;
    let statuses = get_pages(
        client,
        api_url,
        &format!("{repo_path}/commits/{sha}/statuses?per_page=100"),
        token,
        PageShape::Array,
    )
    .await?;
    Ok(PullOut {
        number,
        url: pr
            .get("html_url")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        title: pr
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        state: pull_state(pr),
        head_sha: sha.to_string(),
        created_at: pr
            .get("created_at")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        summary: summary_text(body),
        decisions: decisions_out(&parse_decisions(body)),
        verification: verification_out(body),
        review_section: section_text(body, "## Reviews")
            .unwrap_or_else(|| "not recorded".to_string()),
        forge_reviews: latest_reviews(&reviews),
        checks: checks_out(&check_runs, &statuses),
    })
}

fn pull_state(pr: &Value) -> String {
    if pr.get("merged_at").is_some_and(|value| value.is_string()) {
        return "merged".to_string();
    }
    match pr.get("state").and_then(Value::as_str) {
        Some("open") => "open".to_string(),
        _ => "closed".to_string(),
    }
}

fn decisions_out(parsed: &DecisionsParse) -> DecisionsOut {
    match parsed {
        DecisionsParse::Recorded(entries) => DecisionsOut {
            status: "recorded",
            entries: Some(entries.clone()),
            lines: None,
        },
        DecisionsParse::None => DecisionsOut {
            status: "none",
            entries: None,
            lines: None,
        },
        DecisionsParse::NotRecorded => DecisionsOut {
            status: "not recorded",
            entries: None,
            lines: None,
        },
        DecisionsParse::Unparsed(lines) => DecisionsOut {
            status: "unparsed",
            entries: None,
            lines: Some(lines.clone()),
        },
    }
}

fn verification_out(body: &str) -> VerificationOut {
    let checked = body.matches("- [x]").count() as u64;
    let unchecked = body.matches("- [ ]").count() as u64;
    if checked + unchecked == 0 {
        VerificationOut {
            status: "not recorded",
            checked: None,
            unchecked: None,
        }
    } else {
        VerificationOut {
            status: "recorded",
            checked: Some(checked),
            unchecked: Some(unchecked),
        }
    }
}

fn latest_reviews(reviews: &[Value]) -> Vec<ForgeReview> {
    let mut best: BTreeMap<String, (String, String)> = BTreeMap::new();
    for review in reviews {
        let Some(reviewer) = review.pointer("/user/login").and_then(Value::as_str) else {
            continue;
        };
        let Some(state) = review.get("state").and_then(Value::as_str) else {
            continue;
        };
        let at = review
            .get("submitted_at")
            .and_then(Value::as_str)
            .unwrap_or("");
        // Reviews are oldest first, so a later row wins a shared timestamp.
        match best.get(reviewer) {
            Some((prev, _)) if prev.as_str() > at => {}
            _ => {
                best.insert(reviewer.to_string(), (at.to_string(), state.to_string()));
            }
        }
    }
    best.into_iter()
        .map(|(reviewer, (_, state))| ForgeReview { reviewer, state })
        .collect()
}

#[derive(Clone, Copy)]
enum Bucket {
    Passing,
    Failing,
    Pending,
}

fn checks_out(check_runs: &[Value], statuses: &[Value]) -> ChecksOut {
    let mut runs: BTreeMap<String, Bucket> = BTreeMap::new();
    for run in check_runs {
        let Some(name) = run
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
        else {
            continue;
        };
        let bucket = check_run_bucket(
            run.get("status").and_then(Value::as_str).unwrap_or(""),
            run.get("conclusion").and_then(Value::as_str),
        );
        // GitHub lists check runs newest first. The first row for a name is
        // that check, including a queued rerun with no completion time and a
        // newer failure whose older sibling completed later.
        runs.entry(name.to_string()).or_insert(bucket);
    }
    let mut contexts: BTreeMap<String, Bucket> = BTreeMap::new();
    for status in statuses {
        let Some(name) = status
            .get("context")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
        else {
            continue;
        };
        let bucket = status_bucket(status.get("state").and_then(Value::as_str).unwrap_or(""));
        // Commit statuses are newest first, so the first row for a context wins.
        contexts.entry(name.to_string()).or_insert(bucket);
    }
    let mut passing = 0;
    let mut failing = 0;
    let mut pending = 0;
    let mut failing_names = Vec::new();
    for (name, bucket) in runs.into_iter().chain(contexts) {
        match bucket {
            Bucket::Passing => passing += 1,
            Bucket::Failing => {
                failing += 1;
                failing_names.push(name);
            }
            Bucket::Pending => pending += 1,
        }
    }
    failing_names.sort();
    failing_names.dedup();
    ChecksOut {
        passing,
        failing,
        pending,
        failing_names,
    }
}

fn check_run_bucket(status: &str, conclusion: Option<&str>) -> Bucket {
    match status {
        "queued" | "in_progress" | "waiting" | "pending" | "requested" => Bucket::Pending,
        "completed" => match conclusion.unwrap_or("") {
            "success" | "neutral" | "skipped" => Bucket::Passing,
            "failure" | "cancelled" | "timed_out" | "action_required" | "stale"
            | "startup_failure" => Bucket::Failing,
            _ => Bucket::Pending,
        },
        _ => Bucket::Pending,
    }
}

fn status_bucket(state: &str) -> Bucket {
    match state {
        "success" => Bucket::Passing,
        "failure" | "error" => Bucket::Failing,
        _ => Bucket::Pending,
    }
}

// The vector cases include non-string bodies. Production always has a string.
#[allow(dead_code)]
pub(crate) fn parse_decisions_body(body: &Value) -> DecisionsParse {
    match body.as_str() {
        Some(text) => parse_decisions(text),
        None => DecisionsParse::NotRecorded,
    }
}

pub(crate) fn parse_decisions(body: &str) -> DecisionsParse {
    let lines = normalized_lines(body);
    let headings = lines
        .iter()
        .filter(|line| line.as_str() == "## Decisions")
        .count();
    if headings == 0 {
        return DecisionsParse::NotRecorded;
    }
    if headings > 1 {
        return DecisionsParse::Unparsed(lines);
    }
    let start = lines
        .iter()
        .position(|line| line == "## Decisions")
        .unwrap_or(0)
        + 1;
    let mut section = Vec::new();
    for line in &lines[start..] {
        if line.starts_with("## ") {
            break;
        }
        section.push(line.clone());
    }
    // The cap is the sum of the nonblank lines. Breaks between them do not count.
    let section_characters: usize = section
        .iter()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.chars().count())
        .sum();
    if section_characters > MAX_SECTION_CHARACTERS {
        return DecisionsParse::Unparsed(section);
    }
    let content: Vec<&str> = section
        .iter()
        .map(String::as_str)
        .filter(|line| !line.trim().is_empty())
        .collect();
    if content.is_empty() {
        return DecisionsParse::Unparsed(section);
    }
    if content.len() == 1 && content[0] == NONE_LINE {
        return DecisionsParse::None;
    }
    if content.iter().any(|line| line.starts_with("None:")) {
        return DecisionsParse::Unparsed(section);
    }
    let mut entries = Vec::new();
    for line in content {
        let Some(entry) = parse_entry(line) else {
            return DecisionsParse::Unparsed(section);
        };
        entries.push(entry);
    }
    if entries.len() > MAX_ENTRIES
        || entries
            .iter()
            .enumerate()
            .any(|(index, entry)| entry.n != index as u64 + 1)
    {
        return DecisionsParse::Unparsed(section);
    }
    DecisionsParse::Recorded(entries)
}

fn normalized_lines(body: &str) -> Vec<String> {
    body.split('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line).to_string())
        .collect()
}

fn parse_entry(line: &str) -> Option<DecisionEntry> {
    if line != line.trim() || !line.ends_with('.') {
        return None;
    }
    let (number, rest) = line.split_once(". ")?;
    if number.is_empty()
        || number.starts_with('0')
        || !number.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let n = number.parse().ok()?;
    let (choice, rest) = rest.split_once(". Rejected: ")?;
    let (rejected, rest) = rest.split_once(", because ")?;
    // The source delimiter is the last `. (source) Evidence:` so a reason may
    // contain parentheses and periods.
    let (reason_and_source, evidence_raw) = rest.rsplit_once(") Evidence: ")?;
    if !evidence_raw.ends_with('.') {
        return None;
    }
    let evidence = &evidence_raw[..evidence_raw.len() - 1];
    let (reason, source) = reason_and_source.rsplit_once(". (")?;
    if !matches!(source, "plan" | "plan review" | "diff review") {
        return None;
    }
    if blank(choice) || blank(rejected) || blank(reason) || blank(evidence) {
        return None;
    }
    Some(DecisionEntry {
        n,
        choice: choice.to_string(),
        rejected: rejected.to_string(),
        reason: reason.to_string(),
        source: source.to_string(),
        evidence: evidence.to_string(),
    })
}

fn blank(value: &str) -> bool {
    value.trim().is_empty() || value != value.trim()
}

fn summary_text(body: &str) -> String {
    let mut kept = Vec::new();
    for line in normalized_lines(body) {
        if line.starts_with("## ") {
            break;
        }
        kept.push(line);
    }
    clip_chars(&kept.join("\n"), SUMMARY_CHARS)
}

fn section_text(body: &str, heading: &str) -> Option<String> {
    let lines = normalized_lines(body);
    let start = lines.iter().position(|line| line == heading)? + 1;
    let mut kept = Vec::new();
    for line in &lines[start..] {
        if line.starts_with("## ") {
            break;
        }
        kept.push(line.as_str());
    }
    Some(clip_chars(kept.join("\n").trim_matches('\n'), REVIEW_CHARS))
}

fn clip_chars(text: &str, max: usize) -> String {
    text.chars().take(max).collect()
}

fn parse_reference(raw: &str) -> Result<IssueRef> {
    let raw = raw.trim();
    if let Some(rest) = raw
        .strip_prefix("https://github.com/")
        .or_else(|| raw.strip_prefix("https://www.github.com/"))
    {
        let rest = rest.split(['?', '#']).next().unwrap_or(rest);
        let mut parts = rest.trim_end_matches('/').split('/');
        let owner = parts.next().unwrap_or("");
        let repo = parts.next().unwrap_or("");
        let kind = parts.next().unwrap_or("");
        let number = parts.next().unwrap_or("");
        if parts.next().is_some() {
            return Err(usage("malformed GitHub issue reference"));
        }
        if kind == "pull" {
            return Err(usage(
                "the reference is a pull request; pass the issue it closes",
            ));
        }
        if kind != "issues" {
            return Err(usage("unsupported tracker or malformed issue reference"));
        }
        return issue_ref(owner, repo, number);
    }
    if raw.contains("://") {
        return Err(usage("unsupported tracker or malformed issue reference"));
    }
    let Some((slug, number)) = raw.rsplit_once('#') else {
        return Err(usage("malformed issue reference"));
    };
    let Some((owner, repo)) = slug.split_once('/') else {
        return Err(usage("malformed issue reference"));
    };
    if repo.contains('/') {
        return Err(usage("malformed issue reference"));
    }
    issue_ref(owner, repo, number)
}

fn issue_ref(owner: &str, repo: &str, number: &str) -> Result<IssueRef> {
    if !ident(owner) || !ident(repo) {
        return Err(usage("malformed issue reference"));
    }
    let number: u64 = number
        .parse()
        .map_err(|_| usage("malformed issue reference"))?;
    if number == 0 {
        return Err(usage("malformed issue reference"));
    }
    Ok(IssueRef {
        owner: owner.to_string(),
        repo: repo.to_string(),
        number,
    })
}

fn ident(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.')
}

pub(crate) fn gh_hostname(api_url: &str) -> Result<String> {
    let url = reqwest::Url::parse(api_url.trim()).map_err(|_| usage("malformed --api-url"))?;
    if url.scheme() != "https" && url.scheme() != "http" {
        return Err(usage("malformed --api-url"));
    }
    let host = url.host_str().ok_or_else(|| usage("malformed --api-url"))?;
    if host == "api.github.com" {
        Ok("github.com".to_string())
    } else {
        Ok(host.to_string())
    }
}

pub(crate) fn gh_token_args(host: &str) -> Vec<String> {
    let mut args = vec!["auth".to_string(), "token".to_string()];
    if host != "github.com" {
        args.push("--hostname".to_string());
        args.push(host.to_string());
    }
    args
}

fn forge_token(host: &str) -> Result<String> {
    for key in ["GH_TOKEN", "GITHUB_TOKEN"] {
        if let Ok(value) = std::env::var(key) {
            let value = value.trim();
            if !value.is_empty() {
                return Ok(value.to_string());
            }
        }
    }
    let output = Command::new("gh").args(gh_token_args(host)).output().ok();
    if let Some(output) = output {
        if output.status.success() {
            let token = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !token.is_empty() {
                return Ok(token);
            }
        }
    }
    Err(anyhow::Error::from(
        CliError::failure("no GitHub token is available for this forge")
            .with_fix("run `gh auth login` or set GH_TOKEN"),
    ))
}

fn github_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(30))
        .no_proxy()
        .build()
        .context("building the GitHub client")
}

#[derive(Clone, Copy)]
enum PageShape {
    Array,
    CheckRuns,
}

async fn get_object(
    client: &reqwest::Client,
    api_url: &str,
    path: &str,
    token: &str,
) -> Result<Value> {
    let (value, _) = request_json(client, &absolute(api_url, path), token).await?;
    if !value.is_object() {
        return Err(failure(
            "GitHub API returned a response that is not an object",
        ));
    }
    Ok(value)
}

async fn get_pages(
    client: &reqwest::Client,
    api_url: &str,
    path: &str,
    token: &str,
    shape: PageShape,
) -> Result<Vec<Value>> {
    let mut url = absolute(api_url, path);
    let mut items = Vec::new();
    for _ in 0..MAX_PAGES {
        let (value, next) = request_json(client, &url, token).await?;
        match shape {
            PageShape::Array => {
                let page = value
                    .as_array()
                    .cloned()
                    .ok_or_else(|| failure("GitHub API returned a response that is not a list"))?;
                if page.is_empty() {
                    break;
                }
                items.extend(page);
            }
            PageShape::CheckRuns => {
                let page = value
                    .get("check_runs")
                    .and_then(Value::as_array)
                    .cloned()
                    .ok_or_else(|| failure("GitHub check runs response has no check_runs"))?;
                if page.is_empty() {
                    break;
                }
                items.extend(page);
            }
        }
        let Some(next) = next else { break };
        if !same_api_origin(api_url, &next) {
            return Err(failure("GitHub pagination left the API host"));
        }
        url = next;
    }
    Ok(items)
}

fn same_api_origin(api_url: &str, next: &str) -> bool {
    let Ok(base) = reqwest::Url::parse(api_url) else {
        return false;
    };
    let Ok(next) = reqwest::Url::parse(next) else {
        return false;
    };
    base.scheme() == next.scheme()
        && base.host_str() == next.host_str()
        && base.port_or_known_default() == next.port_or_known_default()
}

fn absolute(api_url: &str, path: &str) -> String {
    if path.starts_with("http://") || path.starts_with("https://") {
        path.to_string()
    } else {
        format!("{api_url}{path}")
    }
}

async fn request_json(
    client: &reqwest::Client,
    url: &str,
    token: &str,
) -> Result<(Value, Option<String>)> {
    let response = client
        .get(url)
        .header("authorization", format!("Bearer {token}"))
        .header("accept", "application/vnd.github+json")
        .header("user-agent", "curie")
        .header("x-github-api-version", "2022-11-28")
        .send()
        .await
        .map_err(github_transport)?;
    let status = response.status();
    let next = next_link(
        response
            .headers()
            .get("link")
            .and_then(|value| value.to_str().ok()),
    );
    let rate_remaining = response
        .headers()
        .get("x-ratelimit-remaining")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    let body = response.text().await.map_err(github_transport)?;
    if !status.is_success() {
        return Err(http_error(status.as_u16(), &rate_remaining, &body));
    }
    let value =
        serde_json::from_str(&body).context("GitHub API returned a body that is not JSON")?;
    Ok((value, next))
}

fn github_transport(error: reqwest::Error) -> anyhow::Error {
    // `Response::text` reports a short read as a decode error. That is a dropped
    // connection, the same class as a connect or timeout failure.
    if error.is_connect()
        || error.is_timeout()
        || error.is_request()
        || error.is_body()
        || error.is_decode()
    {
        return anyhow::Error::from(CliError::transient(format!(
            "GitHub API request failed: {error}"
        )));
    }
    anyhow::Error::from(error)
}

fn next_link(header: Option<&str>) -> Option<String> {
    let header = header?;
    for part in header.split(',') {
        let mut pieces = part.split(';');
        let url = pieces
            .next()
            .unwrap_or("")
            .trim()
            .trim_start_matches('<')
            .trim_end_matches('>');
        let rel = pieces.next().unwrap_or("");
        if rel.contains("rel=\"next\"") && !url.is_empty() {
            return Some(url.to_string());
        }
    }
    None
}

fn http_error(status: u16, rate_remaining: &str, body: &str) -> anyhow::Error {
    let rate = status == 429
        || ((status == 403 || status == 429) && rate_remaining == "0")
        || body.to_ascii_lowercase().contains("rate limit");
    if status >= 500 || rate {
        return anyhow::Error::from(CliError::transient(format!("GitHub API returned {status}")));
    }
    failure(format!("GitHub API returned {status}"))
}

fn usage(message: impl Into<String>) -> anyhow::Error {
    crate::exit::usage(message)
}

fn failure(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::from(CliError::failure(message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn github_api_omits_the_gh_hostname_flag() {
        assert_eq!(gh_hostname("https://api.github.com").unwrap(), "github.com");
        assert_eq!(gh_token_args("github.com"), vec!["auth", "token"]);
        assert_eq!(
            gh_token_args("ghe.example"),
            vec!["auth", "token", "--hostname", "ghe.example"]
        );
    }

    #[test]
    fn factory_decisions_vector() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../tests/vectors/factory-decisions-section.json");
        let raw =
            std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {path:?}: {error}"));
        let document: Value = serde_json::from_str(&raw).expect("vector json");
        let cases = document["cases"].as_array().expect("cases");
        assert!(!cases.is_empty());
        for case in cases {
            let id = case["id"].as_str().unwrap_or("?");
            let parsed = parse_decisions_body(&case["body"]);
            if case["valid"].as_bool() != Some(true) {
                assert!(
                    matches!(
                        parsed,
                        DecisionsParse::NotRecorded | DecisionsParse::Unparsed(_)
                    ),
                    "{id} must not be recorded: {parsed:?}"
                );
                continue;
            }
            let expected = case["entries"]
                .as_array()
                .unwrap_or_else(|| panic!("{id} entries"));
            if expected.is_empty() {
                assert!(matches!(parsed, DecisionsParse::None), "{id}: {parsed:?}");
                continue;
            }
            let DecisionsParse::Recorded(entries) = parsed else {
                panic!("{id} should be recorded: {parsed:?}");
            };
            assert_eq!(entries.len(), expected.len(), "{id}");
            for (entry, want) in entries.iter().zip(expected) {
                assert_eq!(entry.n, want["n"].as_u64().unwrap(), "{id}");
                assert_eq!(entry.choice, want["choice"].as_str().unwrap(), "{id}");
                assert_eq!(entry.rejected, want["rejected"].as_str().unwrap(), "{id}");
                assert_eq!(entry.reason, want["reason"].as_str().unwrap(), "{id}");
                assert_eq!(entry.source, want["source"].as_str().unwrap(), "{id}");
                assert_eq!(entry.evidence, want["evidence"].as_str().unwrap(), "{id}");
            }
        }
    }

    #[test]
    fn decisions_section_character_cap() {
        let line_for = |choice_len: usize| {
            format!(
                "1. {}. Rejected: a, because r. (plan) Evidence: e.",
                "c".repeat(choice_len)
            )
        };
        let mut choice_len = 1;
        while line_for(choice_len).chars().count() < MAX_SECTION_CHARACTERS {
            choice_len += 1;
        }
        assert_eq!(line_for(choice_len).chars().count(), MAX_SECTION_CHARACTERS);
        let at_cap = parse_decisions(&format!("## Decisions\n{}\n", line_for(choice_len)));
        assert!(
            matches!(at_cap, DecisionsParse::Recorded(ref entries) if entries.len() == 1),
            "{at_cap:?}"
        );
        let over = parse_decisions(&format!("## Decisions\n{}\n", line_for(choice_len + 1)));
        assert!(matches!(over, DecisionsParse::Unparsed(_)), "{over:?}");
    }
}
