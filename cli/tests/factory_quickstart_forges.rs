//! GitHub describe output uses current contract fixtures derived from the
//! 20fccad31 capture (#4397 AC 4), including the selected agent's runner env.
//! Forge-kind flags retain their specified behavior (AC 5).
//!
//! Set `QUICKSTART_GOLDEN_CAPTURE=<dir>` to write the goldens instead of
//! comparing. Fixture updates require review of the full current plan.

use curie::factory_quickstart::{
    describe, quickstart_plan, PlanInput, ReleaseRead, DEFAULT_AGENT, DEFAULT_BUDGET_USD,
    DEFAULT_DEADLINE_SECONDS, DEFAULT_KIND_NAME, DEFAULT_MODEL, DEFAULT_OUTPUT_TOKENS_PER_RUN,
};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const OLD_KEY: &str = "api.githubFactoryIntake";
const NEW_KEY: &str = "factory.intake";
const HEADER_END: &str = "# ---";

fn golden_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/factory_quickstart")
}

fn base() -> PlanInput {
    PlanInput {
        repos: vec!["acme/widgets".to_string()],
        app_id: Some("12345".into()),
        private_key_file: Some("/keys/app.pem".into()),
        explicit_context: None,
        current_context: None,
        kind_name: DEFAULT_KIND_NAME.into(),
        existing_kind_clusters: Vec::new(),
        namespace: "curie".into(),
        release: "curie".into(),
        model: DEFAULT_MODEL.into(),
        org: None,
        execution_deadline_seconds: DEFAULT_DEADLINE_SECONDS,
        budget_usd: DEFAULT_BUDGET_USD,
        output_tokens: DEFAULT_OUTPUT_TOKENS_PER_RUN,
        chart: "charts/curie".into(),
        credential_in_env: true,
        release_has_real_model: false,
        chart_explicit: false,
        interactive: true,
        release_read: ReleaseRead::NotAtTarget,
        agent: DEFAULT_AGENT.into(),
        label: None,
        plugin_dir: None,
        model_explicit: false,
        recorded: None,
        bindings: Vec::new(),
        runner_image: None,
        jira: None,
    }
}

fn kind_path() -> PlanInput {
    base()
}

fn explicit_context() -> PlanInput {
    let mut input = base();
    input.explicit_context = Some("kind-curie-factory".into());
    input
}

fn current_context() -> PlanInput {
    let mut input = base();
    input.current_context = Some("kind-curie-factory".into());
    input
}

fn multi_repo() -> PlanInput {
    let mut input = explicit_context();
    input.repos = vec!["a/x".to_string(), "b/y".to_string()];
    input
}

/// `bundle_dir` embeds the temp dir; pin it so the output is reproducible.
fn pinned_tmpdir() {
    let dir = "/tmp/factory-quickstart-golden";
    std::fs::create_dir_all(dir).unwrap();
    std::env::set_var("TMPDIR", dir);
}

fn plan_text(input: &PlanInput) -> String {
    pinned_tmpdir();
    let planned = quickstart_plan(input).expect("the github plan builds");
    let mut text = describe(&planned).join("\n");
    text.push('\n');
    text
}

fn assert_golden(name: &str, input: &PlanInput) {
    let actual = plan_text(input);
    let path = golden_dir().join(format!("describe_4346_{name}.txt"));
    if let Ok(dir) = std::env::var("QUICKSTART_GOLDEN_CAPTURE") {
        let header = format!(
            "# Current describe contract based on commit 20fccad31 (the #4346 merge on next), case {name}.\n\
             # Uses {NEW_KEY} (#4397) and includes the selected agent's runnerEnv action.\n\
             # Compare directly, without runtime substitutions.\n\
             {HEADER_END}\n"
        );
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            Path::new(&dir).join(format!("describe_4346_{name}.txt")),
            format!("{header}{actual}"),
        )
        .unwrap();
        return;
    }
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("missing golden {}: {err}", path.display()));
    let (header, body) = raw
        .split_once(&format!("{HEADER_END}\n"))
        .expect("golden header ends with `# ---`");
    assert!(header.contains("20fccad31"), "{header}");
    assert!(
        header.contains(NEW_KEY) && header.contains("runnerEnv"),
        "{header}"
    );
    assert!(
        body.contains(NEW_KEY),
        "golden must use the current intake key"
    );
    assert!(
        !body.contains(OLD_KEY),
        "golden must omit the removed intake key"
    );
    assert_eq!(actual, body, "describe output for {name} drifted");
}

#[test]
fn factory_quickstart_describe_matches_the_4346_golden_kind_path() {
    assert_golden("kind_path", &kind_path());
}

#[test]
fn factory_quickstart_describe_matches_the_4346_golden_explicit_context() {
    assert_golden("explicit_context", &explicit_context());
}

#[test]
fn factory_quickstart_describe_matches_the_4346_golden_current_context() {
    assert_golden("current_context", &current_context());
}

#[test]
fn factory_quickstart_describe_matches_the_4346_golden_multi_repo() {
    assert_golden("multi_repo", &multi_repo());
}

// ---------------------------------------------------------------------------
// Forge kind flags (AC 5)
// ---------------------------------------------------------------------------

struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

impl Run {
    fn all(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

/// Run `curie factory quickstart --dry-run` against an explicit context with
/// stub kubectl and helm on PATH, so no cluster or network is involved.
fn quickstart(extra: &[&str]) -> Run {
    pinned_tmpdir();
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    for tool in ["kubectl", "helm"] {
        let path = bin.join(tool);
        std::fs::write(&path, "#!/bin/sh\nexit 0\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }
    let output = Command::new(env!("CARGO_BIN_EXE_curie"))
        .args([
            "factory",
            "quickstart",
            "--repo",
            "acme/widgets",
            "--context",
            "kind-curie-factory",
            "--dry-run",
            "--chart",
        ])
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("../charts/curie"))
        .args(extra)
        .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
        .env("HOME", dir.path())
        .env("TMPDIR", dir.path())
        .env("KUBECONFIG", dir.path().join("kubeconfig"))
        .env("CURIE_CREDENTIALS", "sk-or-fixture")
        .env_remove("CI")
        .env("NO_COLOR", "1")
        .stdin(Stdio::null())
        .output()
        .unwrap();
    Run {
        code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

/// The Jira URL and token flags of the Bitbucket pairing.
const TRACKER_FLAGS: [(&str, &str); 2] = [
    ("--tracker-url", "https://example.atlassian.net"),
    ("--tracker-token-file", "/tmp/jira-token"),
];

const SHARED_FLAGS: [(&str, &str); 4] = [
    ("--forge-url", "https://forge.example.com"),
    ("--token-file", "/tmp/forge-token"),
    ("--ca-file", "/tmp/forge-ca.pem"),
    ("--review-account", "review-bot"),
];

#[test]
fn factory_quickstart_bitbucket_without_jira_is_a_pairing_refusal() {
    // Bitbucket has no tracker of its own; it pairs only with Jira Cloud.
    for kind in ["bitbucket-cloud", "bitbucket-dc"] {
        let run = quickstart(&["--code-host", kind]);
        let text = run.all();
        assert_ne!(run.code, 0, "{kind}: {text}");
        assert!(
            text.contains("pairs only with --tracker jira-cloud"),
            "{kind}: {text}"
        );
        assert!(text.contains(kind), "{kind}: {text}");
        assert!(!text.contains("not installed"), "{kind}: {text}");
    }
}

#[test]
fn factory_quickstart_gitlab_with_a_github_partner_is_a_pairing_refusal() {
    // #4358 installs jira-cloud and #4399 gitlab, so no tracker kind is reserved.
    // GitLab pairs only with itself, on either side.
    for args in [["--tracker", "gitlab"], ["--code-host", "gitlab"]] {
        let run = quickstart(&args);
        let text = run.all();
        assert_ne!(run.code, 0, "{args:?}: {text}");
        assert!(text.contains("pairs only with"), "{args:?}: {text}");
        assert!(text.contains("gitlab"), "{args:?}: {text}");
        assert!(!text.contains("not installed"), "{args:?}: {text}");
    }
}

#[test]
fn factory_quickstart_shared_flags_reach_the_bitbucket_module_not_the_parser() {
    for (flag, value) in SHARED_FLAGS.iter().chain(&TRACKER_FLAGS) {
        let run = quickstart(&["--code-host", "bitbucket-dc", flag, value]);
        let text = run.all();
        assert!(
            !text.contains("unexpected argument"),
            "{flag} must parse for a non-github kind: {text}"
        );
        assert_ne!(run.code, 0, "{flag}: {text}");
        assert!(
            text.contains("pairs only with --tracker jira-cloud"),
            "{flag}: {text}"
        );
    }
}

#[test]
fn factory_quickstart_tracker_flags_apply_only_beside_bitbucket() {
    for (flag, value) in TRACKER_FLAGS {
        for extra in [
            &[][..],
            &["--tracker", "jira-cloud"][..],
            &[
                "--code-host",
                "gitlab",
                "--tracker",
                "gitlab",
                "--token-file",
                "/tmp/gitlab-token",
            ][..],
        ] {
            let mut args = extra.to_vec();
            args.extend([flag, value]);
            let run = quickstart(&args);
            let text = run.all();
            assert_ne!(run.code, 0, "{args:?}: {text}");
            assert!(
                text.contains(flag),
                "{args:?}: refusal names {flag}: {text}"
            );
        }
    }
}

#[test]
fn factory_quickstart_github_refuses_each_shared_non_github_flag() {
    for (flag, value) in SHARED_FLAGS {
        let run = quickstart(&[flag, value]);
        let text = run.all();
        assert_ne!(run.code, 0, "{flag}: {text}");
        assert!(
            !text.contains("unexpected argument"),
            "{flag} must be a known flag the command refuses for github: {text}"
        );
        assert!(text.contains(flag), "refusal names {flag}: {text}");
        assert!(!text.contains("not installed"), "{flag}: {text}");
    }
}

#[test]
fn factory_quickstart_default_github_invocation_still_plans() {
    let run = quickstart(&["--json"]);
    assert_eq!(run.code, 0, "{}", run.all());
    let body: serde_json::Value = serde_json::from_str(&run.stdout).expect("one plan object");
    let lines = body["plan"].as_array().expect("plan lines");
    assert!(
        lines.iter().any(
            |line| line.as_str().unwrap().contains("curie cluster deploy")
                || line.as_str().unwrap().contains("curie cluster up")
        ),
        "{body}"
    );

    let explicit = quickstart(&["--json", "--code-host", "github", "--tracker", "github"]);
    assert_eq!(explicit.code, 0, "{}", explicit.all());
    assert_eq!(explicit.stdout, run.stdout);
}
