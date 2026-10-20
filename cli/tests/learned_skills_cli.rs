//! Integration: the `skills` verb group and `overrides --learned-skills` at
//! BOTH tiers (epic #4780, ADR 0237).
//!
//! The tests drive the BUILT BINARY against an in-process mock of the platform
//! API and assert the HTTP requests the CLI makes and what it prints. Nothing
//! inside the CLI is faked.
//!
//! Assumption pinned for the implementer: `--api-url`/`--api-key` (and
//! `--json` before the tier) are accepted after the verb arguments, and a
//! skill `<ref>` is passed to the API verbatim (slug or id, no CLI-side
//! resolution), per plan R13 where the API resolves the ref.

use crate::support;

use std::process::Command;
use support::{serve, MockServer, Request, Response};

const AGENT_ID: &str = "66666666-6666-6666-6666-666666666666";
const AGENT_NAME: &str = "acme-bot";
const SKILL_ID: &str = "77777777-7777-7777-7777-777777777777";
const SLUG: &str = "deploy-notes";
const DESCRIPTION: &str = "Use when deploying \"acme\" services";
const BODY: &str = "Step one: check the rollout.\nStep two: verify.\n";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn skills_path() -> String {
    format!("/agents/{AGENT_ID}/learned-skills")
}

fn agent_json(learned: bool) -> String {
    format!(
        r##"{{"id":"{AGENT_ID}","name":"{AGENT_NAME}","channels":[{{"kind":"slack","address":"#x"}}],"created_at":"2026-07-05T00:00:00Z","memory":false,"learned_skills":{learned}}}"##
    )
}

fn skill_json(revision: u32, deleted: bool) -> serde_json::Value {
    serde_json::json!({
        "id": SKILL_ID,
        "slug": SLUG,
        "description": DESCRIPTION,
        "body": BODY,
        "revision": revision,
        "author": "operator",
        "conversation": null,
        "updated_at": "2026-10-20T00:00:00Z",
        "created_at": "2026-10-19T00:00:00Z",
        "deleted": deleted,
    })
}

fn revision_json(n: u32, action: &str, body: &str) -> serde_json::Value {
    // The API always sends `deleted`.
    serde_json::json!({
        "deleted": false,
        "revision": n,
        "action": action,
        "slug": SLUG,
        "description": DESCRIPTION,
        "body": body,
        "digest": format!("digest-{n}"),
        "author": "operator",
        "conversation": null,
        "turn": null,
        "source": "operator",
        "reverted_to": null,
        "created_at": "2026-10-20T00:00:00Z",
    })
}

/// Path without its query string.
fn route(req: &Request) -> &str {
    req.path.split('?').next().unwrap_or(&req.path)
}

/// A mock API with one agent, one skill at `head` revision, and a revision
/// history whose bodies differ so a diff has -/+ lines.
fn api(head: u32, deleted: bool, learned: bool) -> MockServer {
    let mut revs = vec![revision_json(1, "create", "alpha\nbeta\n")];
    for n in 2..=head {
        revs.push(revision_json(n, "patch", &format!("alpha\nbeta {n}\n")));
    }
    api_with(head, deleted, learned, revs)
}

/// Like `api`, with the revision history the API answers given verbatim.
fn api_with(head: u32, deleted: bool, learned: bool, revs: Vec<serde_json::Value>) -> MockServer {
    serve(move |req| {
        let (m, p) = (req.method.as_str(), route(req));
        let base = skills_path();
        let one = format!("{base}/{SLUG}");
        // Mutations address the skill by the uuid the GET returned.
        let by_id = format!("{base}/{SKILL_ID}");
        match m {
            "GET" if p == "/agents" => Response::json(200, &format!("[{}]", agent_json(learned))),
            "GET" if p == format!("/agents/{AGENT_ID}") => {
                Response::json(200, &agent_json(learned))
            }
            "PATCH" if p == format!("/agents/{AGENT_ID}") => {
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                let on = body["learned_skills"].as_bool().unwrap_or(learned);
                Response::json(200, &agent_json(on))
            }
            "GET" if p == base => Response::json(
                200,
                &serde_json::json!({"enabled": true, "skills": [skill_json(head, false)]})
                    .to_string(),
            ),
            "GET" if p == one => Response::json(200, &skill_json(head, deleted).to_string()),
            "GET" if p == format!("{one}/revisions") => {
                Response::json(200, &serde_json::json!({"revisions": revs}).to_string())
            }
            "POST" if p == format!("{by_id}/revert") => {
                Response::json(200, &skill_json(head + 1, false).to_string())
            }
            "DELETE" if p == by_id => Response::json(200, &skill_json(head + 1, true).to_string()),
            _ => Response::json(405, r#"{"detail":"unexpected request"}"#),
        }
    })
}

struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

impl Run {
    fn output(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

fn run(argv: &[&str]) -> Run {
    let output = Command::new(bin())
        .args(argv)
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env("KUBECONFIG", "/nonexistent/curie-test-kubeconfig")
        .output()
        .unwrap_or_else(|e| panic!("run curie {}: {e}", argv.join(" ")));
    Run {
        code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

/// `[json?] <tier> <group> <agent> <verb args...> --api-url U --api-key k`.
fn tier_run(tier: &str, json: bool, group_args: &[&str], server: &MockServer) -> Run {
    let mut argv: Vec<&str> = Vec::new();
    if json {
        argv.push("--json");
    }
    argv.push(tier);
    argv.extend_from_slice(group_args);
    argv.extend_from_slice(&["--api-url", &server.base_url, "--api-key", "k"]);
    run(&argv)
}

fn requests(server: &MockServer, method: &str) -> Vec<Request> {
    server
        .recorded()
        .into_iter()
        .filter(|r| r.method == method)
        .collect()
}

#[test]
fn skills_help_lists_the_seven_verbs_at_both_tiers() {
    for tier in ["local", "cluster"] {
        let out = run(&[tier, "skills", "--help"]);
        assert_eq!(out.code, 0, "{tier} skills --help: {}", out.output());
        for verb in [
            "list", "show", "diff", "history", "revert", "delete", "export",
        ] {
            assert!(
                out.stdout.lines().any(|l| l.trim_start().starts_with(verb)),
                "{tier} skills --help must list `{verb}`: {}",
                out.stdout
            );
        }
    }
}

#[test]
fn json_list_prints_an_array_of_skill_objects_at_both_tiers() {
    for tier in ["local", "cluster"] {
        let server = api(2, false, true);
        let out = tier_run(tier, true, &["skills", AGENT_NAME, "list"], &server);
        assert_eq!(out.code, 0, "{tier} list: {}", out.output());
        let value: serde_json::Value = serde_json::from_str(out.stdout.trim())
            .unwrap_or_else(|e| panic!("{tier}: stdout must be JSON: {e}: {}", out.stdout));
        let arr = value
            .as_array()
            .unwrap_or_else(|| panic!("{tier}: list --json must be an array: {value}"));
        assert_eq!(arr.len(), 1, "{tier}: {value}");
        for key in [
            "id",
            "slug",
            "revision",
            "description",
            "author",
            "updated_at",
        ] {
            assert!(
                arr[0].get(key).is_some(),
                "{tier}: missing `{key}`: {value}"
            );
        }
        assert_eq!(arr[0]["slug"], SLUG);
        assert_eq!(arr[0]["id"], SKILL_ID);
        assert_eq!(arr[0]["revision"], 2);
        let gets: Vec<String> = requests(&server, "GET")
            .iter()
            .map(|r| route(r).to_string())
            .collect();
        assert!(gets.contains(&"/agents".to_string()), "{tier}: {gets:?}");
        assert!(gets.contains(&skills_path()), "{tier}: {gets:?}");
    }
}

#[test]
fn overrides_learned_skills_sends_the_bool_in_the_patch_body_at_both_tiers() {
    for tier in ["local", "cluster"] {
        for (flag, expected) in [("on", true), ("off", false)] {
            let server = api(1, false, !expected);
            let out = tier_run(
                tier,
                false,
                &["overrides", AGENT_NAME, "--learned-skills", flag],
                &server,
            );
            assert_eq!(out.code, 0, "{tier} {flag}: {}", out.output());
            let patches = requests(&server, "PATCH");
            assert_eq!(patches.len(), 1, "{tier} {flag}: {:?}", server.recorded());
            assert_eq!(route(&patches[0]), format!("/agents/{AGENT_ID}"));
            let body: serde_json::Value = serde_json::from_slice(&patches[0].body).unwrap();
            assert_eq!(
                body,
                serde_json::json!({"learned_skills": expected}),
                "{tier} {flag}: only the learned_skills key travels"
            );
        }
    }
}

#[test]
fn overrides_inspect_shows_learned_skills_on_and_off() {
    for (stored, word) in [(true, "on"), (false, "off")] {
        let server = api(1, false, stored);
        let out = tier_run("local", false, &["overrides", AGENT_NAME], &server);
        assert_eq!(out.code, 0, "{}", out.output());
        assert!(
            out.stdout.contains(&format!("learned skills {word}")),
            "inspect must say `learned skills {word}`: {}",
            out.stdout
        );
        assert!(requests(&server, "PATCH").is_empty());

        let json = tier_run("local", true, &["overrides", AGENT_NAME], &server);
        let value: serde_json::Value = serde_json::from_str(json.stdout.trim()).unwrap();
        assert_eq!(value["learned_skills"], stored, "{value}");
    }
}

#[test]
fn revert_reads_the_head_revision_and_sends_it_as_expected_revision() {
    for tier in ["local", "cluster"] {
        let server = api(3, false, true);
        let out = tier_run(
            tier,
            false,
            &["skills", AGENT_NAME, "revert", SLUG],
            &server,
        );
        assert_eq!(out.code, 0, "{tier} revert: {}", out.output());
        let rec = server.recorded();
        let read = rec
            .iter()
            .position(|r| r.method == "GET" && route(r) == format!("{}/{SLUG}", skills_path()))
            .unwrap_or_else(|| panic!("{tier}: revert must GET the skill first: {rec:?}"));
        let post = rec
            .iter()
            .position(|r| r.method == "POST")
            .unwrap_or_else(|| panic!("{tier}: revert must POST: {rec:?}"));
        assert!(read < post, "{tier}: the read precedes the revert");
        assert_eq!(
            route(&rec[post]),
            format!("{}/{SKILL_ID}/revert", skills_path()),
            "{tier}: the mutation targets the id from the GET, not the typed ref"
        );
        let body: serde_json::Value = serde_json::from_slice(&rec[post].body).unwrap();
        assert_eq!(body["expected_revision"], 3, "{tier}: {body}");
        assert!(
            body.get("to_revision").is_none_or(|v| v.is_null()),
            "{tier}: no --to means no to_revision: {body}"
        );
    }
}

#[test]
fn revert_to_sends_to_revision() {
    let server = api(4, false, true);
    let out = tier_run(
        "local",
        false,
        &["skills", AGENT_NAME, "revert", SLUG, "--to", "2"],
        &server,
    );
    assert_eq!(out.code, 0, "{}", out.output());
    let posts = requests(&server, "POST");
    assert_eq!(posts.len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&posts[0].body).unwrap();
    assert_eq!(
        body,
        serde_json::json!({"expected_revision": 4, "to_revision": 2})
    );
}

#[test]
fn revert_works_on_a_deleted_skill_the_undo_after_delete_path() {
    // R13: the API serves a deleted skill (deleted=true) to the operator, and
    // the CLI must still read its head revision and revert it.
    let server = api(5, true, true);
    let out = tier_run(
        "local",
        false,
        &["skills", AGENT_NAME, "revert", SLUG],
        &server,
    );
    assert_eq!(out.code, 0, "{}", out.output());
    let posts = requests(&server, "POST");
    assert_eq!(posts.len(), 1, "{:?}", server.recorded());
    let body: serde_json::Value = serde_json::from_slice(&posts[0].body).unwrap();
    assert_eq!(body["expected_revision"], 5);
    assert_eq!(
        route(&posts[0]),
        format!("{}/{SKILL_ID}/revert", skills_path())
    );
}

#[test]
fn delete_sends_expected_revision_in_the_query() {
    for tier in ["local", "cluster"] {
        let server = api(3, false, true);
        let out = tier_run(
            tier,
            false,
            &["skills", AGENT_NAME, "delete", SLUG],
            &server,
        );
        assert_eq!(out.code, 0, "{tier} delete: {}", out.output());
        let deletes = requests(&server, "DELETE");
        assert_eq!(deletes.len(), 1, "{tier}: {:?}", server.recorded());
        assert_eq!(
            deletes[0].path,
            format!("{}/{SKILL_ID}?expected_revision=3", skills_path()),
            "{tier}: the head revision read from the GET travels as expected_revision"
        );
    }
}

/// A mock whose skill is typed as a slug but carries a different uuid; any
/// mutation addressed by the slug is a 405.
fn slug_vs_id_api() -> MockServer {
    const OTHER_SLUG: &str = "deploy-app";
    const ID_A: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    serve(move |req| {
        let (m, p) = (req.method.as_str(), route(req));
        let base = skills_path();
        let mut skill = skill_json(7, false);
        skill["id"] = serde_json::json!(ID_A);
        skill["slug"] = serde_json::json!(OTHER_SLUG);
        match m {
            "GET" if p == "/agents" => Response::json(200, &format!("[{}]", agent_json(true))),
            "GET" if p == format!("/agents/{AGENT_ID}") => Response::json(200, &agent_json(true)),
            "GET" if p == format!("{base}/{OTHER_SLUG}") => Response::json(200, &skill.to_string()),
            "POST" if p == format!("{base}/{ID_A}/revert") => {
                Response::json(200, &skill.to_string())
            }
            "DELETE" if p == format!("{base}/{ID_A}") => Response::json(200, &skill.to_string()),
            _ => Response::json(405, r#"{"detail":"unexpected request"}"#),
        }
    })
}

#[test]
fn revert_and_delete_mutate_by_the_uuid_the_get_returned_not_the_typed_slug() {
    let id_a = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    for tier in ["local", "cluster"] {
        let server = slug_vs_id_api();
        let out = tier_run(
            tier,
            false,
            &["skills", AGENT_NAME, "revert", "deploy-app"],
            &server,
        );
        assert_eq!(out.code, 0, "{tier} revert: {}", out.output());
        let posts = requests(&server, "POST");
        assert_eq!(posts.len(), 1, "{tier}: {:?}", server.recorded());
        assert_eq!(route(&posts[0]), format!("{}/{id_a}/revert", skills_path()));
        let body: serde_json::Value = serde_json::from_slice(&posts[0].body).unwrap();
        assert_eq!(body["expected_revision"], 7, "{tier}: {body}");

        let server = slug_vs_id_api();
        let out = tier_run(
            tier,
            false,
            &["skills", AGENT_NAME, "delete", "deploy-app"],
            &server,
        );
        assert_eq!(out.code, 0, "{tier} delete: {}", out.output());
        let deletes = requests(&server, "DELETE");
        assert_eq!(deletes.len(), 1, "{tier}: {:?}", server.recorded());
        assert_eq!(
            deletes[0].path,
            format!("{}/{id_a}?expected_revision=7", skills_path())
        );
        assert!(
            !server
                .recorded()
                .iter()
                .any(|r| r.path.contains("deploy-app")
                    && (r.method == "POST" || r.method == "DELETE")),
            "{tier}: no mutation may carry the typed slug: {:?}",
            server.recorded()
        );
    }
}

#[test]
fn diff_defaults_to_head_minus_one_against_head_as_a_unified_diff() {
    let server = api(3, false, true);
    let out = tier_run(
        "local",
        false,
        &["skills", AGENT_NAME, "diff", SLUG],
        &server,
    );
    assert_eq!(out.code, 0, "{}", out.output());
    // Revision 2 body "beta 2" -> revision 3 body "beta 3".
    assert!(out.stdout.contains("-beta 2"), "{}", out.stdout);
    assert!(out.stdout.contains("+beta 3"), "{}", out.stdout);
    assert!(
        !out.stdout.contains("-beta\n") && !out.stdout.contains("+beta 2"),
        "default compares head-1 to head only: {}",
        out.stdout
    );
}

#[test]
fn diff_from_and_to_select_the_revisions() {
    let server = api(3, false, true);
    let out = tier_run(
        "cluster",
        false,
        &[
            "skills", AGENT_NAME, "diff", SLUG, "--from", "1", "--to", "3",
        ],
        &server,
    );
    assert_eq!(out.code, 0, "{}", out.output());
    assert!(out.stdout.contains("-beta"), "{}", out.stdout);
    assert!(out.stdout.contains("+beta 3"), "{}", out.stdout);
}

#[test]
fn export_writes_the_skill_tree_with_generated_frontmatter() {
    for tier in ["local", "cluster"] {
        let server = api(2, false, true);
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().to_str().unwrap();
        let out = tier_run(
            tier,
            false,
            &["skills", AGENT_NAME, "export", "--out", out_dir],
            &server,
        );
        assert_eq!(out.code, 0, "{tier} export: {}", out.output());
        let file = dir.path().join("skills").join(SLUG).join("SKILL.md");
        let text = std::fs::read_to_string(&file)
            .unwrap_or_else(|e| panic!("{tier}: {} must exist: {e}", file.display()));
        let expected_head = format!(
            "---\nname: {SLUG}\ndescription: {}\n---\n\n",
            serde_json::to_string(DESCRIPTION).unwrap()
        );
        assert!(
            text.starts_with(&expected_head),
            "{tier}: frontmatter must match the runner's: {text:?}"
        );
        assert_eq!(
            text[expected_head.len()..].trim_end(),
            BODY.trim_end(),
            "{tier}: body follows the frontmatter"
        );
    }
}

#[test]
fn show_prints_the_body_and_history_lists_each_revision() {
    let server = api(3, false, true);
    let shown = tier_run(
        "local",
        false,
        &["skills", AGENT_NAME, "show", SLUG],
        &server,
    );
    assert_eq!(shown.code, 0, "{}", shown.output());
    assert!(
        shown.stdout.contains("Step one: check the rollout."),
        "{}",
        shown.stdout
    );

    let history = tier_run(
        "local",
        false,
        &["skills", AGENT_NAME, "history", SLUG],
        &server,
    );
    assert_eq!(history.code, 0, "{}", history.output());
    assert!(history.stdout.contains("create"), "{}", history.stdout);
    assert!(history.stdout.contains("patch"), "{}", history.stdout);
}

fn diff_lines(revs: Vec<serde_json::Value>) -> Run {
    let server = api_with(2, false, true, revs);
    tier_run(
        "local",
        false,
        &["skills", AGENT_NAME, "diff", SLUG],
        &server,
    )
}

#[test]
fn diff_of_identical_revisions_prints_the_header_and_no_hunk() {
    let out = diff_lines(vec![
        revision_json(1, "create", "alpha\nbeta\n"),
        revision_json(2, "patch", "alpha\nbeta\n"),
    ]);
    assert_eq!(out.code, 0, "{}", out.output());
    assert!(!out.stdout.contains("@@"), "no hunk: {}", out.stdout);
    let body: Vec<&str> = out
        .stdout
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with("---") && !l.starts_with("+++"))
        .collect();
    assert!(body.is_empty(), "header lines only: {}", out.stdout);
    assert!(out.stdout.contains("---") && out.stdout.contains("+++"));
}

#[test]
fn diff_shows_a_body_that_differs_only_by_a_trailing_newline() {
    let out = diff_lines(vec![
        revision_json(1, "create", "alpha\nbeta"),
        revision_json(2, "patch", "alpha\nbeta\n"),
    ]);
    assert_eq!(out.code, 0, "{}", out.output());
    let changed = out.stdout.lines().any(|l| {
        (l.starts_with('-') && !l.starts_with("---"))
            || (l.starts_with('+') && !l.starts_with("+++"))
    });
    assert!(
        changed || out.stdout.contains("No newline"),
        "a newline-only change must be visible: {:?}",
        out.stdout
    );
}

#[test]
fn history_fails_when_a_revision_omits_deleted() {
    let mut rev = revision_json(1, "create", "alpha\n");
    rev.as_object_mut().unwrap().remove("deleted");
    let server = api_with(1, false, true, vec![rev]);
    let out = tier_run(
        "local",
        false,
        &["skills", AGENT_NAME, "history", SLUG],
        &server,
    );
    assert_ne!(out.code, 0, "{}", out.output());
}
