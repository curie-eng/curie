//! `curie guide` (#322 / ADR-0021): the self-describing harness primer.
//!
//! These tests pin the acceptance criteria: the primer prints to stdout, `--json`
//! emits a structured variant with the data on stdout (human text goes to stderr),
//! the primer is self-contained and roughly 100 lines, and -- the drift guard --
//! every `curie ...` command the primer prints resolves to a real command in
//! the CLI's own machine-readable manifest (`curie schema`). The test reads the
//! grammar from that manifest, so it tracks the CLI surface automatically and
//! fails the build if the primer ever references a command that does not exist.

use std::collections::{BTreeMap, BTreeSet};
use std::process::{Command, Output};

use serde_json::Value;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn run(args: &[&str]) -> Output {
    Command::new(bin()).args(args).output().expect("run curie")
}

fn out_str(o: &Output) -> String {
    String::from_utf8(o.stdout.clone()).expect("stdout utf-8")
}

fn err_str(o: &Output) -> String {
    String::from_utf8(o.stderr.clone()).expect("stderr utf-8")
}

/// The CLI's real command surface, read from `curie schema` (the #145/#278
/// manifest): a map from a parent command path ("" == root) to the set of its
/// direct subcommand names. This is the ground truth the primer is validated
/// against, so the test tracks the grammar automatically.
struct Surface {
    children: BTreeMap<String, BTreeSet<String>>,
}

impl Surface {
    fn load() -> Surface {
        let out = run(&["schema"]);
        assert!(
            out.status.success(),
            "curie schema failed:\n{}",
            err_str(&out)
        );
        let root: Value = serde_json::from_str(&out_str(&out)).expect("manifest is json");
        let mut children = BTreeMap::new();
        walk(&root, String::new(), &mut children);
        Surface { children }
    }

    /// Resolve a printed `curie ...` invocation to its subcommand path.
    /// `Ok(Some(path))` when it resolves, `Ok(None)` when the line carries no
    /// invocation, `Err(reason)` when a bare token that should name a subcommand
    /// does not exist in the grammar (the drift/typo this guard exists to catch).
    fn resolve(&self, line: &str) -> Result<Option<String>, String> {
        let idx = match line.find("curie ") {
            Some(i) => i,
            None => return Ok(None),
        };
        let rest = &line[idx + "curie ".len()..];
        let empty = BTreeSet::new();
        let mut path = String::new();
        let mut matched = 0usize;
        for raw in rest.split_whitespace() {
            let tok =
                raw.trim_matches(|c| c == '`' || c == '"' || c == '.' || c == ',' || c == ')');
            if tok.is_empty() {
                break;
            }
            if tok.starts_with('-') {
                break; // flags: the command path is complete
            }
            let here = self.children.get(&path).unwrap_or(&empty);
            if here.contains(tok) {
                path = if path.is_empty() {
                    tok.to_string()
                } else {
                    format!("{path} {tok}")
                };
                matched += 1;
            } else if here.is_empty() {
                break; // leaf reached: this token is a positional value
            } else {
                return Err(format!(
                    "`curie {path} {tok}` -- `{tok}` is not a real subcommand"
                ));
            }
        }
        if matched == 0 {
            return Err(format!("`curie {rest}` names no real command"));
        }
        Ok(Some(path))
    }
}

fn walk(node: &Value, prefix: String, out: &mut BTreeMap<String, BTreeSet<String>>) {
    let mut names = BTreeSet::new();
    if let Some(subs) = node.get("subcommands").and_then(|s| s.as_array()) {
        for sub in subs {
            let name = sub
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or_default()
                .to_string();
            names.insert(name.clone());
            let child_prefix = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix} {name}")
            };
            walk(sub, child_prefix, out);
        }
    }
    out.insert(prefix, names);
}

#[test]
fn guide_prints_primer_to_stdout() {
    let out = run(&["guide"]);
    assert!(
        out.status.success(),
        "curie guide failed:\n{}",
        err_str(&out)
    );
    let text = out_str(&out);
    assert!(!text.trim().is_empty(), "primer stdout was empty");
    // Self-contained: it covers all three tiers, the eval gate, the parity
    // concept, and at least one non-discoverable landmine.
    for needle in [
        "skill",
        "local",
        "cluster",
        "eval",
        "parity",
        "secretKeyRef",
    ] {
        assert!(text.contains(needle), "primer missing `{needle}`\n{text}");
    }
    // "Roughly 100 lines": a real primer, neither a stub nor a sprawling manual.
    // The ceiling grew with the Cron triggers section (#4012).
    let lines = text.lines().count();
    assert!(
        (60..=200).contains(&lines),
        "primer is {lines} lines, expected roughly 100"
    );
}

#[test]
fn guide_distinguishes_skill_tier_from_bundle_artifact() {
    let markdown = run(&["guide"]);
    assert!(
        markdown.status.success(),
        "curie guide failed:\n{}",
        err_str(&markdown)
    );
    let json = run(&["guide", "--json"]);
    assert!(
        json.status.success(),
        "curie guide --json failed:\n{}",
        err_str(&json)
    );

    let markdown = out_str(&markdown);
    let json = out_str(&json);
    for (surface, text) in [("markdown", &markdown), ("json", &json)] {
        for fact in ["runner only tier", "skills/<name>/SKILL.md"] {
            assert!(
                text.contains(fact),
                "{surface} guide must distinguish its runner tier from the bundle artifact with `{fact}`\n{text}"
            );
        }
    }
}

#[test]
fn guide_json_emits_pure_structured_data_on_stdout() {
    let out = run(&["guide", "--json"]);
    assert!(
        out.status.success(),
        "curie guide --json failed:\n{}",
        err_str(&out)
    );
    // stdout is exclusively the data payload: it parses as one JSON value with no
    // human prose bleeding in (that belongs on stderr, per ADR-0021 decision 1).
    let v: Value = serde_json::from_str(&out_str(&out)).expect("stdout is pure json");
    assert!(
        v.get("parity_ladder")
            .and_then(|l| l.as_array())
            .is_some_and(|a| !a.is_empty()),
        "json missing a non-empty parity_ladder"
    );
    assert!(
        v.get("landmines")
            .and_then(|l| l.as_array())
            .is_some_and(|a| !a.is_empty()),
        "json missing a non-empty landmines list"
    );
    assert!(
        v.get("verify_first")
            .and_then(|vf| vf.get("commands"))
            .and_then(|c| c.as_array())
            .is_some_and(|a| !a.is_empty()),
        "json missing verify_first.commands"
    );
}

#[test]
fn guide_documents_cluster_up_gvisor_inference_and_fail_closed_contradictions() {
    // AC (#1662, ADR 0114): a real-model `cluster up` may infer gVisor off only
    // from the exact admission symptom, retry once, and keep every other failure
    // closed. Explicit auto or require modes contradict that detected result.
    let md = out_str(&run(&["guide"]));
    let json = out_str(&run(&["guide", "--json"]));
    for (label, text) in [("markdown", &md), ("json", &json)] {
        assert!(
            text.contains("security.gvisor.mode=off"),
            "{label} primer missing the inferred override `security.gvisor.mode=off`\n{text}"
        );
        assert!(
            text.contains("curie-preflight-gvisor")
                && text.contains("RuntimeClass gvisor is not found"),
            "{label} primer missing the exact admission symptom\n{text}"
        );
        assert!(
            text.contains("retries once"),
            "{label} primer missing the one retry limit\n{text}"
        );
        assert!(
            text.contains("Other failures remain closed"),
            "{label} primer missing the fail closed behavior for other failures\n{text}"
        );
        assert!(
            text.contains(
                "Explicit auto or require modes contradict the detected result and are errors"
            ),
            "{label} primer missing the explicit mode contradiction error\n{text}"
        );
        assert!(
            text.contains("before the first install") && text.contains("NotFound"),
            "{label} primer missing the pre-install NotFound RuntimeClass lookup\n{text}"
        );
    }
}

#[test]
fn guide_documents_the_approval_plane() {
    // AC (#1051): the approval plane is the least derivable subsystem in the
    // product and the primer used to omit it entirely, so a coding agent reading
    // only the CLI concluded cross-channel approval did not exist. Pin the four
    // facts it cannot derive: the tool that raises a request, the where/who split
    // on a route binding, authorized requester self-confirmation through the
    // configured approver set, and the resume-turn prefix a skill must handle.
    // Both renderings carry them.
    let md = out_str(&run(&["guide"]));
    let json = out_str(&run(&["guide", "--json"]));
    for (label, text) in [("markdown", &md), ("json", &json)] {
        for needle in [
            "mcp__curie__request_approval",
            "approval_routes",
            "--route-resolution",
            "--routes-from",
            "approvers.group",
            "text-only",
            "interaction=None",
            "configured approval channel",
            "without disclosing its identifier",
            "endpoint",
            "adapter",
            "verified Slack",
            "scoped credential",
            "Authorized requesters may self-confirm",
            "configured approver set",
            "[approval resolved]",
            // #1086. Each is a fact an agent cannot derive from the command
            // tree, and each was reachable only from docs/approvals.md, which
            // the primer's own premise says the agent does not read.
            //
            // A missing token leaves group membership unverified. The public
            // refusal must say so while preserving the setup facts below.
            "SLACK_BOT_TOKEN",
            "usergroups:read",
            // #1431. InvalidApprovers and failed group lookup are independent
            // refusals. An operator cannot safely infer the missing credential
            // from the group lookup response itself.
            "could not verify approvers",
            "could not verify approver group membership",
            "the refusal does not name the missing token",
            // The two refusals a second approver actually meets. Everything the
            // primer covered before was a first-approver problem.
            "409",
            "410",
        ] {
            assert!(
                text.contains(needle),
                "{label} primer missing the approval fact `{needle}`\n{text}"
            );
        }
        assert!(
            !text.contains(
                "every resolution is refused as not-an-approver with nothing naming the token as the cause"
            ),
            "{label} primer preserves the obsolete group lookup refusal\n{text}"
        );
    }
    let approvals_document = include_str!("../../docs/approvals.md");
    let invalid_approvers_row = approvals_document
        .lines()
        .find(|line| line.starts_with("| `403 ") && line.contains("could not verify approvers"))
        .expect("approval troubleshooting has an InvalidApprovers row");
    let group_lookup_row = approvals_document
        .lines()
        .find(|line| {
            line.starts_with("| `403 ")
                && line.contains("could not verify approver group membership")
        })
        .expect("approval troubleshooting has a failed group lookup row");
    assert_ne!(
        invalid_approvers_row, group_lookup_row,
        "InvalidApprovers and failed group lookup must remain separate troubleshooting rows"
    );
    // The section is structural in the markdown, not a stray landmine mention.
    assert!(
        md.contains("## Approvals"),
        "markdown primer has no Approvals section\n{md}"
    );
}

/// ADR-0106 made the principal, rather than a caller-selected name or channel,
/// the approval boundary.  This is deliberately a guide test instead of a
/// source-string assertion: the generated human and `--json` guides are what a
/// terminal operator actually uses to recover from a missing credential.  Both
/// forms must teach the two valid routes without preserving a copy-pasteable
/// version of the identity-forgery interface.
#[test]
fn guide_replaces_asserted_resolver_flags_with_authenticated_principal_paths() {
    let md = out_str(&run(&["guide"]));
    let json = out_str(&run(&["guide", "--json"]));

    for (label, text) in [("markdown", &md), ("json", &json)] {
        for fact in [
            "CURIE_APPROVAL_PRINCIPAL_TOKEN",
            "authenticated principal",
            "explicit-user",
            "authenticated card",
            "self-confirm",
            "mint",
        ] {
            assert!(
                text.contains(fact),
                "{label} guide must explain the authenticated approval path with `{fact}`\n{text}"
            );
        }
        for retired in ["--as", "--actor-channel", "Self-approval is refused"] {
            assert!(
                !text.contains(retired),
                "{label} guide must not preserve retired asserted-identity guidance `{retired}`\n{text}"
            );
        }
    }
}

#[test]
fn approval_card_channel_null_explanations_preserve_compatibility_meaning() {
    // #1431: `card_channel` is absent only for an older row or a direct API
    // write that omitted the field. It does not describe a routeless request.
    // These are four independent public consumers of that fact.
    for (surface, text) in [
        (
            "approval schema",
            include_str!("../schema/approvals.schema.json"),
        ),
        ("approval recipe", include_str!("../src/recipes.rs")),
        (
            "approval JSON projection",
            include_str!("../src/commands/approvals.rs"),
        ),
        (
            "approval documentation",
            include_str!("../../docs/approvals.md"),
        ),
    ] {
        for fact in ["older row", "direct API write", "omitted"] {
            assert!(
                text.contains(fact),
                "{surface} does not explain null `card_channel` with `{fact}`"
            );
        }
        assert!(
            !text.contains("names no route"),
            "{surface} still claims null `card_channel` means the record names no route"
        );
    }
}

#[test]
fn guide_documents_cron_triggers() {
    // AC (#4012): nothing in the released CLI said cron triggers exist. Both
    // renderings carry the manifest shape and the verbs that read the record,
    // and the markdown keeps them inside one Cron triggers section.
    let md = out_str(&run(&["guide"]));
    let json_text = out_str(&run(&["guide", "--json"]));
    for (label, text) in [("markdown", &md), ("json", &json_text)] {
        for needle in [
            "triggers",
            "schedule",
            "timezone",
            "prompt",
            "target",
            "schedules",
            "hook fire",
            "failed",
        ] {
            assert!(
                text.contains(needle),
                "{label} primer missing the cron fact `{needle}`\n{text}"
            );
        }
    }

    let start = md
        .find("## Cron triggers")
        .unwrap_or_else(|| panic!("markdown primer has no Cron triggers section\n{md}"));
    let rest = &md[start..];
    let section = match rest[1..].find("\n## ") {
        Some(end) => &rest[..end + 1],
        None => rest,
    };
    for needle in ["triggers", "schedules", "hook fire"] {
        assert!(
            section.contains(needle),
            "Cron triggers section missing `{needle}`\n{section}"
        );
    }

    let surface = Surface::load();
    let json: Value = serde_json::from_str(&json_text).expect("guide --json is json");
    let commands = json["cron"]["commands"]
        .as_array()
        .expect("cron.commands is an array");
    assert!(!commands.is_empty(), "cron.commands is empty");
    for cmd in commands {
        let cmd = cmd.as_str().expect("command is a string");
        assert!(
            matches!(surface.resolve(cmd), Ok(Some(_))),
            "cron command `{cmd}` does not resolve to a real command"
        );
        assert!(
            section.contains(cmd),
            "cron command `{cmd}` is absent from the markdown Cron triggers section"
        );
    }
}

#[test]
fn guide_is_registered_in_the_manifest() {
    let surface = Surface::load();
    assert!(
        surface
            .children
            .get("")
            .is_some_and(|top| top.contains("guide")),
        "`guide` is not a real subcommand in the CLI manifest surface"
    );
}

#[test]
fn every_command_the_primer_prints_exists_in_the_cli_surface() {
    // AC #4: the primer must not drift from the grammar. Validate both the
    // Markdown default and the --json commands against the live manifest.
    let surface = Surface::load();

    let md = out_str(&run(&["guide"]));
    let mut resolved = Vec::new();
    for line in md.lines() {
        match surface.resolve(line) {
            Ok(Some(path)) => resolved.push(path),
            Ok(None) => {}
            Err(e) => panic!("primer prints a command not in the CLI surface: {e}"),
        }
    }
    // The primer genuinely walks the parity ladder (not a contentless doc that
    // trips no commands): the core rungs must be present.
    for expected in ["init", "skill up", "skill eval", "local up", "cluster up"] {
        assert!(
            resolved.iter().any(|p| p == expected),
            "primer never prints `curie {expected}`; resolved={resolved:?}"
        );
    }

    // The --json parity ladder draws from the same source of truth: every command
    // it lists resolves against the grammar and appears verbatim in the Markdown.
    let json: Value =
        serde_json::from_str(&out_str(&run(&["guide", "--json"]))).expect("guide --json is json");
    let ladder = json["parity_ladder"]
        .as_array()
        .expect("parity_ladder is an array");
    for rung in ladder {
        let cmd = rung["command"].as_str().expect("rung command is a string");
        assert!(
            cmd.starts_with("curie "),
            "rung command `{cmd}` is not a curie invocation"
        );
        match surface.resolve(cmd) {
            Ok(Some(_)) => {}
            other => panic!("json rung `{cmd}` does not resolve to a real command: {other:?}"),
        }
        assert!(
            md.contains(cmd),
            "json rung `{cmd}` is absent from the Markdown primer (source-of-truth drift)"
        );
    }
    for cmd in json["verify_first"]["commands"]
        .as_array()
        .expect("verify_first.commands is an array")
    {
        let cmd = cmd.as_str().expect("command is a string");
        assert!(
            matches!(surface.resolve(cmd), Ok(Some(_))),
            "verify_first command `{cmd}` does not resolve to a real command"
        );
    }
}
