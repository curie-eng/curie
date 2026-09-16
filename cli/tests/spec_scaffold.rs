//! Integration tests for issue #325: `curie init --from-spec <PATH>`.
//!
//! These pin the acceptance criteria for scaffolding a Claude Code plugin
//! bundle NON-INTERACTIVELY from an agent-authored spec file, and for the
//! scaffolded `evals/cases.json` being loadable by the SAME loader that
//! `curie skill eval` uses (`curie::evals::load_suite`). The parity
//! guarantee is the whole point: a spec authored by an agent must produce a
//! bundle whose eval suite the platform eval path accepts unchanged.
//!
//! Written test-first: the `curie::spec` module, `scaffold_from_spec`, and
//! the `--from-spec` clap flag do not exist yet, so this file fails to compile
//! and/or fails at runtime until the implementer builds the feature.
//!
//! Note on the fixtures: `instructions` bodies keep their markdown headings off
//! the opening JSON quote (a leading sentence first) so the raw-string literals
//! never contain a `"#` sequence that would prematurely close an `r#"..."#`
//! delimiter. The `\n` escapes are literal in the raw string and parse to real
//! newlines via serde_json, which is what an authored markdown body would carry.

use std::path::Path;
use std::process::Command;

use curie::evals::{load_suite, GraderKind};
use curie::scaffold::{read_manifest, scaffold, scaffold_from_spec};
use curie::spec::parse;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn output_text(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

/// A valid two-skill spec with one connector and one eval. The single eval
/// grader is FALSIFIABLE -- it requires the agent to name itself, so a broken,
/// empty, or off-topic turn fails it. This is the same footing #553 put the
/// `init` lane on; the spec lane was left behind on a `contains "all done"`
/// grader tuned to the fake model's canned reply (#612). No grader needs to be
/// fake-compatible any more: the fake tier is a plumbing fixture and is never
/// graded at all, so a grader written to match its canned text buys nothing but
/// a false green.
fn valid_spec_json() -> &'static str {
    r#"{
      "name": "deal-desk",
      "description": "Prices and reviews deal desk requests.",
      "skills": [
        {
          "name": "deal-desk",
          "description": "Invoke when a rep submits a pricing exception request.",
          "allowed_tools": ["WebSearch", "WebFetch"],
          "instructions": "Deal desk skill body.\n\n## When to run\nA rep asks for a pricing exception.\n"
        },
        {
          "name": "renewal-review",
          "description": "Invoke when a renewal needs a discount review.",
          "instructions": "Renewal review skill body.\n\n## When to run\nA renewal needs review.\n"
        }
      ],
      "connectors": {
        "crm": { "command": "crm-mcp", "args": ["--stdio"] }
      },
      "evals": [
        { "id": "prices-a-deal", "input": "Quote 20% off for Acme as the deal-desk agent", "grader": { "kind": "contains", "expected": "deal-desk", "case_sensitive": false } }
      ]
    }"#
}

/// Write `body` to `<dir>/spec.json` and return the path.
fn write_spec(dir: &Path, body: &str) -> std::path::PathBuf {
    let path = dir.join("spec.json");
    std::fs::write(&path, body).expect("write spec fixture");
    path
}

// --- library-level scaffold behavior --------------------------------------

/// The happy path: a valid spec scaffolds every bundle artifact with the
/// content the spec dictates. Deleting the impl (returning no files) or writing
/// the wrong name/description makes concrete file-content assertions fail.
#[test]
fn scaffolds_the_full_bundle_from_a_valid_spec() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("bundle");
    let spec = parse(valid_spec_json()).expect("valid spec parses");
    let created = scaffold_from_spec(&out, &spec).expect("scaffold succeeds");
    assert!(!created.is_empty(), "scaffold should report created files");

    // 1. Manifest: name + version from the spec, description carried through.
    let (name, version) = read_manifest(&out).unwrap();
    assert_eq!(name, "deal-desk", "manifest name comes from spec.name");
    assert_eq!(version, "0.1.0", "scaffold pins version 0.1.0");
    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(out.join(".claude-plugin/plugin.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        manifest["description"],
        "Prices and reviews deal desk requests."
    );
    assert_eq!(
        manifest["systemPrompt"],
        "You are the deal-desk agent. Help users with deal-desk requests."
    );

    // 5. .gitignore ignores local workstation state.
    let gitignore = std::fs::read_to_string(out.join(".gitignore")).unwrap();
    assert!(gitignore.contains(".curie/"), "{gitignore}");
}

#[test]
fn default_scaffold_documents_a_file_backed_secret() {
    let dir = tempfile::tempdir().unwrap();
    scaffold(dir.path(), "acme").expect("default scaffold succeeds");

    let connectors = std::fs::read_to_string(dir.path().join("connectors.yaml")).unwrap();
    let mut lines = connectors.lines();
    let secret_files = lines
        .find(|line| line.trim() == "#     secret_files:")
        .expect("connectors.yaml must show the commented secret_files mapping");
    assert_eq!(secret_files, "#     secret_files:");

    let projection = lines
        .next()
        .expect("secret_files must include a secret name and file path");
    let mapping = projection
        .strip_prefix("#       ")
        .expect("the file projection must remain commented");
    let (name, path) = mapping
        .split_once(':')
        .expect("the file projection must be a YAML mapping");
    assert!(!name.trim().is_empty(), "the secret name must be nonempty");
    assert!(
        path.trim().starts_with('/'),
        "the projection must show an absolute file path: {projection}"
    );
}

/// A spec declaring `secrets` and `approvalPolicy` scaffolds a gated, authed
/// bundle whose manifest carries both verbatim -- no hand-editing plugin.json
/// (#549). The gate is a fully-namespaced live tool name for the `crm` connector.
#[test]
fn scaffolds_secrets_and_approval_policy_into_the_manifest() {
    let body = r#"{
      "name": "deal-desk",
      "description": "Gated authed deal desk.",
      "skills": [
        { "name": "deal-desk", "description": "Do it.", "instructions": "Body.\n" }
      ],
      "connectors": { "crm": { "command": "crm-mcp", "args": ["--stdio"] } },
      "secrets": ["CRM_API_TOKEN"],
      "approvalPolicy": {
        "gates": [
          { "gate": "mcp__plugin_deal-desk_crm__create_deal", "route": "default" }
        ]
      },
      "evals": [
        { "id": "e1", "input": "hi", "grader": { "kind": "contains", "expected": "ok" } }
      ]
    }"#;
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("bundle");
    let spec = parse(body).expect("gated authed spec parses");
    scaffold_from_spec(&out, &spec).expect("scaffold succeeds");

    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(out.join(".claude-plugin/plugin.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(manifest["secrets"], serde_json::json!(["CRM_API_TOKEN"]));
    assert_eq!(
        manifest["approvalPolicy"]["gates"][0]["gate"],
        "mcp__plugin_deal-desk_crm__create_deal"
    );
    assert_eq!(manifest["approvalPolicy"]["gates"][0]["route"], "default");
}

/// A spec gate marked `grantableViaPolicy: true` scaffolds that flag verbatim
/// into the emitted manifest's approvalPolicy gate (#558), so a deployed bundle
/// carries the operator opt-in the runner/worker read to mint a one-shot grant.
/// A gate that omits it scaffolds no `true` flag (default no-grant) and still
/// validates.
#[test]
fn scaffolds_grantable_via_policy_flag_into_the_manifest() {
    let body = r#"{
      "name": "deal-desk",
      "description": "Gated deal desk with a grantable gate.",
      "skills": [
        { "name": "deal-desk", "description": "Do it.", "instructions": "Body.\n" }
      ],
      "approvalPolicy": {
        "gates": [
          { "gate": "close_issue", "route": "deal-desk", "grantableViaPolicy": true },
          { "gate": "escalate", "route": "managers" }
        ]
      },
      "evals": [
        { "id": "e1", "input": "hi", "grader": { "kind": "contains", "expected": "ok" } }
      ]
    }"#;
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("bundle");
    let spec = parse(body).expect("grantable-gate spec parses");
    scaffold_from_spec(&out, &spec).expect("scaffold succeeds");

    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(out.join(".claude-plugin/plugin.json")).unwrap(),
    )
    .unwrap();
    let gates = &manifest["approvalPolicy"]["gates"];
    // The opted-in gate carries the flag verbatim.
    assert_eq!(gates[0]["gate"], "close_issue");
    assert_eq!(gates[0]["grantableViaPolicy"], serde_json::json!(true));
    // The non-opted gate scaffolds no `true` flag (omitted or false).
    assert_ne!(gates[1]["grantableViaPolicy"], serde_json::json!(true));
}

/// A spec that declares neither omits both keys, so the default scaffold shape is
/// unchanged (no empty `secrets: []` / `approvalPolicy` noise).
#[test]
fn omits_secrets_and_approval_policy_when_absent() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("bundle");
    let spec = parse(valid_spec_json()).expect("valid spec parses");
    scaffold_from_spec(&out, &spec).expect("scaffold succeeds");
    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(out.join(".claude-plugin/plugin.json")).unwrap(),
    )
    .unwrap();
    assert!(
        manifest.get("secrets").is_none(),
        "no secrets key: {manifest}"
    );
    assert!(
        manifest.get("approvalPolicy").is_none(),
        "no approvalPolicy key: {manifest}"
    );
}

#[test]
fn rejects_a_secret_name_that_is_not_env_var_shaped() {
    let body = r#"{
      "name": "x", "description": "d",
      "skills": [ { "name": "s", "description": "d", "instructions": "b\n" } ],
      "secrets": ["lowercase-bad"],
      "evals": [ { "id": "e", "input": "i", "grader": { "kind": "contains", "expected": "ok" } } ]
    }"#;
    let err = parse(body)
        .expect_err("bad secret name must error")
        .to_string();
    assert!(err.contains("lowercase-bad"), "names the offender: {err}");
}

#[test]
fn rejects_an_approval_gate_missing_its_route() {
    let body = r#"{
      "name": "x", "description": "d",
      "skills": [ { "name": "s", "description": "d", "instructions": "b\n" } ],
      "approvalPolicy": { "gates": [ { "gate": "Bash", "route": "" } ] },
      "evals": [ { "id": "e", "input": "i", "grader": { "kind": "contains", "expected": "ok" } } ]
    }"#;
    let err = parse(body).expect_err("empty route must error").to_string();
    assert!(
        err.to_lowercase().contains("route"),
        "mentions route: {err}"
    );
}

/// An `mcp__` gate that is not fully namespaced for a declared connector silently
/// fails to gate at runtime, so the spec rejects it up front (#549 / ADR-0010).
#[test]
fn rejects_a_mis_namespaced_mcp_approval_gate() {
    let body = r#"{
      "name": "deal-desk", "description": "d",
      "skills": [ { "name": "s", "description": "d", "instructions": "b\n" } ],
      "connectors": { "crm": { "command": "crm-mcp" } },
      "approvalPolicy": { "gates": [ { "gate": "mcp__crm__create_deal", "route": "default" } ] },
      "evals": [ { "id": "e", "input": "i", "grader": { "kind": "contains", "expected": "ok" } } ]
    }"#;
    let err = parse(body)
        .expect_err("bare mcp gate must error")
        .to_string();
    assert!(
        err.contains("mcp__plugin_deal-desk_crm__"),
        "shows the expected namespaced shape: {err}"
    );
}

/// Every skill in a multi-skill spec becomes its own SKILL.md carrying that
/// skill's name, description, allowed-tools (only when present, as the single
/// canonical space-separated scalar), and body.
#[test]
fn writes_one_skill_md_per_skill_with_frontmatter_and_body() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("bundle");
    let spec = parse(valid_spec_json()).unwrap();
    scaffold_from_spec(&out, &spec).unwrap();

    // Skill one: has allowed_tools -> ONE `allowed-tools` line carrying the
    // space-joined entries, which is the shape the Agent Skills specification
    // calls canonical (ADR-0135). description and the joined value are emitted as
    // quoted YAML scalars (arbitrary agent-authored text must be a quoted scalar
    // or it corrupts/invalidates the frontmatter `plugin_format.validate_bundle`
    // parses with `yaml.safe_load`). The join is lossless because `spec::parse`
    // refuses any entry that carries a depth-0 separator.
    let deal = std::fs::read_to_string(out.join("skills/deal-desk/SKILL.md")).unwrap();
    assert!(deal.starts_with("---\nname: deal-desk\n"), "{deal}");
    assert!(
        deal.contains("description: \"Invoke when a rep submits a pricing exception request.\""),
        "{deal}"
    );
    assert!(
        deal.contains("allowed-tools: \"WebSearch WebFetch\"\n"),
        "the canonical single-line scalar, in spec order: {deal}"
    );
    // A bare `contains("allowed-tools:")` passes on BOTH shapes, so the block
    // form has to be asserted absent or a regression to it goes unnoticed.
    assert!(
        !deal.contains("  - \""),
        "the block list form must be gone entirely: {deal}"
    );
    // Instructions body lands after the frontmatter.
    assert!(deal.contains("Deal desk skill body."), "{deal}");
    assert!(
        deal.contains("A rep asks for a pricing exception."),
        "{deal}"
    );

    // Skill two: no allowed_tools -> the allowed-tools key is omitted entirely.
    let renewal = std::fs::read_to_string(out.join("skills/renewal-review/SKILL.md")).unwrap();
    assert!(
        renewal.starts_with("---\nname: renewal-review\n"),
        "{renewal}"
    );
    assert!(
        renewal.contains("description: \"Invoke when a renewal needs a discount review.\""),
        "{renewal}"
    );
    assert!(
        !renewal.contains("allowed-tools:"),
        "empty allowed_tools must omit the key entirely\n{renewal}"
    );
    assert!(renewal.contains("Renewal review skill body."), "{renewal}");
}

/// Regression: a description containing a colon-space (`Deal desk: pricing
/// exceptions`) is a bare YAML scalar corruptor -- `yaml.safe_load` raises
/// `mapping values are not allowed here` and rejects the bundle. The rendered
/// frontmatter must quote it so the exact quoted line is present.
#[test]
fn description_with_a_colon_is_rendered_as_a_quoted_scalar() {
    let body = r#"{
      "name": "colon-desc",
      "description": "Bundle desc.",
      "skills": [
        { "name": "solo", "description": "Deal desk: pricing exceptions", "instructions": "Body.\n" }
      ],
      "evals": [
        { "id": "e1", "input": "go", "grader": { "kind": "contains", "expected": "all done" } }
      ]
    }"#;
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("bundle");
    let spec = parse(body).unwrap();
    scaffold_from_spec(&out, &spec).unwrap();

    let solo = std::fs::read_to_string(out.join("skills/solo/SKILL.md")).unwrap();
    assert!(
        solo.contains("description: \"Deal desk: pricing exceptions\""),
        "colon-space description must be a quoted scalar\n{solo}"
    );
}

/// Regression: a description containing ` #` (`Reviews deals #1 priority`) is
/// treated as a comment by `yaml.safe_load` and silently TRUNCATES the value.
/// Quoting keeps the ` #` inside the string.
#[test]
fn description_with_a_hash_is_rendered_as_a_quoted_scalar() {
    let body = r#"{
      "name": "hash-desc",
      "description": "Bundle desc.",
      "skills": [
        { "name": "solo", "description": "Reviews deals #1 priority", "instructions": "Body.\n" }
      ],
      "evals": [
        { "id": "e1", "input": "go", "grader": { "kind": "contains", "expected": "all done" } }
      ]
    }"#;
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("bundle");
    let spec = parse(body).unwrap();
    scaffold_from_spec(&out, &spec).unwrap();

    let solo = std::fs::read_to_string(out.join("skills/solo/SKILL.md")).unwrap();
    assert!(
        solo.contains("description: \"Reviews deals #1 priority\""),
        "` #` description must be quoted so it is not read as a YAML comment\n{solo}"
    );
}

/// A connector in the spec becomes a `.mcp.json` `mcpServers` entry verbatim.
#[test]
fn writes_mcp_json_with_the_spec_connectors() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("bundle");
    let spec = parse(valid_spec_json()).unwrap();
    scaffold_from_spec(&out, &spec).unwrap();

    let mcp: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(out.join(".mcp.json")).unwrap()).unwrap();
    assert!(mcp["mcpServers"].is_object(), "{mcp}");
    assert_eq!(mcp["mcpServers"]["crm"]["command"], "crm-mcp", "{mcp}");
}

/// A spec with no connectors still writes a valid `.mcp.json` with an empty
/// `mcpServers` object (the default), not a missing file.
#[test]
fn writes_empty_mcp_servers_when_no_connectors() {
    let body = r#"{
      "name": "no-conn",
      "description": "No connectors here.",
      "skills": [
        { "name": "solo", "description": "Do the thing.", "instructions": "Solo body.\n" }
      ],
      "evals": [
        { "id": "e1", "input": "go", "grader": { "kind": "contains", "expected": "all done" } }
      ]
    }"#;
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("bundle");
    let spec = parse(body).unwrap();
    scaffold_from_spec(&out, &spec).unwrap();

    let mcp: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(out.join(".mcp.json")).unwrap()).unwrap();
    assert!(mcp["mcpServers"].is_object(), "{mcp}");
    assert_eq!(
        mcp["mcpServers"].as_object().unwrap().len(),
        0,
        "no connectors must yield an empty mcpServers object\n{mcp}"
    );
}

/// The scaffolded `evals/cases.json` is the suite object and is loadable by the
/// SAME loader `curie skill eval` uses. This is the parity guarantee: the
/// eval suite an agent-authored spec produces must load unchanged on the eval
/// path. Deleting the eval-writing impl breaks `load_suite`.
#[test]
fn scaffolded_evals_load_through_the_skill_eval_loader() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("bundle");
    let spec = parse(valid_spec_json()).unwrap();
    scaffold_from_spec(&out, &spec).unwrap();

    let suite = load_suite(&out.join("evals/cases.json")).expect("suite loads");
    assert_eq!(suite.name, "deal-desk", "suite name is the spec name");
    assert_eq!(suite.cases.len(), 1, "one eval in the spec -> one case");
    let case = &suite.cases[0];
    assert_eq!(case.id, "prices-a-deal");
    assert_eq!(case.grader.kind, GraderKind::Contains);
    assert_eq!(case.grader.expected, "deal-desk");
}

/// A spec-scaffolded grader carries through to the eval path FALSIFIABLE, on the
/// same footing as the `init` lane's seed (#553 / #527). #612: this lane kept a
/// grader tuned to the fake model's canned "all done", which passed on output the
/// fake produces regardless of the agent -- a green that proved nothing. Since
/// the fake tier is never graded at all, a grader may and must be written to
/// judge the real work.
#[test]
fn a_spec_scaffolded_grader_is_not_satisfied_by_the_fake_models_canned_reply() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("bundle");
    let spec = parse(valid_spec_json()).unwrap();
    scaffold_from_spec(&out, &spec).unwrap();

    let suite = load_suite(&out.join("evals/cases.json")).expect("suite loads");
    let grader = &suite.cases[0].grader;
    // "all done" is `fake.py::default_turn()`'s final text, whatever the input.
    assert!(
        !grader.grade("all done", &[]),
        "a spec-scaffolded grader must judge the real work, not the fake's canned text"
    );
    assert!(
        !grader.grade("", &[]),
        "an empty turn must never satisfy a scaffolded grader"
    );
    assert!(
        grader.grade("The deal-desk agent quotes 20% off for Acme.", &[]),
        "an on-topic answer must still pass"
    );
}

// --- validation / error behavior ------------------------------------------

/// A spec `name` that is not kebab-case is rejected, and the message names the
/// offending value so the author can fix it.
#[test]
fn rejects_a_non_kebab_spec_name() {
    let body = valid_spec_json().replace("\"name\": \"deal-desk\"", "\"name\": \"Deal_Desk\"");
    let err = parse(&body)
        .expect_err("bad spec name must error")
        .to_string();
    assert!(
        err.contains("Deal_Desk"),
        "message must name the value: {err}"
    );
}

/// A skill `name` that is not kebab-case is rejected with an actionable message.
#[test]
fn rejects_a_non_kebab_skill_name() {
    let body = valid_spec_json().replace(
        "\"name\": \"renewal-review\"",
        "\"name\": \"Renewal_Review\"",
    );
    let err = parse(&body)
        .expect_err("bad skill name must error")
        .to_string();
    assert!(
        err.contains("Renewal_Review"),
        "message must name the value: {err}"
    );
}

/// Two skills sharing a name is an authoring error (they collide on the same
/// `skills/<name>/SKILL.md` path), rejected before any disk write.
#[test]
fn rejects_duplicate_skill_names() {
    let body = r#"{
      "name": "dupe",
      "description": "Two skills, one name.",
      "skills": [
        { "name": "same", "description": "First.", "instructions": "A body.\n" },
        { "name": "same", "description": "Second.", "instructions": "B body.\n" }
      ],
      "evals": [
        { "id": "e1", "input": "go", "grader": { "kind": "contains", "expected": "all done" } }
      ]
    }"#;
    let err = parse(body)
        .expect_err("duplicate skill names must error")
        .to_string();
    assert!(
        err.contains("same"),
        "message must name the duplicate: {err}"
    );
}

/// A spec must declare at least one skill.
#[test]
fn rejects_an_empty_skills_array() {
    let body = r#"{
      "name": "empty-skills",
      "description": "No skills.",
      "skills": [],
      "evals": [
        { "id": "e1", "input": "go", "grader": { "kind": "contains", "expected": "all done" } }
      ]
    }"#;
    let err = parse(body)
        .expect_err("empty skills must error")
        .to_string();
    assert!(
        err.to_lowercase().contains("skill"),
        "message must mention skills: {err}"
    );
}

/// A spec must declare at least one eval (mirrors `load_suite`'s empty-cases
/// rejection: an empty suite is not runnable).
#[test]
fn rejects_an_empty_evals_array() {
    let body = r#"{
      "name": "empty-evals",
      "description": "No evals.",
      "skills": [
        { "name": "solo", "description": "Do it.", "instructions": "Solo body.\n" }
      ],
      "evals": []
    }"#;
    let err = parse(body).expect_err("empty evals must error").to_string();
    assert!(
        err.to_lowercase().contains("eval"),
        "message must mention evals: {err}"
    );
}

/// A connector server object with neither `command` nor `url` is unusable and
/// must be rejected rather than scaffolded into a broken `.mcp.json`.
#[test]
fn rejects_a_connector_without_command_or_url() {
    let body = r#"{
      "name": "bad-conn",
      "description": "Broken connector.",
      "skills": [
        { "name": "solo", "description": "Do it.", "instructions": "Solo body.\n" }
      ],
      "connectors": {
        "crm": { "args": ["--stdio"] }
      },
      "evals": [
        { "id": "e1", "input": "go", "grader": { "kind": "contains", "expected": "all done" } }
      ]
    }"#;
    let err = parse(body)
        .expect_err("connector without command/url must error")
        .to_string();
    assert!(
        err.contains("command") || err.contains("url") || err.contains("crm"),
        "message must explain the missing command/url: {err}"
    );
}

/// A connector whose `command` is present but not a STRING (`{"command": 42}`)
/// is rejected: the frozen `McpServer` types `command`/`url` as `str`, so a
/// non-string would pass a mere presence check but make `validate_bundle` reject
/// the emitted `.mcp.json`. The message must name the connector and the field.
#[test]
fn rejects_a_connector_with_a_non_string_command() {
    let body = r#"{
      "name": "typed-conn",
      "description": "Wrong-typed connector.",
      "skills": [
        { "name": "solo", "description": "Do it.", "instructions": "Solo body.\n" }
      ],
      "connectors": {
        "crm": { "command": 42 }
      },
      "evals": [
        { "id": "e1", "input": "go", "grader": { "kind": "contains", "expected": "all done" } }
      ]
    }"#;
    let err = parse(body)
        .expect_err("non-string command must error")
        .to_string();
    assert!(
        err.contains("crm") && err.contains("command") && err.contains("string"),
        "message must name the connector and the wrong-typed field: {err}"
    );
}

/// An unknown top-level field (a typo like `skils`) is rejected via
/// `deny_unknown_fields`, so a mistyped spec fails loudly instead of silently
/// dropping the intended field. This is the intended strict-parse design.
#[test]
fn rejects_an_unknown_top_level_field() {
    let body = r#"{
      "name": "typo",
      "description": "Typo'd key.",
      "skils": [
        { "name": "solo", "description": "Do it.", "instructions": "Solo body.\n" }
      ],
      "evals": [
        { "id": "e1", "input": "go", "grader": { "kind": "contains", "expected": "all done" } }
      ]
    }"#;
    let err = parse(body)
        .expect_err("unknown field must error")
        .to_string();
    assert!(
        err.contains("skils"),
        "message must name the unknown field: {err}"
    );
}

/// An eval carrying an invalid regex grader is rejected at parse/scaffold time,
/// reusing the `load_suite` regex-at-load discipline (a bad pattern fails now,
/// not mid-run).
#[test]
fn rejects_an_invalid_regex_grader_in_a_spec_eval() {
    let body = r#"{
      "name": "bad-regex",
      "description": "Invalid regex grader.",
      "skills": [
        { "name": "solo", "description": "Do it.", "instructions": "Solo body.\n" }
      ],
      "evals": [
        { "id": "e1", "input": "go", "grader": { "kind": "regex", "expected": "(unclosed" } }
      ]
    }"#;
    // Either parse or scaffold must reject it; assert the combined pipeline errors.
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("bundle");
    let result = parse(body).and_then(|spec| scaffold_from_spec(&out, &spec).map(|_| ()));
    let err = result
        .expect_err("invalid regex grader must error")
        .to_string();
    assert!(
        err.contains("(unclosed") || err.to_lowercase().contains("regex"),
        "message must name the bad pattern: {err}"
    );
}

// --- collision refusal (mirrors existing scaffold discipline) --------------

/// Scaffolding twice into the same dir refuses the second time rather than
/// truncating the first bundle.
#[test]
fn refuses_to_scaffold_twice_into_the_same_dir() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("bundle");
    let spec = parse(valid_spec_json()).unwrap();
    scaffold_from_spec(&out, &spec).unwrap();
    assert!(
        scaffold_from_spec(&out, &spec).is_err(),
        "second scaffold into the same dir must refuse"
    );
}

/// If a single target file already exists, scaffold refuses, leaves that file
/// untouched, and does not create the manifest. Mirrors the existing
/// `refuses_to_truncate_any_existing_target_even_without_a_manifest` test.
#[test]
fn refuses_to_truncate_an_existing_target_and_writes_no_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("bundle");
    std::fs::create_dir_all(&out).unwrap();
    let existing = r#"{"mcpServers":{"important":{"command":"x"}}}"#;
    std::fs::write(out.join(".mcp.json"), existing).unwrap();

    let spec = parse(valid_spec_json()).unwrap();
    let err = scaffold_from_spec(&out, &spec).unwrap_err().to_string();
    assert!(err.contains(".mcp.json"), "{err}");
    assert_eq!(
        std::fs::read_to_string(out.join(".mcp.json")).unwrap(),
        existing,
        "existing file must be untouched"
    );
    assert!(
        !out.join(".claude-plugin/plugin.json").exists(),
        "no manifest may be written when a target collides"
    );
}

// --- CLI surface through the curie binary --------------------------------

/// `curie init --from-spec <valid>` is fully non-interactive (no stdin),
/// exits 0, and produces a bundle whose manifest name is the SPEC's name (not
/// any positional argument -- none is passed).
#[test]
fn cli_init_from_spec_scaffolds_non_interactively() {
    let dir = tempfile::tempdir().unwrap();
    let spec_path = write_spec(dir.path(), valid_spec_json());
    let out = dir.path().join("bundle");

    let output = Command::new(bin())
        .arg("init")
        .arg("--from-spec")
        .arg(&spec_path)
        .arg("--dir")
        .arg(&out)
        .stdin(std::process::Stdio::null())
        .output()
        .expect("run curie init --from-spec");

    assert!(
        output.status.success(),
        "expected success\n{}",
        output_text(&output)
    );
    let (name, _version) = read_manifest(&out).expect("manifest written");
    assert_eq!(
        name, "deal-desk",
        "bundle name is the spec name, not a positional"
    );
}

/// `curie init --from-spec <invalid>` exits non-zero and names the problem.
#[test]
fn cli_init_from_spec_rejects_an_invalid_spec() {
    let dir = tempfile::tempdir().unwrap();
    // Non-kebab name is a deterministic input error.
    let body = valid_spec_json().replace("\"name\": \"deal-desk\"", "\"name\": \"Deal_Desk\"");
    let spec_path = write_spec(dir.path(), &body);
    let out = dir.path().join("bundle");

    let output = Command::new(bin())
        .arg("init")
        .arg("--from-spec")
        .arg(&spec_path)
        .arg("--dir")
        .arg(&out)
        .stdin(std::process::Stdio::null())
        .output()
        .expect("run curie init --from-spec");

    assert!(
        !output.status.success(),
        "expected failure for an invalid spec\n{}",
        output_text(&output)
    );
    assert!(
        output_text(&output).contains("Deal_Desk"),
        "message must name the offending value\n{}",
        output_text(&output)
    );
}

/// `curie init` with NEITHER a positional name NOR `--from-spec` exits
/// non-zero and tells the user to pass a name or `--from-spec`.
#[test]
fn cli_init_without_name_or_spec_errors_with_guidance() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("bundle");

    let output = Command::new(bin())
        .arg("init")
        .arg("--dir")
        .arg(&out)
        .stdin(std::process::Stdio::null())
        .output()
        .expect("run curie init with no name");

    assert!(
        !output.status.success(),
        "expected failure when neither name nor --from-spec is given\n{}",
        output_text(&output)
    );
    let text = output_text(&output).to_lowercase();
    assert!(
        text.contains("name") && text.contains("from-spec"),
        "message must point at name or --from-spec\n{}",
        output_text(&output)
    );
}

// --- the canonical `allowed-tools` scalar and its entry rule (D4 / ADR-0135) --
//
// The emitted scalar is separator-delimited at paren depth 0, so an entry that
// itself carries a depth-0 separator cannot round-trip: joining it and parsing it
// back yields a DIFFERENT list. That is not cosmetic. The runner's #1852
// gate-shadow check reads those entries, and `_entry_tool("Bash(git")` returns
// None while `_entry_tool("commit:*)")` returns a garbage name -- so a corrupted
// entry drops out of shadow detection entirely and the bundle boots reporting a
// gate as armed while the tool runs unapproved. The spec refuses such an entry
// before any byte is written.

/// A specifier containing a SPACE is a legitimate Claude Code permission rule
/// (`Bash(git commit:*)`), and the depth-aware join round-trips it, so the spec
/// must ACCEPT it and the scaffold must emit it intact.
#[test]
fn accepts_a_specifier_containing_a_space_and_emits_it_intact() {
    let body = r#"{
      "name": "paren-desk",
      "description": "Parens survive the join.",
      "skills": [
        {
          "name": "paren-desk",
          "description": "Invoke to commit.",
          "allowed_tools": ["Bash(git commit:*)", "Read"],
          "instructions": "Paren desk skill body.\n"
        }
      ],
      "evals": [
        { "id": "e1", "input": "go", "grader": { "kind": "contains", "expected": "ok" } }
      ]
    }"#;
    let spec = parse(body).expect("a paren-bearing entry must be accepted");

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("bundle");
    scaffold_from_spec(&out, &spec).unwrap();

    let skill = std::fs::read_to_string(out.join("skills/paren-desk/SKILL.md")).unwrap();
    assert!(
        skill.contains("allowed-tools: \"Bash(git commit:*) Read\"\n"),
        "the specifier must survive the join unsplit: {skill}"
    );
}

/// A comma or space OUTSIDE parens, or an unbalanced paren, cannot survive the
/// canonical join, so `parse` must Err and the message must name the entry --
/// the author has to know which one to fix.
#[test]
fn rejects_an_allowed_tools_entry_that_cannot_round_trip() {
    // A comma at depth 0; whitespace at depth 0; depth never returns to 0; a
    // close paren at depth 0.
    for entry in ["Read,Write", "Read Write", "Bash(git", "oops)"] {
        let body = format!(
            r#"{{
              "name": "bad-desk",
              "description": "An entry that cannot round-trip.",
              "skills": [
                {{
                  "name": "bad-desk",
                  "description": "Invoke never.",
                  "allowed_tools": ["Read", {entry:?}],
                  "instructions": "Bad desk skill body.\n"
                }}
              ],
              "evals": [
                {{ "id": "e1", "input": "go", "grader": {{ "kind": "contains", "expected": "ok" }} }}
              ]
            }}"#
        );
        let err = parse(&body)
            .expect_err("an unserializable allowed_tools entry must error")
            .to_string();
        assert!(
            err.contains(entry),
            "message must name the offending entry {entry:?}: {err}"
        );
    }
}
