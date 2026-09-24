//! Stream B tests for `curie skill check` (issue #337): the offline,
//! credential-free MCP load check.
//!
//! These pin the frozen Section-3 runner<->CLI JSON seam and the docker argv
//! for the one-shot check container. They are written test-first: the public
//! API they reference (`docker::CheckSpec`, `commands::parse_check_report`,
//! `commands::check_outcome`, `commands::CheckReport`) does not exist yet, so
//! this binary fails to compile until the Implementer adds it. That RED state
//! is the intended contract handoff.

use curie::commands::{check_outcome, parse_check_report};
use curie::docker::CheckSpec;
use curie::exit::ExitClass;

/// A realistic green seam payload (Section 3): declared server registered as a
/// connected, plugin-owned (`scope: "dynamic"`) server with tools.
const GREEN_JSON: &str = r#"{
  "check": "mcp-load",
  "version": 1,
  "plugin_dir": "/plugin",
  "declared": [
    { "name": "text-stats-engine", "source": "plugin.json", "form": "inline" }
  ],
  "registered": [
    {
      "name": "plugin:text-stats-engine:text-stats-engine",
      "status": "connected",
      "tools": ["count_words", "top_words"],
      "error": null,
      "scope": "dynamic"
    }
  ],
  "matches": [
    { "declared": "text-stats-engine", "registered": "plugin:text-stats-engine:text-stats-engine", "connected": true, "tool_count": 2 }
  ],
  "verdict": "green",
  "reasons": [],
  "hints": []
}"#;

/// A realistic red seam payload (Section 3): the #336 string-pointer form, whose
/// declared server never registers. `reasons` non-empty; the inline-object
/// fingerprint rides in `hints`.
const RED_JSON: &str = r#"{
  "check": "mcp-load",
  "version": 1,
  "plugin_dir": "/plugin",
  "declared": [
    { "name": "text-stats-engine", "source": "plugin.json", "form": "string_pointer" }
  ],
  "registered": [],
  "matches": [
    { "declared": "text-stats-engine", "registered": null, "connected": false, "tool_count": 0 }
  ],
  "verdict": "red",
  "reasons": [
    "declared text-stats-engine never registered",
    "declared 1 MCP server(s); none registered with tools"
  ],
  "hints": [
    "plugin.json 'mcpServers' is a string pointer; the real loader silently ignores this form — inline the object"
  ]
}"#;

/// The verbatim runner shape: `registered[].tools` items are SDK `McpToolInfo`
/// OBJECTS (`{"name": ..., "annotations"?: ...}`), NOT strings. This is the real
/// contract the runner emits; it must parse and map to Ok.
const GREEN_OBJECT_TOOLS_JSON: &str = r#"{
  "check": "mcp-load",
  "version": 1,
  "plugin_dir": "/plugin",
  "declared": [
    { "name": "mcp-green", "source": "plugin.json", "form": "inline" }
  ],
  "registered": [
    {
      "name": "plugin:mcp-green:green-probe",
      "status": "connected",
      "serverInfo": { "name": "mcp-green-probe", "version": "1.28.1" },
      "scope": "dynamic",
      "tools": [ { "name": "word_count", "annotations": {} } ]
    }
  ],
  "matches": [
    { "declared": "mcp-green", "registered": "plugin:mcp-green:green-probe", "connected": true, "tool_count": 1 }
  ],
  "verdict": "green",
  "reasons": [],
  "hints": []
}"#;

/// A realistic invalid-bundle seam payload (Section 3): the bundle dir exists
/// but fails structural `plugin_format` validation, which the runner reports as
/// `verdict: "invalid_bundle"` with the validation errors in `reasons`. The CLI
/// must map this to a Usage error (exit 2), matching the runner's own exit 2.
const INVALID_BUNDLE_JSON: &str = r#"{
  "check": "mcp-load",
  "version": 1,
  "plugin_dir": "/plugin",
  "declared": [],
  "registered": [],
  "matches": [],
  "verdict": "invalid_bundle",
  "reasons": [
    "skills/: no SKILL.md found for declared skill 'text-stats'",
    ".claude-plugin/plugin.json: 'name' is required"
  ],
  "hints": []
}"#;

/// A future-version payload the CLI must hard-fail on (Section 7(e)).
const VERSION_2_JSON: &str = r#"{
  "check": "mcp-load",
  "version": 2,
  "plugin_dir": "/plugin",
  "declared": [],
  "registered": [],
  "matches": [],
  "verdict": "green",
  "reasons": [],
  "hints": []
}"#;

// --- Test 6: CheckSpec::run_args exact argv --------------------------------

#[test]
fn check_run_args_are_the_exact_offline_argv() {
    let spec = CheckSpec {
        image: "curie-runner".into(),
        plugin_dir: "/tmp/deal-desk".into(),
        timeout_s: 30,
    };
    let args = spec.run_args();

    // The argv MUST be exactly this vector, in this order. The check runs
    // untrusted bundle MCP code, so it is offline (--network none) AND hardened
    // at the container level (#631): read-only rootfs + tmpfs, cap-drop ALL,
    // no-new-privileges.
    let expected: Vec<String> = [
        "run",
        "--rm",
        "--network",
        "none",
        "--read-only",
        "--tmpfs",
        "/tmp:rw,mode=1777",
        "--tmpfs",
        "/home/runner:rw,mode=1777",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "-v",
        "/tmp/deal-desk:/plugin:ro",
        "-e",
        "CURIE_PLUGIN_DIR=/plugin",
        "-e",
        "CURIE_CHECK_TIMEOUT_S=30",
        "curie-runner",
        "python",
        "-m",
        "curie_runner.check",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    assert_eq!(
        args, expected,
        "check run_args drifted from the frozen argv"
    );

    let joined = args.join(" ");
    // Offline contract: the check container is network-isolated.
    assert!(
        joined.contains("--network none"),
        "check must run with --network none (offline contract)"
    );
    // Read-only bundle mount.
    assert!(joined.contains("-v /tmp/deal-desk:/plugin:ro"));
    // The timeout is plumbed through as the container deadline.
    assert!(joined.contains("-e CURIE_CHECK_TIMEOUT_S=30"));
    // The CMD override runs check mode, not the ACI session entrypoint.
    assert!(joined.ends_with("python -m curie_runner.check"));

    // Absence: check mode is not an ACI session and carries no credentials.
    assert!(!joined.contains("-p "), "check must not publish a port");
    assert!(
        !args.iter().any(|a| a == "-p"),
        "check must not publish a port"
    );
    assert!(
        !joined.contains("--name"),
        "check is one-shot, no container name"
    );
    assert!(!joined.contains("CURIE_SESSION_ID"));
    assert!(!joined.contains("CURIE_SANDBOX_ID"));
    assert!(!joined.contains("CURIE_BUDGET"));
    assert!(!joined.contains("CURIE_FAKE_MODEL"));
    // No credential env of any kind (spike-verified credential-free connect).
    assert!(!joined.contains("ANTHROPIC"));
    assert!(!joined.contains("CLAUDE_CODE_OAUTH_TOKEN"));
    assert!(!joined.contains("API_KEY"));
}

#[test]
fn check_run_args_carry_the_specced_timeout() {
    let spec = CheckSpec {
        image: "ghcr.io/example/curie-runner:1.2.3".into(),
        plugin_dir: "/work/bundle".into(),
        timeout_s: 45,
    };
    let joined = spec.run_args().join(" ");
    assert!(joined.contains("-e CURIE_CHECK_TIMEOUT_S=45"));
    assert!(joined.contains("-v /work/bundle:/plugin:ro"));
    assert!(joined.contains("ghcr.io/example/curie-runner:1.2.3"));
}

const INVALID_BUNDLE_FIX: &str =
    "correct the invalid bundle declaration named in the error and run curie skill check again";

/// One runner `PluginBundleError` line. It already starts with the bundle headline.
const RUNNER_SHAPED_BUNDLE_REASON: &str = "invalid plugin bundle at /plugin: [mcp.declared_pointer] plugin.json: manifest mcpServers is the path '.mcp.json', a form the loader ignores";

/// Bare validator lines. The bracketed codes are opaque and must pass through.
const BARE_POINTER_REASON: &str =
    "[mcp.declared_pointer] plugin.json: manifest mcpServers is a path the loader ignores";
const BARE_CRON_REASON: &str =
    "[triggers.cron_invalid_schedule] triggers/nightly.md: schedule is not accepted";

fn invalid_bundle_with_reasons(reasons: &[&str]) -> curie::commands::CheckReport {
    let mut report =
        parse_check_report(INVALID_BUNDLE_JSON).expect("invalid_bundle seam JSON parses");
    report.reasons = reasons.iter().map(|reason| reason.to_string()).collect();
    report
}

fn assert_invalid_bundle_failure(err: &curie::exit::CliError) {
    assert_eq!(
        err.class,
        ExitClass::Usage,
        "invalid_bundle is a Usage error (exit 2), matching the runner's exit 2"
    );
    assert_eq!(err.class.code(), 2, "Usage exits 2");
    assert!(
        !err.message.contains("MCP"),
        "a bundle validation failure must not mention MCP, got: {}",
        err.message
    );
    assert_eq!(
        err.fix.as_deref(),
        Some(INVALID_BUNDLE_FIX),
        "fix must name the invalid bundle declaration, not only plugin.json and skills/"
    );
}

// --- Test 7: verdict-JSON -> outcome mapping -------------------------------

#[test]
fn green_report_parses_and_maps_to_ok() {
    let report = parse_check_report(GREEN_JSON).expect("green seam JSON parses");
    assert_eq!(report.version, 1);
    assert_eq!(report.verdict, "green");
    assert!(report.reasons.is_empty(), "green carries no reasons");

    check_outcome(&report).expect("a green verdict maps to Ok(())");
}

#[test]
fn green_report_with_object_tools_parses_and_maps_to_ok() {
    // Guards the real runner seam: `tools` items are McpToolInfo objects, not
    // strings. `registered` is opaque pass-through JSON, so any tool/server
    // shape parses cleanly (the whole tools-shape bug class is eliminated).
    let report =
        parse_check_report(GREEN_OBJECT_TOOLS_JSON).expect("object-tools seam JSON parses");
    assert_eq!(report.version, 1);
    assert_eq!(report.verdict, "green");
    assert_eq!(report.registered.len(), 1);
    assert!(
        !report.registered.is_empty(),
        "the object-shaped registered server round-trips through opaque JSON"
    );

    check_outcome(&report).expect("a green verdict with object tools maps to Ok(())");
}

#[test]
fn red_report_maps_to_failure_with_a_fix_hint() {
    let report = parse_check_report(RED_JSON).expect("red seam JSON parses");
    assert_eq!(report.verdict, "red");
    assert!(!report.reasons.is_empty(), "red must carry reasons");

    let err = check_outcome(&report).expect_err("a red verdict maps to Err");
    assert_eq!(
        err.class,
        ExitClass::Failure,
        "red is a plain Failure (exit 1)"
    );
    assert_eq!(err.class.code(), 1, "Failure exits 1");
    assert_eq!(err.message, "MCP load check reported red");
    assert!(
        !err.message.contains("invalid plugin bundle"),
        "a red MCP load failure must not be reported as an invalid plugin bundle, got: {}",
        err.message
    );
    let fix = err
        .fix
        .expect("red outcome must carry an actionable fix hint");
    assert!(
        fix.contains("raise --timeout"),
        "the red fix must still point at raising --timeout, got: {fix}"
    );
    assert_eq!(
        fix,
        "read the printed reason(s): fix the server's command/args, forward its credential with curie skill up --secret <NAME>, or raise --timeout if MCP init ran long"
    );
}

#[test]
fn invalid_bundle_report_maps_to_usage_exit_2_with_reasons() {
    let report = parse_check_report(INVALID_BUNDLE_JSON).expect("invalid_bundle seam JSON parses");
    assert_eq!(report.verdict, "invalid_bundle");
    assert!(
        !report.reasons.is_empty(),
        "invalid_bundle must carry structural reasons"
    );

    let err = check_outcome(&report).expect_err("an invalid_bundle verdict maps to Err");
    assert_invalid_bundle_failure(&err);
    // The structural validation errors must surface in the message so the user
    // sees WHY the bundle is invalid. Both fixture reasons stay intact.
    assert!(
        err.message
            .contains("skills/: no SKILL.md found for declared skill 'text-stats'"),
        "the message must carry the structural reasons, got: {}",
        err.message
    );
    assert!(
        err.message
            .contains(".claude-plugin/plugin.json: 'name' is required"),
        "the message must carry the structural reasons, got: {}",
        err.message
    );
    assert_eq!(
        err.message,
        "invalid plugin bundle: skills/: no SKILL.md found for declared skill 'text-stats'; .claude-plugin/plugin.json: 'name' is required"
    );
}

#[test]
fn invalid_bundle_with_empty_reasons_is_exactly_invalid_plugin_bundle() {
    let report = invalid_bundle_with_reasons(&[]);
    assert!(report.reasons.is_empty(), "this case carries no reasons");

    let err = check_outcome(&report).expect_err("an invalid_bundle verdict maps to Err");
    assert_invalid_bundle_failure(&err);
    assert_eq!(err.message, "invalid plugin bundle");
}

#[test]
fn invalid_bundle_runner_shaped_reason_is_unchanged_without_a_doubled_headline() {
    // The runner already emits one reason that starts with "invalid plugin bundle".
    // That string is the whole message; prefixing it doubles the headline.
    let err = check_outcome(&invalid_bundle_with_reasons(&[RUNNER_SHAPED_BUNDLE_REASON]))
        .expect_err("an invalid_bundle verdict maps to Err");
    assert_invalid_bundle_failure(&err);
    assert_eq!(err.message, RUNNER_SHAPED_BUNDLE_REASON);
    assert!(
        err.message.contains("[mcp.declared_pointer]"),
        "the bracketed code must survive verbatim, got: {}",
        err.message
    );
    assert!(
        !err.message
            .contains("invalid plugin bundle: invalid plugin bundle"),
        "the headline must not be doubled, got: {}",
        err.message
    );
}

#[test]
fn invalid_bundle_bare_reasons_preserve_opaque_validation_codes() {
    // Codes are opaque text. This does not define cron scheduling.
    let err = check_outcome(&invalid_bundle_with_reasons(&[
        BARE_POINTER_REASON,
        BARE_CRON_REASON,
    ]))
    .expect_err("an invalid_bundle verdict maps to Err");
    assert_invalid_bundle_failure(&err);
    assert_eq!(
        err.message,
        format!("invalid plugin bundle: {BARE_POINTER_REASON}; {BARE_CRON_REASON}")
    );
    assert!(
        err.message.contains("[mcp.declared_pointer]"),
        "the pointer code must survive verbatim, got: {}",
        err.message
    );
    assert!(
        err.message.contains("[triggers.cron_invalid_schedule]"),
        "the cron code must survive verbatim, got: {}",
        err.message
    );
}

#[test]
fn version_mismatch_is_a_parse_error_naming_the_contract() {
    let err = parse_check_report(VERSION_2_JSON)
        .expect_err("a version != 1 payload must hard-fail the parse");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("version") || msg.contains("contract"),
        "the version-drift error must name the version/contract, got: {msg}"
    );
}

#[test]
fn garbage_stdout_is_a_parse_error() {
    parse_check_report("this is not json at all {{{")
        .expect_err("unparseable stdout must be an error, not a silent green");
}
