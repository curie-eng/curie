//! Unit-level coverage for the semantic exit-code + error-classification
//! contract (ADR-0021 decision 1, AC 2 and AC 4). These reference the
//! yet-to-exist `curie::exit` module, so the file will not compile until the
//! implementer adds it; that red state is the contract handoff.

use curie::exit::{self, CliError, ExitClass};

#[test]
fn exit_class_codes_are_stable() {
    assert_eq!(ExitClass::Success.code(), 0);
    assert_eq!(ExitClass::Failure.code(), 1);
    assert_eq!(ExitClass::Usage.code(), 2);
    assert_eq!(ExitClass::Transient.code(), 3);
}

#[test]
fn classify_usage_error_is_usage_class() {
    let err = exit::usage("bad");
    let (class, _fix) = exit::classify(&err);
    assert_eq!(class, ExitClass::Usage);
}

#[test]
fn classify_transient_error_is_transient_with_fix() {
    let err = exit::transient("net");
    let (class, fix) = exit::classify(&err);
    assert_eq!(class, ExitClass::Transient);
    assert!(fix.is_some(), "transient errors carry a retry hint");
}

#[test]
fn classify_plain_anyhow_is_failure_with_no_fix() {
    let err = anyhow::anyhow!("boom");
    let (class, fix) = exit::classify(&err);
    assert_eq!(class, ExitClass::Failure);
    assert_eq!(fix, None);
}

#[test]
fn classify_finds_clierror_through_context_wrapping() {
    // A tagged CliError buried under an anyhow context layer must still be
    // discovered by walking the error chain: class + fix survive wrapping.
    let base: anyhow::Error = CliError::usage("nope").with_fix("do X").into();
    let wrapped = base.context("outer");
    let (class, fix) = exit::classify(&wrapped);
    assert_eq!(class, ExitClass::Usage);
    assert_eq!(fix.as_deref(), Some("do X"));
}

#[test]
fn error_json_carries_message_and_fix() {
    let err: anyhow::Error = CliError::usage("nope").with_fix("do X").into();
    let value = exit::error_json(&err);
    assert_eq!(value["error"], "nope");
    assert_eq!(value["fix"], "do X");
}

#[test]
fn error_json_fix_is_null_for_plain_error() {
    let err = anyhow::anyhow!("kaboom");
    let value = exit::error_json(&err);
    assert_eq!(value["error"], "kaboom");
    assert!(value["fix"].is_null());
}

/// #1040: `<local|cluster> info --check-mcp` is a plain FAILURE (exit 1), and
/// deliberately not `Unsupported`. The flag is still DECLARED at those tiers so
/// it can be declined with a reason rather than rejected as an unknown-flag typo
/// (#771, ADR-0041), but that is where the resemblance ends.
///
/// Exit 4 is reserved for a concept absent BY CONSTRUCTION, one no input and no
/// future release can answer here. This is not that: a deployed bundle's bytes
/// are reachable as `(path, content)` pairs via `ApiClient::bundle_files` and
/// `run_check_report` takes a `PathBuf`, so materializing them and running the
/// identical probe is mechanically available today. Only the code to do it is
/// missing. "Not yet" is not "never", and an agent that reads exit 4 stops
/// retrying and stops asking, which is the lie ADR-0041 exists to prevent.
///
/// The `fix` and Display properties are the same ones the
/// `skill_versions_unavailable` unit tests assert, and they exist because the two
/// consumers read different fields: a machine branches on `{error, fix}`, while a
/// human sees only `{err:#}`.
#[test]
fn info_check_mcp_is_a_failure_with_a_fix_naming_the_skill_tier() {
    let err = curie::commands::info_check_mcp_unavailable();
    let (class, fix) = exit::classify(&err);
    assert_eq!(
        class,
        ExitClass::Failure,
        "an unbuilt step is exit 1; exit 4 would claim the tier can NEVER answer, \
         which would stop an agent retrying against a capability that is coming"
    );
    assert_ne!(
        class,
        ExitClass::Unsupported,
        "reserved for by-construction absence, which this is not"
    );

    let fix = fix.expect("the decline must carry an actionable alternative");
    assert_eq!(
        fix,
        curie::commands::INFO_CHECK_MCP_ALT,
        "the machine-readable `fix` is the single-sourced alternative verbatim, so \
         the runtime answer and the clap help text cannot drift (#459)"
    );
    assert!(
        fix.contains("curie skill info"),
        "the alternative must name the tier that CAN answer, got {fix:?}"
    );

    // The human path renders only Display and discards `fix`, so an alternative
    // that lived only in `fix` would tell a person why the verb cannot answer and
    // never where it can.
    let display = format!("{err:#}");
    assert!(
        display.contains("info --check-mcp"),
        "the message must name the concept being declined, got: {display}"
    );
    assert!(
        display.contains(curie::commands::INFO_CHECK_MCP_REASON),
        "the message must carry the REASON verbatim, got: {display}"
    );
    assert!(
        display.contains(curie::commands::INFO_CHECK_MCP_ALT),
        "the message must ALSO carry the alternative, got: {display}"
    );
}
