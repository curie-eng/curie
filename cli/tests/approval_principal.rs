//! Authenticated approval principals at the CLI boundary (#1531, ADR-0106).
//!
//! These tests intentionally drive the compiled binary for clap and output
//! behavior, and a wire-level stub for the resolve/mint boundary.  The old
//! implementation accepted `--as` and `--actor-channel`, then put those caller
//! assertions in the JSON body.  A unit test that merely constructs an
//! `ApprovalCmd` cannot catch either regression: clap may still accept retired
//! flags, or an API client may quietly omit the principal header.  The API owns
//! authorization; this suite owns the CLI's proof that it does not invent one.

use crate::support;

use std::process::{Command, Output};

use serde_json::{json, Value};
use support::{serve, MockServer, Response};

const TEST_API_KEY: &str = "test-key";
const APPROVAL_ID: &str = "22222222-2222-2222-2222-222222222222";
const OPERATOR_SUBJECT: &str = "U0OPERATOR";
// Deliberately looks like a real credential rather than a generic `token`: a
// redaction regression must not pass only because the fixture is too vague to
// reach the output path that logs a principal.
const OPERATOR_PRINCIPAL: &str = "apr.test.operator-principal-that-must-not-leak";
const CONSOLE_CODE: &str = "clc.test-console-code-delivered-once";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn approval_record_json(subject: &str) -> String {
    format!(
        r#"{{"id":"{APPROVAL_ID}","agent_id":"11111111-1111-1111-1111-111111111111","author":"{OPERATOR_SUBJECT}","route":"explicit-reviewers","gate_kind":"policy","granted_tool":"Bash","status":"approved","conversation_id":"thread-1","summary":"approve release","expires_at":null,"resolved_by":"{subject}","card_channel":"C0CARD01","reply_channel":"C0TURN01"}}"#
    )
}

/// Run a local resolve with no ambient values inherited from the developer's
/// shell.  A test must never accidentally pass because an operator happened to
/// have a principal in their environment.
fn local_resolve(server: &MockServer, token: Option<&str>) -> Output {
    let mut command = Command::new(bin());
    command
        .args([
            "local",
            "approvals",
            "weather",
            "--resolve",
            APPROVAL_ID,
            "--note",
            "approved after review",
            "--api-url",
            &server.base_url,
            "--api-key",
            TEST_API_KEY,
            "--json",
        ])
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env_remove("CURIE_APPROVAL_PRINCIPAL_TOKEN")
        .env("NO_COLOR", "1");
    if let Some(token) = token {
        command.env("CURIE_APPROVAL_PRINCIPAL_TOKEN", token);
    }
    command
        .output()
        .expect("run curie local approvals --resolve")
}

/// The two caller-asserted identity flags must be rejected *before* either
/// durable-approval tier reaches connection setup.  `skill approvals` has no
/// durable resolver (ADR-0077), but it still must reject the retired spellings
/// rather than accepting them and only then reporting that the tier is absent.
#[test]
fn every_tier_rejects_retired_identity_assertion_flags() {
    let cases: &[(&[&str], &str)] = &[
        (
            &[
                "skill",
                "approvals",
                "--resolve",
                APPROVAL_ID,
                "--as",
                "U0ASSERTED",
            ],
            "--as",
        ),
        (
            &[
                "skill",
                "approvals",
                "--resolve",
                APPROVAL_ID,
                "--actor-channel",
                "C0EXAMPLE1",
            ],
            "--actor-channel",
        ),
        (
            &[
                "local",
                "approvals",
                "weather",
                "--resolve",
                APPROVAL_ID,
                "--as",
                "U0ASSERTED",
            ],
            "--as",
        ),
        (
            &[
                "local",
                "approvals",
                "weather",
                "--resolve",
                APPROVAL_ID,
                "--actor-channel",
                "C0EXAMPLE1",
            ],
            "--actor-channel",
        ),
        (
            &[
                "cluster",
                "approvals",
                "weather",
                "--resolve",
                APPROVAL_ID,
                "--as",
                "U0ASSERTED",
            ],
            "--as",
        ),
        (
            &[
                "cluster",
                "approvals",
                "weather",
                "--resolve",
                APPROVAL_ID,
                "--actor-channel",
                "C0EXAMPLE1",
            ],
            "--actor-channel",
        ),
    ];

    for (argv, retired) in cases {
        let output = Command::new(bin())
            .args(*argv)
            .env_remove("CURIE_API_URL")
            .env_remove("CURIE_API_KEY")
            .env_remove("CURIE_APPROVAL_PRINCIPAL_TOKEN")
            .output()
            .unwrap_or_else(|err| panic!("run curie {}: {err}", argv.join(" ")));
        let combined = text(&output);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{retired} must be rejected by clap before a tier attempts any connection; output:\n{combined}"
        );
        assert!(
            combined.contains(retired)
                && (combined.contains("unexpected argument")
                    || combined.contains("unrecognized option")),
            "{retired} must be an unknown retired flag rather than an accepted alias; output:\n{combined}"
        );
    }
}

/// No principal means no resolve.  The fix must name the environment input and
/// a recovery action, but must not turn the platform API key into a fallback
/// identity credential.
#[test]
fn local_resolve_requires_an_env_backed_principal_with_an_actionable_fix() {
    let server = serve(|request| {
        panic!(
            "a missing principal must fail before HTTP, received {} {}",
            request.method, request.path
        )
    });
    let output = local_resolve(&server, None);
    let combined = text(&output);

    assert_eq!(
        output.status.code(),
        Some(2),
        "a missing principal is an invocation/credential-use error, not a platform request; output:\n{combined}"
    );
    assert!(
        combined.contains("CURIE_APPROVAL_PRINCIPAL_TOKEN"),
        "the error must name the env-backed credential to set; output:\n{combined}"
    );
    assert!(
        combined.contains("mint") || combined.contains("Mint"),
        "the error must give the operator a usable mint recovery, not merely say authentication failed; output:\n{combined}"
    );
}

/// The principal is the *only* resolver identity the CLI may send.  Drive the
/// actual executable so this catches a clap/command/API-client mismatch, then
/// inspect the recorded request instead of accepting a hand-assembled body.
/// The returned record is the observable audit identity on this CLI surface.
#[test]
fn local_resolve_sends_the_env_principal_and_only_decision_note_body() {
    let server = serve(
        |request| match (request.method.as_str(), request.path.as_str()) {
            ("POST", path) if path == format!("/approvals/{APPROVAL_ID}/resolve") => {
                Response::json(200, &approval_record_json(OPERATOR_SUBJECT))
            }
            other => panic!("unexpected request: {other:?}"),
        },
    );

    let output = local_resolve(&server, Some(OPERATOR_PRINCIPAL));
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "principal-backed resolve against a well-formed API must succeed; stdout: {stdout}\nstderr: {stderr}"
    );

    let rendered: Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|err| panic!("--json resolve output must be one object: {err}; {stdout}"));
    assert_eq!(
        rendered["resolved"]["resolved_by"],
        OPERATOR_SUBJECT,
        "the returned durable record must name the authenticated token subject, not a CLI assertion: {rendered}"
    );

    let recorded = server.recorded();
    assert_eq!(
        recorded.len(),
        1,
        "resolve must make exactly one request: {recorded:?}"
    );
    let request = &recorded[0];
    assert_eq!(
        request.header("X-Curie-Approval-Principal"),
        Some(OPERATOR_PRINCIPAL),
        "the operator token must cross the real HTTP boundary in the dedicated principal header"
    );
    let body: Value = serde_json::from_slice(&request.body)
        .unwrap_or_else(|err| panic!("resolve body must be JSON: {err}"));
    assert_eq!(
        body,
        json!({"decision": "approved", "note": "approved after review"}),
        "the body is policy input only: no caller-chosen identity or channel may survive: {body}"
    );
    assert!(
        !stdout.contains(OPERATOR_PRINCIPAL) && !stderr.contains(OPERATOR_PRINCIPAL),
        "a resolve token is never output or logged; stdout: {stdout:?}, stderr: {stderr:?}"
    );
}

/// Both administrator bootstrap actions are explicit local/cluster approval
/// modes.  They are deliberately separate because an operator principal is
/// reusable for a shift, while a subject-bound login code is exchanged once by
/// the browser.  Their machine outputs are typed objects, never an unlabelled
/// blob a script could confuse with a resolution result.
#[test]
fn local_and_cluster_expose_the_same_principal_bootstrap_modes() {
    for tier in ["local", "cluster"] {
        let output = Command::new(bin())
            .args([tier, "approvals", "weather", "--help"])
            .output()
            .unwrap_or_else(|err| panic!("run {tier} approvals --help: {err}"));
        let help = text(&output);
        assert!(
            output.status.success(),
            "{tier} approvals help must render: {help}"
        );
        for mode in ["--mint-operator-principal", "--mint-console-login-code"] {
            assert!(
                help.contains(mode),
                "{tier} must expose the same administrative bootstrap mode {mode}; help:\n{help}"
            );
        }
    }
}

#[test]
fn operator_and_console_bootstrap_emit_typed_single_delivery_outputs() {
    let server = serve(
        |request| match (request.method.as_str(), request.path.as_str()) {
            ("POST", "/approvals/principals/operator") => {
                let body: Value =
                    serde_json::from_slice(&request.body).expect("operator mint body");
                assert_eq!(body, json!({"subject": OPERATOR_SUBJECT}));
                Response::json(
                    201,
                    r#"{"token":"apr.test-issued-operator-token","subject":"U0OPERATOR","expires_at":"2026-08-31T12:00:00Z"}"#,
                )
            }
            ("POST", "/console/login-codes") => {
                let body: Value = serde_json::from_slice(&request.body).expect("console mint body");
                assert_eq!(body, json!({"subject": "U0CONSOLE"}));
                Response::json(
                    201,
                    &format!(
                        r#"{{"code":"{CONSOLE_CODE}","subject":"U0CONSOLE","expires_at":"2026-08-31T12:00:00Z"}}"#
                    ),
                )
            }
            other => panic!("unexpected request: {other:?}"),
        },
    );

    let cases = [
        (
            "--mint-operator-principal",
            OPERATOR_SUBJECT,
            "operator_principal",
            "token",
            "apr.test-issued-operator-token",
        ),
        (
            "--mint-console-login-code",
            "U0CONSOLE",
            "console_login_code",
            "code",
            CONSOLE_CODE,
        ),
    ];

    for (mode, subject, output_key, secret_key, secret) in cases {
        let output = Command::new(bin())
            .args([
                "local",
                "approvals",
                "weather",
                mode,
                subject,
                "--api-url",
                &server.base_url,
                "--api-key",
                TEST_API_KEY,
                "--json",
            ])
            .env_remove("CURIE_API_URL")
            .env_remove("CURIE_API_KEY")
            .env_remove("CURIE_APPROVAL_PRINCIPAL_TOKEN")
            .output()
            .unwrap_or_else(|err| panic!("run {mode}: {err}"));
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "{mode} must succeed against its typed API response; stdout: {stdout}\nstderr: {stderr}"
        );
        let value: Value = serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|err| panic!("{mode} --json must be JSON: {err}; {stdout}"));
        assert_eq!(value[output_key]["subject"], subject, "{mode}: {value}");
        assert_eq!(value[output_key][secret_key], secret, "{mode}: {value}");
        assert_eq!(
            stdout.matches(secret).count(),
            1,
            "{mode} delivers its one-time secret exactly once in the typed stdout result: {stdout}"
        );
        assert!(
            !stderr.contains(secret),
            "{mode} must not leak its delivery secret through progress/error output: {stderr}"
        );
    }
}
