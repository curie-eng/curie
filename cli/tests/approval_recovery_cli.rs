//! The administrative approval-recovery surface at the CLI boundary (#2753).
//!
//! Two verbs map one-to-one onto the recovery routes:
//!
//! - `--report-identity` -> `GET  /approvals/identity-report`
//! - `--recover <ID>`    -> `POST /approvals/{id}/recover`
//!
//! These drive the
//! compiled binary against a wire-level stub for the same reason
//! `approval_principal.rs` does: a unit test over a constructed `ApprovalCmd`
//! cannot catch clap accepting a combination the handler forbids, nor an API
//! client quietly renaming a body field or dropping the principal header.
//!
//! The recovery grant is installation-wide and audited, so the refusals matter
//! as much as the happy path: a missing principal and a missing reason are
//! DISTINCT messages, and neither may read as a membership or permission-denied
//! failure.

mod support;

use std::process::{Command, Output};

use serde_json::{json, Value};
use support::{serve, MockServer, Request, Response};

const TEST_API_KEY: &str = "test-key";
const APPROVAL_ID: &str = "33333333-3333-3333-3333-333333333333";
const OPERATOR_PRINCIPAL: &str = "apr.test.operator-principal-that-must-not-leak";
/// Caller-supplied, and deliberately stable across invocations: the server's
/// idempotency is keyed on it, so the CLI regenerating or decorating it would
/// silently turn a retry into a second administrative act.
const RECOVERY_KEY: &str = "rk-2753-operator-chosen-0001";
const REASON: &str = "card identity unreconstructable after the 0.9.1 upgrade";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

/// The body `POST /approvals/{id}/recover` actually returns: `ApprovalRecoveryOut`
/// from `apps/api/src/curie_api/schemas.py`, mirrored in the committed
/// `apps/api/openapi.json`. Keyed `approval_id`, NOT `id`, and carrying none of
/// the conversation fields an approval record requires.
///
/// The first stub for this test returned an approval record instead, which is
/// exactly what the client was (wrongly) decoding, so the two agreed with each
/// other and disagreed with the API. Every successful recovery failed at
/// response decoding AFTER the mutation had committed.
fn recovery_outcome_json() -> String {
    format!(
        r#"{{"approval_id":"{APPROVAL_ID}","status":"rejected","recovery_key":"{RECOVERY_KEY}","reason":"{REASON}","actor":"U0OPERATOR","recovered_at":"2026-09-18T10:00:00Z"}}"#
    )
}

/// The body `GET /approvals/identity-report` returns: `ApprovalIdentityReportOut`.
/// Observations live in `facts`, a string array, and the skeleton under
/// `declarations`. `has_reply_placeholder` is an ordinary descriptive column,
/// NOT a fact, which is why a renderer that printed true-valued booleans named
/// the wrong thing.
const IDENTITY_REPORT_JSON: &str = r#"{
  "approvals": [
    {
      "id": "33333333-3333-3333-3333-333333333333",
      "agent_id": "11111111-1111-1111-1111-111111111111",
      "status": "pending",
      "route": "explicit-reviewers",
      "reply_kind": "email",
      "reply_adapter": null,
      "reply_channel": "ops@example.com",
      "card_channel": null,
      "has_reply_placeholder": true,
      "created_at": "2026-09-18T09:00:00Z",
      "facts": ["route_declared_but_unbound", "reply_identity_unreconstructable"]
    },
    {
      "id": "44444444-4444-4444-4444-444444444444",
      "agent_id": "11111111-1111-1111-1111-111111111111",
      "status": "pending",
      "route": null,
      "reply_kind": "slack",
      "reply_adapter": null,
      "reply_channel": "C0REPLY",
      "card_channel": "C0CARD",
      "has_reply_placeholder": false,
      "created_at": "2026-09-18T09:01:00Z",
      "facts": ["card_identity_missing"]
    },
    {
      "id": "55555555-5555-5555-5555-555555555555",
      "agent_id": "11111111-1111-1111-1111-111111111111",
      "status": "pending",
      "route": null,
      "reply_kind": "slack",
      "reply_adapter": null,
      "reply_channel": "C0REPLY",
      "card_channel": "C0CARD",
      "has_reply_placeholder": true,
      "created_at": "2026-09-18T09:02:00Z",
      "facts": []
    }
  ],
  "declarations": [
    {
      "approval_id": "33333333-3333-3333-3333-333333333333",
      "reply_kind": null,
      "reply_adapter": null,
      "actor": null,
      "reason": null
    }
  ]
}"#;

/// Run `curie <tier> approvals <agent> ...` with no ambient values inherited
/// from the developer's shell, so no test can pass because an operator happened
/// to have a principal exported.
fn run(tier: &str, args: &[&str], server: Option<&MockServer>, principal: Option<&str>) -> Output {
    let mut command = Command::new(bin());
    command.arg(tier).arg("approvals").arg("weather").args(args);
    if let Some(server) = server {
        command.args(["--api-url", &server.base_url, "--api-key", TEST_API_KEY]);
    }
    command
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env_remove("CURIE_APPROVAL_PRINCIPAL_TOKEN")
        .env("NO_COLOR", "1");
    if let Some(principal) = principal {
        command.env("CURIE_APPROVAL_PRINCIPAL_TOKEN", principal);
    }
    command.output().unwrap_or_else(|err| {
        panic!(
            "run curie {tier} approvals weather {}: {err}",
            args.join(" ")
        )
    })
}

/// A server that fails the test if anything reaches it. Used by every refusal
/// case: "refused at the CLI" means no HTTP was issued at all, not that the API
/// happened to reject it.
fn no_request_server() -> MockServer {
    serve(|request: &Request| {
        panic!(
            "this invocation must be refused before any HTTP call, received {} {}",
            request.method, request.path
        )
    })
}

fn body_of(request: &Request) -> Value {
    serde_json::from_slice(&request.body)
        .unwrap_or_else(|err| panic!("request body must be JSON: {err}"))
}

// --------------------------------------------------------------------------
// Flag surface
// --------------------------------------------------------------------------

/// Every tier that serves the durable approvals handler must expose both
/// verbs plus their inputs. A verb that exists only at `cluster` would make the
/// documented local rehearsal of a recovery impossible.
#[test]
fn local_and_cluster_expose_the_recovery_verbs() {
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
        for flag in [
            "--report-identity",
            "--recover",
            "--reason",
            "--recovery-key",
        ] {
            assert!(
                help.contains(flag),
                "{tier} must expose {flag}; help:\n{help}"
            );
        }
    }
}

/// Each recovery verb addresses a different object from the tool gates, the
/// route map and the pending-record verbs, so a combination is refused with a
/// usage error naming the conflict rather than one silently winning.
#[test]
fn each_recovery_verb_refuses_combination_with_every_other_approvals_mode() {
    let verbs: &[&[&str]] = &[
        &["--report-identity"],
        &[
            "--recover",
            APPROVAL_ID,
            "--reason",
            REASON,
            "--recovery-key",
            RECOVERY_KEY,
        ],
    ];
    let conflicts: &[&[&str]] = &[
        &["--list"],
        &["--resolve", "22222222-2222-2222-2222-222222222222"],
        &["--list-routes"],
        &["--gate", "Bash"],
    ];

    let server = no_request_server();
    for verb in verbs {
        for conflict in conflicts {
            let mut args: Vec<&str> = verb.to_vec();
            args.extend_from_slice(conflict);
            let output = run("local", &args, Some(&server), Some(OPERATOR_PRINCIPAL));
            let combined = text(&output);
            assert_eq!(
                output.status.code(),
                Some(2),
                "`{}` + `{}` must be a usage refusal (exit 2), not silently accepted; output:\n{combined}",
                verb.join(" "),
                conflict.join(" ")
            );
            assert!(
                combined.contains(verb[0]) && combined.contains(conflict[0]),
                "the refusal must name BOTH sides of the conflict so the operator knows what to drop; output:\n{combined}"
            );
        }
    }
}

// --------------------------------------------------------------------------
// Required inputs, refused before any HTTP call
// --------------------------------------------------------------------------

/// `reason` is operator free text that reaches the durable audit row. An empty
/// or absent one would write an audit entry that explains nothing, so it is
/// refused at the CLI rather than sent for the API to reject.
#[test]
fn recovery_verbs_refuse_a_missing_or_empty_reason_before_any_request() {
    let server = no_request_server();
    let cases: &[(&str, Vec<&str>)] = &[
        (
            "--recover without --reason",
            vec!["--recover", APPROVAL_ID, "--recovery-key", RECOVERY_KEY],
        ),
        (
            "--recover with a blank --reason",
            vec![
                "--recover",
                APPROVAL_ID,
                "--reason",
                "   ",
                "--recovery-key",
                RECOVERY_KEY,
            ],
        ),
    ];

    for (label, args) in cases {
        let output = run("local", args, Some(&server), Some(OPERATOR_PRINCIPAL));
        let combined = text(&output);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{label} must be a usage refusal; output:\n{combined}"
        );
        assert!(
            combined.contains("--reason"),
            "{label}: the refusal must name --reason; output:\n{combined}"
        );
    }
    assert!(
        server.recorded().is_empty(),
        "no recovery request may be issued for an invocation missing its reason"
    );
}

/// The recovery key is CALLER-supplied precisely so a retry is idempotent. A
/// CLI-generated one would make every retry a fresh administrative act, so an
/// absent key is refused rather than filled in.
#[test]
fn recover_refuses_a_missing_recovery_key_before_any_request() {
    let server = no_request_server();
    let output = run(
        "local",
        &["--recover", APPROVAL_ID, "--reason", REASON],
        Some(&server),
        Some(OPERATOR_PRINCIPAL),
    );
    let combined = text(&output);
    assert_eq!(
        output.status.code(),
        Some(2),
        "--recover without --recovery-key must be a usage refusal, never a generated key; output:\n{combined}"
    );
    assert!(
        combined.contains("--recovery-key"),
        "the refusal must name --recovery-key; output:\n{combined}"
    );
    assert!(
        server.recorded().is_empty(),
        "a keyless recover must not reach the API"
    );
}

/// Attribution comes from the operator principal, exactly as `--resolve`'s does.
/// The refusal prints the mint-it-first hint and must not suggest that the
/// platform API key can stand in as an identity.
#[test]
fn recovery_verbs_require_the_operator_principal_with_the_mint_hint() {
    let server = no_request_server();
    for args in [
        vec![
            "--recover",
            APPROVAL_ID,
            "--reason",
            REASON,
            "--recovery-key",
            RECOVERY_KEY,
        ],
    ] {
        let output = run("local", &args, Some(&server), None);
        let combined = text(&output);
        assert_eq!(
            output.status.code(),
            Some(2),
            "a missing principal is an input error, not a platform failure; output:\n{combined}"
        );
        assert!(
            combined.contains("CURIE_APPROVAL_PRINCIPAL_TOKEN"),
            "the refusal must name the env-backed credential; output:\n{combined}"
        );
        assert!(
            combined.contains("mint") || combined.contains("Mint"),
            "the refusal must give the operator the mint recovery; output:\n{combined}"
        );
    }
}

// --------------------------------------------------------------------------
// Request shape
// --------------------------------------------------------------------------

/// The identity report is a pure read of installation-wide FACTS. One GET, the
/// platform key, no body, and no agent lookup in front of it.
#[test]
fn report_identity_issues_one_get_to_the_identity_report_route() {
    let server =
        serve(
            |request: &Request| match (request.method.as_str(), request.path.as_str()) {
                ("GET", path) if path.starts_with("/approvals/identity-report") => {
                    Response::json(200, IDENTITY_REPORT_JSON)
                }
                other => panic!("unexpected request: {other:?}"),
            },
        );

    let output = run(
        "local",
        &["--report-identity", "--json"],
        Some(&server),
        None,
    );
    let combined = text(&output);
    assert!(
        output.status.success(),
        "the identity report is a pure read and needs no principal; output:\n{combined}"
    );

    let recorded = server.recorded();
    assert_eq!(
        recorded.len(),
        1,
        "the report must be exactly one request with no agent lookup in front of it: {recorded:?}"
    );
    assert_eq!(recorded[0].method, "GET");
    assert!(
        recorded[0].path.starts_with("/approvals/identity-report"),
        "wrong path: {}",
        recorded[0].path
    );
    assert_eq!(
        recorded[0].header("X-API-Key"),
        Some(TEST_API_KEY),
        "the report is platform-key authorized"
    );

    let rendered: Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|err| panic!("--json report must be one object: {err}; {combined}"));
    assert_eq!(
        rendered["identity_report"]["approvals"][0]["facts"],
        json!([
            "route_declared_but_unbound",
            "reply_identity_unreconstructable"
        ]),
        "the machine output must carry the reporter's facts array verbatim: {rendered}"
    );
    assert!(
        rendered["identity_report"]["declarations"].is_array(),
        "`declarations` is the artifact the operator fills in; it must survive --json: {rendered}"
    );
}

/// The ORDINARY terminal report must name the obligations. A renderer reading
/// boolean columns hid `card_identity_missing` and
/// `reply_identity_unreconstructable` entirely -- the two things the operator
/// has to recover -- and printed the unrelated `has_reply_placeholder` instead.
#[test]
fn the_human_report_names_each_row_s_facts_and_counts_the_declarations() {
    let server =
        serve(
            |request: &Request| match (request.method.as_str(), request.path.as_str()) {
                ("GET", path) if path.starts_with("/approvals/identity-report") => {
                    Response::json(200, IDENTITY_REPORT_JSON)
                }
                other => panic!("unexpected request: {other:?}"),
            },
        );

    let output = run("local", &["--report-identity"], Some(&server), None);
    let combined = text(&output);
    assert!(
        output.status.success(),
        "the human report must render; output:\n{combined}"
    );

    for fact in [
        "route_declared_but_unbound",
        "reply_identity_unreconstructable",
        "card_identity_missing",
    ] {
        assert!(
            combined.contains(fact),
            "the operator must read the observed fact {fact}; output:\n{combined}"
        );
    }
    assert!(
        !combined.contains("has_reply_placeholder"),
        "`has_reply_placeholder` is a descriptive column, not a fact; printing it names the wrong obligation; output:\n{combined}"
    );
    assert!(
        combined.contains("55555555-5555-5555-5555-555555555555"),
        "a row with no fact is reported, not dropped: the reporter excludes nothing; output:\n{combined}"
    );
    assert!(
        combined.contains("declarations carries 1 entr"),
        "the declaration count comes from `declarations`, and reading a field the API does not send always reported zero; output:\n{combined}"
    );
}

/// The recover body is exactly the three fields the route validates. Asserting
/// the whole serialized object means a rename of any one of them fails here.
#[test]
fn recover_posts_disposition_reason_and_key_with_both_credentials() {
    let server =
        serve(
            |request: &Request| match (request.method.as_str(), request.path.as_str()) {
                ("POST", path) if path == format!("/approvals/{APPROVAL_ID}/recover") => {
                    Response::json(200, &recovery_outcome_json())
                }
                other => panic!("unexpected request: {other:?}"),
            },
        );

    let output = run(
        "local",
        &[
            "--recover",
            APPROVAL_ID,
            "--reason",
            REASON,
            "--recovery-key",
            RECOVERY_KEY,
            "--json",
        ],
        Some(&server),
        Some(OPERATOR_PRINCIPAL),
    );
    let combined = text(&output);
    assert!(
        output.status.success(),
        "a well-formed recover against an enabled installation must succeed; output:\n{combined}"
    );

    let recorded = server.recorded();
    assert_eq!(recorded.len(), 1, "recover is one request: {recorded:?}");
    let request = &recorded[0];
    assert_eq!(
        request.header("X-API-Key"),
        Some(TEST_API_KEY),
        "the platform key authorizes the recovery route"
    );
    assert_eq!(
        request.header("X-Curie-Approval-Principal"),
        Some(OPERATOR_PRINCIPAL),
        "the principal carries ATTRIBUTION for the audit row, in the same header --resolve uses"
    );
    assert_eq!(
        body_of(request),
        json!({
            "disposition": "rejected",
            "reason": REASON,
            "recovery_key": RECOVERY_KEY,
        }),
        "the body is exactly the route's three validated fields"
    );

    let rendered: Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|err| panic!("--json recover must be one object: {err}; {combined}"));
    assert_eq!(
        rendered["recovered"],
        json!({
            "approval_id": APPROVAL_ID,
            "status": "rejected",
            "recovery_key": RECOVERY_KEY,
            "reason": REASON,
            "actor": "U0OPERATOR",
            "recovered_at": "2026-09-18T10:00:00Z",
        }),
        "the RECORDED outcome is the observable result, field for field as the API sent it: {rendered}"
    );
    assert!(
        !combined.contains(OPERATOR_PRINCIPAL),
        "the principal token is never echoed: {combined}"
    );
}

// --------------------------------------------------------------------------
// Failure reporting
// --------------------------------------------------------------------------

/// A 409 means another actor already disposed of the row under a different key.
/// Reporting that as a success would tell the operator their reason and their
/// attribution landed when they did not.
#[test]
fn a_409_conflict_is_reported_as_a_conflict_and_not_as_a_success() {
    let server = serve(|_request: &Request| {
        Response::json(
            409,
            r#"{"detail":"approval 33333333-3333-3333-3333-333333333333 is no longer pending; it was resolved by another actor"}"#,
        )
    });

    let output = run(
        "local",
        &[
            "--recover",
            APPROVAL_ID,
            "--reason",
            REASON,
            "--recovery-key",
            RECOVERY_KEY,
        ],
        Some(&server),
        Some(OPERATOR_PRINCIPAL),
    );
    let combined = text(&output);
    assert_eq!(
        output.status.code(),
        Some(1),
        "a conflict must be a non-zero exit; output:\n{combined}"
    );
    assert!(
        combined.contains("no longer pending"),
        "the conflict reason must reach the operator; output:\n{combined}"
    );
}

// --------------------------------------------------------------------------
// Dry run and tier availability
// --------------------------------------------------------------------------

/// The skill tier keeps no durable approval store, so both verbs are
/// DECLINED with that reason (exit 4), exactly as `--list`/`--resolve` are, and
/// never rejected as if they were an unknown-flag typo (exit 2).
#[test]
fn the_skill_tier_declines_both_verbs_with_the_durable_store_reason() {
    let cases: &[Vec<&str>] = &[
        vec!["--report-identity"],
        vec![
            "--recover",
            APPROVAL_ID,
            "--reason",
            REASON,
            "--recovery-key",
            RECOVERY_KEY,
        ],
    ];

    for args in cases {
        let output = Command::new(bin())
            .arg("skill")
            .arg("approvals")
            .args(args)
            .env_remove("CURIE_API_URL")
            .env_remove("CURIE_API_KEY")
            .env_remove("CURIE_APPROVAL_PRINCIPAL_TOKEN")
            .env("NO_COLOR", "1")
            .output()
            .unwrap_or_else(|err| panic!("run skill approvals {}: {err}", args.join(" ")));
        let combined = text(&output);
        assert_eq!(
            output.status.code(),
            Some(4),
            "`skill approvals {}` must DECLINE with a reason (exit 4), not read as an unknown flag; output:\n{combined}",
            args.join(" ")
        );
        assert!(
            combined.contains("durable"),
            "the decline must give the no-durable-store reason; output:\n{combined}"
        );
    }
}
