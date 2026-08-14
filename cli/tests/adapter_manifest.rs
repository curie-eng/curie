//! Contract gate for the `curie adapter` verb family (issue #1516, Stream B).
//!
//! Two things are proven here, and neither is provable by reading source.
//!
//! **The cross language contract.** `packages/channel-protocol` owns the adapter
//! binding profile as Pydantic models, exports
//! `schema/adapter-profile.schema.json`, and commits
//! `schema/adapter-profile.corpus.json` as the shared fixture. Python asserts its
//! half in `test_manifest_corpus.py`; this file asserts the Rust half over the
//! byte identical corpus. Every valid case must parse AND carry its parsed values
//! back out through `curie adapter validate --json`, and the valid cases
//! deliberately differ in `kind`, `endpoint` presence, `address.pattern`,
//! `credentials.egress` and `conformance.mints_reply_ref`, so an implementation
//! that returns a constant instead of the parsed value dies on the comparison
//! rather than tracking a self updating expectation.
//!
//! The corpus tags each invalid case with the mechanism that enforces it:
//!
//! - **tier 1 and 2** are enforced by the exported JSON Schema, so this file
//!   asserts the committed schema itself rejects them.
//! - **tier 3** is an admitted PAIRED validator (one in Python, one in Rust),
//!   because JSON Schema cannot express "the Rust regex crate can compile this
//!   string". The assertion is inverted: the schema must ACCEPT a tier 3 case and
//!   the Rust verb must still reject it. That inversion is what stops a later
//!   reader assuming the rule exports.
//!
//! **The credential selection boundary.** A binding profile is a third party
//! file. The worker indexes its GLOBAL credential map by the slug the route
//! carries (`apps/worker/src/curie_worker/reply_sink.py::HttpReplyAdapter::_secret_for`),
//! so a profile controlled slug reaching the write path would let one adapter's
//! file select another adapter's secret and have the platform POST it to the host
//! that same file names. The profile's `credentials.egress` is therefore a non
//! binding SUGGESTION, `--adapter-slug` is a required operator input on `bind`,
//! and the same reasoning makes `--address` required on the three live verbs and
//! the `smoke-test` egress secret operator supplied at the invocation boundary.
//! The tests below drive the real binary against a loopback recorder and assert
//! on the bytes that reach the wire, so a fallback to the profile's value is
//! caught as a changed request body rather than a changed comment.
//!
//! Everything here is hermetic: no Curie install, no cluster, no network beyond
//! loopback, and every port is ephemeral.

mod support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use support::{serve, MockServer, Request, Response};

// ─── Fixtures ────────────────────────────────────────────────────────────────

/// The committed profile schema, the same bytes `cli/src/adapter.rs` embeds.
const PROFILE_SCHEMA: &str =
    include_str!("../../packages/channel-protocol/schema/adapter-profile.schema.json");

/// The committed cross language corpus, the same bytes
/// `packages/channel-protocol/tests/test_manifest_corpus.py` reads.
const PROFILE_CORPUS: &str =
    include_str!("../../packages/channel-protocol/schema/adapter-profile.corpus.json");

fn corpus() -> serde_json::Value {
    serde_json::from_str(PROFILE_CORPUS).expect("adapter-profile.corpus.json is valid JSON")
}

fn valid_cases() -> Vec<serde_json::Value> {
    corpus()["valid"]
        .as_array()
        .expect("corpus carries a valid array")
        .clone()
}

/// Every invalid case as `(why, tier, profile)`.
fn invalid_cases() -> Vec<(String, u64, serde_json::Value)> {
    corpus()["invalid"]
        .as_array()
        .expect("corpus carries an invalid array")
        .iter()
        .map(|case| {
            (
                case["why"].as_str().expect("case carries why").to_string(),
                case["tier"].as_u64().expect("case carries a tier"),
                case["profile"].clone(),
            )
        })
        .collect()
}

/// The exported schema compiled with its root pointed at `AdapterProfile`, the
/// same entry point `cli/src/adapter.rs` uses. This is the tier 1 and 2
/// authority; it is deliberately NOT the tier 3 authority.
fn profile_validator() -> jsonschema::Validator {
    let mut doc: serde_json::Value =
        serde_json::from_str(PROFILE_SCHEMA).expect("adapter-profile.schema.json is valid JSON");
    doc["$ref"] = serde_json::Value::String("#/$defs/AdapterProfile".to_string());
    jsonschema::validator_for(&doc).expect("the AdapterProfile def compiles to a validator")
}

/// The corpus case with no `endpoint` at all. `endpoint` is OPTIONAL in the
/// schema and REQUIRED by the verbs that build a request from it, which is the
/// asymmetry several tests below turn on.
fn endpointless_valid_case() -> serde_json::Value {
    valid_cases()
        .into_iter()
        .find(|case| case.get("endpoint").is_none() || case["endpoint"].is_null())
        .expect("the corpus carries a valid case with no endpoint")
}

/// A valid case that DOES carry an endpoint, for the tests that need a route.
fn routed_valid_case() -> serde_json::Value {
    valid_cases()
        .into_iter()
        .find(|case| case.get("endpoint").is_some_and(|e| e.is_string()))
        .expect("the corpus carries a valid case with an endpoint")
}

// ─── Process harness ─────────────────────────────────────────────────────────

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

/// A profile written where the verbs read it. JSON is a subset of YAML, so the
/// corpus values land byte faithfully with no re-encoding step of our own to get
/// wrong.
fn write_profile(dir: &Path, profile: &serde_json::Value) -> PathBuf {
    let path = dir.join("adapter.yaml");
    std::fs::write(
        &path,
        serde_json::to_string_pretty(profile).expect("profile serializes"),
    )
    .expect("write adapter.yaml");
    path
}

/// Run `curie <args>` with a closed stdin, so a command that reaches an
/// interactive confirmation fails immediately instead of hanging the suite.
fn run(args: &[&str]) -> Output {
    run_with_env(args, &[])
}

fn run_with_env(args: &[&str], env: &[(&str, &str)]) -> Output {
    let mut cmd = Command::new(bin());
    cmd.args(args).stdin(Stdio::null());
    for (key, value) in env {
        cmd.env(key, value);
    }
    cmd.output().expect("run curie")
}

fn text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn code(output: &Output) -> i32 {
    output.status.code().expect("curie exited with a code")
}

fn stdout_json(output: &Output) -> serde_json::Value {
    let raw = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(raw.trim()).unwrap_or_else(|e| {
        panic!(
            "--json stdout must be one JSON object: {e}\nstdout: {raw}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

/// The `--help` long flags for one verb, so a refusal test cannot pass merely
/// because clap rejected a flag the verb never declared.
fn help_flags(args: &[&str]) -> std::collections::BTreeSet<String> {
    let output = run(&[args, &["--help"]].concat());
    assert_eq!(
        code(&output),
        0,
        "expected `curie {} --help` to route\n{}",
        args.join(" "),
        text(&output)
    );
    text(&output)
        .split_whitespace()
        .filter(|token| token.starts_with("--") && token.len() > 2)
        .map(|token| token.trim_end_matches([',', '=', '<']).to_string())
        .collect()
}

/// Precondition for every refusal test below. Exit 2 is also what clap returns
/// for a subcommand it has never heard of, so an exit-2 assertion proves nothing
/// until the verb itself routes. Asserting that first is what stops a refusal
/// test greening for the wrong reason, before the verb exists and after someone
/// later deletes it.
fn assert_verb_routes(verb: &str) {
    let output = run(&["adapter", verb, "--help"]);
    assert_eq!(
        code(&output),
        0,
        "`curie adapter {verb}` must route before any of its refusals mean \
         anything\n{}",
        text(&output)
    );
}

// ─── Loopback recorders ──────────────────────────────────────────────────────

/// A platform API stand-in on an ephemeral port. It answers the two routes the
/// live verbs touch and records every request, so the tests assert on the bytes
/// the CLI actually put on the wire.
fn api_recorder() -> MockServer {
    serve(|req: &Request| {
        if req.path.starts_with("/channels/token") {
            Response::json(200, r#"{"token":"chn_test_token_value"}"#)
        } else {
            Response::json(
                200,
                r#"{"id":"demo","name":"demo","channel":{"kind":"email","address":"agent@example.test"}}"#,
            )
        }
    })
}

/// An adapter egress endpoint stand-in on an ephemeral port.
fn endpoint_recorder() -> MockServer {
    serve(|_req: &Request| Response::json(200, r#"{"ok":true}"#))
}

fn bodies(server: &MockServer) -> Vec<String> {
    server
        .recorded()
        .iter()
        .map(|req| String::from_utf8_lossy(&req.body).into_owned())
        .collect()
}

/// The JSON body of the first request whose method and path prefix match.
fn recorded_body(server: &MockServer, method: &str, path_prefix: &str) -> serde_json::Value {
    let recorded = server.recorded();
    let req = recorded
        .iter()
        .find(|req| req.method == method && req.path.starts_with(path_prefix))
        .unwrap_or_else(|| {
            panic!(
                "no {method} {path_prefix} request was recorded; saw {:?}",
                recorded
                    .iter()
                    .map(|r| format!("{} {}", r.method, r.path))
                    .collect::<Vec<_>>()
            )
        });
    serde_json::from_slice(&req.body).unwrap_or_else(|e| {
        panic!(
            "{method} {path_prefix} body must be JSON: {e}\n{}",
            String::from_utf8_lossy(&req.body)
        )
    })
}

fn tempdir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

// ─── The corpus is the drift gate between the two validators ─────────────────

/// Every valid corpus profile parses in Rust AND its parsed values come back out.
///
/// The comparison is against the corpus value, never against a value this test
/// computed, so the only way to green it is to carry the parsed field through.
/// The corpus cases differ from each other on all five compared fields, which is
/// what kills an implementation that hardcodes or drops one: a constant cannot
/// satisfy `email` and `ms_teams` and `webhook` at once.
///
/// Mutation to run against the implementation: replace the parsed
/// `credentials.egress` with a literal, or drop `endpoint` from the payload.
/// Both must red here.
#[test]
fn rust_accepts_every_valid_corpus_profile() {
    let cases = valid_cases();
    assert!(
        cases.len() >= 2,
        "value parity needs at least two differing valid cases"
    );

    // Prove the fixture itself can kill a constant before trusting it to.
    let mut seen: BTreeMap<&str, std::collections::BTreeSet<String>> = BTreeMap::new();
    for case in &cases {
        seen.entry("kind")
            .or_default()
            .insert(case["kind"].to_string());
        seen.entry("egress")
            .or_default()
            .insert(case["credentials"]["egress"].to_string());
        seen.entry("pattern")
            .or_default()
            .insert(case["address"]["pattern"].to_string());
        seen.entry("endpoint").or_default().insert(
            case.get("endpoint")
                .cloned()
                .unwrap_or_default()
                .to_string(),
        );
        seen.entry("mints_reply_ref")
            .or_default()
            .insert(case["conformance"]["mints_reply_ref"].to_string());
    }
    for (field, values) in &seen {
        assert!(
            values.len() >= 2,
            "the corpus must carry at least two DIFFERENT {field} values, or a \
             constant returning implementation survives; saw {values:?}"
        );
    }

    for case in &cases {
        let dir = tempdir();
        let path = write_profile(dir.path(), case);
        let output = run(&[
            "adapter",
            "validate",
            "-f",
            path.to_str().unwrap(),
            "--json",
        ]);
        assert_eq!(
            code(&output),
            0,
            "a valid corpus profile must validate: {case}\n{}",
            text(&output)
        );

        let payload = stdout_json(&output);
        assert_eq!(payload["ok"], serde_json::json!(true), "payload: {payload}");

        let parsed = &payload["profile"];
        assert_eq!(parsed["version"], case["version"], "payload: {payload}");
        assert_eq!(parsed["kind"], case["kind"], "payload: {payload}");
        assert_eq!(
            parsed["address"]["pattern"], case["address"]["pattern"],
            "payload: {payload}"
        );
        assert_eq!(
            parsed["address"]["example"], case["address"]["example"],
            "payload: {payload}"
        );
        assert_eq!(
            parsed["credentials"]["egress"], case["credentials"]["egress"],
            "payload: {payload}"
        );
        assert_eq!(
            parsed["credentials"]["egress_secret_env"], case["credentials"]["egress_secret_env"],
            "payload: {payload}"
        );
        assert_eq!(
            parsed["conformance"]["mints_reply_ref"], case["conformance"]["mints_reply_ref"],
            "payload: {payload}"
        );
        // Absent in the file must read back as JSON null, never a missing key,
        // so a consumer can branch on it unconditionally.
        let expected_endpoint = case
            .get("endpoint")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        assert_eq!(parsed["endpoint"], expected_endpoint, "payload: {payload}");
    }
}

/// Every invalid corpus case is refused by the verb, whatever its tier. This is
/// the union assertion; the two tier specific tests below prove WHICH mechanism
/// caught each one.
#[test]
fn rust_rejects_every_invalid_corpus_profile() {
    assert_verb_routes("validate");
    for (why, tier, profile) in invalid_cases() {
        let dir = tempdir();
        let path = write_profile(dir.path(), &profile);
        let output = run(&["adapter", "validate", "-f", path.to_str().unwrap()]);
        assert_eq!(
            code(&output),
            2,
            "tier {tier} case must exit 2 (usage): {why}\n{}",
            text(&output)
        );
    }
}

/// Tiers 1 and 2 are the exported schema's job, so the committed schema must
/// reject them on its own. If a case tagged tier 1 slips through the schema, the
/// tag is a lie and the Python side is enforcing something Rust only happens to
/// mirror.
#[test]
fn the_committed_schema_rejects_every_tier_1_and_2_invalid_case() {
    let validator = profile_validator();
    for (why, tier, profile) in invalid_cases() {
        if tier > 2 {
            continue;
        }
        assert!(
            !validator.is_valid(&profile),
            "tier {tier} claims the exported schema catches this, but it validated: {why}"
        );
    }
}

/// Tier 3 is the admitted paired validator, and this is the INVERTED assertion
/// that keeps the admission honest: the schema must ACCEPT the case (JSON Schema
/// cannot express "the Rust regex crate compiles this") while the Rust verb still
/// refuses it. A tier 3 case the schema unexpectedly catches is as wrong as a
/// tier 1 case it misses, because it would mean the pairing was never needed.
///
/// Mutation to run against the corpus: retag one tier 3 case as tier 1. The test
/// above must then red.
#[test]
fn rust_rejects_every_tier_3_invalid_case_in_code() {
    assert_verb_routes("validate");
    let validator = profile_validator();
    let mut tier_3 = 0;
    for (why, tier, profile) in invalid_cases() {
        if tier != 3 {
            continue;
        }
        tier_3 += 1;
        assert!(
            validator.is_valid(&profile),
            "a tier 3 case must be one the exported schema ACCEPTS, or it is not \
             a paired validator at all: {why}"
        );

        let dir = tempdir();
        let path = write_profile(dir.path(), &profile);
        let output = run(&["adapter", "validate", "-f", path.to_str().unwrap()]);
        assert_eq!(
            code(&output),
            2,
            "the Rust side must carry its half of the paired validator: {why}\n{}",
            text(&output)
        );
    }
    assert!(
        tier_3 >= 3,
        "the corpus must carry a tier 3 case per Rust unsupported construct \
         (lookahead, lookbehind, backreference); found {tier_3}"
    );
}

/// The floor the tier 3 rule exists to guarantee: every pattern the corpus calls
/// valid actually compiles with the crate `curie adapter` matches addresses with.
#[test]
fn rust_regex_compiles_every_valid_corpus_pattern() {
    for case in valid_cases() {
        let pattern = case["address"]["pattern"]
            .as_str()
            .expect("address.pattern is a string");
        regex::Regex::new(pattern).unwrap_or_else(|e| {
            panic!("valid corpus pattern must compile in Rust: {pattern}: {e}")
        });
    }
}

// ─── The version check runs first ────────────────────────────────────────────

/// A profile written against a version this build does not understand is refused
/// with BOTH versions named, so the operator learns what to do rather than
/// reading a field level schema error about a shape that was never theirs.
#[test]
fn version_mismatch_is_a_usage_error_naming_both() {
    assert_verb_routes("validate");
    let mut profile = routed_valid_case();
    profile["version"] = serde_json::json!("1.1");

    let dir = tempdir();
    let path = write_profile(dir.path(), &profile);
    let output = run(&["adapter", "validate", "-f", path.to_str().unwrap()]);

    assert_eq!(code(&output), 2, "{}", text(&output));
    let message = text(&output);
    assert!(
        message.contains("1.1"),
        "the message must name the file's version\n{message}"
    );
    assert!(
        message.contains("1.0"),
        "the message must name the version this build understands\n{message}"
    );
}

/// The ordering assertion, which is the only thing the test above cannot prove:
/// a file that is BOTH a wrong version and structurally invalid must report the
/// version, because the schema for a version we do not speak has no authority
/// over that file.
///
/// Mutation to run against the implementation: move the version check after
/// schema validation. This must red while the test above stays green.
#[test]
fn version_is_checked_before_schema_validation() {
    assert_verb_routes("validate");
    let mut profile = routed_valid_case();
    profile["version"] = serde_json::json!("1.1");
    profile["retries"] = serde_json::json!(3);

    let dir = tempdir();
    let path = write_profile(dir.path(), &profile);
    let output = run(&["adapter", "validate", "-f", path.to_str().unwrap()]);

    assert_eq!(code(&output), 2, "{}", text(&output));
    let message = text(&output);
    assert!(
        message.contains("1.1"),
        "the version mismatch must be what is reported\n{message}"
    );
    assert!(
        !message.contains("retries"),
        "the unknown key must NOT be reported: a schema for a version we do not \
         speak has no authority over this file\n{message}"
    );
}

// ─── `--address` is an operator input, never `address.example` ───────────────

/// `address.example` is authoring documentation. Every live verb must declare
/// `--address` so no code path can reach for the profile's example instead, and
/// the two flags that make a route operator owned must exist on `bind`.
#[test]
fn the_live_verbs_declare_the_operator_owned_flags() {
    for verb in ["bind", "token", "smoke-test"] {
        let flags = help_flags(&["adapter", verb]);
        assert!(
            flags.contains("--address"),
            "`adapter {verb}` must declare --address; saw {flags:?}"
        );
    }

    let bind = help_flags(&["adapter", "bind"]);
    for flag in ["--adapter-slug", "--endpoint", "--yes"] {
        assert!(
            bind.contains(flag),
            "`adapter bind` must declare {flag}; saw {bind:?}"
        );
    }
}

/// Omitting `--address` is a usage error on every verb that resolves against a
/// concrete `(kind, address)` pair, and nothing reaches the API first.
#[test]
fn the_live_verbs_require_an_explicit_address() {
    for verb in ["bind", "token", "smoke-test"] {
        assert_verb_routes(verb);
    }
    let api = api_recorder();
    let profile = routed_valid_case();
    let dir = tempdir();
    let path = write_profile(dir.path(), &profile);
    let file = path.to_str().unwrap();

    let invocations: Vec<Vec<String>> = vec![
        vec![
            "adapter".into(),
            "bind".into(),
            "-f".into(),
            file.into(),
            "demo".into(),
            "--adapter-slug".into(),
            "operator-owned".into(),
            "--yes".into(),
            "--api-url".into(),
            api.base_url.clone(),
        ],
        vec![
            "adapter".into(),
            "token".into(),
            "-f".into(),
            file.into(),
            "--api-url".into(),
            api.base_url.clone(),
        ],
        vec![
            "adapter".into(),
            "smoke-test".into(),
            "-f".into(),
            file.into(),
            "--yes".into(),
            "--api-url".into(),
            api.base_url.clone(),
        ],
    ];

    for argv in invocations {
        let borrowed: Vec<&str> = argv.iter().map(String::as_str).collect();
        let output = run(&borrowed);
        assert_eq!(
            code(&output),
            2,
            "{:?} must be a usage error without --address\n{}",
            argv,
            text(&output)
        );
    }

    assert!(
        api.recorded().is_empty(),
        "no request may leave before an address is supplied; saw {:?}",
        bodies(&api)
    );
    let example = profile["address"]["example"].as_str().unwrap();
    for body in bodies(&api) {
        assert!(
            !body.contains(example),
            "the profile's address.example must never become live state: {body}"
        );
    }
}

/// A supplied address is checked against the profile's own pattern before any
/// request, so the operator reads a usage error instead of discovering the
/// mismatch as a 404 from a live ingress.
#[test]
fn address_mismatch_is_a_usage_error() {
    assert_verb_routes("validate");
    let profile = routed_valid_case();
    let dir = tempdir();
    let path = write_profile(dir.path(), &profile);

    let output = run(&[
        "adapter",
        "validate",
        "-f",
        path.to_str().unwrap(),
        "--address",
        "definitely not this shape",
    ]);
    assert_eq!(code(&output), 2, "{}", text(&output));
    let message = text(&output);
    assert!(
        message.contains("definitely not this shape"),
        "the message must name the address\n{message}"
    );
    assert!(
        message.contains(profile["address"]["pattern"].as_str().unwrap()),
        "the message must name the pattern\n{message}"
    );
}

/// The false positive control for the test above: an address of the declared
/// shape passes, so the check is a real match and not a blanket refusal.
#[test]
fn an_address_matching_the_pattern_validates() {
    let profile = routed_valid_case();
    let dir = tempdir();
    let path = write_profile(dir.path(), &profile);

    let output = run(&[
        "adapter",
        "validate",
        "-f",
        path.to_str().unwrap(),
        "--address",
        profile["address"]["example"].as_str().unwrap(),
        "--json",
    ]);
    assert_eq!(code(&output), 0, "{}", text(&output));
    assert_eq!(stdout_json(&output)["ok"], serde_json::json!(true));
}

// ─── The credential selection boundary ───────────────────────────────────────

/// A profile whose `credentials.egress` names ANOTHER adapter's slug must not be
/// able to put that slug on the wire. The operator supplies `--adapter-slug` and
/// that value, and only that value, becomes the route's `adapter` field, because
/// the worker uses it to index a map holding every adapter's secret.
///
/// Mutation to run against the implementation: fall back to
/// `profile.credentials.egress` when the flag is absent, or prefer the profile's
/// value when the two differ. This must red.
#[test]
fn a_profile_slug_alone_never_reaches_the_write_payload() {
    let api = api_recorder();
    let mut profile = routed_valid_case();
    profile["credentials"]["egress"] = serde_json::json!("victim-adapter");

    let dir = tempdir();
    let path = write_profile(dir.path(), &profile);
    let address = profile["address"]["example"].as_str().unwrap().to_string();

    let output = run(&[
        "adapter",
        "bind",
        "-f",
        path.to_str().unwrap(),
        "demo",
        "--address",
        &address,
        "--adapter-slug",
        "operator-owned",
        "--yes",
        "--api-url",
        &api.base_url,
    ]);

    let body = recorded_body(&api, "PATCH", "/agents/");
    assert_eq!(
        body["channel"]["adapter"],
        serde_json::json!("operator-owned"),
        "the written slug must be the operator's, exit {}\n{}",
        code(&output),
        text(&output)
    );
    for raw in bodies(&api) {
        assert!(
            !raw.contains("victim-adapter"),
            "the profile's suggested slug must never reach the API: {raw}"
        );
    }
}

/// There is no invocation in which the profile's value is the only source, so
/// the fallback the test above forbids has nowhere to hide.
#[test]
fn bind_requires_adapter_slug() {
    assert_verb_routes("bind");
    let api = api_recorder();
    let profile = routed_valid_case();
    let dir = tempdir();
    let path = write_profile(dir.path(), &profile);
    let address = profile["address"]["example"].as_str().unwrap().to_string();

    let output = run(&[
        "adapter",
        "bind",
        "-f",
        path.to_str().unwrap(),
        "demo",
        "--address",
        &address,
        "--yes",
        "--api-url",
        &api.base_url,
    ]);

    assert_eq!(code(&output), 2, "{}", text(&output));
    assert!(
        api.recorded().is_empty(),
        "nothing may be written without an operator supplied slug; saw {:?}",
        bodies(&api)
    );
}

/// When the operator's slug and the profile's suggestion differ, the operator
/// sees both values and has to confirm. Confirming a value the profile chose is
/// not operator owned credential mapping, so the confirmation is what makes the
/// suggestion safe to keep in the file at all.
#[test]
fn mismatched_slug_requires_confirmation() {
    assert_verb_routes("bind");
    let api = api_recorder();
    let mut profile = routed_valid_case();
    profile["credentials"]["egress"] = serde_json::json!("victim-adapter");

    let dir = tempdir();
    let path = write_profile(dir.path(), &profile);
    let address = profile["address"]["example"].as_str().unwrap().to_string();

    let output = run(&[
        "adapter",
        "bind",
        "-f",
        path.to_str().unwrap(),
        "demo",
        "--address",
        &address,
        "--adapter-slug",
        "operator-owned",
        "--api-url",
        &api.base_url,
    ]);

    assert_ne!(
        code(&output),
        0,
        "a slug mismatch must not proceed unconfirmed\n{}",
        text(&output)
    );
    let message = text(&output);
    assert!(
        message.contains("operator-owned") && message.contains("victim-adapter"),
        "both values must be shown so the operator can tell them apart\n{message}"
    );
    assert!(
        api.recorded().is_empty(),
        "nothing may be written before the mismatch is confirmed; saw {:?}",
        bodies(&api)
    );
}

/// The API refuses a non `slack` binding whose reply route is half set, so `bind`
/// writes all four fields or the operator gets a token mint 409 later instead.
#[test]
fn bind_writes_all_four_route_fields() {
    let api = api_recorder();
    let profile = routed_valid_case();
    let dir = tempdir();
    let path = write_profile(dir.path(), &profile);
    let address = profile["address"]["example"].as_str().unwrap().to_string();

    run(&[
        "adapter",
        "bind",
        "-f",
        path.to_str().unwrap(),
        "demo",
        "--address",
        &address,
        "--adapter-slug",
        "operator-owned",
        "--yes",
        "--api-url",
        &api.base_url,
    ]);

    let body = recorded_body(&api, "PATCH", "/agents/");
    let channel = &body["channel"];
    assert_eq!(channel["kind"], profile["kind"], "body: {body}");
    assert_eq!(
        channel["address"],
        serde_json::json!(address),
        "body: {body}"
    );
    assert_eq!(channel["endpoint"], profile["endpoint"], "body: {body}");
    assert_eq!(
        channel["adapter"],
        serde_json::json!("operator-owned"),
        "body: {body}"
    );
}

// ─── `endpoint` is schema optional and verb required ─────────────────────────

/// A profile with no `endpoint` is a legal profile: an author publishes the
/// address shape and credential identities long before any one install has a
/// route. Both the committed schema and the verb that only reads must accept it.
#[test]
fn an_endpointless_profile_is_schema_valid_and_validates() {
    let profile = endpointless_valid_case();
    assert!(
        profile_validator().is_valid(&profile),
        "endpoint must be OPTIONAL in the committed schema: {profile}"
    );

    let dir = tempdir();
    let path = write_profile(dir.path(), &profile);
    let output = run(&[
        "adapter",
        "validate",
        "-f",
        path.to_str().unwrap(),
        "--json",
    ]);
    assert_eq!(code(&output), 0, "{}", text(&output));

    let payload = stdout_json(&output);
    assert_eq!(payload["ok"], serde_json::json!(true), "{payload}");
    assert!(
        payload["profile"]["endpoint"].is_null(),
        "an absent endpoint reads back as null, never a missing key: {payload}"
    );
}

/// The other half of that asymmetry: a verb that must build a request against a
/// route fails as a usage error naming the missing field and the flag that
/// supplies it, never as a confusing HTTP error against an empty string.
#[test]
fn missing_endpoint_is_a_usage_error_on_the_verbs_that_need_one() {
    for verb in ["bind", "smoke-test"] {
        assert_verb_routes(verb);
    }
    let api = api_recorder();
    let profile = endpointless_valid_case();
    let dir = tempdir();
    let path = write_profile(dir.path(), &profile);
    let file = path.to_str().unwrap().to_string();
    let address = profile["address"]["example"].as_str().unwrap().to_string();

    let invocations: Vec<Vec<String>> = vec![
        vec![
            "adapter".into(),
            "bind".into(),
            "-f".into(),
            file.clone(),
            "demo".into(),
            "--address".into(),
            address.clone(),
            "--adapter-slug".into(),
            "operator-owned".into(),
            "--yes".into(),
            "--api-url".into(),
            api.base_url.clone(),
        ],
        vec![
            "adapter".into(),
            "smoke-test".into(),
            "-f".into(),
            file.clone(),
            "--address".into(),
            address.clone(),
            "--yes".into(),
            "--api-url".into(),
            api.base_url.clone(),
        ],
    ];

    for argv in invocations {
        let borrowed: Vec<&str> = argv.iter().map(String::as_str).collect();
        let output = run(&borrowed);
        assert_eq!(
            code(&output),
            2,
            "{:?} must be a usage error with no route\n{}",
            argv,
            text(&output)
        );
        let message = text(&output);
        assert!(
            message.contains("endpoint"),
            "the message must name the missing field\n{message}"
        );
        assert!(
            message.contains("--endpoint"),
            "the message must name the flag that supplies it\n{message}"
        );
    }

    assert!(
        api.recorded().is_empty(),
        "no request may be built against a route that does not exist; saw {:?}",
        bodies(&api)
    );
}

/// The false positive control: the named flag really does supply the route, so
/// the refusal above is a missing input and not a rejection of the profile.
#[test]
fn an_explicit_endpoint_override_supplies_the_missing_route_on_bind() {
    let api = api_recorder();
    let profile = endpointless_valid_case();
    let dir = tempdir();
    let path = write_profile(dir.path(), &profile);
    let address = profile["address"]["example"].as_str().unwrap().to_string();

    run(&[
        "adapter",
        "bind",
        "-f",
        path.to_str().unwrap(),
        "demo",
        "--address",
        &address,
        "--adapter-slug",
        "operator-owned",
        "--endpoint",
        "https://override.example.test/curie/reply",
        "--yes",
        "--api-url",
        &api.base_url,
    ]);

    let body = recorded_body(&api, "PATCH", "/agents/");
    assert_eq!(
        body["channel"]["endpoint"],
        serde_json::json!("https://override.example.test/curie/reply"),
        "body: {body}"
    );
}

// ─── The egress secret is operator supplied at the invocation boundary ───────

/// `credentials.egress_secret_env` documents what the ADAPTER reads. It is never
/// an instruction to this CLI, because a profile that could name a variable could
/// name `AWS_SECRET_ACCESS_KEY` and have the platform POST it to the host that
/// same file names. The only accepted sources are a file and stdin, and there is
/// no flag through which a name could be passed either.
#[test]
fn smoke_test_takes_its_secret_only_from_an_operator_supplied_source() {
    let flags = help_flags(&["adapter", "smoke-test"]);
    for flag in ["--secret-file", "--secret-stdin", "--yes"] {
        assert!(
            flags.contains(flag),
            "`adapter smoke-test` must declare {flag}; saw {flags:?}"
        );
    }
    for forbidden in ["--secret-env", "--secret", "--secret-name"] {
        assert!(
            !flags.contains(forbidden),
            "`adapter smoke-test` must expose no flag naming an environment \
             variable or carrying a secret value: {forbidden}; saw {flags:?}"
        );
    }
}

/// The boundary itself, asserted behaviorally: with the variable the profile
/// names exported and holding a sentinel, and no explicit source given, the
/// command refuses and the sentinel reaches neither the endpoint, the API, nor
/// the operator's terminal.
#[test]
fn smoke_test_never_reads_the_variable_the_profile_names() {
    assert_verb_routes("smoke-test");
    let api = api_recorder();
    let endpoint = endpoint_recorder();
    let sentinel = "SENTINEL-PROFILE-NAMED-VALUE-1516";

    let mut profile = routed_valid_case();
    profile["endpoint"] = serde_json::json!(endpoint.base_url);
    let named_variable = profile["credentials"]["egress_secret_env"]
        .as_str()
        .unwrap()
        .to_string();

    let dir = tempdir();
    let path = write_profile(dir.path(), &profile);
    let address = profile["address"]["example"].as_str().unwrap().to_string();

    let output = run_with_env(
        &[
            "adapter",
            "smoke-test",
            "-f",
            path.to_str().unwrap(),
            "--address",
            &address,
            "--allow-insecure",
            "--yes",
            "--api-url",
            &api.base_url,
        ],
        &[(named_variable.as_str(), sentinel)],
    );

    assert_eq!(
        code(&output),
        2,
        "an explicit secret source is required\n{}",
        text(&output)
    );
    for raw in bodies(&endpoint).into_iter().chain(bodies(&api)) {
        assert!(
            !raw.contains(sentinel),
            "a profile named variable must never be resolved and sent: {raw}"
        );
    }
    assert!(
        !text(&output).contains(sentinel),
        "a profile named variable must never be read back to the operator"
    );
}

// ─── `token` is the single deliberate secret carrying payload ────────────────

/// `token` returns a bearer credential, so its payload carries the value under an
/// explicit `token` field plus `"secret": true`. The value goes to stdout and
/// nowhere else, so a caller can pipe stdout while keeping stderr in a log.
#[test]
fn token_writes_the_token_to_stdout_and_to_no_diagnostic_stream() {
    let api = api_recorder();
    let profile = routed_valid_case();
    let dir = tempdir();
    let path = write_profile(dir.path(), &profile);
    let address = profile["address"]["example"].as_str().unwrap().to_string();

    let output = run(&[
        "adapter",
        "token",
        "-f",
        path.to_str().unwrap(),
        "--address",
        &address,
        "--api-url",
        &api.base_url,
        "--json",
    ]);
    assert_eq!(code(&output), 0, "{}", text(&output));

    let payload = stdout_json(&output);
    assert_eq!(
        payload["token"],
        serde_json::json!("chn_test_token_value"),
        "payload: {payload}"
    );
    assert_eq!(
        payload["secret"],
        serde_json::json!(true),
        "the one payload that carries a secret must say so: {payload}"
    );
    assert!(
        !String::from_utf8_lossy(&output.stderr).contains("chn_test_token_value"),
        "the token must not reach stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // The API resolves on the PAIR, never the address alone, so both halves go up.
    let body = recorded_body(&api, "POST", "/channels/token");
    assert_eq!(body["kind"], profile["kind"], "body: {body}");
    assert_eq!(body["address"], serde_json::json!(address), "body: {body}");
}

/// `ChannelTokenRequest.ttl_s` is `gt=0, le=604800`. A value the API refuses is a
/// usage error here, so the operator reads a fix hint rather than a 422.
#[test]
fn token_refuses_a_ttl_the_api_would_reject() {
    assert_verb_routes("token");
    let api = api_recorder();
    let profile = routed_valid_case();
    let dir = tempdir();
    let path = write_profile(dir.path(), &profile);
    let address = profile["address"]["example"].as_str().unwrap().to_string();

    for ttl in ["0", "604801"] {
        let output = run(&[
            "adapter",
            "token",
            "-f",
            path.to_str().unwrap(),
            "--address",
            &address,
            "--ttl-s",
            ttl,
            "--api-url",
            &api.base_url,
        ]);
        assert_eq!(
            code(&output),
            2,
            "--ttl-s {ttl} is outside the API's bounds\n{}",
            text(&output)
        );
    }
    assert!(
        api.recorded().is_empty(),
        "an out of bounds ttl must never reach the API; saw {:?}",
        bodies(&api)
    );
}

// ─── `scaffold` stays minimal ────────────────────────────────────────────────

/// `scaffold` writes one profile and prints next steps. No project tree, no
/// README: the file it writes has to be one the validator accepts, and the
/// address it was given has to match the pattern it generated, or the first
/// thing an author does after scaffolding is fix the scaffold's own output.
#[test]
fn scaffold_writes_exactly_one_file_the_validator_accepts() {
    let dir = tempdir();
    let output = run(&[
        "adapter",
        "scaffold",
        "demo",
        "--kind",
        "email",
        "--address",
        "agent@example.test",
        "--endpoint",
        "https://h.example.test/curie",
        "--adapter",
        "demo-egress",
        "--dir",
        dir.path().to_str().unwrap(),
    ]);
    assert_eq!(code(&output), 0, "{}", text(&output));

    let mut written = Vec::new();
    let mut stack = vec![dir.path().to_path_buf()];
    while let Some(next) = stack.pop() {
        for entry in std::fs::read_dir(&next).expect("read scaffold dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                stack.push(path);
            } else {
                written.push(
                    path.strip_prefix(dir.path())
                        .expect("under the scaffold dir")
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    written.sort();
    assert_eq!(
        written,
        vec!["demo/adapter.yaml".to_string()],
        "scaffold writes the profile and nothing else"
    );

    let profile = dir.path().join("demo/adapter.yaml");
    let validated = run(&[
        "adapter",
        "validate",
        "-f",
        profile.to_str().unwrap(),
        "--address",
        "agent@example.test",
        "--json",
    ]);
    assert_eq!(
        code(&validated),
        0,
        "the scaffolded profile must accept the address it was scaffolded for\n{}",
        text(&validated)
    );
    assert_eq!(stdout_json(&validated)["ok"], serde_json::json!(true));
}
