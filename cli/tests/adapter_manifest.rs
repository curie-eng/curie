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
//! `credentials.egress` and `credentials.egress_secret_env`, so an
//! implementation that returns a constant instead of the parsed value dies on
//! the comparison rather than tracking a self updating expectation.
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

use curie::adapter::{ADAPTER_SECRET_HEADER, MAX_ACK_BODY_BYTES};
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

/// The egress secret the smoke-test probes are given. Distinctive enough that a
/// negative probe DERIVED from it (the real value plus a suffix) is detectable
/// by substring, which is the whole assertion.
const REAL_EGRESS_SECRET: &str = "operator-real-egress-secret-1516";

/// Drive `adapter smoke-test` against a loopback endpoint and API, and hand back
/// its `--json` payload. The verb exits non-zero whenever a check fails, so the
/// code is deliberately not asserted here: the payload is the evidence, and it
/// is emitted before the exit code is applied.
fn smoke_test_payload(api: &MockServer, endpoint: &MockServer, secret: &str) -> serde_json::Value {
    let dir = tempdir();
    let mut profile = routed_valid_case();
    profile["endpoint"] = serde_json::json!(endpoint.base_url.clone());
    let path = write_profile(dir.path(), &profile);
    let address = profile["address"]["example"].as_str().unwrap().to_string();
    let secret_file = dir.path().join("egress.secret");
    std::fs::write(&secret_file, secret).expect("write the egress secret");

    let output = run(&[
        "adapter",
        "smoke-test",
        "-f",
        path.to_str().unwrap(),
        "--address",
        &address,
        "--secret-file",
        secret_file.to_str().unwrap(),
        // The recorders are loopback http, and refusing cleartext is a
        // separate, already covered rule.
        "--allow-insecure",
        "--yes",
        "--api-url",
        &api.base_url,
        "--json",
    ]);
    stdout_json(&output)
}

/// The same drive as [`smoke_test_payload`], with `--enqueue`, which is the only
/// flag that makes the verb post a turn at all.
fn smoke_test_payload_enqueuing(
    api: &MockServer,
    endpoint: &MockServer,
    secret: &str,
) -> serde_json::Value {
    let dir = tempdir();
    let mut profile = routed_valid_case();
    profile["endpoint"] = serde_json::json!(endpoint.base_url.clone());
    let path = write_profile(dir.path(), &profile);
    let address = profile["address"]["example"].as_str().unwrap().to_string();
    let secret_file = dir.path().join("egress.secret");
    std::fs::write(&secret_file, secret).expect("write the egress secret");

    let output = run(&[
        "adapter",
        "smoke-test",
        "-f",
        path.to_str().unwrap(),
        "--address",
        &address,
        "--secret-file",
        secret_file.to_str().unwrap(),
        "--allow-insecure",
        "--enqueue",
        "--yes",
        "--api-url",
        &api.base_url,
        "--json",
    ]);
    stdout_json(&output)
}

/// Every value that reached the endpoint under the egress secret header, in
/// order. This is the wire, not a log line, so a derived probe cannot hide.
fn secrets_sent(server: &MockServer) -> Vec<String> {
    server
        .recorded()
        .iter()
        .filter_map(|req| req.header(ADAPTER_SECRET_HEADER).map(str::to_string))
        .collect()
}

/// A syntactically valid JSON acknowledgement of EXACTLY `total` bytes, so the
/// cap tests turn on size alone and never on a parse failure.
fn json_body_of(total: usize) -> Response {
    const OPEN: &str = "{\"pad\":\"";
    const CLOSE: &str = "\"}";
    let body = format!(
        "{OPEN}{}{CLOSE}",
        "a".repeat(total - OPEN.len() - CLOSE.len())
    );
    assert_eq!(
        body.len(),
        total,
        "the fixture must be exactly {total} bytes"
    );
    Response {
        status: 200,
        content_type: "application/json".into(),
        body: body.into_bytes(),
    }
}

/// The scheme, host and port of a URL, computed with plain string operations.
///
/// Deliberately NOT `reqwest::Url`, which is the crate the implementation
/// redacts with: an expectation computed by the code under test is the code
/// under test agreeing with itself.
fn origin_of(url: &str) -> String {
    let (scheme, rest) = url
        .split_once("://")
        .unwrap_or_else(|| panic!("an endpoint carries a scheme: {url}"));
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .expect("a URL carries an authority");
    format!("{scheme}://{authority}")
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
/// `endpoint` is the one field compared in REDUCED form, because a profile route
/// can carry a token in its path or query and this payload reaches CI artifacts
/// and log aggregators. Its scheme, host and port still come from the corpus, so
/// a constant returning implementation dies here just the same.
///
/// Mutation to run against the implementation: replace the parsed
/// `credentials.egress` with a literal, drop `endpoint` from the payload, or
/// emit the endpoint whole. All three must red here.
#[test]
fn rust_accepts_every_valid_corpus_profile() {
    let cases = valid_cases();
    let mut redactions_proven = 0usize;
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
        seen.entry("egress_secret_env")
            .or_default()
            .insert(case["credentials"]["egress_secret_env"].to_string());
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
            parsed["conformance"]["wire_version"], case["conformance"]["wire_version"],
            "payload: {payload}"
        );
        assert!(
            parsed["conformance"].get("mints_reply_ref").is_none(),
            "mints_reply_ref left the contract; no payload may still report it: {payload}"
        );
        // The endpoint is the one parsed field the payload REDUCES rather than
        // carries: a profile route can hold a token in its path or query, and
        // `--json` is the form that reaches CI artifacts and log aggregators.
        // Absent in the file must still read back as JSON null, never a missing
        // key, so a consumer can branch on it unconditionally.
        match case.get("endpoint").and_then(|e| e.as_str()) {
            Some(raw) => {
                let origin = origin_of(raw);
                assert_eq!(
                    parsed["endpoint"],
                    serde_json::json!(origin),
                    "payload: {payload}"
                );
                let tail = &raw[origin.len()..];
                if !tail.is_empty() {
                    redactions_proven += 1;
                    assert!(
                        !payload.to_string().contains(tail),
                        "no part of the endpoint's path or query may appear anywhere in \
                         the payload; {tail:?} did: {payload}"
                    );
                }
            }
            None => assert!(
                parsed["endpoint"].is_null(),
                "an absent endpoint reads back as null: {payload}"
            ),
        }
    }

    assert!(
        redactions_proven > 0,
        "the corpus must carry at least one endpoint with a path or query, or the \
         redaction assertion above cannot tell a reduction from a passthrough"
    );
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

/// The corpus case whose `why` carries this fragment, so a test names the rule
/// it is exercising rather than an array index that renumbers on the next edit.
fn invalid_case_saying(fragment: &str) -> serde_json::Value {
    invalid_cases()
        .into_iter()
        .find(|(why, _, _)| why.contains(fragment))
        .unwrap_or_else(|| panic!("the corpus carries an invalid case whose why says {fragment:?}"))
        .2
}

/// Unquoted `version: 1.1` is a YAML float, so a version check that asked only
/// for a string view finds nothing, skips its own branch, and reports whatever
/// the closed schema found instead. A check that treats it as a MISSING key is
/// just as wrong: it sends the author hunting for a key sitting on line one.
///
/// The corpus case carries an unknown `future_field` alongside it precisely so
/// the two failures compete, and the version has to win.
///
/// Mutation to run against the implementation: read the version with
/// `value.get("version").and_then(|v| v.as_str())`. The unknown property leaks
/// into the message and this reds.
#[test]
fn a_numeric_version_is_a_version_error_that_names_the_value_and_the_quoted_form() {
    assert_verb_routes("validate");
    let profile = invalid_case_saying("version is the NUMBER 1.1");
    let dir = tempdir();
    let path = write_profile(dir.path(), &profile);

    let output = run(&["adapter", "validate", "-f", path.to_str().unwrap()]);
    assert_eq!(code(&output), 2, "{}", text(&output));
    let message = text(&output);

    assert!(
        message.contains("1.1"),
        "the message must name the value it found\n{message}"
    );
    assert!(
        message.contains("not a string") && message.contains("number"),
        "the message must say the value is present and of the wrong type\n{message}"
    );
    assert!(
        message.contains(&format!("version: \"{}\"", "1.0")),
        "the message must name the quoted form the author has to write\n{message}"
    );
    assert!(
        !message.contains("future_field"),
        "the version refusal must WIN: a schema for a version we do not speak has \
         no authority over this file\n{message}"
    );
    assert!(
        !message.contains("no version key"),
        "a present value of the wrong type is not a missing key, and reporting it \
         as one sends the author looking for a key that is already there\n{message}"
    );
}

/// The other two branches of the same check, kept distinct for the same reason.
/// A missing key is refused BEFORE the schema, so the operator reads the version
/// rule rather than `version is a required property` buried in a list.
#[test]
fn an_absent_and_an_empty_version_are_each_their_own_refusal() {
    assert_verb_routes("validate");
    let base = routed_valid_case();

    let mut absent = base.clone();
    absent.as_object_mut().unwrap().remove("version");
    let mut empty = base.clone();
    empty["version"] = serde_json::json!("");

    for (profile, expected) in [(absent, "no version key"), (empty, "empty version key")] {
        let dir = tempdir();
        let path = write_profile(dir.path(), &profile);
        let output = run(&["adapter", "validate", "-f", path.to_str().unwrap()]);
        assert_eq!(code(&output), 2, "{}", text(&output));
        let message = text(&output);
        assert!(
            message.contains(expected),
            "the refusal must say {expected:?}\n{message}"
        );
        assert!(
            message.contains(&format!("version: \"{}\"", "1.0")),
            "every version refusal names the quoted form\n{message}"
        );
    }
}

// ─── The Rust regex dialect IS the tier 3 floor ──────────────────────────────

/// Every construct `channel_protocol.manifest._RUST_UNSUPPORTED_GROUPS` and
/// `_RUST_UNSUPPORTED_ESCAPES` name must actually be refused by the crate this
/// CLI matches addresses with. The Python table is a hand written mirror of a
/// crate's behaviour, so it can only be trusted against the crate itself: a
/// construct listed there that `regex` in fact ACCEPTS would mean Python is
/// refusing profiles `curie adapter validate` would take, which is drift in the
/// direction no corpus case can catch.
#[test]
fn the_regex_crate_refuses_every_construct_python_lists_as_unsupported() {
    let unsupported = [
        ("lookahead", "^(?=.*@)[a-z0-9@.]+$"),
        ("negative lookahead", "^(?!admin)[a-z]+$"),
        ("lookbehind", "(?<=@)[a-z0-9.]+$"),
        ("negative lookbehind", "(?<!@)[a-z0-9.]+$"),
        ("backreference", r"^([a-z0-9]+)@\1\.example\.test$"),
        ("named backreference", "^(?P<a>[a-z]+)(?P=a)$"),
        ("atomic group", "^(?>a)$"),
        (
            "conditional group",
            r"^([a-z]+)?(?(1)@example\.test|admin)$",
        ),
        (
            "inline comment group",
            r"^[a-z0-9]+(?#the local part)@example\.test$",
        ),
        ("ASCII flag", r"(?a)^[a-z0-9]+@example\.test$"),
        ("end of string anchor", r"^[a-z0-9]+@example\.test\Z"),
        (
            "named character escape",
            r"^[a-z0-9]+\N{COMMERCIAL AT}example\.test$",
        ),
    ];
    for (name, pattern) in unsupported {
        assert!(
            regex::Regex::new(pattern).is_err(),
            "the regex crate ACCEPTS {name} ({pattern:?}), so listing it as unsupported \
             makes the Python validator refuse a profile this CLI would take"
        );
    }
}

/// The false positive control, and the reason the Python table's non entries are
/// non entries. These four are accepted by the crate, so listing any of them
/// would refuse a profile that is fine on both sides.
#[test]
fn the_regex_crate_accepts_the_constructs_python_deliberately_does_not_list() {
    let supported = [
        ("non capturing group", "^[a-z0-9]+(?:-[a-z0-9]+)*$"),
        ("named group", "^(?P<local>[a-z]+)@example$"),
        ("inline case insensitive flag", "(?i)^[a-z]+$"),
        ("possessive quantifier", "^[a-z]++$"),
    ];
    for (name, pattern) in supported {
        assert!(
            regex::Regex::new(pattern).is_ok(),
            "the regex crate refuses {name} ({pattern:?}); Python leaves it off the \
             unsupported table, so the two validators now disagree"
        );
    }
}

/// The end to end half of the control: a valid corpus profile whose pattern uses
/// an accepted construct still validates through the real binary, so the tier 3
/// scan cannot be greened by refusing everything with a `(?` in it.
#[test]
fn a_profile_using_an_accepted_group_construct_still_validates() {
    let profile = valid_cases()
        .into_iter()
        .find(|case| {
            case["address"]["pattern"]
                .as_str()
                .unwrap_or_default()
                .contains("(?")
        })
        .expect("the corpus carries a valid pattern using a group construct");

    let dir = tempdir();
    let path = write_profile(dir.path(), &profile);
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
        "an accepted construct must not be refused\n{}",
        text(&output)
    );
    assert_eq!(stdout_json(&output)["ok"], serde_json::json!(true));
}

// ─── A schema diagnostic never echoes the endpoint ───────────────────────────

/// The profile the schema refuses for embedding userinfo is exactly the one
/// whose rejected instance value IS a credential. A validation error renders the
/// instance it rejected, so the diagnostic has to carry the same reduction the
/// payloads and the human renders carry.
///
/// Mutation to run against the implementation: format the errors as `{e}` with
/// no scrub. This reds.
#[test]
fn a_schema_diagnostic_never_echoes_the_endpoints_credentials() {
    assert_verb_routes("validate");
    for (endpoint, secret) in [
        (
            "https://user:passw0rd@adapter.example.test/hook",
            "passw0rd",
        ),
        (
            "https://adapter.example.test/hook?ingress_token=qu3rys3cret",
            "qu3rys3cret",
        ),
    ] {
        let mut profile = routed_valid_case();
        profile["endpoint"] = serde_json::json!(endpoint);
        // Force a schema refusal that is NOT about the endpoint too, so the
        // scrub is proven on a diagnostic the endpoint merely rides along in.
        profile["kind"] = serde_json::json!("NotASlug");

        let dir = tempdir();
        let path = write_profile(dir.path(), &profile);
        let output = run(&["adapter", "validate", "-f", path.to_str().unwrap()]);
        assert_eq!(code(&output), 2, "{}", text(&output));

        let message = text(&output);
        assert!(
            !message.contains(secret),
            "a schema diagnostic must not echo the endpoint's credential {secret:?}\n{message}"
        );
        assert!(!message.contains("/hook"), "nor its path\n{message}");
    }
}

// ─── `scaffold` creates exclusively, it does not check then write ────────────

/// The no overwrite guarantee has to be the CREATE, not an `exists()` test
/// followed by a write. Those are two operations, and a symlink slips between
/// them: `Path::exists` FOLLOWS the link, so a DANGLING one answers false, and
/// `fs::write` follows it too and creates the victim at the other end. The
/// check passes, the write lands somewhere else, and the guarantee is gone
/// without any race being needed.
///
/// A link to an existing file does not discriminate, because `exists()` happens
/// to catch that one; the dangling case is the whole test.
///
/// Mutation to run against the implementation: restore the `file.exists()` check
/// plus `std::fs::write`. The victim path gets created and this reds.
#[test]
#[cfg(unix)]
fn scaffold_refuses_a_dangling_symlink_rather_than_writing_through_it() {
    let dir = tempdir();
    let victim = dir.path().join("victim.yaml");
    assert!(!victim.exists(), "the victim must not exist yet");

    let root = dir.path().join("root");
    std::fs::create_dir_all(root.join("demo")).expect("create the scaffold dir");
    let planted = root.join("demo/adapter.yaml");
    std::os::unix::fs::symlink(&victim, &planted).expect("plant the dangling symlink");

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
        root.to_str().unwrap(),
    ]);

    assert_eq!(
        code(&output),
        2,
        "an entry already at the destination is a usage error, a dangling symlink \
         included\n{}",
        text(&output)
    );
    assert!(
        !victim.exists(),
        "scaffold wrote THROUGH the link and created {}; exclusive creation is what \
         makes the no overwrite rule true",
        victim.display()
    );
    assert!(
        text(&output).contains("never overwrites"),
        "the refusal must name the rule\n{}",
        text(&output)
    );
}

/// The plain half of the same rule, so the symlink test above is not the only
/// thing holding it up.
#[test]
fn scaffold_refuses_an_existing_profile() {
    let dir = tempdir();
    std::fs::create_dir_all(dir.path().join("demo")).expect("create the scaffold dir");
    let existing = dir.path().join("demo/adapter.yaml");
    std::fs::write(&existing, "mine\n").expect("write the existing profile");

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

    assert_eq!(code(&output), 2, "{}", text(&output));
    assert!(
        text(&output).contains("never overwrites"),
        "the refusal must name the rule\n{}",
        text(&output)
    );
    assert_eq!(
        std::fs::read_to_string(&existing).expect("the profile survives"),
        "mine\n"
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

// ─── The reported endpoint is reduced, the written one is whole ──────────────

/// `bind --json` is the form most likely to be redirected into a CI artifact, a
/// ticket or a log aggregator, and an endpoint can carry a token in its path or
/// query. The payload therefore reports scheme, host and port only, while the
/// route WRITTEN to the API stays whole, because that is the route the worker
/// has to POST to.
///
/// Mutation to run against the implementation: report `binding.endpoint`
/// unreduced. The stdout half must red while the wire half stays green.
#[test]
fn bind_reports_a_reduced_endpoint_while_writing_the_whole_route() {
    let api = api_recorder();
    let mut profile = routed_valid_case();
    let route = "https://adapter.example.test/curie/reply?ingress_token=s3cr3t-in-the-query";
    profile["endpoint"] = serde_json::json!(route);

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
        "--json",
    ]);
    assert_eq!(code(&output), 0, "{}", text(&output));

    let payload = stdout_json(&output);
    assert_eq!(
        payload["endpoint"],
        serde_json::json!(origin_of(route)),
        "payload: {payload}"
    );
    let whole = text(&output);
    for leaked in ["ingress_token", "s3cr3t-in-the-query", "/curie/reply"] {
        assert!(
            !whole.contains(leaked),
            "no part of the endpoint's path or query may reach stdout or stderr; \
             {leaked:?} did:\n{whole}"
        );
    }

    // The false positive control: the reduction is a reporting decision, not a
    // truncation of the route itself.
    let body = recorded_body(&api, "PATCH", "/agents/");
    assert_eq!(
        body["channel"]["endpoint"],
        serde_json::json!(route),
        "the whole route must still be written: {body}"
    );
}

// ─── The negative probe carries a constant, never the real secret ────────────

/// An adapter's rejected-auth path is one of the likeliest places for a
/// credential to be written to disk in plaintext ("rejected secret X"), and the
/// adapter is third party infrastructure whose logs the operator does not
/// control. The wrong secret the negative probe sends must therefore be a fixed
/// constant, never the operator's real one with something appended.
///
/// Mutation to run against the implementation: build the wrong secret as
/// `format!("{secret}-not-the-secret")`. The containment assertion must red
/// while the refusal assertion stays green, which is the whole point: a derived
/// value proves the same thing and leaks while doing it.
#[test]
fn the_negative_probe_sends_a_constant_that_does_not_carry_the_real_secret() {
    assert_verb_routes("smoke-test");
    let api = api_recorder();
    let endpoint = serve(|req: &Request| {
        if req.header(ADAPTER_SECRET_HEADER) == Some(REAL_EGRESS_SECRET) {
            Response::json(200, r#"{"ok":true}"#)
        } else {
            Response::json(401, r#"{"detail":"rejected"}"#)
        }
    });

    let payload = smoke_test_payload(&api, &endpoint, REAL_EGRESS_SECRET);

    assert_eq!(
        payload["egress_positive"]["ok"],
        serde_json::json!(true),
        "the endpoint accepts the real secret, so the positive probe must pass: {payload}"
    );
    assert_eq!(
        payload["egress_negative"]["ok"],
        serde_json::json!(true),
        "a wrong secret must still be PROVEN refused: {payload}"
    );
    assert_eq!(
        payload["egress_negative"]["status"],
        serde_json::json!(401),
        "the refusal must be the endpoint's, read off its answer: {payload}"
    );

    let sent = secrets_sent(&endpoint);
    assert_eq!(
        sent.len(),
        2,
        "the egress probes are one positive and one negative; saw {sent:?}"
    );
    assert_eq!(
        sent.iter().filter(|s| *s == REAL_EGRESS_SECRET).count(),
        1,
        "exactly one probe may carry the real secret; saw {sent:?}"
    );
    for value in &sent {
        assert!(
            value == REAL_EGRESS_SECRET || !value.contains(REAL_EGRESS_SECRET),
            "the wrong secret must not be derived from the real one, or a rejected \
             auth log line at the adapter hands the real value back for the cost of \
             stripping a suffix: {value:?}"
        );
    }
}

/// The false positive control for the test above: the negative check is a real
/// check, so an endpoint that accepts ANY secret fails it. Without this, the
/// refusal assertion would green against a probe that never happened.
#[test]
fn an_endpoint_that_accepts_any_secret_fails_the_negative_check() {
    let api = api_recorder();
    let endpoint = endpoint_recorder();

    let payload = smoke_test_payload(&api, &endpoint, REAL_EGRESS_SECRET);

    assert_eq!(
        payload["egress_negative"]["ok"],
        serde_json::json!(false),
        "an endpoint answering 2xx to a wrong secret is not authenticating the \
         platform at all: {payload}"
    );
    assert_eq!(
        payload["verdict"],
        serde_json::json!("fail"),
        "a failed check must fail the verdict: {payload}"
    );
}

// ─── The acknowledgement body is read under a cap ────────────────────────────

/// The adapter under test is untrusted, so its answer is read with a running
/// total and refused once it passes the cap. Reading it whole and measuring
/// afterwards enforces nothing: the memory is already allocated by then, and a
/// multi gigabyte chunked answer is an OOM of the operator's shell.
///
/// Oversize is a FAILURE and never a truncation, because judging the first N
/// bytes of a body that was never sent whole hands the parser a value nobody
/// produced. Asserted at the check verdict, which is the observable this test
/// can hold; the streaming itself is pinned by the boundary control below.
#[test]
fn an_oversize_acknowledgement_body_fails_the_egress_check() {
    let api = api_recorder();
    let endpoint = serve(|_req: &Request| json_body_of(MAX_ACK_BODY_BYTES + 1024));

    let payload = smoke_test_payload(&api, &endpoint, REAL_EGRESS_SECRET);

    assert_eq!(
        payload["egress_positive"]["status"],
        serde_json::json!(200),
        "the endpoint answered 2xx, so the failure below is about SIZE: {payload}"
    );
    assert_eq!(
        payload["egress_positive"]["ok"],
        serde_json::json!(false),
        "an oversize acknowledgement must be refused: {payload}"
    );
    let detail = payload["egress_positive"]["detail"]
        .as_str()
        .unwrap_or_default();
    assert!(
        detail.contains(&MAX_ACK_BODY_BYTES.to_string()),
        "the detail must name the cap that was exceeded: {detail}"
    );
}

/// The boundary control: a body of EXACTLY the cap still passes, so the refusal
/// above is a real ceiling and not a blanket refusal of any body large enough to
/// arrive in more than one chunk.
#[test]
fn an_acknowledgement_body_exactly_at_the_cap_still_passes() {
    let api = api_recorder();
    let endpoint = serve(|_req: &Request| json_body_of(MAX_ACK_BODY_BYTES));

    let payload = smoke_test_payload(&api, &endpoint, REAL_EGRESS_SECRET);

    assert_eq!(
        payload["egress_positive"]["ok"],
        serde_json::json!(true),
        "the worker refuses a body OVER the cap, so exactly the cap passes: {payload}"
    );
}

// ─── The enqueued turn body is pinned to the API's own model ─────────────────

/// The API router that DEFINES `TurnIn`, read as the authority it is.
///
/// There is no committed artifact describing `TurnIn`, and that is not an
/// oversight to route around quietly. `POST /channels/turns` takes a bare
/// `Request` so it can enforce its size bound BEFORE authentication and before
/// JSON parsing, then parses `TurnIn` by hand; FastAPI therefore never sees a
/// body model, emits no `requestBody`, and `TurnIn` is absent from the committed
/// `apps/api/openapi.json` (which is generated and drift gated, and does carry
/// `ChannelBinding`, `ChannelTokenRequest` and `TurnAccepted`).
///
/// So this reads the model's own source rather than restating its fields here.
/// A hand written list of required fields is the drift that produced the bug
/// this test exists for: the payload was written once against a model that later
/// grew `reply_ref`, and nothing failed until a live install answered 422.
/// Reading the authority cannot go stale; a second copy of it always can.
const CHANNELS_ROUTER: &str = include_str!("../../apps/api/src/curie_api/routers/channels.py");

/// The committed, drift gated OpenAPI export, for the half of the body that
/// `TurnIn` inherits and that FastAPI DOES publish.
const API_OPENAPI: &str = include_str!("../../apps/api/openapi.json");

/// `(base class name, own required fields)` for a Pydantic model, read out of
/// the router source.
///
/// A required field is an annotated attribute with no `=`: a default of any kind
/// makes it optional, and `model_config` is an assignment rather than an
/// annotation, so neither reaches the set.
fn pydantic_model(source: &str, name: &str) -> (String, std::collections::BTreeSet<String>) {
    let header = format!("\nclass {name}(");
    let start = source
        .find(&header)
        .unwrap_or_else(|| panic!("{name} is defined in the router source"))
        + 1;
    let rest = &source[start..];
    let base = rest[rest.find('(').expect("a class header has a base list") + 1..]
        .split([')', ','])
        .next()
        .expect("a base list names a class")
        .trim()
        .to_string();
    let body_start = rest.find('\n').expect("a class header ends") + 1;
    let body = &rest[body_start..];
    let end = body.find("\nclass ").unwrap_or(body.len());

    let mut required = std::collections::BTreeSet::new();
    for line in body[..end].lines() {
        let Some(declaration) = line.strip_prefix("    ") else {
            continue;
        };
        if declaration.starts_with(' ') || declaration.starts_with('#') {
            continue;
        }
        let Some((field, annotation)) = declaration.split_once(": ") else {
            continue;
        };
        if annotation.contains('=') || !field.chars().all(|c| c.is_ascii_lowercase() || c == '_') {
            continue;
        }
        required.insert(field.to_string());
    }
    assert!(
        !required.is_empty(),
        "{name} parsed to no required fields, so this gate would pass on an empty body"
    );
    (base, required)
}

/// Every field `TurnIn` requires: its own, read from the model source, plus the
/// ones it inherits, read from the committed OpenAPI export.
fn turn_in_required_fields() -> std::collections::BTreeSet<String> {
    let (base, mut required) = pydantic_model(CHANNELS_ROUTER, "TurnIn");
    let openapi: serde_json::Value =
        serde_json::from_str(API_OPENAPI).expect("apps/api/openapi.json is valid JSON");
    let inherited = openapi["components"]["schemas"][&base]["required"]
        .as_array()
        .unwrap_or_else(|| {
            panic!("the committed OpenAPI export declares {base}, which TurnIn inherits")
        });
    required.extend(
        inherited
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string)),
    );
    required
}

/// A platform API stand-in that also answers the turn ingress, reporting the
/// duplicate verdict the round trip probe reads.
fn api_recorder_with_turns() -> MockServer {
    let seen = std::sync::Mutex::new(std::collections::BTreeSet::<String>::new());
    serve(move |req: &Request| {
        if req.path.starts_with("/channels/token") {
            return Response::json(200, r#"{"token":"chn_test_token_value"}"#);
        }
        if req.path.starts_with("/channels/turns") {
            let body: serde_json::Value =
                serde_json::from_slice(&req.body).unwrap_or(serde_json::Value::Null);
            // Stand in for the platform's real refusal: a body missing anything
            // TurnIn requires is a 422, exactly as the live API answered.
            let missing: Vec<String> = turn_in_required_fields()
                .into_iter()
                .filter(|field| body.get(field).is_none_or(serde_json::Value::is_null))
                .collect();
            if !missing.is_empty() {
                return Response::json(
                    422,
                    &format!(r#"{{"detail":[{{"type":"missing","loc":{missing:?}}}]}}"#),
                );
            }
            let delivery = body["delivery_id"].as_str().unwrap_or_default().to_string();
            let duplicate = !seen.lock().expect("the recorder lock").insert(delivery);
            return Response::json(
                200,
                &format!(r#"{{"event_id":"chn-x","stream_id":"1-0","duplicate":{duplicate}}}"#),
            );
        }
        Response::json(
            200,
            r#"{"id":"demo","name":"demo","channel":{"kind":"email","address":"agent@example.test"}}"#,
        )
    })
}

/// The body `--enqueue` actually puts on the wire must satisfy every field the
/// platform's own `TurnIn` requires.
///
/// This is the regression gate for a bug only a live install surfaced: the
/// payload omitted `reply_ref`, the API answered 422 with
/// `{"type":"missing","loc":["body","reply_ref"]}`, and the verb reported the
/// round trip as failed. Nothing in the suite could see it, because nothing
/// compared the request body against the model that judges it.
///
/// Mutation to run against the implementation: drop `reply_ref` from the body.
/// This reds.
#[test]
fn the_enqueued_turn_body_carries_every_field_the_platform_requires() {
    assert_verb_routes("smoke-test");
    let api = api_recorder_with_turns();
    let endpoint = endpoint_recorder();

    let payload = smoke_test_payload_enqueuing(&api, &endpoint, REAL_EGRESS_SECRET);

    let turns: Vec<serde_json::Value> = api
        .recorded()
        .iter()
        .filter(|req| req.method == "POST" && req.path.starts_with("/channels/turns"))
        .map(|req| serde_json::from_slice(&req.body).expect("the turn body is JSON"))
        .collect();
    assert_eq!(
        turns.len(),
        2,
        "the round trip posts the same delivery twice; saw {turns:?}"
    );

    let required = turn_in_required_fields();
    assert!(
        required.contains("reply_ref"),
        "the authority must still require reply_ref, or this gate proves nothing \
         about the field the live 422 named; required {required:?}"
    );
    for (nth, body) in turns.iter().enumerate() {
        for field in &required {
            assert!(
                body.get(field).is_some_and(|v| !v.is_null()),
                "post {} omits {field:?}, which TurnIn requires; the platform answers \
                 422 for exactly this. body: {body}",
                nth + 1
            );
        }
    }

    // Byte identical across both posts, or the second describes a DIFFERENT
    // delivery and the duplicate verdict below is meaningless.
    assert_eq!(
        turns[0], turns[1],
        "both posts must send the identical body, or the platform is being asked \
         to deduplicate two different deliveries"
    );

    assert_eq!(
        payload["round_trip"]["duplicate"],
        serde_json::json!(true),
        "the re-post must be reported as the duplicate: {payload}"
    );
    assert_eq!(
        payload["round_trip"]["ok"],
        serde_json::json!(true),
        "a body the platform accepts must produce a passing round trip: {payload}"
    );
}

/// The control that keeps the test above honest: the recorder really does refuse
/// a body missing a required field, so a green round trip is evidence and not an
/// artifact of a permissive stand-in.
#[test]
fn the_turn_recorder_refuses_a_body_missing_a_required_field() {
    let api = api_recorder_with_turns();
    let required = turn_in_required_fields();

    let mut complete = serde_json::json!({});
    for field in &required {
        complete[field] = serde_json::json!("x");
    }

    assert_eq!(
        post_turn(&api, &complete),
        200,
        "a complete body must be accepted, or every refusal below is vacuous"
    );

    for field in &required {
        let mut missing = complete.clone();
        missing.as_object_mut().expect("an object").remove(field);
        assert_eq!(
            post_turn(&api, &missing),
            422,
            "a body missing {field:?} must be refused, or the gate above cannot see \
             an omission"
        );
    }
}

/// POST one turn body to the recorder, returning its status.
///
/// Reads the STATUS LINE only and then drops the connection. The recorder keeps
/// a connection open for pipelined requests, so reading to EOF would block until
/// a close that never comes.
fn post_turn(api: &MockServer, body: &serde_json::Value) -> u16 {
    let encoded = serde_json::to_vec(body).expect("the body serializes");
    let address: std::net::SocketAddr = api
        .base_url
        .trim_start_matches("http://")
        .parse()
        .expect("the recorder base url is an address");
    let mut stream = std::net::TcpStream::connect(address).expect("connect to the recorder");
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .expect("a read timeout, so a stuck recorder fails the test instead of hanging it");

    use std::io::{BufRead, Write};
    let request = format!(
        "POST /channels/turns HTTP/1.1\r\nHost: local\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\n\r\n",
        encoded.len()
    );
    stream.write_all(request.as_bytes()).expect("write headers");
    stream.write_all(&encoded).expect("write body");
    stream.flush().expect("flush the request");

    let mut status_line = String::new();
    std::io::BufReader::new(&stream)
        .read_line(&mut status_line)
        .expect("read the status line");
    status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or_else(|| panic!("the recorder answered a status line: {status_line:?}"))
}

// ─── A probe failure never escapes the report ────────────────────────────────

/// Every dependency broken at once still produces a COMPLETE verdict, not an
/// error path.
///
/// The Python kit's equivalent hazard (finding 12) is that probe discovery
/// converts a failure to missing evidence while a LATER call of the same probe
/// raises uncaught. The Rust verb has no discovery-then-reuse seam, and every
/// probe converts its own transport error, non 2xx status and unreadable body
/// into a `CheckResult` at the call site, so there is no `?` between the first
/// probe and the emit. This pins that property behaviorally rather than by
/// reading the code: with a dead endpoint AND an API answering 500, the verb
/// must still emit every check, a `fail` verdict and the failure exit code.
#[test]
fn every_probe_failure_becomes_a_reported_check_rather_than_an_error_exit() {
    let api = serve(|_req: &Request| Response::json(500, r#"{"detail":"boom"}"#));
    // Bound then dropped, so the port answers nothing at all: a connection
    // refused is the transport failure no status code can stand in for.
    let dead = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        drop(listener);
        format!("http://{addr}")
    };

    let dir = tempdir();
    let mut profile = routed_valid_case();
    profile["endpoint"] = serde_json::json!(dead);
    let path = write_profile(dir.path(), &profile);
    let address = profile["address"]["example"].as_str().unwrap().to_string();
    let secret_file = dir.path().join("egress.secret");
    std::fs::write(&secret_file, REAL_EGRESS_SECRET).expect("write the secret");

    let output = run(&[
        "adapter",
        "smoke-test",
        "-f",
        path.to_str().unwrap(),
        "--address",
        &address,
        "--secret-file",
        secret_file.to_str().unwrap(),
        "--allow-insecure",
        "--enqueue",
        "--yes",
        "--api-url",
        &api.base_url,
        "--json",
    ]);

    assert_eq!(
        code(&output),
        1,
        "a failing probe is a FAILURE exit, never an error path that drops the \
         report\n{}",
        text(&output)
    );
    let payload = stdout_json(&output);
    for check in [
        "egress_positive",
        "egress_negative",
        "binding",
        "round_trip",
    ] {
        assert!(
            payload[check].is_object(),
            "{check} must still be reported when everything is broken: {payload}"
        );
        assert_eq!(
            payload[check]["ok"],
            serde_json::json!(false),
            "{check} must be reported as failed: {payload}"
        );
    }
    assert_eq!(
        payload["verdict"],
        serde_json::json!("fail"),
        "payload: {payload}"
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
