//! Integration: the agent-facing `--json` outputs must validate against the
//! committed JSON Schemas (ADR-0021 decision 1, AC 1 and AC 4). The schema
//! files under `cli/schema/` and the `status_json`/`eval_json` builders do not
//! exist yet, so this file will not compile / the schema loads fail at red; the
//! implementer creates the schemas alongside the `--json` wiring.

use std::collections::BTreeSet;

use curie::commands::{eval_json, status_json};
use curie::evals::CaseOutcome;
use curie::exit;
use curie::message::{message_dry_run_json, MessageOutcomeOutput};
use curie::observability::{local_endpoints, Endpoint, ObservabilityOutput};
use curie::ui::{CliOutput, DryRunPlan};
use curie_aci_protocol::{OutboundEvent, SessionStatus, PROTOCOL_VERSION};

fn load_schema(name: &str) -> serde_json::Value {
    let path = format!("{}/schema/{}", env!("CARGO_MANIFEST_DIR"), name);
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("committed schema {path} must exist: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("schema {path} must be valid JSON: {e}"))
}

fn validator(schema: &serde_json::Value) -> jsonschema::Validator {
    jsonschema::validator_for(schema).expect("schema compiles to a validator")
}

#[test]
fn status_json_validates_against_status_schema() {
    let schema = load_schema("status.schema.json");
    let digest = "a".repeat(64);
    let value = status_json(
        "http://127.0.0.1:8787",
        &SessionStatus::Done,
        Some(digest.as_str()),
    );
    let v = validator(&schema);
    assert!(
        v.is_valid(&value),
        "status_json output must validate against status.schema.json: {value}"
    );
    // The recorded bundle identity (#1087) is what makes an agent able to
    // confirm that messaging and eval ran the same artifact.
    assert_eq!(value["bundle_digest"], serde_json::json!(digest));
}

/// The no-runner-recorded case (#1087, edge case E10): an agent pointing `skill
/// status` at an arbitrary `--url`, or a `.curie/runner.json` written before
/// #1087, must still emit the key -- as JSON `null`, never a missing key -- so a
/// consumer can read it unconditionally. Proves the schema's
/// `["string", "null"]` union, not just the happy path.
#[test]
fn status_json_validates_with_no_recorded_digest() {
    let schema = load_schema("status.schema.json");
    let value = status_json("http://127.0.0.1:8787", &SessionStatus::Done, None);
    let v = validator(&schema);
    assert!(
        v.is_valid(&value),
        "a null bundle_digest must still validate against status.schema.json: {value}"
    );
    assert!(
        value.get("bundle_digest").is_some(),
        "the key is always emitted, never omitted: {value}"
    );
    assert!(value["bundle_digest"].is_null(), "{value}");
}

#[test]
fn eval_json_validates_against_eval_schema() {
    let schema = load_schema("eval.schema.json");
    // Two cases, one pass one fail: (id, outcome, seconds, output) rows plus the
    // roll-up. The failing case carries a non-empty reply for diagnosis (#548).
    let results = vec![
        (
            "case-pass".to_string(),
            CaseOutcome::Pass,
            1.5_f64,
            "the answer is 4".to_string(),
        ),
        (
            "case-fail".to_string(),
            CaseOutcome::Fail,
            0.25_f64,
            "i do not know".to_string(),
        ),
    ];
    let value = eval_json(&results, None);
    let v = validator(&schema);
    assert!(
        v.is_valid(&value),
        "eval_json output must validate against eval.schema.json: {value}"
    );
}

/// #1907: a single sample must still report N, passes, and policy so a
/// stochastic miss is labeled as one draw, not unexplained tier drift.
#[test]
fn eval_json_exposes_sampling_on_every_row() {
    let schema = load_schema("eval.schema.json");
    let results = vec![
        (
            "identity".to_string(),
            CaseOutcome::Fail,
            10.6_f64,
            "¿Quién eres?".to_string(),
        ),
        (
            "translate".to_string(),
            CaseOutcome::Pass,
            1.0_f64,
            "hola".to_string(),
        ),
    ];
    let value = eval_json(&results, None);
    assert!(
        validator(&schema).is_valid(&value),
        "sampling fields must validate: {value}"
    );
    assert_eq!(value["samples"], 1, "{value}");
    assert_eq!(value["policy"], "majority", "{value}");
    let identity = &value["cases"][0];
    assert_eq!(identity["id"], "identity", "{value}");
    assert_eq!(identity["samples"], 1, "{value}");
    assert_eq!(identity["passes"], 0, "{value}");
    assert_eq!(identity["policy"], "majority", "{value}");
    let translate = &value["cases"][1];
    assert_eq!(translate["samples"], 1, "{value}");
    assert_eq!(translate["passes"], 1, "{value}");
}

/// #1087 AC2 is a "confirm" criterion, so the digest has to be readable from the
/// MACHINE surface: `docs/agents.md` bans stderr as agent-facing evidence, and a
/// human note is all the sweep used to emit. Both states are pinned -- a real
/// digest and the null one -- because an agent consumer reads the key
/// unconditionally, so it must never be simply omitted.
#[test]
fn eval_json_carries_the_bundle_digest_and_emits_null_when_none_applies() {
    let schema = load_schema("eval.schema.json");
    let v = validator(&schema);
    let results = vec![(
        "case-pass".to_string(),
        CaseOutcome::Pass,
        1.0_f64,
        "4".to_string(),
    )];

    let digest = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
    let with_digest = eval_json(&results, Some(digest));
    assert!(v.is_valid(&with_digest), "{with_digest}");
    assert_eq!(
        with_digest["bundle_digest"], digest,
        "the eval payload must report the bundle it graded, so an agent can \
         compare it against `skill status --json`: {with_digest}"
    );

    let without = eval_json(&results, None);
    assert!(v.is_valid(&without), "{without}");
    assert!(
        without.get("bundle_digest").is_some(),
        "the key is always emitted, never omitted: {without}"
    );
    assert!(
        without["bundle_digest"].is_null(),
        "no digest applies, so it is null -- never a borrowed one: {without}"
    );
}

/// The non-graded row is the new contract surface (ADR-0055, #612/#606): it must
/// validate, report `outcome: "plumbing_ok"` with a NULL `passed`, and land in
/// its own roll-up count rather than being folded into passed or failed.
///
/// Deleting the tri-state (making `passed` a bare bool) fails the null assert;
/// deriving `failed` as `total - passed` again fails the `failed` assert, which
/// is the false red R1 rejected.
#[test]
fn a_plumbing_ok_row_validates_and_is_neither_passed_nor_failed() {
    let schema = load_schema("eval.schema.json");
    let results = vec![(
        "case-plumbing".to_string(),
        CaseOutcome::PlumbingOk,
        0.5_f64,
        "all done".to_string(),
    )];
    let value = eval_json(&results, None);
    let v = validator(&schema);
    assert!(
        v.is_valid(&value),
        "a plumbing_ok row must validate against eval.schema.json: {value}"
    );
    assert_eq!(value["plumbing_ok"], 1, "{value}");
    assert_eq!(
        value["failed"], 0,
        "a non-graded row is not a failure; `failed` must be counted, not derived: {value}"
    );
    assert_eq!(value["cases"][0]["outcome"], "plumbing_ok", "{value}");
    assert!(
        value["cases"][0]["passed"].is_null(),
        "a non-graded row claims neither verdict: {value}"
    );
}

/// The roll-up partitions the rows: every case lands in exactly one of the three
/// counts. A mixed run is where a naive `total - passed` or a plumbing row
/// silently folded into `passed` would show up.
#[test]
fn the_eval_rollup_partitions_every_row_across_the_three_outcomes() {
    let schema = load_schema("eval.schema.json");
    let results = vec![
        (
            "p".to_string(),
            CaseOutcome::Pass,
            1.0_f64,
            "right".to_string(),
        ),
        (
            "f".to_string(),
            CaseOutcome::Fail,
            1.0_f64,
            "wrong".to_string(),
        ),
        (
            "k".to_string(),
            CaseOutcome::PlumbingOk,
            1.0_f64,
            "all done".to_string(),
        ),
    ];
    let value = eval_json(&results, None);
    assert!(validator(&schema).is_valid(&value), "{value}");
    assert_eq!(value["total"], 3, "{value}");
    assert_eq!(value["passed"], 1, "{value}");
    assert_eq!(
        value["failed"], 1,
        "only the graded failure counts: {value}"
    );
    assert_eq!(value["plumbing_ok"], 1, "{value}");
}

#[test]
fn error_json_validates_against_error_schema() {
    let schema = load_schema("error.schema.json");
    let err = exit::usage("x").context("y");
    let value = exit::error_json(&err);
    let v = validator(&schema);
    assert!(
        v.is_valid(&value),
        "error_json output must validate against error.schema.json: {value}"
    );
}

// ---------------------------------------------------------------------------
// #955: the message-schema gate. THIS is the canonical statement of why the
// section is shaped the way it is; everything below points back here instead of
// restating it.
//
// These tests drive `MessageOutcomeOutput::to_json()` directly rather than the
// underlying builder functions, so every enum variant is forced through the
// committed `message.schema.json` gate regardless of whether its `to_json` arm
// delegates to a schema-gated builder or inlines its own JSON literal.
// `Enqueued` (#770/ADR-0078) is the proof case: its arm used to inline
// `serde_json::json!({"status": "enqueued", ...})`, which the pre-#955
// four-branch `oneOf` had no branch for and therefore REJECTED. It now
// delegates to `message_enqueued_json` like its siblings (see that builder's
// doc comment in `cli/src/message.rs`), but driving through `to_json()` here
// still matters: it is what would catch a FUTURE arm that inlines the way
// `Enqueued` once did. The schema carries an `enqueued` branch, and the tests
// below are what pin that fix.
//
// Two mechanisms stop a future variant escaping the same way, and both are
// deliberate. Compile-time exhaustiveness (the no-wildcard `match` in
// `message_outcome_variant_name`) is the cheap trap: it fails the BUILD when a
// variant is added with no arm. `message_outcome_samples_cover_every_declared_variant`
// closes the other half at run time: a variant that gets an arm but no
// constructed sample must fail too, so both additions are mandatory together.
// That gate's expected set is DERIVED from the enum's own AST and must NOT be
// replaced by a hand-written list of variant names -- such a list stays
// trivially equal to whatever the test already covers, which is exactly how a
// new inlined `to_json` arm would slip past the gate the way `Enqueued` did.

// The `syn` walk that reads the enum's variants straight out of the source, in
// the same `#[path = ...]` shape the sibling AST gates use
// (`cli/tests/schema_inventory.rs`, `cli/tests/api_emit_parity.rs`).
#[path = "support/enum_variants.rs"]
mod enum_variants;

/// The compiled `message.schema.json` validator. Every message-schema test
/// below takes it from here rather than re-inlining the load + compile pair.
fn message_validator() -> jsonschema::Validator {
    validator(&load_schema("message.schema.json"))
}

/// The variant set of `MessageOutcomeOutput`, derived from the AST of
/// `cli/src/message.rs` -- the enum definition itself, which is the only source
/// of truth a developer adding a variant cannot forget to update. Deriving it
/// rather than listing it by hand is deliberate; see the #955 section note
/// above for why a hand-written list would defeat the gate.
fn message_outcome_declared_variants() -> BTreeSet<String> {
    let path = format!("{}/src/message.rs", env!("CARGO_MANIFEST_DIR"));
    let src = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cli/src/message.rs must be readable at {path}: {e}"));
    let names = enum_variants::variant_names(&src, "MessageOutcomeOutput");
    assert!(
        !names.is_empty(),
        "no `enum MessageOutcomeOutput` found in {path}: the #955 coverage gate cannot derive \
         its expected variant set, so it would pass vacuously"
    );
    names
}

/// The variant NAME (as spelled in the enum) of a constructed outcome, matched
/// with **no wildcard arm** -- the compile-time half of the two-mechanism trap
/// described in the #955 section note above. Do not add a wildcard arm.
fn message_outcome_variant_name(value: &MessageOutcomeOutput) -> &'static str {
    match value {
        MessageOutcomeOutput::Replied { .. } => "Replied",
        MessageOutcomeOutput::NoEdit { .. } => "NoEdit",
        MessageOutcomeOutput::AwaitingApproval { .. } => "AwaitingApproval",
        MessageOutcomeOutput::TimedOut { .. } => "TimedOut",
        MessageOutcomeOutput::Enqueued { .. } => "Enqueued",
    }
}

/// An awaiting-approval outcome, parameterized only by the reply text: `Some`
/// models a parked worker that left placeholder text we could read, `None`
/// models one that left nothing at all.
fn awaiting_approval_outcome(reply: Option<&str>) -> MessageOutcomeOutput {
    MessageOutcomeOutput::AwaitingApproval {
        thread: "1700000000.000100".to_string(),
        reply: reply.map(str::to_string),
        tier: "workspace",
        agent: None,
        channel: "C123".to_string(),
    }
}

/// One constructed instance of every `MessageOutcomeOutput` variant, reused
/// by the schema-validation loop and the variant-coverage test below.
fn message_outcome_samples() -> Vec<MessageOutcomeOutput> {
    vec![
        MessageOutcomeOutput::Replied {
            thread: "1700000000.000100".to_string(),
            reply: "the answer is 42".to_string(),
        },
        MessageOutcomeOutput::NoEdit {
            thread: "1700000000.000100".to_string(),
        },
        awaiting_approval_outcome(Some("card text")),
        MessageOutcomeOutput::TimedOut {
            diagnostics: None,
            resume_note: None,
        },
        MessageOutcomeOutput::Enqueued {
            channel: "C123".to_string(),
            thread: "1700000000.000100".to_string(),
        },
    ]
}

#[test]
fn message_outcome_samples_cover_every_declared_variant() {
    // The run-time half of the #955 two-mechanism trap; see the section note
    // above. The expected set comes from the enum's own AST, never a list
    // maintained here.
    let declared = message_outcome_declared_variants();
    let covered: BTreeSet<String> = message_outcome_samples()
        .iter()
        .map(|outcome| message_outcome_variant_name(outcome).to_string())
        .collect();

    let uncovered: Vec<&String> = declared.difference(&covered).collect();
    assert!(
        uncovered.is_empty(),
        "MessageOutcomeOutput variant(s) {uncovered:?} are declared in \
         cli/src/message.rs but have no sample in message_outcome_samples(), so \
         their `to_json()` never reaches the message.schema.json gate (#955); \
         add one constructed sample per variant"
    );

    let unknown: Vec<&String> = covered.difference(&declared).collect();
    assert!(
        unknown.is_empty(),
        "message_outcome_variant_name() claims variant(s) {unknown:?} that \
         cli/src/message.rs does not declare; each arm's name string must match \
         its enum variant's spelling exactly, or the coverage check above is \
         comparing against the wrong set"
    );
}

// ─── The derived-variant walk's own teeth (#955) ─────────────────────────────
// The gate above is only as honest as the set `variant_names` derives: a
// silently-SHORT set makes the coverage comparison vacuously green, which is the
// exact defect #955 exists to close. `enum_variants` therefore panics rather
// than under-reporting, and the cases below prove each of those panics fires by
// EXECUTION -- the same "guard rejects a violating input" convention the sibling
// AST gates follow (`cli/tests/schema_inventory.rs`, `cli/tests/api_emit_parity.rs`),
// over small inline source fixtures, since `variant_names` takes source text and
// needs no file on disk. They live here, beside the `#[path]` include and the
// real-tree assertion that consumes it, because that is where each sibling gate
// keeps its own rejection cases.

/// A well-formed declaration: the positive control. Without it the three
/// rejection tests below would also pass against a walk that panicked
/// unconditionally or derived garbage.
const SAMPLE_ENUM_SRC: &str = r#"
pub enum Sample {
    Alpha { thread: String },
    Beta,
    Gamma(u8),
}
"#;

#[test]
fn variant_names_derives_every_variant_of_a_well_formed_enum() {
    let expected: BTreeSet<String> = ["Alpha", "Beta", "Gamma"]
        .into_iter()
        .map(str::to_string)
        .collect();
    assert_eq!(
        enum_variants::variant_names(SAMPLE_ENUM_SRC, "Sample"),
        expected,
        "the walk must enumerate every variant shape (struct, unit, tuple)"
    );
    // The absent-enum case is empty, NOT a panic: turning that into a failure is
    // the caller's job (`message_outcome_declared_variants` asserts non-empty),
    // and conflating the two would make the panics below unreadable.
    assert!(
        enum_variants::variant_names(SAMPLE_ENUM_SRC, "NotDeclared").is_empty(),
        "an absent enum yields an empty set for the caller to reject"
    );
}

#[test]
#[should_panic(expected = "does not parse as Rust")]
fn variant_names_panics_on_source_that_does_not_parse() {
    // Silently returning nothing here is what the old walk did, and the caller
    // then misreported it as "no enum found" -- an absent enum and an
    // unparseable file must never look alike.
    enum_variants::variant_names("pub enum Sample { Alpha,", "Sample");
}

#[test]
#[should_panic(expected = "2 declarations of `enum Sample`")]
fn variant_names_panics_on_a_second_declaration_of_the_same_enum() {
    const TWO_DECLARATIONS_SRC: &str = r#"
pub enum Sample {
    Alpha,
}

mod fixtures {
    pub enum Sample {
        Beta,
    }
}
"#;
    // Unioned, these would describe neither enum, so the gate would compare the
    // real `match` against a fiction.
    enum_variants::variant_names(TWO_DECLARATIONS_SRC, "Sample");
}

#[test]
#[should_panic(expected = "Sample::Beta")]
fn variant_names_panics_on_a_cfg_gated_variant() {
    const CFG_VARIANT_SRC: &str = r#"
pub enum Sample {
    Alpha,
    #[cfg(unix)]
    Beta,
}
"#;
    // The walk reads text and cannot know whether `Beta` exists in the build the
    // no-wildcard `match` is compiled into.
    enum_variants::variant_names(CFG_VARIANT_SRC, "Sample");
}

#[test]
#[should_panic(expected = "mod fixtures (enclosing enum Sample)")]
fn variant_names_panics_on_an_enum_inside_a_cfg_gated_module() {
    const CFG_MODULE_SRC: &str = r#"
#[cfg(test)]
mod fixtures {
    pub enum Sample {
        Alpha,
    }
}
"#;
    // An enclosing `#[cfg]` gates the declaration just as surely as one written
    // on the enum itself; the expected message pins that it is the ENCLOSURE
    // that was detected, not some other cfg position.
    enum_variants::variant_names(CFG_MODULE_SRC, "Sample");
}

#[test]
fn message_outcome_replied_validates_against_message_schema() {
    let v = message_validator();
    // Replied case: a non-null reply and finalized true.
    let replied = MessageOutcomeOutput::Replied {
        thread: "1700000000.000100".to_string(),
        reply: "the answer is 42".to_string(),
    }
    .to_json();
    assert!(
        v.is_valid(&replied),
        "MessageOutcomeOutput::Replied::to_json must validate against message.schema.json: {replied}"
    );
    // Pin the values, not just the types: the reply text must pass through, the
    // thread must echo the input, and finalized must track reply.is_some().
    assert_eq!(replied["reply"], serde_json::json!("the answer is 42"));
    assert_eq!(replied["thread"], serde_json::json!("1700000000.000100"));
    assert_eq!(replied["finalized"], serde_json::json!(true));
}

#[test]
fn message_outcome_no_edit_validates_against_message_schema() {
    let v = message_validator();
    // No-edit completion: reply null, finalized false, must also validate.
    let no_edit = MessageOutcomeOutput::NoEdit {
        thread: "1700000000.000100".to_string(),
    }
    .to_json();
    assert!(
        v.is_valid(&no_edit),
        "MessageOutcomeOutput::NoEdit::to_json must validate against message.schema.json: {no_edit}"
    );
    // Pin the no-edit values: null reply, thread passthrough, finalized false.
    assert!(
        no_edit["reply"].is_null(),
        "no-edit reply must be JSON null: {no_edit}"
    );
    assert_eq!(no_edit["thread"], serde_json::json!("1700000000.000100"));
    assert_eq!(no_edit["finalized"], serde_json::json!(false));
}

#[test]
fn message_outcome_timed_out_validates_against_message_schema() {
    let v = message_validator();
    let timed_out = MessageOutcomeOutput::TimedOut {
        diagnostics: None,
        resume_note: None,
    }
    .to_json();
    assert!(
        v.is_valid(&timed_out),
        "MessageOutcomeOutput::TimedOut::to_json must validate against message.schema.json: {timed_out}"
    );
    // Pin the timeout shape: null reply, finalized false, timed_out true.
    assert!(
        timed_out["reply"].is_null(),
        "timeout reply must be JSON null: {timed_out}"
    );
    assert_eq!(timed_out["finalized"], serde_json::json!(false));
    assert_eq!(timed_out["timed_out"], serde_json::json!(true));
}

#[test]
fn message_outcome_awaiting_approval_validates_and_is_distinct() {
    // #529: the awaiting-approval object is finalized:false + awaiting_approval:true,
    // a distinct terminal state from a reply or a timeout.
    let v = message_validator();
    let awaiting = awaiting_approval_outcome(Some("card text")).to_json();
    assert!(
        v.is_valid(&awaiting),
        "MessageOutcomeOutput::AwaitingApproval::to_json must validate against message.schema.json: {awaiting}"
    );
    assert_eq!(awaiting["finalized"], serde_json::json!(false));
    assert_eq!(awaiting["awaiting_approval"], serde_json::json!(true));
    assert_eq!(awaiting["reply"], serde_json::json!("card text"));
    assert_eq!(awaiting["thread"], serde_json::json!("1700000000.000100"));
}

/// The no-reply approval card is a DISTINCT schema shape from the one above:
/// the worker parked without any placeholder text we could read, so `reply` is
/// JSON null (the branch allows both). Kept as its own test rather than a
/// second entry in `message_outcome_samples()`, which must stay exactly one
/// sample per variant for the AST-derived coverage gate to compare cleanly.
#[test]
fn message_outcome_awaiting_approval_with_no_reply_validates() {
    let v = message_validator();
    let awaiting = awaiting_approval_outcome(None).to_json();
    assert!(
        v.is_valid(&awaiting),
        "an awaiting-approval payload with a null reply must validate against message.schema.json: {awaiting}"
    );
    assert!(
        awaiting["reply"].is_null(),
        "an unseen approval card carries a JSON null reply: {awaiting}"
    );
    assert_eq!(awaiting["finalized"], serde_json::json!(false));
    assert_eq!(awaiting["awaiting_approval"], serde_json::json!(true));
    assert_eq!(awaiting["thread"], serde_json::json!("1700000000.000100"));
}

#[test]
fn message_outcome_enqueued_validates_against_message_schema() {
    // The #955 proof case (see the section note above): this assertion fails
    // again the moment the `enqueued` branch is dropped from the schema or
    // `message_enqueued_json`'s output drifts away from it.
    let v = message_validator();
    let enqueued = MessageOutcomeOutput::Enqueued {
        channel: "C123".to_string(),
        thread: "1700000000.000100".to_string(),
    }
    .to_json();
    assert!(
        v.is_valid(&enqueued),
        "MessageOutcomeOutput::Enqueued::to_json must validate against message.schema.json: {enqueued}"
    );
    assert_eq!(enqueued["channel"], serde_json::json!("C123"));
    assert_eq!(enqueued["thread"], serde_json::json!("1700000000.000100"));
}

#[test]
fn message_dry_run_json_validates_against_message_schema() {
    let v = message_validator();
    // Explicit channel (local target).
    let with_channel = message_dry_run_json(
        "local",
        "curie:turns",
        Some("C123"),
        Some("http://localhost:8155/api/"),
    );
    assert!(
        v.is_valid(&with_channel),
        "message_dry_run_json (with channel) must validate: {with_channel}"
    );
    assert_eq!(with_channel["dry_run"], serde_json::json!(true));
    assert_eq!(with_channel["target"], serde_json::json!("local"));
    assert_eq!(with_channel["channel"], serde_json::json!("C123"));
    // Null channel (cluster target, sole-agent resolution).
    let no_channel = message_dry_run_json("cluster", "curie:turns", None, None);
    assert!(
        v.is_valid(&no_channel),
        "message_dry_run_json (no channel) must validate: {no_channel}"
    );
    assert!(
        no_channel["channel"].is_null(),
        "omitted channel must be JSON null: {no_channel}"
    );
    assert_eq!(no_channel["target"], serde_json::json!("cluster"));
    assert!(
        no_channel["reply_endpoint"].is_null(),
        "cluster relay has no callback endpoint: {no_channel}"
    );
}

/// The direct statement of #955's acceptance criterion, carrying BOTH schema
/// properties in a single pass over every emitted payload:
///
/// 1. **Every variant validates.** Each `MessageOutcomeOutput` variant's
///    `to_json()`, plus the dry-run descriptor, clears the committed schema.
/// 2. **Exactly one branch matches.** `message.schema.json`'s root schema is
///    a BARE `oneOf` (no sibling keywords, no wrapping `allOf`/`$ref`), which
///    is what makes `is_valid` and "matches exactly one branch" the same
///    check here -- a payload satisfying two branches would fail validation,
///    since it would make the emitted object ambiguous to an agent consumer
///    discriminating on shape. That root shape is PINNED below rather than
///    assumed: restructuring `message.schema.json` so the root is no longer a
///    bare `oneOf` (wrapped in `allOf`, given sibling keywords, swapped for
///    `anyOf`) turns this test red, instead of leaving `is_valid` quietly
///    passing while the test's name overstates what it proves.
///
/// Driven off the SAME sample set the AST coverage gate checks, so coverage and
/// validation cannot drift apart -- the coverage gate only proves a sample
/// exists, and without this loop the per-variant tests that actually validate
/// could be deleted one by one without the gate noticing. Those per-variant
/// tests stay: they carry field-level assertions this loop does not.
#[test]
fn every_message_payload_matches_exactly_one_message_schema_branch() {
    // Property 2's precondition, pinned: the root is a BARE `oneOf`, so
    // "validates" and "matches exactly one branch" are the same check. Pure
    // annotations may sit alongside it; any other keyword (`allOf`, `anyOf`,
    // `type`, `$ref`, `not`) would change what `is_valid` means.
    let schema = load_schema("message.schema.json");
    let root = schema
        .as_object()
        .expect("message.schema.json's root must be a JSON object");
    let branches = root
        .get("oneOf")
        .and_then(serde_json::Value::as_array)
        .expect("message.schema.json's root must carry a `oneOf` array");
    assert!(
        branches.len() > 1,
        "a single-branch `oneOf` makes `exactly one branch` a vacuous claim: {branches:#?}"
    );
    const ANNOTATIONS: [&str; 6] = [
        "$schema",
        "$id",
        "$comment",
        "title",
        "description",
        "$defs",
    ];
    let siblings: Vec<&str> = root
        .keys()
        .map(String::as_str)
        .filter(|k| *k != "oneOf" && !ANNOTATIONS.contains(k))
        .collect();
    assert!(
        siblings.is_empty(),
        "message.schema.json's root must stay a bare `oneOf`; keyword(s) {siblings:?} beside it \
         would stop `is_valid` meaning `matches exactly one branch`, which is what this test's \
         name claims"
    );

    let v = message_validator();
    for (label, value) in message_outcome_samples()
        .iter()
        .map(|outcome| {
            (
                format!(
                    "MessageOutcomeOutput::{}::to_json",
                    message_outcome_variant_name(outcome)
                ),
                outcome.to_json(),
            )
        })
        .chain([(
            "message_dry_run_json".to_string(),
            message_dry_run_json("local", "s", Some("C1"), Some("http://x/api/")),
        )])
    {
        assert!(
            v.is_valid(&value),
            "{label} must satisfy exactly one branch of the message.schema.json oneOf: {value}"
        );
    }
}

#[test]
fn message_schema_gate_has_teeth() {
    // negative control: proves the schema gate discriminates
    let v = message_validator();
    let mut value = MessageOutcomeOutput::Replied {
        thread: "1700000000.000100".to_string(),
        reply: "hi".to_string(),
    }
    .to_json();
    // Strip a required key; a schema with real teeth must now reject.
    value
        .as_object_mut()
        .expect("MessageOutcomeOutput::Replied::to_json returns a JSON object")
        .remove("reply");
    assert!(
        !v.is_valid(&value),
        "message schema must reject an object missing the required `reply` key"
    );
}

#[test]
fn observability_json_validates_against_observability_schema() {
    let schema = load_schema("observability.schema.json");
    let v = validator(&schema);
    // Both row shapes must validate: a browsable row (url set, note null) and a
    // degraded row (url null, note set).
    let value = ObservabilityOutput::Surfaces(vec![
        Endpoint {
            name: "Curie Console".to_string(),
            url: Some("http://localhost:28080/?api=1".to_string()),
            note: None,
            browsable: true,
        },
        Endpoint {
            name: "Curie API".to_string(),
            url: None,
            note: Some("service curie-ui not found".to_string()),
            browsable: false,
        },
    ])
    .to_json();
    assert!(
        v.is_valid(&value),
        "ObservabilityOutput::to_json must validate against observability.schema.json: {value}"
    );
    // Pin the values, not just the types: a degraded row must never smuggle its
    // message into `url`, or an agent cannot parse `url` as a URL.
    assert!(value["surfaces"][1]["url"].is_null());
    assert_eq!(
        value["surfaces"][1]["note"],
        serde_json::json!("service curie-ui not found")
    );
}

#[test]
fn local_endpoints_json_validates_against_observability_schema() {
    // The real local-tier payload (not a hand-built fixture) must satisfy the
    // committed schema -- this is what `local observability --json` emits.
    let schema = load_schema("observability.schema.json");
    let value = ObservabilityOutput::Surfaces(local_endpoints()).to_json();
    let v = validator(&schema);
    assert!(
        v.is_valid(&value),
        "local_endpoints payload must validate against observability.schema.json: {value}"
    );
}

#[test]
fn observability_dry_run_json_validates_against_observability_schema() {
    // The `--dry-run` branch (cluster tier only) must validate against the SAME
    // committed schema that documents `cluster observability --dry-run --json`
    // -- a consumer validating all `cluster observability --json` output against
    // one schema must not have a legitimate invocation rejected. Built through
    // the real DryRunPlan::to_json, not a hand-written literal, so this test
    // cannot drift from what the command actually emits.
    let schema = load_schema("observability.schema.json");
    let v = validator(&schema);
    let value = ObservabilityOutput::DryRun(DryRunPlan {
        lines: vec![
            "kubectl get pods -n curie".to_string(),
            "helm get values curie".to_string(),
        ],
    })
    .to_json();
    assert!(
        v.is_valid(&value),
        "ObservabilityOutput::DryRun must validate against observability.schema.json: {value}"
    );
    // Pin the values, not just the types: dry_run must be the literal true and
    // plan must pass the lines through verbatim.
    assert_eq!(value["dry_run"], serde_json::json!(true));
    assert_eq!(
        value["plan"],
        serde_json::json!(["kubectl get pods -n curie", "helm get values curie"])
    );
}

#[test]
fn observability_schema_gate_has_teeth() {
    // negative control: proves the schema gate discriminates
    let schema = load_schema("observability.schema.json");
    let mut value = ObservabilityOutput::Surfaces(vec![Endpoint {
        name: "Curie Console".to_string(),
        url: Some("http://localhost:28080/?api=1".to_string()),
        note: None,
        browsable: true,
    }])
    .to_json();
    // Strip a required per-row key; a schema with real teeth must now reject.
    value["surfaces"][0]
        .as_object_mut()
        .expect("each surface row is a JSON object")
        .remove("browsable");
    let v = validator(&schema);
    assert!(
        !v.is_valid(&value),
        "observability schema must reject a row missing the required `browsable` key"
    );
}

#[test]
fn observability_runs_query_schema_is_closed_and_bounded() {
    let schema = load_schema("observability-runs.schema.json");
    let v = validator(&schema);
    let valid = serde_json::json!({
        "limit": 1,
        "count": 1,
        "runs": [{
            "id": "trace-1",
            "name": "curie-run:agent-example-thread-1",
            "timestamp": "2026-08-22T00:00:00Z",
            "sessionId": "session-1",
            "metadata": {"terminal_outcome": "completed"}
        }]
    });
    assert!(
        v.is_valid(&valid),
        "bounded runs payload must validate: {valid}"
    );
    let mut unnamed = valid.clone();
    unnamed["runs"][0]["name"] = serde_json::Value::Null;
    assert!(
        v.is_valid(&unnamed),
        "the console/API contract permits unnamed traces: {unnamed}"
    );

    let mut missing_count = valid.clone();
    missing_count.as_object_mut().unwrap().remove("count");
    assert!(
        !v.is_valid(&missing_count),
        "runs schema must require an explicit returned count"
    );

    let too_many: Vec<_> = (0..101)
        .map(|index| {
            serde_json::json!({
                "id": format!("trace-{index}"),
                "name": "curie-run:agent-example-thread-1",
                "timestamp": "2026-08-22T00:00:00Z"
            })
        })
        .collect();
    let over_bound = serde_json::json!({"limit": 100, "count": 101, "runs": too_many});
    assert!(
        !v.is_valid(&over_bound),
        "runs schema must reject a result larger than the public maximum"
    );
    assert!(
        !v.is_valid(&serde_json::json!({"limit": 0, "count": 0, "runs": []})),
        "the documented lower bound is one"
    );
    assert!(
        !v.is_valid(&serde_json::json!({"limit": 1, "count": 1, "runs": [{}]})),
        "every typed list row must carry the identity needed by `run <trace-id>`"
    );
}

#[test]
fn observability_run_query_schema_requires_the_complete_trace_tree() {
    let schema = load_schema("observability-run.schema.json");
    let v = validator(&schema);
    let valid = serde_json::json!({
        "trace": {
            "id": "trace-1",
            "sessionId": "session-1",
            "metadata": {"terminal_outcome": "completed"}
        },
        "tree": [{
            "id": "span-1",
            "type": "SPAN",
            "name": null,
            "startTime": null,
            "model": null,
            "usageDetails": null,
            "children": []
        }],
        "sandbox_id": null,
        "approval_decision": null
    });
    assert!(
        v.is_valid(&valid),
        "complete TraceTree must validate: {valid}"
    );

    let mut missing_correlation = valid.clone();
    missing_correlation
        .as_object_mut()
        .unwrap()
        .remove("sandbox_id");
    assert!(
        !v.is_valid(&missing_correlation),
        "stable nullable correlation fields are emitted, not omitted"
    );

    let mut incomplete_node = valid.clone();
    incomplete_node["tree"][0]
        .as_object_mut()
        .unwrap()
        .remove("children");
    assert!(
        !v.is_valid(&incomplete_node),
        "every typed observation node carries its children"
    );

    let mut extra = valid;
    extra["backend_credentials"] = serde_json::json!("must-never-exist");
    assert!(
        !v.is_valid(&extra),
        "the top-level CLI result is closed to accidental backend fields"
    );
}

#[test]
fn observability_metrics_query_schema_covers_complete_summary_and_series() {
    let schema = load_schema("observability-metrics.schema.json");
    let v = validator(&schema);
    let summary = serde_json::json!({
        "start": "2026-08-22T00:00:00Z",
        "end": "2026-08-23T00:00:00Z",
        "runs": 3,
        "latency_p95_ms": 25.0,
        "tokens": 42,
        "cost_usd": 0.0,
        "cost_known": false,
        "error_rate": 0.0
    });
    let series = serde_json::json!({
        "metric": "runs",
        "granularity": "day",
        "start": "2026-08-22T00:00:00Z",
        "end": "2026-08-23T00:00:00Z",
        "points": [{"ts": "2026-08-22T00:00:00Z", "value": 3.0}]
    });
    assert!(
        v.is_valid(&summary),
        "complete summary DTO must validate: {summary}"
    );
    assert!(
        v.is_valid(&series),
        "complete series DTO must validate: {series}"
    );

    let mut missing_cost_state = summary;
    missing_cost_state
        .as_object_mut()
        .unwrap()
        .remove("cost_known");
    assert!(
        !v.is_valid(&missing_cost_state),
        "cost_known cannot be projected away from the existing API DTO"
    );

    let mut invalid_granularity = series;
    invalid_granularity["granularity"] = serde_json::json!("minute");
    assert!(
        !v.is_valid(&invalid_granularity),
        "series granularity is the bounded hour/day/week CLI enum"
    );

    let too_many_points = serde_json::json!({
        "metric": "runs",
        "granularity": "hour",
        "start": "2026-01-01T00:00:00Z",
        "end": "2026-12-31T00:00:00Z",
        "points": (0..1001)
            .map(|index| serde_json::json!({"ts": format!("point-{index}"), "value": 0.0}))
            .collect::<Vec<_>>()
    });
    assert!(
        !v.is_valid(&too_many_points),
        "metric-series results must have a finite public point bound"
    );
}

#[test]
fn eval_schema_gate_has_teeth() {
    // negative control: proves the schema gate discriminates
    let schema = load_schema("eval.schema.json");
    let results = vec![(
        "only".to_string(),
        CaseOutcome::Pass,
        1.0_f64,
        "ok".to_string(),
    )];
    let mut value = eval_json(&results, None);
    // Strip a required top-level key; a schema with real teeth must now reject.
    value
        .as_object_mut()
        .expect("eval_json returns a JSON object")
        .remove("total");
    let v = validator(&schema);
    assert!(
        !v.is_valid(&value),
        "eval schema must reject an object missing the required `total` key"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// #634: every OTHER agent-facing result family validates against its committed,
// versioned schema too (the inventory in cli/schema/index.json). Each output's
// real `to_json` is built here (not a hand-written fixture) so the check cannot
// drift from what the command emits. The inventory gate
// (cli/tests/schema_inventory.rs) proves the set below is exhaustive.
// ─────────────────────────────────────────────────────────────────────────────

use curie::api::{ApprovalRecord, MemoryEntry, Version};
use curie::commands::{
    ApprovalsOutput, BudgetOutput, BumpVersionOutput, ChartCheckOutcome, ChartCheckOutput,
    CheckMatch, CheckReport, ConnectorBuildOutput, ConnectorBuildRecord, DeclaredServer,
    DeleteOutput, DeployOutput, KillOutput, ListAgentsOutput, LocalAgentSummary, MemoryOutput,
    ResetThreadOutput, ResumeOutput, SkillApprovalsOutput, SkillMessageOutput, SweepRow,
    VersionsOutput,
};
use curie::comms::CommsOutput;
use curie::local::{
    LocalDownOutput, LocalRebuildOutput, LocalStatusOutput, LocalUpOutput, ModelMode,
};
use curie::ops::{
    ClusterDownOutput, ClusterRollbackOutput, ClusterStatus, ClusterStatusOutput, ClusterUpOutput,
    ClusterUpgradeOutput, PodRow,
};
use curie::secrets::SecretsListOutput;

fn assert_valid(schema_file: &str, value: &serde_json::Value) {
    let schema = load_schema(schema_file);
    let v = validator(&schema);
    assert!(
        v.is_valid(value),
        "output must validate against {schema_file}: {value}"
    );
}

/// The uniform dry-run plan every verb shares (DryRunPlan family + the DryRun
/// branch of every result-family schema).
#[test]
fn dry_run_plan_validates() {
    let plan = DryRunPlan {
        lines: vec![
            "helm upgrade curie".to_string(),
            "kubectl get pods".to_string(),
        ],
    };
    assert_valid("dry-run.schema.json", &plan.to_json());
}

#[test]
fn chart_check_output_validates() {
    let output = ChartCheckOutput {
        passed: 2,
        total: 2,
        scripts: vec![
            ChartCheckOutcome {
                name: "first-assertion.sh".to_string(),
                passed: true,
            },
            ChartCheckOutcome {
                name: "second-assertion.sh".to_string(),
                passed: true,
            },
        ],
    };
    assert_valid("chart-check.schema.json", &output.to_json());
}

#[test]
fn init_output_validates() {
    use std::path::PathBuf;
    let with_spec = curie::commands::InitOutput {
        name: "deal-desk".to_string(),
        dir: PathBuf::from("deal-desk"),
        from_spec: Some(PathBuf::from("spec.yaml")),
        created: vec![PathBuf::from("deal-desk/.claude-plugin/plugin.json")],
        success_msg: "ok".to_string(),
    };
    assert_valid("init.schema.json", &with_spec.to_json());
    let plain = curie::commands::InitOutput {
        name: "deal-desk".to_string(),
        dir: PathBuf::from("deal-desk"),
        from_spec: None,
        created: vec![],
        success_msg: "ok".to_string(),
    };
    assert_valid("init.schema.json", &plain.to_json());
}

#[test]
fn list_agents_output_validates() {
    let out = ListAgentsOutput {
        agents: vec![LocalAgentSummary {
            name: "deal-desk".to_string(),
            description: "quotes".to_string(),
            directory: "agents/deal-desk".to_string(),
        }],
    };
    assert_valid("list-agents.schema.json", &out.to_json());
    // Empty list is still a valid payload.
    assert_valid(
        "list-agents.schema.json",
        &ListAgentsOutput { agents: vec![] }.to_json(),
    );
}

#[test]
fn bump_version_output_validates() {
    let out = BumpVersionOutput {
        version: "1.2.3".to_string(),
    };
    assert_valid("bump-version.schema.json", &out.to_json());
}

#[test]
fn skill_message_awaiting_approval_output_preserves_final_approval_fields() {
    let final_frame = OutboundEvent::Final {
        version: PROTOCOL_VERSION.to_string(),
        text: "the answer is 42".to_string(),
        status: SessionStatus::AwaitingApproval,
        approval_summary: Some("Approve the proposed operation".to_string()),
        approval_route: Some("reviewers".to_string()),
        approval_gate_kind: Some("permission".to_string()),
        approval_granted_tool: Some("ExampleTool".to_string()),
        input_tokens: Some(10),
        output_tokens: Some(5),
    };
    let out = SkillMessageOutput::from_final("the answer is 42".to_string(), &final_frame)
        .expect("a final frame produces skill message output");
    let value = out.to_json();
    assert_valid("skill-message.schema.json", &value);
    assert_eq!(value["reply"], serde_json::json!("the answer is 42"));
    assert_eq!(value["status"], serde_json::json!("awaiting-approval"));
    assert_eq!(value["finalized"], serde_json::json!(false));
    assert_eq!(
        value["approval_summary"],
        serde_json::json!("Approve the proposed operation")
    );
    assert_eq!(value["approval_route"], serde_json::json!("reviewers"));
    assert_eq!(value["approval_gate_kind"], serde_json::json!("permission"));
    assert_eq!(
        value["approval_granted_tool"],
        serde_json::json!("ExampleTool")
    );
}

#[test]
fn skill_message_awaiting_approval_output_is_not_finalized() {
    let final_frame = OutboundEvent::Final {
        version: PROTOCOL_VERSION.to_string(),
        text: "I need approval before continuing".to_string(),
        status: SessionStatus::AwaitingApproval,
        approval_summary: Some("Approve the proposed operation".to_string()),
        approval_route: Some("reviewers".to_string()),
        approval_gate_kind: Some("policy".to_string()),
        approval_granted_tool: None,
        input_tokens: None,
        output_tokens: None,
    };
    let out = SkillMessageOutput::from_final(
        "I need approval before continuing".to_string(),
        &final_frame,
    )
    .expect("an awaiting-approval final frame produces skill message output");
    let value = out.to_json();
    assert_valid("skill-message.schema.json", &value);
    assert_eq!(value["status"], serde_json::json!("awaiting-approval"));
    assert_eq!(value["finalized"], serde_json::json!(false));
    assert_eq!(
        value["approval_summary"],
        serde_json::json!("Approve the proposed operation")
    );
    assert_eq!(value["approval_route"], serde_json::json!("reviewers"));
    assert_eq!(value["approval_gate_kind"], serde_json::json!("policy"));
    assert_eq!(value["approval_granted_tool"], serde_json::Value::Null);
}

#[test]
fn skill_message_only_marks_awaiting_approval_as_not_finalized() {
    for status in [
        SessionStatus::Done,
        SessionStatus::IdleAwaitingInput,
        SessionStatus::ClassifiedFailure,
    ] {
        let final_frame = OutboundEvent::Final {
            version: PROTOCOL_VERSION.to_string(),
            text: String::new(),
            status,
            approval_summary: None,
            approval_route: None,
            approval_gate_kind: None,
            approval_granted_tool: None,
            input_tokens: None,
            output_tokens: None,
        };
        let out = SkillMessageOutput::from_final(String::new(), &final_frame)
            .expect("a final frame produces skill message output");
        assert!(
            out.finalized,
            "non-parked status must be finalized: {out:?}"
        );
        let json = out.to_json();
        assert_valid("skill-message.schema.json", &json);
        assert!(json["approval_summary"].is_null());
        assert!(json["approval_route"].is_null());
        assert!(json["approval_gate_kind"].is_null());
        assert!(json["approval_granted_tool"].is_null());
    }
}

#[test]
fn skill_message_v1_payload_without_approval_metadata_remains_valid() {
    assert_valid(
        "skill-message.schema.json",
        &serde_json::json!({
            "reply": "done",
            "status": "done",
            "finalized": true
        }),
    );
}

#[test]
fn deploy_output_validates() {
    let out = DeployOutput {
        plugin_name: "deal-desk".to_string(),
        label: "v1".to_string(),
        env: "prod".to_string(),
        agent_name: "deal-desk".to_string(),
        agent_id: "a_1".to_string(),
        version_label: "v1".to_string(),
        version_id: "ver_1".to_string(),
        channel: "C123".to_string(),
        bundle_ref: "s3://bundles/x".to_string(),
        bundle_sha256: "abc".to_string(),
        bundle_size_bytes: 4096,
        deployment_id: "dep_1".to_string(),
        deployment_environment: "prod".to_string(),
        deployment_status: "active".to_string(),
    };
    assert_valid("deploy.schema.json", &out.to_json());
}

fn deploy_result_for_target(target: &str) -> serde_json::Value {
    let environment = if target == "prod" { "prod" } else { "dev" };
    DeployOutput {
        plugin_name: "acme-bundle".to_string(),
        label: "v1-test".to_string(),
        env: environment.to_string(),
        agent_name: format!("acme-{target}"),
        agent_id: format!("agent-{target}"),
        version_label: "v1-test".to_string(),
        version_id: format!("version-{target}"),
        channel: if target == "prod" {
            "C0EXAMPLE1".to_string()
        } else {
            "C0EXAMPLE2".to_string()
        },
        bundle_ref: format!("bundles/{target}.tar.gz"),
        bundle_sha256: format!("sha-{target}"),
        bundle_size_bytes: if target == "prod" { 202 } else { 101 },
        deployment_id: format!("deployment-{target}"),
        deployment_environment: environment.to_string(),
        deployment_status: "active".to_string(),
    }
    .to_json()
}

#[test]
fn deploy_all_targets_success_output_validates() {
    let value = serde_json::json!({
        "results": [
            {"target": "dev", "result": deploy_result_for_target("dev")},
            {"target": "prod", "result": deploy_result_for_target("prod")}
        ]
    });
    assert_valid("deploy.schema.json", &value);
}

#[test]
fn deploy_all_targets_failure_outputs_validate() {
    let first_failure = serde_json::json!({
        "failed_target": "dev",
        "stage": "deploy",
        "completed": [],
        "error": "creating the deployment failed with 500",
        "fix": null
    });
    assert_valid("deploy.schema.json", &first_failure);

    let later_failure = serde_json::json!({
        "failed_target": "prod",
        "stage": "deploy",
        "completed": [
            {"target": "dev", "result": deploy_result_for_target("dev")}
        ],
        "error": "creating the deployment failed with 500",
        "fix": null
    });
    assert_valid("deploy.schema.json", &later_failure);

    let connector_failure = serde_json::json!({
        "failed_target": "dev",
        "stage": "connector_sync",
        "completed": [],
        "failed_result": deploy_result_for_target("dev"),
        "error": "connector sync failed",
        "fix": null
    });
    assert_valid("deploy.schema.json", &connector_failure);
}

#[test]
fn diff_output_validates() {
    let mut entries = curie::installation::diff_plan(
        &std::collections::BTreeMap::from([
            ("inference.deploy".to_string(), "true".to_string()),
            ("ui.deploy".to_string(), "false".to_string()),
            ("worker.replicas".to_string(), "2".to_string()),
        ]),
        Some(&serde_json::json!({
            "ui": {"deploy": true},
            "dispatcher": {"slack": {"botToken": "xoxb-live"}},
            "inference": {"deploy": true},
            "agentSandbox": {"runner": {"fakeModel": false}},
        })),
    );
    entries.push(curie::installation::DiffEntry {
        key: "api.githubToken".to_string(),
        kind: curie::installation::DiffKind::Unknown,
        from: Some("<secret>".to_string()),
        to: None,
        unresolved_credential: Some("CURIE_1426_GITHUB_CREDENTIAL".to_string()),
    });
    entries.sort_by(|left, right| left.key.cmp(&right.key));

    let out = curie::installation::DiffOutput {
        unresolved_credentials: vec!["CURIE_1426_GITHUB_CREDENTIAL".to_string()],
        namespace: "acme-bot".to_string(),
        release: "acme-bot".to_string(),
        release_exists: true,
        // Mismatched on purpose: the real cluster ran 0.5.1 against a 0.6.0
        // CLI, and that is the state the warning exists for.
        chart_deployed: Some("curie-0.5.1".to_string()),
        chart_target: "0.6.0".to_string(),
        entries,
        // #1352: the common case is nothing to lose, and the payload must still
        // carry the key so a consumer that reads it cannot mistake "not
        // reported" for "no removals".
        stateful_removals: Vec::new(),
        // #1352: and the same reasoning for the rename discriminator -- an
        // upgrade that renames no object store must say so as `null`, not by
        // omitting the key, or a consumer cannot tell it from a CLI that does
        // not report renames at all.
        migration: None,
    };
    let json = out.to_json();
    assert_valid("diff.schema.json", &json);
    assert_eq!(
        json["stateful_removals"],
        serde_json::json!([]),
        "an empty removal list must still be emitted as an array: {json}"
    );
    assert_eq!(
        json["migration"],
        serde_json::Value::Null,
        "no store rename must still be emitted, as an explicit null: {json}"
    );
    assert!(
        json.get("migration").is_some(),
        "the migration key must be PRESENT, not merely absent-and-read-as-null: {json}"
    );

    // Every classification the schema enumerates must be reachable from a real
    // plan, or the enum is documenting states the code cannot produce.
    let kinds: Vec<&str> = json["entries"]
        .as_array()
        .expect("entries is an array")
        .iter()
        .map(|e| e["kind"].as_str().expect("kind is a string"))
        .collect();
    for expected in [
        "add",
        "change",
        "unchanged",
        "preserved",
        "reset to chart default",
        "unknown",
    ] {
        assert!(
            kinds.contains(&expected),
            "fixture should exercise {expected}: got {kinds:?}"
        );
    }

    // The live bot token is in the fixture; it must not be in the payload.
    let rendered = serde_json::to_string(&json).expect("payload serializes");
    assert!(
        !rendered.contains("xoxb-live"),
        "a live secret reached the --json payload: {rendered}"
    );

    // The mismatch must be machine-readable, not only a human note: an agent
    // consumer gating on this diff has to be able to see that it does not
    // describe a safe apply.
    assert_eq!(json["chart_version_differs"], serde_json::json!(true));

    let entries = json["entries"].as_array().expect("entries is an array");
    let unknown = entries
        .iter()
        .find(|entry| entry["kind"] == "unknown")
        .expect("fixture contains an unknown entry");
    assert_eq!(
        unknown["unresolved_credential"],
        "CURIE_1426_GITHUB_CREDENTIAL"
    );
}

/// #1352: the schema addition has to be EXERCISED, not merely accepted. A
/// removal-carrying payload is the shape an agent consumer gates a destructive
/// apply on, and `RemovalCause` has no serde derive -- `to_json` writes the
/// encoding by hand, so nothing but this test holds the two cause spellings and
/// the conditional `renamed_to` in place.
#[test]
fn diff_output_with_stateful_removals_validates() {
    let out = curie::installation::DiffOutput {
        unresolved_credentials: Vec::new(),
        namespace: "acme-bot".to_string(),
        release: "acme-bot".to_string(),
        release_exists: true,
        chart_deployed: Some("curie-0.6.0".to_string()),
        chart_target: "0.6.0".to_string(),
        entries: vec![curie::installation::DiffEntry {
            key: "worker.replicas".to_string(),
            kind: curie::installation::DiffKind::Change,
            from: Some("1".to_string()),
            to: Some("2".to_string()),
            unresolved_credential: None,
        }],
        stateful_removals: vec![
            // The chart does not render this component at all: a chart version
            // renamed or dropped it, and `--migrate-store` is the remedy.
            curie::ops::StatefulRemoval {
                name: "acme-bot-minio".to_string(),
                component: "minio".to_string(),
                cause: curie::ops::RemovalCause::ComponentGone,
            },
            // The component survives under another resource name: a values
            // difference, whose remedy is declaring `nameOverride` instead.
            curie::ops::StatefulRemoval {
                name: "acme-bot-postgres".to_string(),
                component: "postgres".to_string(),
                cause: curie::ops::RemovalCause::RenamedTo("acme-bot-curie-postgres".to_string()),
            },
        ],
        // The discriminator that makes the `component_gone` remedy actionable:
        // the store IS renamed here, so `--migrate-store` is the operator's
        // path for the minio removal above -- and would still be a dead end for
        // the postgres rename beside it. COMPONENT names, never resource names:
        // `acme-bot-minio` embeds the chart fullname, which a `nameOverride`
        // moves, while `minio` is what every consumer can match on (#1352).
        migration: Some(("minio".to_string(), "rustfs".to_string())),
    };

    let json = out.to_json();
    assert_valid("diff.schema.json", &json);

    let removals = json["stateful_removals"]
        .as_array()
        .expect("stateful_removals is an array");
    assert_eq!(
        removals.len(),
        2,
        "both removals must survive to_json: {json}"
    );

    let gone = &removals[0];
    assert_eq!(gone["name"], "acme-bot-minio");
    assert_eq!(gone["component"], "minio");
    assert_eq!(gone["cause"], "component_gone");
    assert!(
        gone.get("renamed_to").is_none(),
        "a component that is gone has no rename target: {gone}"
    );

    let renamed = &removals[1];
    assert_eq!(renamed["name"], "acme-bot-postgres");
    assert_eq!(renamed["component"], "postgres");
    assert_eq!(renamed["cause"], "renamed");
    assert_eq!(
        renamed["renamed_to"], "acme-bot-curie-postgres",
        "the rename target is the half the operator needs to fix their file: {renamed}"
    );

    // The count an agent gates on has to include the removals, or a payload
    // whose only change is store destruction reads as `changes: 1`.
    assert_eq!(
        json["changes"],
        serde_json::json!(3),
        "one changed entry plus two removals: {json}"
    );

    // `to_json` writes this object by hand too, so nothing but this holds the
    // `from`/`to` spelling in place. Both removals above report the SAME
    // `component_gone`/`renamed` pair whether or not a store moves, so without
    // this object the payload cannot say which of them `--migrate-store` can
    // actually carry -- the ambiguity #1352 exists to close.
    assert_eq!(
        json["migration"],
        serde_json::json!({"from": "minio", "to": "rustfs"}),
        "the store rename must survive to_json as a component pair: {json}"
    );
}

#[test]
fn diff_schema_keeps_unresolved_credential_marker_optional() {
    let schema = load_schema("diff.schema.json");
    let entry_schema = &schema["properties"]["entries"]["items"];
    assert!(
        entry_schema["properties"]["unresolved_credential"].is_object(),
        "degraded entries need a per-entry unresolved credential marker"
    );
    let required = entry_schema["required"]
        .as_array()
        .expect("entry required list");
    assert!(
        !required.iter().any(|name| name == "unresolved_credential"),
        "the per-entry marker must remain optional for known entries"
    );

    let description = schema["properties"]["unresolved_credentials"]["description"]
        .as_str()
        .expect("unresolved credential description");
    assert!(
        !description.to_ascii_lowercase().contains("unaffected"),
        "the schema must disclose degraded entries: {description}"
    );
    assert!(
        description.to_ascii_lowercase().contains("unknown"),
        "the schema must explain the unknown classification: {description}"
    );
}

// #1352: `curie diff --chart` reports the version of the chart it actually
// RENDERS, so it reads that chart's own Chart.yaml rather than this CLI's
// package version. Answered from the real file the flag points at: a stub
// inventing a version here would make the CHART VERSION MISMATCH note this
// run emits a fiction. Shell builtins only -- PATH here holds these two
// stubs and nothing else, so `cat` would silently produce an empty chart.
// This is shell text spliced verbatim into a generated `helm` stub script
// (not a Rust comment), so the explanation travels with the branch into
// every stub that embeds it.
const HELM_SHOW_CHART_STUB_BRANCH: &str = r#"if [ "$1" = show ] && [ "$2" = chart ]; then
    # #1352: `curie diff --chart` reports the version of the chart it actually
    # RENDERS, so it reads that chart's own Chart.yaml rather than this CLI's
    # package version. Answered from the real file the flag points at: a stub
    # inventing a version here would make the CHART VERSION MISMATCH note this
    # run emits a fiction. Shell builtins only -- PATH here holds these two
    # stubs and nothing else, so `cat` would silently produce an empty chart.
    while IFS= read -r chart_line; do
        printf '%s\n' "$chart_line"
    done < "$3/Chart.yaml"
    exit 0
fi
"#;

// #1352: `curie diff` now reads the live StatefulSets before it will render
// the target chart, the same stateful-removal guard `curie apply` runs. An
// empty live list is the "fresh install, nothing at stake" answer, so the
// guard short-circuits before it would ever need the chart's rendered
// StatefulSet specs -- which is why this stub needs no `template` branch,
// unlike the helm stub's `show chart` branch above.
fn write_stateful_probe_stubs(bin_dir: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;

    let kubectl = bin_dir.join("kubectl");
    std::fs::write(
        &kubectl,
        r#"#!/bin/sh
if [ "$1" = get ] && [ "$2" = statefulset ]; then
    printf '%s\n' '{"apiVersion":"v1","items":[],"kind":"List","metadata":{"resourceVersion":""}}'
    exit 0
fi
exit 64
"#,
    )
    .expect("write kubectl stub");
    let mut permissions = std::fs::metadata(&kubectl)
        .expect("kubectl stub metadata")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&kubectl, permissions).expect("make kubectl stub executable");
}

#[test]
fn credentialless_diff_human_output_names_export_and_never_claims_a_reset() {
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::process::Command;

    let temp = tempfile::tempdir().expect("temporary directory");
    let config = temp.path().join("curie.yaml");
    std::fs::write(
        &config,
        concat!(
            "version: 1\n",
            "install:\n",
            "  namespace: acme\n",
            "  release: acme\n",
            "credentials:\n",
            "  model: CURIE_1426_MODEL_CREDENTIAL\n",
        ),
    )
    .expect("write configuration");
    let bin_dir = temp.path().join("bin");
    std::fs::create_dir(&bin_dir).expect("create binary directory");
    let helm = bin_dir.join("helm");
    std::fs::write(
        &helm,
        format!(
            "#!/bin/sh\n{}{}",
            HELM_SHOW_CHART_STUB_BRANCH,
            r#"if [ "$1" = get ] && [ "$2" = values ]; then
    printf '%s\n' '{"agentSandbox":{"runner":{"credentials":"model live","fakeModel":false}},"ui":{"service":{"type":"NodePort"}},"langfuse":{"web":{"service":{"type":"NodePort"}}}}'
    exit 0
fi
if [ "$1" = list ]; then
    printf '%s\n' '[{"name":"acme","chart":"curie-0.6.0"}]'
    exit 0
fi
exit 64
"#
        ),
    )
    .expect("write helm stub");
    let mut permissions = std::fs::metadata(&helm)
        .expect("helm stub metadata")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&helm, permissions).expect("make helm stub executable");

    write_stateful_probe_stubs(&bin_dir);

    let output = Command::new(env!("CARGO_BIN_EXE_curie"))
        .arg("diff")
        .arg("--file")
        .arg(&config)
        // #1352: diff now resolves a chart exactly as apply does, so a dev
        // build with no `charts/curie` under the process cwd errors before
        // ever reaching the credential-leniency behaviour under test. The
        // test binary's cwd is the `cli` crate dir, which has no
        // `charts/curie`, so point at the repository's own chart the same
        // way an operator would with `--chart`.
        .arg("--chart")
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../charts/curie"))
        .env("PATH", &bin_dir)
        .env("CURIE_CONFIG_DIR", temp.path().join("config"))
        .env_remove("CURIE_MODEL")
        .env_remove("CURIE_1426_MODEL_CREDENTIAL")
        .output()
        .expect("run credentialless diff");
    let rendered = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.status.success(),
        "credentialless diff failed: {rendered}"
    );
    assert!(
        rendered.contains("export CURIE_1426_MODEL_CREDENTIAL="),
        "the remedy must name the missing export with an assignment: {rendered}"
    );
    assert!(
        !rendered.contains("`export CURIE_1426_MODEL_CREDENTIAL`"),
        "the remedy must not be a bare export with no value: {rendered}"
    );
    let assignment = rendered
        .split("export CURIE_1426_MODEL_CREDENTIAL=")
        .nth(1)
        .and_then(|rest| rest.split('`').next())
        .map(str::trim)
        .expect("the remedy must contain an assignment value");
    assert!(
        !assignment.is_empty(),
        "the remedy assignment must not be empty: {rendered}"
    );
    assert!(
        !rendered.contains("model live"),
        "the diff must not expose the live credential value: {rendered}"
    );
    assert!(
        !rendered.contains("comparison above is unaffected"),
        "the output must not claim certainty: {rendered}"
    );
    assert!(
        !rendered.contains("Declare it in curie.yaml to keep it"),
        "an already declared credential must not be called a reset: {rendered}"
    );
}

#[test]
fn credentialless_diff_without_a_release_does_not_claim_every_declared_value_is_created() {
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::process::Command;

    let temp = tempfile::tempdir().expect("temporary directory");
    let config = temp.path().join("curie.yaml");
    std::fs::write(
        &config,
        concat!(
            "version: 1\n",
            "install:\n",
            "  namespace: acme\n",
            "  release: acme\n",
            "credentials:\n",
            "  model: CURIE_1426_MODEL_CREDENTIAL\n",
        ),
    )
    .expect("write configuration");
    let bin_dir = temp.path().join("bin");
    std::fs::create_dir(&bin_dir).expect("create binary directory");
    let helm = bin_dir.join("helm");
    std::fs::write(
        &helm,
        format!(
            "#!/bin/sh\n{}{}",
            HELM_SHOW_CHART_STUB_BRANCH,
            r##"if [ "$1" = get ] && [ "$2" = values ]; then
    printf '%s\n' 'Error: release: not found' >&2
    exit 1
fi
if [ "$1" = list ]; then
    printf '%s\n' '[]'
    exit 0
fi
exit 64
"##
        ),
    )
    .expect("write helm stub");
    let mut permissions = std::fs::metadata(&helm)
        .expect("helm stub metadata")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&helm, permissions).expect("make helm stub executable");

    write_stateful_probe_stubs(&bin_dir);

    let output = Command::new(env!("CARGO_BIN_EXE_curie"))
        .arg("diff")
        .arg("--file")
        .arg(&config)
        // #1352: diff now resolves a chart exactly as apply does, so a dev
        // build with no `charts/curie` under the process cwd errors before
        // ever reaching the credential-leniency behaviour under test. The
        // test binary's cwd is the `cli` crate dir, which has no
        // `charts/curie`, so point at the repository's own chart the same
        // way an operator would with `--chart`.
        .arg("--chart")
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../charts/curie"))
        .env("PATH", &bin_dir)
        .env("CURIE_CONFIG_DIR", temp.path().join("config"))
        .env_remove("CURIE_MODEL")
        .env_remove("CURIE_1426_MODEL_CREDENTIAL")
        .output()
        .expect("run credentialless diff without a release");
    let rendered = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.status.success(),
        "credentialless diff failed: {rendered}"
    );
    assert!(
        !rendered.contains("every value below would be created"),
        "an unresolved declared value cannot be described as certainly created: {rendered}"
    );
    assert!(
        rendered.contains("? agentSandbox.runner.credentials: unknown"),
        "the unresolved declared value must be represented as unknown: {rendered}"
    );
}

#[test]
fn seal_output_validates() {
    // Built from a real seal, so the payload is the shape the verb emits.
    let kp = curie::sealing::generate_keypair();
    let sealed = curie::sealing::seal(&kp.public_key, "a-real-looking-token").expect("seals");
    let out = curie::seal::SealOutput {
        connector: "grafana".to_string(),
        env_name: "GRAFANA_TOKEN".to_string(),
        sealed,
        public_key: kp.public_key.clone(),
    };
    let json = out.to_json();
    assert_valid("seal.schema.json", &json);

    // The plaintext must not survive anywhere in the payload -- this is the
    // whole point of the verb, and the payload is what gets committed.
    let rendered = serde_json::to_string(&json).expect("serializes");
    assert!(
        !rendered.contains("a-real-looking-token"),
        "the plaintext reached the --json payload: {rendered}"
    );
}

#[test]
fn doctor_output_validates() {
    // Built through `evaluate` rather than hand-listed, so this validates the
    // shape the real verb emits and not a parallel one.
    let facts = curie::doctor::Facts {
        model_credential: Some("CURIE_CREDENTIALS".to_string()),
        model_credential_source: Some("environment".to_string()),
        // A dated snapshot, so the schema is validated against the pinned
        // branch of the model-pin check rather than its advisory branch. It
        // arrives from the RELEASE DEFAULT rather than the invoking shell,
        // which is the state #1950 is about: the release is what the sandboxes
        // actually boot, and the shell is not a declared producer of the value
        // at all.
        model_shell: None,
        model_release_default: Some("claude-haiku-4-5-20251001".to_string()),
        model_release_key: Some(curie::doctor::ReleaseModelKey::Runner),
        model_release_fake: false,
        model_agent_overrides: vec![],
        model_credential_provider: None,
        docker_ok: true,
        bundle_name: Some("my-agent".to_string()),
        kube_context: Some("minikube".to_string()),
        target: Some(("acme".to_string(), "acme".to_string())),
        release: curie::doctor::ReleaseProbe::Installed {
            chart: "curie-0.6.0".to_string(),
        },
        sandbox_egress_cidrs: vec!["160.79.104.0/23".to_string()],
        sandbox_egress_is_reproducible: true,
        slack_app_token: true,
        slack_bot_token: true,
        clone_credential: Some("github app (app_id=1234567)".to_string()),
        api_exposure: None,
        agents: Some(vec![("bot".to_string(), None)]),
    };
    let checks = curie::doctor::evaluate(&facts);
    let out = curie::doctor::DoctorOutput {
        summary: curie::doctor::summary(&checks),
        checks,
    };
    let json = out.to_json();
    assert_valid("doctor.schema.json", &json);

    // Every state the schema enumerates must be reachable from a real run.
    let states: BTreeSet<&str> = json["checks"]
        .as_array()
        .expect("checks is an array")
        .iter()
        .map(|c| c["state"].as_str().expect("state is a string"))
        .collect();
    assert!(states.contains("ok"), "{states:?}");
    assert!(states.contains("missing"), "{states:?}");

    // `ready` is the field a machine consumer gates on, and it was hardcoded
    // true for long enough that no test ever read it. This fixture has two
    // Missing checks (webhook, repo-binding), so `ready` must be false.
    assert_eq!(
        json["ready"],
        serde_json::Value::Bool(false),
        "a report carrying a missing check is not ready: {json}"
    );
    assert_eq!(
        json["deploys_verified"],
        serde_json::Value::Bool(false),
        "an unbound agent is not a verified deploy path: {json}"
    );

    // This payload is pasted into issues; it must never carry a secret value.
    let rendered = serde_json::to_string(&json).expect("serializes");
    for leaked in ["sk-ant-", "xoxb-", "xapp-", "ghp_", "BEGIN RSA"] {
        assert!(
            !rendered.contains(leaked),
            "{leaked} in payload: {rendered}"
        );
    }
}

/// The other direction of the same gate: a fixture with nothing missing must
/// report `ready: true`, so a constant in either position fails.
#[test]
fn doctor_ready_tracks_the_checks() {
    let facts = curie::doctor::Facts {
        model_credential: Some("CURIE_CREDENTIALS".to_string()),
        model_credential_source: Some("environment".to_string()),
        model_shell: Some("claude-haiku-4-5-20251001".to_string()),
        model_release_default: None,
        model_release_key: None,
        model_release_fake: false,
        model_agent_overrides: vec![],
        model_credential_provider: None,
        docker_ok: true,
        bundle_name: Some("my-agent".to_string()),
        kube_context: Some("minikube".to_string()),
        target: Some(("acme".to_string(), "acme".to_string())),
        release: curie::doctor::ReleaseProbe::Installed {
            chart: "curie-0.6.0".to_string(),
        },
        sandbox_egress_cidrs: vec![],
        sandbox_egress_is_reproducible: true,
        slack_app_token: true,
        slack_bot_token: true,
        clone_credential: Some("github app (secret=gh-app)".to_string()),
        api_exposure: Some("NodePort 30799".to_string()),
        agents: Some(vec![("bot".to_string(), Some("acme/bot".to_string()))]),
    };
    let checks = curie::doctor::evaluate(&facts);
    let out = curie::doctor::DoctorOutput {
        summary: curie::doctor::summary(&checks),
        checks,
    };
    let json = out.to_json();
    assert_valid("doctor.schema.json", &json);
    assert_eq!(
        json["ready"],
        serde_json::Value::Bool(true),
        "a fully wired report is ready: {json}"
    );
    assert_eq!(
        json["deploys_verified"],
        serde_json::Value::Bool(true),
        "all-bound agents mean git-push deploys were verified: {json}"
    );
}

#[test]
fn check_output_validates() {
    // CheckOutput::to_json is `serde_json::to_value(report)`; validate that exact
    // shape against the check schema.
    let report = CheckReport {
        check: "mcp_load".to_string(),
        version: 1,
        plugin_dir: "/bundle".to_string(),
        declared: vec![DeclaredServer {
            name: "srv".to_string(),
            source: ".mcp.json".to_string(),
            form: "command".to_string(),
            authed: false,
        }],
        registered: vec![serde_json::json!({"name": "srv"})],
        matches: vec![CheckMatch {
            declared: "srv".to_string(),
            registered: Some("srv".to_string()),
            connected: true,
            tool_count: 3,
        }],
        verdict: "green".to_string(),
        reasons: vec![],
        hints: vec![],
    };
    assert_valid("check.schema.json", &serde_json::to_value(&report).unwrap());
}

#[test]
fn guide_output_validates() {
    // GuideOutput::to_json is `serde_json::to_value(primer())`.
    let value = serde_json::to_value(curie::guide::primer()).unwrap();
    assert_valid("guide.schema.json", &value);
}

#[test]
fn secrets_list_output_validates() {
    let out = SecretsListOutput {
        names: vec!["ANTHROPIC_API_KEY".to_string()],
        entries: vec![curie::secrets::SecretListEntry {
            name: "ANTHROPIC_API_KEY".to_string(),
            scope: None,
            version: None,
        }],
    };
    assert_valid("secrets.schema.json", &out.to_json());
    assert_valid(
        "secrets.schema.json",
        &SecretsListOutput {
            names: vec![],
            entries: vec![],
        }
        .to_json(),
    );
    let scoped = SecretsListOutput {
        names: vec!["K8S_WRITE_KUBECONFIG".to_string()],
        entries: vec![curie::secrets::SecretListEntry {
            name: "K8S_WRITE_KUBECONFIG".to_string(),
            scope: Some(curie::secrets::SecretScope {
                cluster_identity:
                    "ca:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                release: "curie".to_string(),
                namespace: "curie-test".to_string(),
            }),
            version: Some(1),
        }],
    };
    assert_valid("secrets.schema.json", &scoped.to_json());
    let rendered = scoped.to_json().to_string();
    assert!(!rendered.contains("token"));
    assert!(!rendered.contains("kubeconfig"));
}

#[test]
fn sweep_json_validates() {
    let rows = vec![
        SweepRow {
            model: "opus".to_string(),
            passed: 2,
            completed: 3,
            total: 3,
            plumbing: 0,
        },
        SweepRow {
            model: "never".to_string(),
            passed: 0,
            completed: 0,
            total: 3,
            plumbing: 0,
        },
    ];
    assert_valid(
        "sweep.schema.json",
        &curie::commands::sweep_json(&rows, None),
    );
}

/// The sweep half of #1087 AC2: same reasoning as
/// `eval_json_carries_the_bundle_digest_and_emits_null_when_none_applies`. The
/// sweep is where the digest was a stderr note ONLY, so this is the assertion
/// that makes the criterion confirmable at all.
#[test]
fn sweep_json_carries_the_bundle_digest_and_emits_null_when_none_applies() {
    let rows = vec![SweepRow {
        model: "opus".to_string(),
        passed: 3,
        completed: 3,
        total: 3,
        plumbing: 0,
    }];

    let digest = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
    let with_digest = curie::commands::sweep_json(&rows, Some(digest));
    assert_valid("sweep.schema.json", &with_digest);
    assert_eq!(with_digest["bundle_digest"], digest, "{with_digest}");

    let without = curie::commands::sweep_json(&rows, None);
    assert_valid("sweep.schema.json", &without);
    assert!(
        without.get("bundle_digest").is_some(),
        "the key is always emitted, never omitted: {without}"
    );
    assert!(without["bundle_digest"].is_null(), "{without}");
}

#[test]
fn kill_output_validates_both_variants() {
    let done = KillOutput::Done {
        agent: "deal-desk".to_string(),
        killed: true,
    };
    assert_valid("kill.schema.json", &done.to_json());
    let dry = KillOutput::DryRun(DryRunPlan {
        lines: vec!["POST /kill".to_string()],
    });
    assert_valid("kill.schema.json", &dry.to_json());
}

#[test]
fn resume_output_validates_both_variants() {
    let done = ResumeOutput::Done {
        agent: "deal-desk".to_string(),
        killed: false,
    };
    assert_valid("resume.schema.json", &done.to_json());
    let dry = ResumeOutput::DryRun(DryRunPlan {
        lines: vec!["POST /resume".to_string()],
    });
    assert_valid("resume.schema.json", &dry.to_json());
}

#[test]
fn budget_output_validates_all_variants() {
    let some = BudgetOutput::Done {
        agent: "d".to_string(),
        max_usd_per_day: Some(5.0),
    };
    assert_valid("budget.schema.json", &some.to_json());
    let none = BudgetOutput::Done {
        agent: "d".to_string(),
        max_usd_per_day: None,
    };
    assert_valid("budget.schema.json", &none.to_json());
    let dry = BudgetOutput::DryRun(DryRunPlan {
        lines: vec!["PUT /budget".to_string()],
    });
    assert_valid("budget.schema.json", &dry.to_json());
}

#[test]
fn reset_thread_output_validates_both_variants() {
    let done = ResetThreadOutput::Done {
        agent: "d".to_string(),
        thread_key: "C1:U1".to_string(),
        requested: true,
        released: false,
    };
    assert_valid("reset-thread.schema.json", &done.to_json());
    let dry = ResetThreadOutput::DryRun(DryRunPlan {
        lines: vec!["POST /reset".to_string()],
    });
    assert_valid("reset-thread.schema.json", &dry.to_json());
}

#[test]
fn delete_output_validates_both_variants() {
    let done = DeleteOutput::Done {
        agent: "d".to_string(),
    };
    assert_valid("delete.schema.json", &done.to_json());
    let dry = DeleteOutput::DryRun(DryRunPlan {
        lines: vec!["DELETE /agent".to_string()],
    });
    assert_valid("delete.schema.json", &dry.to_json());
}

#[test]
fn versions_output_validates_all_variants() {
    let version = Version {
        id: "ver_1".to_string(),
        version_label: "v1".to_string(),
        commit_sha: Some("deadbeef".to_string()),
        bundle_sha256: Some("abc".to_string()),
        created_by: Some("alice".to_string()),
        created_at: Some("2026-01-01T00:00:00Z".to_string()),
        agent_id: Some("a_1".to_string()),
        bundle_ref: Some("s3://x".to_string()),
    };
    let list = VersionsOutput::List {
        agent: "d".to_string(),
        versions: vec![version],
    };
    assert_valid("versions.schema.json", &list.to_json());
    let empty = VersionsOutput::Empty {
        agent: "d".to_string(),
    };
    assert_valid("versions.schema.json", &empty.to_json());
    let dry = VersionsOutput::DryRun(DryRunPlan {
        lines: vec!["GET /versions".to_string()],
    });
    assert_valid("versions.schema.json", &dry.to_json());
}

#[test]
fn memory_output_validates_all_variants() {
    let entries = vec![MemoryEntry {
        index: 0,
        content: "prefer terse".to_string(),
        version: 1,
        provenance: Default::default(),
    }];
    let list = MemoryOutput::List {
        agent: "d".to_string(),
        entries,
    };
    assert_valid("memory.schema.json", &list.to_json());
    let empty = MemoryOutput::Empty {
        agent: "d".to_string(),
    };
    assert_valid("memory.schema.json", &empty.to_json());
    let dry = MemoryOutput::DryRun(DryRunPlan {
        lines: vec!["GET /memory".to_string()],
    });
    assert_valid("memory.schema.json", &dry.to_json());
    let added = MemoryOutput::Added {
        agent: "d".to_string(),
        index: 0,
        content: "prefer terse".to_string(),
        source: "operator".to_string(),
        fresh_session_required: true,
    };
    assert_valid("memory.schema.json", &added.to_json());
}

fn approval_record() -> ApprovalRecord {
    ApprovalRecord {
        id: "ap_1".to_string(),
        author: "U1".to_string(),
        route: Some("Bash".to_string()),
        gate_kind: Some("tool".to_string()),
        granted_tool: None,
        status: "pending".to_string(),
        conversation_id: "C1".to_string(),
        summary: "run tests".to_string(),
        expires_at: Some("2026-01-01T00:00:00Z".to_string()),
        resolved_by: None,
        // #1078: the persisted card location for a route-bound approval.
        card_channel: Some("CFINANCE01".to_string()),
    }
}

#[test]
fn approvals_output_validates_all_variants() {
    let gates = ApprovalsOutput::Gates {
        agent: "d".to_string(),
        gated_tools: vec!["Bash".to_string()],
        manifest_unreadable: None,
    };
    assert_valid("approvals.schema.json", &gates.to_json());
    let pending = ApprovalsOutput::Pending {
        agent: "d".to_string(),
        records: vec![approval_record()],
        truncated: false,
    };
    assert_valid("approvals.schema.json", &pending.to_json());
    let resolved = ApprovalsOutput::Resolved {
        record: approval_record(),
    };
    assert_valid("approvals.schema.json", &resolved.to_json());
    let routes = ApprovalsOutput::Routes {
        agent: "d".to_string(),
        routes: serde_json::from_value(serde_json::json!({
            "finance": {
                "resolution": {"kind": "slack", "address": "C0EXAMPLE1"}
            }
        }))
        .expect("the route response carries its required resolution"),
    };
    let routes_json = routes.to_json();
    assert_eq!(
        routes_json["routes"]["finance"]["resolution"]["address"],
        "C0EXAMPLE1"
    );
    assert_valid("approvals.schema.json", &routes_json);
    let dry = ApprovalsOutput::DryRun(DryRunPlan {
        lines: vec!["GET /approvals".to_string()],
    });
    assert_valid("approvals.schema.json", &dry.to_json());
}

#[test]
fn skill_approvals_output_validates_both_variants() {
    let gates = SkillApprovalsOutput::Gates {
        gates: vec![("Bash".to_string(), "approval".to_string())],
    };
    assert_valid("skill-approvals.schema.json", &gates.to_json());
    let env = SkillApprovalsOutput::Env {
        env: "CURIE_APPROVALS=Bash".to_string(),
        restart: "curie skill up --replace".to_string(),
        bundle_note: "declared in .claude-plugin".to_string(),
    };
    assert_valid("skill-approvals.schema.json", &env.to_json());
}

#[test]
fn comms_output_validates_both_variants() {
    let done = CommsOutput::Done { connected: true };
    assert_valid("comms.schema.json", &done.to_json());
    let dry = CommsOutput::DryRun(DryRunPlan {
        lines: vec!["helm upgrade".to_string()],
    });
    assert_valid("comms.schema.json", &dry.to_json());
}

#[test]
fn local_up_output_validates_both_variants() {
    let up = LocalUpOutput::Up {
        endpoints: vec![("Curie API".to_string(), "http://localhost:8155".to_string())],
        slack: false,
    };
    assert_valid("local-up.schema.json", &up.to_json());
    let dry = LocalUpOutput::DryRun(DryRunPlan {
        lines: vec!["docker compose up".to_string()],
    });
    assert_valid("local-up.schema.json", &dry.to_json());
}

#[test]
fn local_rebuild_output_validates_both_variants() {
    let rebuilt = LocalRebuildOutput::Rebuilt {
        service: "worker".to_string(),
        model_mode: ModelMode::LiveFromCredential,
    };
    assert_valid("local-rebuild.schema.json", &rebuilt.to_json());
    let dry = LocalRebuildOutput::DryRun(DryRunPlan {
        lines: vec!["docker compose build".to_string()],
    });
    assert_valid("local-rebuild.schema.json", &dry.to_json());
}

#[test]
fn local_status_output_validates_both_variants() {
    let services = LocalStatusOutput::Services {
        rows: vec!["worker  Up 2 minutes".to_string()],
    };
    assert_valid("local-status.schema.json", &services.to_json());
    let dry = LocalStatusOutput::DryRun(DryRunPlan {
        lines: vec!["docker compose ps".to_string()],
    });
    assert_valid("local-status.schema.json", &dry.to_json());
}

#[test]
fn local_down_output_validates_all_variants() {
    let down = LocalDownOutput::Down {
        volumes_wiped: true,
        reaped: 2,
    };
    assert_valid("local-down.schema.json", &down.to_json());
    assert_valid(
        "local-down.schema.json",
        &LocalDownOutput::Aborted.to_json(),
    );
    let dry = LocalDownOutput::DryRun(DryRunPlan {
        lines: vec!["docker compose down".to_string()],
    });
    assert_valid("local-down.schema.json", &dry.to_json());
}

#[test]
fn cluster_up_output_validates_both_variants() {
    let up = ClusterUpOutput::Up {
        namespace: "curie".to_string(),
        release: "curie".to_string(),
    };
    assert_valid("cluster-up.schema.json", &up.to_json());
    let dry = ClusterUpOutput::DryRun(DryRunPlan {
        lines: vec!["helm upgrade".to_string()],
    });
    assert_valid("cluster-up.schema.json", &dry.to_json());
}

#[test]
fn cluster_status_output_validates_both_variants() {
    let status = ClusterStatus {
        namespace: "curie".to_string(),
        revision: "3".to_string(),
        release_state: "deployed".to_string(),
        release_found: true,
        release_missing_note: None,
        pods: vec![PodRow {
            name: "curie-worker-0".to_string(),
            ready: "1/1".to_string(),
            status: "Running".to_string(),
        }],
        ready: 1,
        total: 1,
        unhealthy: vec![],
        pods_listed: true,
        urls: vec![],
        upgrade: curie::ops::UpgradeStatusView::idle(Some("0.8.6".into())),
    };
    let out = ClusterStatusOutput::Status(Box::new(status));
    assert_valid("cluster-status.schema.json", &out.to_json());
    let dry = ClusterStatusOutput::DryRun(DryRunPlan {
        lines: vec!["helm status".to_string()],
    });
    assert_valid("cluster-status.schema.json", &dry.to_json());
}

#[test]
fn cluster_down_output_validates_all_variants() {
    let down = ClusterDownOutput::Down {
        release_was_absent: false,
    };
    assert_valid("cluster-down.schema.json", &down.to_json());
    assert_valid(
        "cluster-down.schema.json",
        &ClusterDownOutput::Aborted.to_json(),
    );
    let dry = ClusterDownOutput::DryRun(DryRunPlan {
        lines: vec!["helm uninstall".to_string()],
    });
    assert_valid("cluster-down.schema.json", &dry.to_json());
}

#[test]
fn connector_build_output_validates_empty_and_populated() {
    // Empty is a real emission, not an accident: `curie build` on a bundle
    // declaring nothing to build still prints one object (#485).
    let empty = ConnectorBuildOutput { connectors: vec![] };
    assert_valid("build.schema.json", &empty.to_json());
    let built = ConnectorBuildOutput {
        connectors: vec![ConnectorBuildRecord {
            name: "tempo".to_string(),
            image: format!("ghcr.io/acme-corp/tempo@sha256:{}", "a".repeat(64)),
            delivery: curie::connector_build::Delivery::Registry,
            platforms: vec!["linux/amd64".to_string(), "linux/arm64".to_string()],
            source_digest: "b".repeat(64),
        }],
    };
    assert_valid("build.schema.json", &built.to_json());
}

#[test]
fn cluster_rollback_output_validates_all_variants() {
    let dry = ClusterRollbackOutput::DryRun(DryRunPlan {
        lines: vec!["helm rollback".to_string()],
    });
    assert_valid("cluster-rollback.schema.json", &dry.to_json());
    assert_valid(
        "cluster-rollback.schema.json",
        &ClusterRollbackOutput::Aborted.to_json(),
    );
    let rolled_back = ClusterRollbackOutput::RolledBack {
        from_revision: 4,
        to_revision: 3,
        skipped: vec![],
        forced: false,
    };
    assert_valid("cluster-rollback.schema.json", &rolled_back.to_json());
    let forced = ClusterRollbackOutput::RolledBack {
        from_revision: 5,
        to_revision: 2,
        skipped: vec![4, 3],
        forced: true,
    };
    assert_valid("cluster-rollback.schema.json", &forced.to_json());
}

#[test]
fn cluster_upgrade_output_validates_dry_run_success_and_failure() {
    let dry = ClusterUpgradeOutput::DryRun(DryRunPlan {
        lines: vec!["phase plan: 0.8.6 -> 0.9.0".to_string()],
    });
    assert_valid("cluster-upgrade.schema.json", &dry.to_json());
    let succeeded = ClusterUpgradeOutput::Completed {
        status: "succeeded".into(),
        phase: "commit".into(),
        target_version: "0.9.0".into(),
        from_version: Some("0.8.6".into()),
        known_good_version: Some("0.9.0".into()),
        resumed: false,
        previous_serving: true,
        unchanged: false,
        plan: vec!["phase plan: 0.8.6 -> 0.9.0".into()],
        convergence: Some(Box::new(curie::ops::Convergence {
            exact: true,
            images: true,
            generations: true,
            replicas: true,
            unavailable_zero: true,
            hooks_healthy: true,
            queues_drained: true,
            manifest_matches: true,
            observed_images: Vec::new(),
        })),
        canary: Some(curie::ops::Canary { passed: true }),
        fail_forward: None,
    };
    assert_valid("cluster-upgrade.schema.json", &succeeded.to_json());
    // ADR-0101: an optional observation field versions the closed schema,
    // while the same new schema must still accept the older payload shape.
    let mut observed = succeeded.to_json();
    observed["convergence"]["observed_images"] = serde_json::json!([{
        "workload": "acme-bot-api",
        "pod": "acme-bot-api-example",
        "container": "api",
        "image": "example.com/acme-api:0.9.0",
        "image_id": "containerd://sha256:example"
    }]);
    assert_valid("cluster-upgrade.schema.json", &observed);
    let schema = load_schema("cluster-upgrade.schema.json");
    assert_eq!(
        schema["$id"],
        "https://schemas.curietech.ai/cli/cluster-upgrade/v1.1.json"
    );
    let check = validator(&schema);
    for invalid in [
        serde_json::json!("not an observation array"),
        serde_json::json!([{"workload": "acme-bot-api"}]),
        serde_json::json!([{"workload": "acme-bot-api", "pod": "pod", "container": "api", "image": 9, "image_id": "opaque"}]),
        serde_json::json!([{"workload": "acme-bot-api", "pod": "pod", "container": "api", "image": "example.com/acme-api:0.9.0", "image_id": "opaque", "unexpected": true}]),
    ] {
        let mut bad = observed.clone();
        bad["convergence"]["observed_images"] = invalid;
        assert!(
            !check.is_valid(&bad),
            "invalid image observation accepted: {bad}"
        );
    }
    let mut old = observed.clone();
    old["convergence"]
        .as_object_mut()
        .unwrap()
        .remove("observed_images");
    assert!(
        check.is_valid(&old),
        "new optional field must not invalidate old output: {old}"
    );

    let failed = ClusterUpgradeOutput::Completed {
        status: "failed".into(),
        phase: "canary".into(),
        target_version: "0.9.0".into(),
        from_version: Some("0.8.6".into()),
        known_good_version: Some("0.8.6".into()),
        resumed: false,
        previous_serving: true,
        unchanged: false,
        plan: vec!["phase plan: 0.8.6 -> 0.9.0".into()],
        convergence: Some(Box::new(curie::ops::Convergence {
            exact: true,
            images: true,
            generations: true,
            replicas: true,
            unavailable_zero: true,
            hooks_healthy: true,
            queues_drained: true,
            manifest_matches: true,
            observed_images: Vec::new(),
        })),
        canary: Some(curie::ops::Canary { passed: false }),
        fail_forward: Some(curie::ops::FailForward {
            command: "curie cluster rollback --yes".into(),
            reason: "canary failed".into(),
        }),
    };
    assert_valid("cluster-upgrade.schema.json", &failed.to_json());
    let mut bad = succeeded.to_json();
    bad["canary"]["passed"] = serde_json::json!(false);
    let schema = load_schema("cluster-upgrade.schema.json");
    let v = validator(&schema);
    assert!(
        !v.is_valid(&bad),
        "success without a passing canary must not validate"
    );
}

#[test]
fn a_new_family_schema_gate_has_teeth() {
    // negative control for the #634 schemas: stripping a required key must be
    // rejected, proving these schemas discriminate (not vacuous `true`).
    let schema = load_schema("kill.schema.json");
    let v = validator(&schema);
    let mut value = KillOutput::Done {
        agent: "d".to_string(),
        killed: true,
    }
    .to_json();
    value.as_object_mut().unwrap().remove("killed");
    assert!(
        !v.is_valid(&value),
        "kill schema must reject a Done object missing the required `killed` key"
    );
}
