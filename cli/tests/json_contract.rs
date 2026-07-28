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
use curie_aci_protocol::SessionStatus;

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
    let value = status_json("http://127.0.0.1:8787", &SessionStatus::Done);
    let v = validator(&schema);
    assert!(
        v.is_valid(&value),
        "status_json output must validate against status.schema.json: {value}"
    );
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
    let value = eval_json(&results);
    let v = validator(&schema);
    assert!(
        v.is_valid(&value),
        "eval_json output must validate against eval.schema.json: {value}"
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
    let value = eval_json(&results);
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
    let value = eval_json(&results);
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
        "http://localhost:8155/api/",
    );
    assert!(
        v.is_valid(&with_channel),
        "message_dry_run_json (with channel) must validate: {with_channel}"
    );
    assert_eq!(with_channel["dry_run"], serde_json::json!(true));
    assert_eq!(with_channel["target"], serde_json::json!("local"));
    assert_eq!(with_channel["channel"], serde_json::json!("C123"));
    // Null channel (cluster target, sole-agent resolution).
    let no_channel =
        message_dry_run_json("cluster", "curie:turns", None, "http://10.1.2.3:8155/api/");
    assert!(
        v.is_valid(&no_channel),
        "message_dry_run_json (no channel) must validate: {no_channel}"
    );
    assert!(
        no_channel["channel"].is_null(),
        "omitted channel must be JSON null: {no_channel}"
    );
    assert_eq!(no_channel["target"], serde_json::json!("cluster"));
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
            message_dry_run_json("local", "s", Some("C1"), "http://x/api/"),
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
fn eval_schema_gate_has_teeth() {
    // negative control: proves the schema gate discriminates
    let schema = load_schema("eval.schema.json");
    let results = vec![(
        "only".to_string(),
        CaseOutcome::Pass,
        1.0_f64,
        "ok".to_string(),
    )];
    let mut value = eval_json(&results);
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
    ApprovalsOutput, BudgetOutput, BumpVersionOutput, CheckMatch, CheckReport, DeclaredServer,
    DeleteOutput, DeployOutput, KillOutput, ListAgentsOutput, LocalAgentSummary, MemoryOutput,
    ResetThreadOutput, ResumeOutput, SkillApprovalsOutput, SkillMessageOutput, SweepRow,
    VersionsOutput,
};
use curie::comms::CommsOutput;
use curie::local::{
    LocalDownOutput, LocalRebuildOutput, LocalStatusOutput, LocalUpOutput, ModelMode,
};
use curie::ops::{ClusterDownOutput, ClusterStatus, ClusterStatusOutput, ClusterUpOutput, PodRow};
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
fn skill_message_output_validates() {
    let out = SkillMessageOutput {
        reply: "the answer is 42".to_string(),
        status: "done".to_string(),
        finalized: true,
    };
    assert_valid("skill-message.schema.json", &out.to_json());
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
    };
    assert_valid("secrets.schema.json", &out.to_json());
    assert_valid(
        "secrets.schema.json",
        &SecretsListOutput { names: vec![] }.to_json(),
    );
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
    assert_valid("sweep.schema.json", &curie::commands::sweep_json(&rows));
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

// ─────────────────────────────────────────────────────────────────────────────
// #1040: `curie <tier> info`. The report is built by the REAL `info::discover`
// pass (never a hand-written fixture) so this check cannot drift from what the
// command emits. Because `info.schema.json` sets `additionalProperties: false`
// everywhere and `diagnostic.kind` is a closed enum, this is what stops a field
// or a kind being added to Rust without the schema moving with it.
// ─────────────────────────────────────────────────────────────────────────────

use curie::info::{discover, BundleOrigin, BundleView, InfoOutput};

/// The ten closed `kind` values. Duplicated from the schema on purpose: the
/// assertion below compares this list against the schema's own enum, so a value
/// removed or renamed on either side is a red test rather than a silent contract
/// break for every consumer that switched on it.
const INFO_DIAGNOSTIC_KINDS: &[&str] = &[
    "manifest",
    "skill",
    "mcp",
    "evals",
    "secret",
    "boot_env",
    "approval_gate",
    "artifact",
    "state",
    "deployed",
];

fn info_deployed_origin() -> BundleOrigin {
    BundleOrigin::Deployed {
        agent: "demo".to_string(),
        agent_id: "a_1".to_string(),
        version_id: "ver_1".to_string(),
        version_label: "v1".to_string(),
        environment: "prod".to_string(),
        bundle_sha256: Some("abc123".to_string()),
        channel: "C0DEMO".to_string(),
    }
}

const INFO_MANIFEST: &str = r#"{
  "name": "contract-bundle",
  "version": "0.1.0",
  "description": "A bundle exercising the info report contract.",
  "secrets": ["GITHUB_PERSONAL_ACCESS_TOKEN"],
  "approvalPolicy": { "gates": [{ "gate": "Bash", "route": "slack" }] }
}"#;

const INFO_SKILL_MD: &str = "---\nname: sample\ndescription: A sample skill.\nallowed-tools:\n  - WebSearch\n---\n\n# Sample\n";

const INFO_CASES_JSON: &str = r#"{"name": "contract-bundle", "cases": [{"id": "c1", "input": "hi", "grader": {"kind": "contains", "expected": "hi"}}]}"#;

fn info_files() -> Vec<(String, String)> {
    vec![
        (
            ".claude-plugin/plugin.json".to_string(),
            INFO_MANIFEST.to_string(),
        ),
        (
            ".mcp.json".to_string(),
            r#"{"mcpServers": {"github": {"command": "mcp-server-github", "env": {"GITHUB_PERSONAL_ACCESS_TOKEN": "${GITHUB_PERSONAL_ACCESS_TOKEN}"}}}}"#
                .to_string(),
        ),
        (
            "skills/sample/SKILL.md".to_string(),
            INFO_SKILL_MD.to_string(),
        ),
        (
            "evals/cases.json".to_string(),
            INFO_CASES_JSON.to_string(),
        ),
    ]
}

/// Write the same bundle to a real directory so the SKILL-tier branch (with its
/// own inverted sentinels: a real `bundle.root`, an unavailable
/// `bundle.deployed`) is validated too, not just the deployed one.
fn info_disk_bundle() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    for (rel, content) in info_files() {
        let path = dir.path().join(&rel);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(&path, content).expect("write bundle file");
    }
    dir
}

#[test]
fn info_json_validates_against_info_schema() {
    let schema = load_schema("info.schema.json");
    let v = validator(&schema);

    // (a) A populated SKILL-tier report: a real filesystem root, a resolved
    // skill/mcp/gates/evals inventory, and the deployed-only facts sentinelled.
    let dir = info_disk_bundle();
    let disk_view = BundleView::from_disk(dir.path()).expect("from_disk over a written bundle");
    let disk = InfoOutput::Report(discover(&disk_view)).to_json();
    assert!(
        v.is_valid(&disk),
        "a skill-tier info report must validate against info.schema.json: {disk}"
    );

    // (b) The DEPLOYED tier, exercising the inversion: `bundle.root` and `model`
    // sentinelled, `bundle.deployed` and `channel` real.
    let deployed_view = BundleView::from_files(info_deployed_origin(), info_files());
    let deployed = InfoOutput::Report(discover(&deployed_view)).to_json();
    assert!(
        v.is_valid(&deployed),
        "a deployed-tier info report must validate against info.schema.json: {deployed}"
    );
    assert_eq!(
        deployed["bundle"]["root"]["available"],
        serde_json::json!(false),
        "the deployed branch must exercise the inverted sentinel, not skip it: {deployed}"
    );
    assert_eq!(
        disk["bundle"]["deployed"]["available"],
        serde_json::json!(false),
        "the skill branch must exercise the opposite inversion: {disk}"
    );

    // (d) The dry-run branch of the family's `oneOf`.
    let dry = InfoOutput::DryRun(DryRunPlan {
        lines: vec![
            "GET /agents/<id>/deployments".to_string(),
            "GET /agents/<id>/versions/<vid>/files".to_string(),
        ],
    })
    .to_json();
    assert!(
        v.is_valid(&dry),
        "InfoOutput::DryRun must match the schema's dryRun branch: {dry}"
    );
}

/// (c) The all-sentinel report carrying a diagnostic of EVERY one of the ten
/// closed `kind` values, plus the closed-enum gate itself. `kind` is the coarse
/// axis a consumer switches on and is guaranteed a total match at v1, so the
/// schema's enum and the registry must agree exactly.
#[test]
fn info_schema_accepts_every_diagnostic_kind_and_the_enum_stays_closed() {
    let schema = load_schema("info.schema.json");

    // The schema's own enum is the contract; this list is the expectation.
    let declared: BTreeSet<String> = schema["$defs"]["diagnostic"]["properties"]["kind"]["enum"]
        .as_array()
        .expect("diagnostic.kind must be a closed enum in info.schema.json")
        .iter()
        .map(|v| v.as_str().expect("every kind is a string").to_string())
        .collect();
    let expected: BTreeSet<String> = INFO_DIAGNOSTIC_KINDS
        .iter()
        .map(|k| (*k).to_string())
        .collect();
    assert_eq!(
        declared, expected,
        "removing or renaming a `kind` is a BREAKING change requiring info v2; \
         adding one is additive and belongs in both this list and the schema"
    );

    // A report shaped entirely of sentinels, carrying one diagnostic per kind.
    let v = validator(&schema);
    let all_kinds = all_sentinel_report_with_every_kind();
    assert!(
        v.is_valid(&all_kinds),
        "an all-sentinel report carrying every kind must validate: {all_kinds}"
    );

    // Teeth: an unknown kind must be REJECTED (that is what "closed" means), and
    // a diagnostic missing one of its seven required keys must be rejected too.
    let mut bad_kind = all_kinds.clone();
    bad_kind["diagnostics"][0]["kind"] = serde_json::json!("not_a_kind");
    assert!(
        !v.is_valid(&bad_kind),
        "info.schema.json must reject a diagnostic kind outside the closed enum"
    );
    let mut missing_key = all_kinds.clone();
    missing_key["diagnostics"][0]
        .as_object_mut()
        .expect("object")
        .remove("looked_in");
    assert!(
        !v.is_valid(&missing_key),
        "info.schema.json must reject a diagnostic missing a required key"
    );
    // And an unknown top-level field, since additionalProperties is false.
    let mut extra = all_kinds.clone();
    extra
        .as_object_mut()
        .expect("object")
        .insert("content".to_string(), serde_json::json!("secret bytes"));
    assert!(
        !v.is_valid(&extra),
        "info.schema.json must reject an undeclared top-level field; there is no \
         `content` field anywhere in this contract by design"
    );
}

/// A REAL deployed-tier report (so every field name comes from the
/// implementation, not from a guess) with its four `oneOf` inventory blocks
/// driven to the `unresolved` sentinel and its `diagnostics` replaced by one
/// entry of every closed `kind`. Only the two documented sentinel shapes and the
/// seven-key diagnostic shape are written by hand here; both are pinned by the
/// contract itself.
fn all_sentinel_report_with_every_kind() -> serde_json::Value {
    let view = BundleView::from_files(info_deployed_origin(), info_files());
    let mut report = InfoOutput::Report(discover(&view)).to_json();

    let unresolved = |reason: &str| serde_json::json!({ "resolved": false, "reason": reason });
    report["skills"] = unresolved("the manifest could not be parsed");
    report["mcp_servers"] = unresolved(".mcp.json is not valid JSON");
    report["approval_gates"] = unresolved("approvalPolicy.gates[0] has no route");
    report["evals"] = unresolved("evals/cases.json is absent");

    report["diagnostics"] = serde_json::Value::Array(
        INFO_DIAGNOSTIC_KINDS
            .iter()
            .map(|kind| {
                serde_json::json!({
                    "code": format!("{kind}.example"),
                    "kind": kind,
                    "candidate": "skills/example",
                    "looked_for": "SKILL.md",
                    "looked_in": ["skills/example/SKILL.md"],
                    "reason": "the candidate did not conform",
                    "fix": null,
                })
            })
            .collect(),
    );
    report
}
