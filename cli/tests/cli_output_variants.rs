//! Every variant of every `CliOutput` **enum** must have its `to_json()`
//! validated against the schema `cli/schema/index.json` maps it to (issue #965,
//! follow-up to #955).
//!
//! #955 gave `MessageOutcomeOutput` a per-variant exhaustiveness gate, but wired
//! it to that one enum **by name** -- 1 of the 19 `CliOutput` enums. For the other
//! 18 the escape that caused #955 stayed open: an arm whose `to_json()` inlines a
//! payload that does not match the mapped schema fails nothing.
//!
//! This file closes that by discovering the population instead of naming it, in
//! the shape `support/schema_inventory.rs` already uses:
//!
//! 1. `schema_inventory::cli_output_impls` finds every `impl CliOutput for T` in
//!    `cli/src`.
//! 2. `enum_variants::variant_names` reports T's variants; a T with none is a
//!    struct, not an enum, and is out of scope here (its single `to_json()` is
//!    covered by the per-result tests in `json_contract.rs`).
//! 3. `SAMPLES` below carries one constructed instance per variant. The two
//!    coverage tests assert the registry matches the discovered set **by name**
//!    in both directions, so a new enum or a new variant fails loudly rather than
//!    passing vacuously.
//!
//! Why a registry and not more `syn`: discovery is purely syntactic (which is why
//! it is derived), but *constructing* a variant is not -- that needs the field
//! types, which is type inference, the same wall `support/emit_parity.rs`
//! documents. The registry is the seam that supplies instances; the AST supplies
//! the expectation. Neither can drift silently, because each is checked against
//! the other.
//!
//! No production type was widened for test reach (an explicit #965 acceptance
//! criterion): all 19 enums are already `pub` in `pub mod`s, and the two payloads
//! that are not directly constructible here are built through their own
//! `Deserialize` impls rather than by exposing fields.

use std::collections::{BTreeMap, BTreeSet};

use curie::api::{ApprovalRecord, MemoryEntry, Version};
use curie::commands::{
    ApprovalsOutput, BudgetOutput, DeleteOutput, KillOutput, MemoryOutput, ResetThreadOutput,
    ResumeOutput, SkillApprovalsOutput, VersionsOutput,
};
use curie::comms::CommsOutput;
use curie::info::{
    ArtifactRow, BootEnvRow, BundleInfo, CredentialInfo, Diagnostic, DiagnosticKind, GateRow,
    InfoOutput, InfoReport, Maybe, McpLoad, McpRow, ModelInfo, SecretRow, SecretsBlock, SkillRow,
    Unavailable, Unresolved,
};
use curie::local::{
    LocalDownOutput, LocalRebuildOutput, LocalStatusOutput, LocalUpOutput, ModelMode,
};
use curie::message::MessageOutcomeOutput;
use curie::observability::{Endpoint, ObservabilityOutput};
use curie::ops::{ClusterDownOutput, ClusterStatus, ClusterStatusOutput, ClusterUpOutput};
use curie::ui::{CliOutput, DryRunPlan};

#[path = "support/enum_variants.rs"]
mod enum_variants;
// Only `cli_output_impls` is used here; the rest of the module serves
// `schema_inventory.rs`'s own test. Each `#[path] mod` include is a separate
// compilation unit, so the unused remainder is dead code in THIS one and would
// fail `clippy -D warnings`. Scoped to this include rather than annotating the
// shared file, so the other includers keep their real dead-code coverage.
#[allow(dead_code)]
#[path = "support/schema_inventory.rs"]
mod schema_inventory;

// ---------------------------------------------------------------------------
// Discovery: the expectation side, derived from the source AST.
// ---------------------------------------------------------------------------

fn cli_srcs() -> Vec<(String, String)> {
    let dir = format!("{}/src", env!("CARGO_MANIFEST_DIR"));
    let mut out = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("read cli/src") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            out.push((name, std::fs::read_to_string(&path).expect("read source")));
        }
    }
    assert!(!out.is_empty(), "cli/src must contain .rs sources");
    out
}

/// Every `impl CliOutput for T` where T is declared as an `enum`, mapped to its
/// variant names. Purely syntactic, so it cannot drift from the source.
fn discovered_enums() -> BTreeMap<String, BTreeSet<String>> {
    let srcs = cli_srcs();
    let borrowed: Vec<(&str, &str)> = srcs.iter().map(|(n, s)| (n.as_str(), s.as_str())).collect();
    let impls = schema_inventory::cli_output_impls(&borrowed);
    assert!(
        !impls.is_empty(),
        "no `impl CliOutput for T` found in cli/src: discovery is broken, so every \
         assertion below would pass vacuously"
    );

    let mut found = BTreeMap::new();
    for name in impls {
        // An impl target with no `enum <name>` declaration is a struct; the
        // per-variant question does not apply to it.
        let mut variants = BTreeSet::new();
        for (_, src) in &srcs {
            variants.extend(enum_variants::variant_names(src, &name));
        }
        if !variants.is_empty() {
            found.insert(name, variants);
        }
    }
    assert!(
        found.len() > 1,
        "expected many CliOutput enums, found {}: discovery regressed to the \
         single-enum scope #965 exists to replace",
        found.len()
    );
    found
}

// ---------------------------------------------------------------------------
// Samples: the instance side. One constructed value per variant.
// ---------------------------------------------------------------------------

/// `(variant name as spelled in the enum, that variant's `to_json()`)`.
type VariantJson = (&'static str, serde_json::Value);

/// Pair each constructed sample with the variant name it stands for. The name is
/// written next to the constructor, so a mismatch is visible in review and a
/// missing variant is caught by the coverage test rather than by inspection.
macro_rules! samples {
    ($( $variant:literal => $ctor:expr ),* $(,)?) => {
        vec![ $( ($variant, $ctor.to_json()) ),* ]
    };
}

fn plan() -> DryRunPlan {
    DryRunPlan {
        lines: vec!["a planned step".to_string()],
    }
}

fn version() -> Version {
    serde_json::from_value(serde_json::json!({
        "id": "11111111-1111-4111-8111-111111111111",
        "version_label": "v1",
        "commit_sha": "abc1234",
        "created_at": "2026-01-01T00:00:00Z",
    }))
    .expect("Version mirror deserializes from its own wire shape")
}

fn memory_entry() -> MemoryEntry {
    serde_json::from_value(serde_json::json!({
        "index": 0,
        "content": "remembered fact",
        "version": 1,
    }))
    .expect("MemoryEntry mirror deserializes from its own wire shape")
}

fn approval_record() -> ApprovalRecord {
    serde_json::from_value(serde_json::json!({
        "id": "22222222-2222-4222-8222-222222222222",
        "author": "U123",
        "status": "pending",
        "conversation_id": "1700000000.000100",
        "summary": "approve the deploy",
    }))
    .expect("ApprovalRecord mirror deserializes from its own wire shape")
}

fn cluster_status() -> Box<ClusterStatus> {
    Box::new(ClusterStatus {
        namespace: "curie".to_string(),
        revision: "1".to_string(),
        release_state: "deployed".to_string(),
        release_found: true,
        release_missing_note: None,
        pods: Vec::new(),
        ready: 0,
        total: 0,
        unhealthy: Vec::new(),
        pods_listed: true,
        urls: Vec::new(),
    })
}

fn unavailable(reason: &str, where_it_lives: &str) -> Unavailable {
    Unavailable {
        available: false,
        reason: reason.to_string(),
        where_it_lives: where_it_lives.to_string(),
    }
}

fn unresolved(reason: &str) -> Unresolved {
    Unresolved {
        resolved: false,
        reason: reason.to_string(),
    }
}

/// A POPULATED skill-tier report, not a minimal one: #965 exists so every
/// variant's payload actually reaches the schema gate, and a trivially empty
/// sample would satisfy the registry while exercising none of the shape.
///
/// So this carries a real skill, a real MCP row, a real gate, a declared secret,
/// a boot-env row, artifacts, and two diagnostics across two kinds -- plus BOTH
/// sentinels, kept distinct: `channel`/`comms`/`bundle.deployed`/
/// `model.recorded_runner`/`boot_env[].value_present` are `unavailable`
/// (meaningless at the skill tier), while `evals` is `unresolved` (the concept
/// exists here, this bundle's state blocked it). Conflating the two would let a
/// consumer read "does not exist here" as "exists but is broken".
fn info_report() -> InfoReport {
    InfoReport {
        info: "curie".to_string(),
        version: 1,
        tier: Maybe::Known("skill".to_string()),
        bundle: Maybe::Known(BundleInfo {
            name: Some("weather".to_string()),
            version: Some("0.1.0".to_string()),
            source: "disk".to_string(),
            root: Maybe::Known("/tmp/weather".to_string()),
            manifest_path: Some(".claude-plugin/plugin.json".to_string()),
            manifest_location: Some(".claude-plugin/plugin.json".to_string()),
            deployed: Maybe::Unavailable(unavailable(
                "`skill up` runs bytes on disk, so no version is assigned",
                "`curie local info <agent>`",
            )),
        }),
        skills: Maybe::Known(vec![SkillRow {
            name: "weather".to_string(),
            path: "skills/weather/SKILL.md".to_string(),
            description: Some("Look up a location's weather forecast.".to_string()),
            allowed_tools: vec!["WebSearch".to_string(), "WebFetch".to_string()],
        }]),
        mcp_servers: Maybe::Known(vec![McpRow {
            name: "github".to_string(),
            source: ".mcp.json".to_string(),
            form: "stdio".to_string(),
            authed: true,
            load: McpLoad::NotProbed,
        }]),
        secrets: Maybe::Known(SecretsBlock {
            declared: vec![SecretRow {
                name: "GITHUB_PERSONAL_ACCESS_TOKEN".to_string(),
                satisfied: Maybe::Known(true),
                source: Maybe::Known("shell_env".to_string()),
            }],
        }),
        boot_env: vec![BootEnvRow {
            name: "CURIE_MEMORY_REF".to_string(),
            set_by_this_tier: Maybe::Known(false),
            value_present: Maybe::Unavailable(unavailable(
                "this CLI cannot read a running container's environment",
                "the sandbox itself",
            )),
            note: Some("no durable memory is attached at the skill tier".to_string()),
        }],
        approval_gates: Maybe::Known(vec![GateRow {
            gate: "Bash".to_string(),
            route: "permission".to_string(),
        }]),
        evals: Maybe::Unresolved(unresolved("evals/cases.json is absent")),
        channel: Maybe::Unavailable(unavailable(
            "`skill message` posts straight to the runner; there is no channel",
            "`curie local info <agent>`",
        )),
        comms: Maybe::Unavailable(unavailable(
            "no dispatcher exists at the skill tier",
            "`curie local info <agent>`",
        )),
        model: Maybe::Known(ModelInfo {
            mode: "ambient_sdk_credential".to_string(),
            model_id: None,
            base_url_override: false,
            credential: CredentialInfo {
                name: Some("ANTHROPIC_API_KEY".to_string()),
                source: "shell_env".to_string(),
            },
            recorded_runner: Maybe::Unavailable(unavailable(
                "no runner is recorded for this bundle",
                "`curie skill up` records one",
            )),
            note: "what a `skill up` from this shell WOULD resolve".to_string(),
        }),
        artifacts: vec![
            ArtifactRow {
                kind: "manifest".to_string(),
                path: ".claude-plugin/plugin.json".to_string(),
                exists: true,
            },
            ArtifactRow {
                kind: "eval_suite".to_string(),
                path: "evals/cases.json".to_string(),
                exists: false,
            },
        ],
        // Emitted in (kind, candidate, code) order, as `sort_diagnostics` does.
        diagnostics: vec![
            Diagnostic {
                code: "evals.file_absent".to_string(),
                kind: DiagnosticKind::Evals,
                candidate: "evals/cases.json".to_string(),
                looked_for: "an eval suite".to_string(),
                looked_in: vec!["evals/cases.json".to_string()],
                reason: "the bundle declares no eval suite at the conforming path".to_string(),
                fix: Some("add evals/cases.json".to_string()),
            },
            Diagnostic {
                code: "skill.no_skill_md".to_string(),
                kind: DiagnosticKind::Skill,
                candidate: "skills/drafts".to_string(),
                looked_for: "SKILL.md".to_string(),
                looked_in: vec!["skills/drafts/SKILL.md".to_string()],
                reason: "the directory carries no conforming SKILL.md, so the loader \
                         registers nothing from it"
                    .to_string(),
                fix: None,
            },
        ],
    }
}

/// Every discovered enum, with one constructed sample per variant.
fn registry() -> BTreeMap<&'static str, Vec<VariantJson>> {
    let mut m: BTreeMap<&'static str, Vec<VariantJson>> = BTreeMap::new();

    m.insert(
        "KillOutput",
        samples![
            "DryRun" => KillOutput::DryRun(plan()),
            "Done" => KillOutput::Done { agent: "a".to_string(), killed: true },
        ],
    );
    m.insert(
        "ResumeOutput",
        samples![
            "DryRun" => ResumeOutput::DryRun(plan()),
            "Done" => ResumeOutput::Done { agent: "a".to_string(), killed: false },
        ],
    );
    m.insert(
        "BudgetOutput",
        samples![
            "DryRun" => BudgetOutput::DryRun(plan()),
            "Done" => BudgetOutput::Done { agent: "a".to_string(), max_usd_per_day: Some(1.5) },
        ],
    );
    m.insert(
        "ResetThreadOutput",
        samples![
            "DryRun" => ResetThreadOutput::DryRun(plan()),
            "Done" => ResetThreadOutput::Done {
                agent: "a".to_string(),
                thread_key: "C1:1700000000.000100".to_string(),
                requested: true,
                released: true,
            },
        ],
    );
    m.insert(
        "DeleteOutput",
        samples![
            "DryRun" => DeleteOutput::DryRun(plan()),
            "Done" => DeleteOutput::Done { agent: "a".to_string() },
        ],
    );
    m.insert(
        "VersionsOutput",
        samples![
            "DryRun" => VersionsOutput::DryRun(plan()),
            "Empty" => VersionsOutput::Empty { agent: "a".to_string() },
            "List" => VersionsOutput::List { agent: "a".to_string(), versions: vec![version()] },
        ],
    );
    m.insert(
        "MemoryOutput",
        samples![
            "DryRun" => MemoryOutput::DryRun(plan()),
            "Empty" => MemoryOutput::Empty { agent: "a".to_string() },
            "List" => MemoryOutput::List { agent: "a".to_string(), entries: vec![memory_entry()] },
        ],
    );
    m.insert(
        "ApprovalsOutput",
        samples![
            "DryRun" => ApprovalsOutput::DryRun(plan()),
            "Gates" => ApprovalsOutput::Gates {
                agent: "a".to_string(),
                gated_tools: vec!["Bash".to_string()],
                manifest_unreadable: None,
            },
            "Pending" => ApprovalsOutput::Pending {
                agent: "a".to_string(),
                records: vec![approval_record()],
                truncated: false,
            },
            "Resolved" => ApprovalsOutput::Resolved { record: approval_record() },
        ],
    );
    m.insert(
        "SkillApprovalsOutput",
        samples![
            "Gates" => SkillApprovalsOutput::Gates {
                gates: vec![("Bash".to_string(), "permission".to_string())],
            },
            "Env" => SkillApprovalsOutput::Env {
                env: "CURIE_APPROVAL_REQUIRED_TOOLS=Bash".to_string(),
                restart: "curie skill up --replace".to_string(),
                bundle_note: "declared in the bundle manifest".to_string(),
            },
        ],
    );
    m.insert(
        "CommsOutput",
        samples![
            "DryRun" => CommsOutput::DryRun(plan()),
            "Done" => CommsOutput::Done { connected: true },
        ],
    );
    m.insert(
        "LocalUpOutput",
        samples![
            "DryRun" => LocalUpOutput::DryRun(plan()),
            "Up" => LocalUpOutput::Up {
                endpoints: vec![("api".to_string(), "http://localhost:28000".to_string())],
                slack: false,
            },
        ],
    );
    m.insert(
        "LocalRebuildOutput",
        samples![
            "DryRun" => LocalRebuildOutput::DryRun(plan()),
            "Rebuilt" => LocalRebuildOutput::Rebuilt {
                service: "curie-worker".to_string(),
                model_mode: ModelMode::LiveFromCredential,
            },
        ],
    );
    m.insert(
        "LocalStatusOutput",
        samples![
            "DryRun" => LocalStatusOutput::DryRun(plan()),
            "Services" => LocalStatusOutput::Services { rows: vec!["curie-api  running".to_string()] },
        ],
    );
    m.insert(
        "LocalDownOutput",
        samples![
            "DryRun" => LocalDownOutput::DryRun(plan()),
            "Aborted" => LocalDownOutput::Aborted,
            "Down" => LocalDownOutput::Down { volumes_wiped: false, reaped: 0 },
        ],
    );
    m.insert(
        "MessageOutcomeOutput",
        samples![
            "Replied" => MessageOutcomeOutput::Replied {
                thread: "1700000000.000100".to_string(),
                reply: "the answer is 42".to_string(),
            },
            "NoEdit" => MessageOutcomeOutput::NoEdit { thread: "1700000000.000100".to_string() },
            "AwaitingApproval" => MessageOutcomeOutput::AwaitingApproval {
                thread: "1700000000.000100".to_string(),
                reply: Some("card text".to_string()),
                tier: "workspace",
                agent: None,
                channel: "C123".to_string(),
            },
            "TimedOut" => MessageOutcomeOutput::TimedOut { diagnostics: None, resume_note: None },
            "Enqueued" => MessageOutcomeOutput::Enqueued {
                channel: "C123".to_string(),
                thread: "1700000000.000100".to_string(),
            },
        ],
    );
    m.insert(
        "ObservabilityOutput",
        samples![
            "DryRun" => ObservabilityOutput::DryRun(plan()),
            "Surfaces" => ObservabilityOutput::Surfaces(Vec::<Endpoint>::new()),
        ],
    );
    m.insert(
        "ClusterUpOutput",
        samples![
            "DryRun" => ClusterUpOutput::DryRun(plan()),
            "Up" => ClusterUpOutput::Up {
                namespace: "curie".to_string(),
                release: "curie".to_string(),
            },
        ],
    );
    m.insert(
        "ClusterStatusOutput",
        samples![
            "DryRun" => ClusterStatusOutput::DryRun(plan()),
            "Status" => ClusterStatusOutput::Status(cluster_status()),
        ],
    );
    m.insert(
        "ClusterDownOutput",
        samples![
            "DryRun" => ClusterDownOutput::DryRun(plan()),
            "Aborted" => ClusterDownOutput::Aborted,
            "Down" => ClusterDownOutput::Down { release_was_absent: false },
        ],
    );
    m.insert(
        "InfoOutput",
        samples![
            "DryRun" => InfoOutput::DryRun(plan()),
            "Report" => InfoOutput::Report(info_report()),
        ],
    );

    m
}

// ---------------------------------------------------------------------------
// The gates.
// ---------------------------------------------------------------------------

#[test]
fn every_cli_output_enum_has_samples() {
    let discovered = discovered_enums();
    let registered: BTreeSet<String> = registry().keys().map(|k| k.to_string()).collect();
    let discovered_names: BTreeSet<String> = discovered.keys().cloned().collect();

    let missing: Vec<&String> = discovered_names.difference(&registered).collect();
    assert!(
        missing.is_empty(),
        "CliOutput enum(s) {missing:?} exist in cli/src but have no samples entry in \
         cli/tests/cli_output_variants.rs, so no variant of theirs reaches the schema \
         gate (#965); add one constructed sample per variant"
    );

    let stale: Vec<&String> = registered.difference(&discovered_names).collect();
    assert!(
        stale.is_empty(),
        "samples registered for {stale:?}, which are no longer CliOutput enums in \
         cli/src: remove the stale entry so the registry cannot drift from the source"
    );
}

#[test]
fn every_variant_of_every_cli_output_enum_has_a_sample() {
    let discovered = discovered_enums();
    let registry = registry();

    let mut failures = Vec::new();
    for (name, declared) in &discovered {
        let Some(samples) = registry.get(name.as_str()) else {
            continue; // reported by every_cli_output_enum_has_samples
        };
        let covered: BTreeSet<String> = samples.iter().map(|(v, _)| v.to_string()).collect();
        for uncovered in declared.difference(&covered) {
            failures.push(format!("{name}::{uncovered}"));
        }
        for unknown in covered.difference(declared) {
            failures.push(format!(
                "{name}::{unknown} (sample names a variant that does not exist)"
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "variant(s) {failures:?} are declared in cli/src but have no sample (or name a \
         variant that no longer exists), so their `to_json()` never reaches the schema \
         gate (#965)"
    );
}

#[test]
fn every_variant_sample_validates_against_its_mapped_schema() {
    let index_path = format!("{}/schema/index.json", env!("CARGO_MANIFEST_DIR"));
    let raw = std::fs::read_to_string(&index_path).expect("cli/schema/index.json must exist");
    let index: serde_json::Value = serde_json::from_str(&raw).expect("index.json is valid JSON");
    let results = index["results"].as_array().expect("index.json has results");

    let schema_for = |result: &str| -> Option<String> {
        results.iter().find_map(|e| {
            (e["result"].as_str() == Some(result))
                .then(|| e["schema"].as_str().map(str::to_string))
                .flatten()
        })
    };

    let mut checked = 0usize;
    for (name, samples) in registry() {
        let Some(schema_file) = schema_for(name) else {
            panic!(
                "CliOutput enum {name} has no `results` entry in cli/schema/index.json; \
                 schema_inventory.rs should already have failed on this"
            );
        };
        let path = format!("{}/schema/{}", env!("CARGO_MANIFEST_DIR"), schema_file);
        let schema: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}")),
        )
        .unwrap_or_else(|e| panic!("{path} is valid JSON: {e}"));
        let validator = jsonschema::validator_for(&schema).expect("schema compiles");

        for (variant, value) in samples {
            assert!(
                validator.is_valid(&value),
                "{name}::{variant} `to_json()` does not validate against {schema_file}: \
                 {value}\nerrors: {:?}",
                validator
                    .iter_errors(&value)
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
            );
            checked += 1;
        }
    }
    assert!(
        checked >= 40,
        "only {checked} variant samples validated; the registry has shrunk unexpectedly \
         and the gate would pass vacuously"
    );
}
