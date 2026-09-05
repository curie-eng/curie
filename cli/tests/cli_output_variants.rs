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

use curie::api::{
    ApprovalRecord, ChannelBinding, MemoryEntry, MetricPoint, MetricSeries, MetricsSummary, Version,
};
use curie::commands::{
    ApprovalsOutput, BudgetOutput, ChannelsOutput, DeleteOutput, KillOutput, MemoryOutput,
    OverridesOutput, ResetThreadOutput, ResumeOutput, SkillApprovalsOutput, VersionsOutput,
};
use curie::comms::CommsOutput;
use curie::github_app::GithubAppOutput;
use curie::installation::ApplyOutput;
use curie::local::{
    LocalDownOutput, LocalRebuildOutput, LocalStatusOutput, LocalUpOutput, ModelMode,
};
use curie::message::MessageOutcomeOutput;
use curie::migrate_store::MigrateStoreOutput;
use curie::observability::{Endpoint, ObservabilityMetricsOutput, ObservabilityOutput};
use curie::ops::{
    ClusterDownOutput, ClusterRollbackOutput, ClusterStatus, ClusterStatusOutput, ClusterUpOutput,
    ClusterUpgradeOutput,
};
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
    let mut paths = Vec::new();
    collect_rs_paths(std::path::Path::new(&dir), &mut paths);
    paths.sort();
    let mut out = Vec::new();
    for path in paths {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        out.push((name, std::fs::read_to_string(&path).expect("read source")));
    }
    assert!(!out.is_empty(), "cli/src must contain .rs sources");
    out
}

/// Recursively collect every `.rs` file under `dir`, subdirectories included
/// (`cli/src/ops/*.rs` since the ops module split).
fn collect_rs_paths(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in
        std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()))
    {
        let entry = entry.unwrap_or_else(|e| panic!("read_dir entry in {}: {e}", dir.display()));
        let path = entry.path();
        if path.is_dir() {
            collect_rs_paths(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
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
        upgrade: curie::ops::UpgradeStatusView::idle(None),
    })
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
        "OverridesOutput",
        samples![
            "DryRun" => OverridesOutput::DryRun(plan()),
            // Both nullable fields carry a value here, and the null case rides
            // the schema's `["string","null"]` type plus the round-trip tests
            // in api_lifecycle.rs -- a sample per VARIANT is what this gate
            // wants, not a sample per field state.
            "Done" => OverridesOutput::Done {
                agent: "a".to_string(),
                model: Some("kimi-k2".to_string()),
                thinking: Some("adaptive".to_string()),
                changed: true,
            },
        ],
    );
    m.insert(
        "ChannelsOutput",
        samples![
            "DryRun" => ChannelsOutput::DryRun(plan()),
            // The single Done sample keeps plural coverage while deliberately
            // exercising both row shapes for the optional signal: a legacy stored name
            // carries a warning, while a valid channel ID omits it.
            "Done" => ChannelsOutput::Done {
                agent: "a".to_string(),
                channels: vec![
                    ChannelBinding {
                        kind: "slack".to_string(),
                        address: "#legacy-alerts".to_string(),
                    },
                    ChannelBinding {
                        kind: "slack".to_string(),
                        address: "C0EXAMPLE1".to_string(),
                    },
                ],
                changed: true,
            },
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
            "Added" => MemoryOutput::Added {
                agent: "a".to_string(),
                index: 0,
                content: "remembered fact".to_string(),
                source: "operator".to_string(),
                fresh_session_required: true,
            },
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
            "OperatorPrincipal" => ApprovalsOutput::OperatorPrincipal {
                delivery: curie::api::OperatorPrincipalDelivery {
                    token: "operator-token".to_string(),
                    subject: "operator@example.com".to_string(),
                    expires_at: "2026-08-31T12:00:00Z".to_string(),
                },
            },
            "ConsoleLoginCode" => ApprovalsOutput::ConsoleLoginCode {
                delivery: curie::api::ConsoleLoginCodeDelivery {
                    code: "console-login-code".to_string(),
                    subject: "operator@example.com".to_string(),
                    expires_at: "2026-08-31T12:00:00Z".to_string(),
                },
            },
            // Both binding shapes in one sample so the schema gate sees the
            // resolution/notification split and the optional approvers block.
            // Response deserialization is deliberate: transport fields are
            // write-only and must never be constructible in display output.
            "Routes" => ApprovalsOutput::Routes {
                agent: "a".to_string(),
                routes: serde_json::from_value(serde_json::json!({
                    "deal_desk": {
                        "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
                        "notification": {"kind": "slack", "address": "C0EXAMPLE2"}
                    },
                    "finance": {
                        "resolution": {"kind": "slack", "address": "C0EXAMPLE3"},
                        "approvers": {"group": "S0FINGRP0"}
                    }
                }))
                .expect("approval route response mirror deserializes its display shape"),
            },
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
        "ApplyOutput",
        samples![
            "DryRun" => ApplyOutput::DryRun(plan()),
            "Applied" => ApplyOutput::Applied {
                namespace: "acme-bot".to_string(),
                release: "acme-bot".to_string(),
                comms: true,
            },
        ],
    );
    m.insert(
        "MigrateStoreOutput",
        samples![
            "DryRun" => MigrateStoreOutput::DryRun(plan()),
            "Exported" => MigrateStoreOutput::Exported {
                from: "minio".to_string(),
                to: "rustfs".to_string(),
                objects: 22,
            },
            "Imported" => MigrateStoreOutput::Imported {
                store: "rustfs".to_string(),
                objects: 23,
                missing: vec![],
                added: vec!["bundles/x/new.tar".to_string()],
                staging_kept: false,
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
        "GithubAppOutput",
        samples![
            "DryRun" => GithubAppOutput::DryRun(plan()),
            "Done" => GithubAppOutput::Done { configured: true },
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
        "ObservabilityMetricsOutput",
        samples![
            "Summary" => ObservabilityMetricsOutput::Summary(MetricsSummary {
                start: "2026-08-22T00:00:00Z".to_string(),
                end: "2026-08-23T00:00:00Z".to_string(),
                runs: 3,
                latency_p95_ms: 25.0,
                tokens: 42,
                cost_usd: 0.0,
                cost_known: false,
                error_rate: 0.0,
            }),
            "Series" => ObservabilityMetricsOutput::Series(MetricSeries {
                metric: "runs".to_string(),
                granularity: "day".to_string(),
                start: "2026-08-22T00:00:00Z".to_string(),
                end: "2026-08-23T00:00:00Z".to_string(),
                points: vec![MetricPoint {
                    ts: "2026-08-22T00:00:00Z".to_string(),
                    value: 3.0,
                }],
            }),
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
        "ClusterRollbackOutput",
        samples![
            "DryRun" => ClusterRollbackOutput::DryRun(plan()),
            "Aborted" => ClusterRollbackOutput::Aborted,
            "RolledBack" => ClusterRollbackOutput::RolledBack {
                from_revision: 21,
                to_revision: 19,
                skipped: vec![20],
                forced: false,
            },
        ],
    );
    m.insert(
        "ClusterUpgradeOutput",
        samples![
            "DryRun" => ClusterUpgradeOutput::DryRun(plan()),
            "Completed" => ClusterUpgradeOutput::Completed {
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
            },
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
fn channels_done_sample_covers_warning_present_and_absent_rows() {
    let registry = registry();
    let samples = registry
        .get("ChannelsOutput")
        .expect("ChannelsOutput has variant samples");
    let done = samples
        .iter()
        .find_map(|(variant, value)| (*variant == "Done").then_some(value))
        .expect("ChannelsOutput::Done has a sample");
    let surfaces = done["surfaces"]
        .as_array()
        .expect("ChannelsOutput::Done carries surfaces");

    let legacy = surfaces
        .iter()
        .find(|binding| binding["address"] == "#legacy-alerts")
        .expect("the sample carries the legacy binding");
    assert!(
        legacy
            .get("warning")
            .and_then(serde_json::Value::as_str)
            .is_some(),
        "the legacy row must carry a string warning: {legacy}"
    );

    let valid = surfaces
        .iter()
        .find(|binding| binding["address"] == "C0EXAMPLE1")
        .expect("the sample carries the valid binding");
    assert!(
        valid.get("warning").is_none(),
        "the valid row must omit the warning field: {valid}"
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
