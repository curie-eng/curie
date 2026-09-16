//! Inventory contract gate for the versioned CLI result schemas (issue #634).
//!
//! AC2's anti-drift mechanism: `cli/schema/index.json` maps every agent-facing
//! `--json` result family to a committed, versioned JSON Schema, and this gate
//! proves that inventory honest against the real tree -- every `impl CliOutput`
//! is declared with a schema, every declared schema file exists and compiles,
//! every entry's `version` agrees with the schema's `$id`, and no new
//! direct-`emit_json` result has slipped in unpinned. A new result family that
//! lands without a schema fails here. The rejection cases below drive the same
//! pure comparator over a drifted fixture to prove it rejects each class by
//! execution, mirroring `cli/tests/api_field_parity.rs`.

#[path = "support/schema_inventory.rs"]
mod schema_inventory;

use std::collections::BTreeMap;

use std::collections::BTreeSet;

use schema_inventory::{
    version_from_entry, version_from_id, violations, SchemaState, SchemaVersion, Violation,
};
use serde_json::Value;

// ─── Loaders ─────────────────────────────────────────────────────────────────

fn repo_path(rel: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(rel)
}

/// Every `cli/src/*.rs` file as `(repo-relative path, contents)`. The path form
/// matches the `raw_emit_sites` keys in `index.json`.
fn cli_sources() -> Vec<(String, String)> {
    let dir = repo_path("cli/src");
    let mut paths = Vec::new();
    collect_rs_paths(&dir, &mut paths);
    let mut out = Vec::new();
    for path in paths {
        let rel = path.strip_prefix(&dir).expect("path under cli/src");
        let label = format!("cli/src/{}", rel.to_string_lossy().replace('\\', "/"));
        let text = std::fs::read_to_string(&path).expect("read source");
        out.push((label, text));
    }
    out.sort();
    out
}

/// Recursively collect every `.rs` file under `dir`, subdirectories included
/// (`cli/src/ops/*.rs` since the ops module split).
fn collect_rs_paths(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read cli/src") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.is_dir() {
            collect_rs_paths(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

fn borrowed(srcs: &[(String, String)]) -> Vec<(&str, &str)> {
    srcs.iter().map(|(p, s)| (p.as_str(), s.as_str())).collect()
}

fn index_json() -> Value {
    let raw = std::fs::read_to_string(repo_path("cli/schema/index.json")).expect("read index.json");
    serde_json::from_str(&raw).expect("index.json is valid JSON")
}

/// The committed schema files, each described by whether it compiles to a
/// validator and the version its `$id` declares.
fn on_disk_schemas() -> BTreeMap<String, SchemaState> {
    let dir = repo_path("cli/schema");
    let mut out = BTreeMap::new();
    for entry in std::fs::read_dir(&dir).expect("read cli/schema") {
        let entry = entry.expect("dir entry");
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".schema.json") {
            continue;
        }
        let raw = std::fs::read_to_string(entry.path()).expect("read schema");
        let value: Value = serde_json::from_str(&raw)
            .unwrap_or_else(|e| panic!("committed schema {name} is not valid JSON: {e}"));
        let compiles = jsonschema::validator_for(&value).is_ok();
        let id_version = value
            .get("$id")
            .and_then(Value::as_str)
            .and_then(version_from_id);
        out.insert(
            name,
            SchemaState {
                compiles,
                id_version,
            },
        );
    }
    out
}

// ─── Real-tree assertions (AC1, AC2, AC4 identity) ───────────────────────────

#[test]
fn the_real_inventory_has_no_drift() {
    let srcs = cli_sources();
    let vs = violations(&borrowed(&srcs), &index_json(), &on_disk_schemas());
    assert!(
        vs.is_empty(),
        "cli/schema/index.json has drifted from cli/src. Each entry is fixed by either \
         adding an index.json entry + committed schema for a new result family, or \
         correcting the named schema/version:\n{vs:#?}"
    );
}

#[test]
fn every_committed_schema_compiles_and_declares_a_version() {
    // Independent of the comparator: a directly-stated invariant so a schema
    // that neither compiles nor carries a `/vN` `$id` cannot hide behind a
    // matching (also-broken) index entry.
    for (name, state) in on_disk_schemas() {
        assert!(state.compiles, "schema {name} must compile to a validator");
        assert!(
            state.id_version.is_some(),
            "schema {name} must carry a versioned $id like .../{{name}}/v1.json"
        );
    }
}

#[test]
fn approval_route_split_is_a_v2_schema_contract() {
    let schema: Value = serde_json::from_str(
        &std::fs::read_to_string(repo_path("cli/schema/approvals.schema.json"))
            .expect("read approvals schema"),
    )
    .expect("approvals schema is JSON");
    assert_eq!(
        schema["$id"], "https://schemas.curietech.ai/cli/approvals/v2.1.json",
        "removing the fused channel field and requiring resolution is a major schema break"
    );

    let binding = &schema["$defs"]["approvalRouteBinding"];
    assert!(
        binding["properties"].get("channel").is_none(),
        "the retired fused field must not remain in the v2 result schema"
    );
    assert!(binding["properties"].get("resolution").is_some());
    assert!(binding["properties"].get("notification").is_some());
    assert_eq!(
        binding["properties"]["resolution"]["type"],
        serde_json::json!("object"),
        "the v2 display schema requires the sole resolution surface"
    );
    let required = binding["required"]
        .as_array()
        .expect("binding required list");
    assert!(required.iter().any(|field| field == "resolution"));

    let index = index_json();
    let approvals = index["results"]
        .as_array()
        .expect("results")
        .iter()
        .find(|entry| entry["result"] == "ApprovalsOutput")
        .expect("ApprovalsOutput inventory entry");
    assert_eq!(approvals["version"], "2.1");
}

#[test]
fn version_from_id_parses_the_v_segment() {
    let v = |major, minor| Some(SchemaVersion { major, minor });
    assert_eq!(
        version_from_id("https://schemas.curietech.ai/cli/kill/v1.json"),
        v(1, 0),
        "a bare major reads as minor zero (ADR-0101), so v1 == v1.0"
    );
    assert_eq!(version_from_id(".../foo/v12.json"), v(12, 0));
    assert_eq!(version_from_id(".../foo/v1.1.json"), v(1, 1));
    assert_eq!(version_from_id(".../foo/v2.13.json"), v(2, 13));
    assert_eq!(version_from_id(".../foo/bar.json"), None);
    assert_eq!(version_from_id(".../foo/vX.json"), None);
    // Malformed minors are rejected rather than silently read as major-only:
    // a typo'd `$id` must not pass as a version the index can agree with.
    assert_eq!(version_from_id(".../foo/v1..json"), None);
    assert_eq!(version_from_id(".../foo/v1.x.json"), None);
}

// ADR-0101 widened index.json's `version` from an integer to a "major.minor"
// string. Both spellings stay legal and mean the same thing, so a schema that
// never changed does not have to be rewritten to satisfy the new type.
#[test]
fn version_from_entry_accepts_both_the_integer_and_the_string_spelling() {
    let v = |major, minor| Some(SchemaVersion { major, minor });
    assert_eq!(version_from_entry(Some(&serde_json::json!(1))), v(1, 0));
    assert_eq!(version_from_entry(Some(&serde_json::json!("1.0"))), v(1, 0));
    assert_eq!(version_from_entry(Some(&serde_json::json!("1.1"))), v(1, 1));
    assert_eq!(version_from_entry(Some(&serde_json::json!("2"))), v(2, 0));
    // Zero is not a version, in either spelling.
    assert_eq!(version_from_entry(Some(&serde_json::json!(0))), None);
    assert_eq!(version_from_entry(Some(&serde_json::json!("0.3"))), None);
    assert_eq!(version_from_entry(Some(&serde_json::json!("x"))), None);
    assert_eq!(version_from_entry(None), None);
}

// ─── Guard-rejects-a-violating-input demonstrations (AC2 teeth) ──────────────

fn fixture_src() -> String {
    std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/schema-inventory/drifted-src.rs"),
    )
    .expect("read drifted-src.rs")
}

fn fixture_index() -> Value {
    let raw = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/schema-inventory/drifted-index.json"),
    )
    .expect("read drifted-index.json");
    serde_json::from_str(&raw).expect("drifted-index.json is valid JSON")
}

fn fixture_schemas() -> BTreeMap<String, SchemaState> {
    BTreeMap::from([
        (
            "good.schema.json".to_string(),
            SchemaState {
                compiles: true,
                id_version: Some(SchemaVersion { major: 1, minor: 0 }),
            },
        ),
        (
            "invalid.schema.json".to_string(),
            SchemaState {
                compiles: false,
                id_version: None,
            },
        ),
        (
            "v2.schema.json".to_string(),
            SchemaState {
                compiles: true,
                id_version: Some(SchemaVersion { major: 2, minor: 0 }),
            },
        ),
        // "absent.schema.json" is deliberately not here.
    ])
}

fn fixture_violations() -> Vec<Violation> {
    let src = fixture_src();
    let srcs = vec![("drifted.rs".to_string(), src)];
    violations(&borrowed(&srcs), &fixture_index(), &fixture_schemas())
}

#[test]
fn rejects_an_impl_cli_output_with_no_index_entry() {
    // THE anti-drift teeth: a new result family with no schema.
    let vs = fixture_violations();
    assert!(
        vs.iter().any(
            |v| matches!(v, Violation::UndeclaredResult { result } if result == "UndeclaredOne")
        ),
        "{vs:#?}"
    );
}

#[test]
fn rejects_an_index_entry_with_no_impl() {
    let vs = fixture_violations();
    assert!(
        vs.iter()
            .any(|v| matches!(v, Violation::ResultNotFound { result } if result == "GhostImpl")),
        "{vs:#?}"
    );
}

#[test]
fn rejects_a_missing_schema_file() {
    let vs = fixture_violations();
    assert!(
        vs.iter().any(
            |v| matches!(v, Violation::SchemaFileMissing { result, schema }
            if result == "MissingFileOne" && schema == "absent.schema.json")
        ),
        "{vs:#?}"
    );
}

#[test]
fn rejects_a_schema_that_does_not_compile() {
    let vs = fixture_violations();
    assert!(
        vs.iter()
            .any(|v| matches!(v, Violation::SchemaInvalid { result, .. }
            if result == "InvalidOne")),
        "{vs:#?}"
    );
}

#[test]
fn rejects_an_entry_with_no_version() {
    let vs = fixture_violations();
    assert!(
        vs.iter()
            .any(|v| matches!(v, Violation::MissingVersion { result } if result == "NoVersionOne")),
        "{vs:#?}"
    );
}

#[test]
fn rejects_a_version_that_disagrees_with_the_schema_id() {
    let vs = fixture_violations();
    assert!(
        vs.iter().any(
            |v| matches!(v, Violation::VersionMismatch { result, entry_version, id_version, .. }
            if result == "MismatchOne"
                && *entry_version == SchemaVersion { major: 1, minor: 0 }
                && *id_version == SchemaVersion { major: 2, minor: 0 })
        ),
        "{vs:#?}"
    );
}

#[test]
fn rejects_a_malformed_manifest_entry() {
    let vs = fixture_violations();
    assert!(
        vs.iter()
            .any(|v| matches!(v, Violation::MalformedManifestEntry { .. })),
        "{vs:#?}"
    );
}

#[test]
fn rejects_an_undeclared_raw_emit_json_site() {
    let vs = fixture_violations();
    assert!(
        vs.iter().any(
            |v| matches!(v, Violation::UnexpectedRawEmitter { file, expected, found }
            if file == "drifted.rs" && *expected == 0 && *found == 1)
        ),
        "{vs:#?}"
    );
}

// ─── ADR-0101: an unchanged $id must stay compatible ─────────────────────────

/// Every property a schema accepts, and every property it requires, ANYWHERE in
/// the document. Enough to decide the two compatibility questions ADR-0101 turns
/// on and nothing more.
///
/// Names are qualified by their JSON Pointer path, so a `card_channel` added
/// under `$defs/approvalRecord` is a different name from one added at the root.
/// Without that, moving a property between two objects would cancel out and read
/// as no change at all.
///
/// Walks the whole tree rather than the root plus `oneOf`. The first version of
/// this gate did only those two, and 20 of 39 schemas keep their real content in
/// `$defs` -- including `approvals`, whose entire record shape lives there. The
/// gap surfaced on the very next change to land (#1078, adding
/// `$defs/approvalRecord/card_channel`), which the gate waved straight through.
fn shape(schema: &serde_json::Value) -> (BTreeSet<String>, BTreeSet<String>) {
    let mut props = BTreeSet::new();
    let mut required = BTreeSet::new();
    walk(schema, String::new(), &mut props, &mut required);
    (props, required)
}

/// Recursive half of [`shape`]. `path` is the JSON Pointer to `node`.
fn walk(
    node: &serde_json::Value,
    path: String,
    props: &mut BTreeSet<String>,
    required: &mut BTreeSet<String>,
) {
    match node {
        serde_json::Value::Object(map) => {
            if let Some(p) = map.get("properties").and_then(|p| p.as_object()) {
                props.extend(p.keys().map(|k| format!("{path}/properties/{k}")));
            }
            if let Some(r) = map.get("required").and_then(|r| r.as_array()) {
                required.extend(
                    r.iter()
                        .filter_map(|x| x.as_str())
                        .map(|k| format!("{path}/required/{k}")),
                );
            }
            for (k, v) in map {
                // Prose, not shape: skipping it keeps a reworded description
                // from reading as a compatibility event.
                if matches!(k.as_str(), "description" | "title" | "$comment") {
                    continue;
                }
                walk(v, format!("{path}/{k}"), props, required);
            }
        }
        serde_json::Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                walk(v, format!("{path}/{i}"), props, required);
            }
        }
        _ => {}
    }
}

fn schema_id(v: &serde_json::Value) -> String {
    v["$id"].as_str().unwrap_or_default().to_string()
}

/// ADR-0101's gate: a schema may not change shape while keeping its `$id`.
///
/// Every committed schema is `additionalProperties: false`, so a consumer
/// holding the previous revision REJECTS a payload carrying a property added
/// since. ADR-0074's superseded additive clause said such an addition needed no
/// bump, and three landed that way (#1056, #1057, #1306) with no red build,
/// because nothing compares a schema against its own past.
///
/// Compares SHAPES rather than sample payloads deliberately. A payload-based
/// version of this gate covers only the result families that happen to have a
/// constructed sample -- 23 of 47 when this was written, excluding `guide`,
/// `diff` and `doctor`, which are exactly the three that broke. Shape comparison
/// covers all 39 schemas by construction.
///
/// The baseline is a committed file, not `git show HEAD~1:...`, because CI
/// checks out at depth 1: a history-based lookup would find no previous
/// revision, skip everything, and pass. See `cli/schema/baseline/README.md`.
#[test]
fn a_schema_may_not_change_shape_while_keeping_its_id() {
    let dir = format!("{}/schema", env!("CARGO_MANIFEST_DIR"));
    let mut problems: Vec<String> = Vec::new();
    let mut compared = 0usize;
    let mut versioned = 0usize;

    for entry in std::fs::read_dir(&dir).expect("cli/schema exists") {
        let path = entry.expect("readable entry").path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.ends_with(".schema.json") {
            continue;
        }
        let current: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read schema"))
                .unwrap_or_else(|e| panic!("{name} is valid JSON: {e}"));
        let base_path = format!("{dir}/baseline/{name}");
        let Ok(raw) = std::fs::read_to_string(&base_path) else {
            continue; // a brand-new schema has no previous revision to break
        };
        let baseline: serde_json::Value =
            serde_json::from_str(&raw).unwrap_or_else(|e| panic!("baseline {name}: {e}"));

        if schema_id(&current) != schema_id(&baseline) {
            versioned += 1;
            continue;
        }
        compared += 1;

        let (cur_props, cur_req) = shape(&current);
        let (base_props, base_req) = shape(&baseline);

        let added: Vec<_> = cur_props.difference(&base_props).cloned().collect();
        let removed: Vec<_> = base_props.difference(&cur_props).cloned().collect();
        let newly_required: Vec<_> = cur_req.difference(&base_req).cloned().collect();

        if !added.is_empty() {
            problems.push(format!(
                "  {name} at UNCHANGED $id {} adds {added:?}\n    \
                 A consumer holding this $id rejects the new payload \
                 (additionalProperties: false). Bump the MINOR version.",
                schema_id(&current)
            ));
        }
        if !newly_required.is_empty() {
            problems.push(format!(
                "  {name} at UNCHANGED $id {} newly requires {newly_required:?}\n    \
                 A consumer holding this $id also rejects an OLDER payload, which \
                 no refetch fixes. Bump the MAJOR version.",
                schema_id(&current)
            ));
        }
        if !removed.is_empty() {
            problems.push(format!(
                "  {name} at UNCHANGED $id {} removes {removed:?}\n    \
                 Bump the MAJOR version.",
                schema_id(&current)
            ));
        }
    }

    assert!(
        compared + versioned >= 39,
        "the baseline covered only {} schema(s); cli/schema/baseline/ is stale, so \
         this gate is green because it did nothing",
        compared + versioned
    );
    assert!(
        versioned == 0,
        "These schemas changed their $id without refreshing the compatibility baseline. \
         Refresh it with `curie dev schema-baseline`.\n\n({compared} compared, {versioned} already versioned)"
    );
    assert!(
        problems.is_empty(),
        "These schemas changed shape while keeping their $id, so a conforming \
         consumer is broken under an identifier that promises it is not \
         (ADR-0101). Bump the version, then refresh the baseline with \
         `curie dev schema-baseline`.\n\n{}\n\n({compared} compared, {versioned} already versioned)",
        problems.join("\n")
    );
}
