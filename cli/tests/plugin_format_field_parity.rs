// Field-parity gate for the frozen `packages/plugin-format` hand-mirrors
// (issue #701, the plugin_format sibling of #691's `cli/src/api.rs` gate --
// #691 explicitly did not cover this seam, since it has a different source of
// truth: the frozen package's own schema export, not `apps/api/openapi.json`).
//
// Reuses the EXACT SAME comparator #691 built
// (`cli/tests/support/field_parity.rs`, included below via `#[path = ...]`,
// same as `cli/tests/api_field_parity.rs`). That comparator is generic over
// "a Rust source, a components.schemas-shaped Value, a mirrors/non_mirrors
// manifest" -- so the only new work here is (a) wrapping
// `packages/plugin-format/schema/plugin-format.schema.json`'s `$defs` into the
// `components.schemas` shape the comparator expects, and (b) running it once
// per source file (`cli/src/commands.rs`, `cli/src/spec.rs`), since the
// plugin_format mirrors are split across both, filtering
// `cli/plugin-format-mirrors.json`'s entries to each file's own slice.
//
// The comparator's own violation-class behavior (MissingField, StaleOmission,
// UnsupportedShape/flatten, DuplicateStruct, MalformedManifestEntry, ...) is
// already exhaustively fixture-tested by `cli/tests/api_field_parity.rs`; this
// file does not re-prove that generic machinery. It proves: the real tree is
// clean on THIS seam, the two issue-named silent-drift-prone mirror structs
// stay fully covered, and the gate's own wiring (schema wrapping + per-file
// manifest filtering) actually rejects a deliberately introduced drift.

#[path = "support/field_parity.rs"]
mod field_parity;

use field_parity::{violations, Violation};
use serde_json::Value;

// ─── Loaders ─────────────────────────────────────────────────────────────────

fn repo_text(rel: &str) -> String {
    let path = format!("{}/../{}", env!("CARGO_MANIFEST_DIR"), rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

fn repo_json(rel: &str) -> Value {
    let raw = repo_text(rel);
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {rel}: {e}"))
}

fn fixture_text(name: &str) -> String {
    let path = format!(
        "{}/tests/data/plugin-format-parity/{}",
        env!("CARGO_MANIFEST_DIR"),
        name
    );
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

fn fixture_json(name: &str) -> Value {
    let raw = fixture_text(name);
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {name}: {e}"))
}

/// Wrap the frozen `plugin_format` schema export's `$defs` into the
/// `components.schemas` shape `field_parity::violations` expects (it was
/// written against an OpenAPI doc's shape). `$defs` and `components.schemas`
/// are structurally identical -- a map of schema name to `{properties,
/// required, ...}` -- so no change to the shared comparator is needed.
fn plugin_format_schema_as_components() -> Value {
    let doc = repo_json("packages/plugin-format/schema/plugin-format.schema.json");
    let defs = doc
        .get("$defs")
        .cloned()
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    serde_json::json!({ "components": { "schemas": defs } })
}

/// `cli/plugin-format-mirrors.json`'s mirrors/non_mirrors entries carry a
/// `file` key the shared comparator does not look at (it only reads
/// `struct`/`schema`/`reason`/`omissions`); slice the manifest down to just
/// the entries for one source file so `violations` (which walks ONE rust
/// source at a time) sees only the structs that could possibly be in it.
fn manifest_for_file(manifest: &Value, file: &str) -> Value {
    let filter = |key: &str| -> Vec<Value> {
        manifest
            .get(key)
            .and_then(|v| v.as_array())
            .map(|entries| {
                entries
                    .iter()
                    .filter(|e| e.get("file").and_then(|f| f.as_str()) == Some(file))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    };
    serde_json::json!({
        "mirrors": filter("mirrors"),
        "non_mirrors": filter("non_mirrors"),
    })
}

// ─── Payload matchers (mirrors api_field_parity.rs's; variant + payload) ─────

fn has_missing_field(vs: &[Violation], struct_name: &str, field: &str) -> bool {
    vs.iter().any(|v| {
        matches!(v, Violation::MissingField { struct_name: s, field: f, .. }
            if s == struct_name && f == field)
    })
}

fn has_unknown_field(vs: &[Violation], struct_name: &str, field: &str) -> bool {
    vs.iter().any(|v| {
        matches!(v, Violation::UnknownField { struct_name: s, field: f, .. }
            if s == struct_name && f == field)
    })
}

fn has_undeclared_struct(vs: &[Violation], struct_name: &str) -> bool {
    vs.iter()
        .any(|v| matches!(v, Violation::UndeclaredStruct { struct_name: s } if s == struct_name))
}

fn mentions_struct(vs: &[Violation], struct_name: &str) -> bool {
    vs.iter().any(|v| match v {
        Violation::MissingField { struct_name: s, .. }
        | Violation::UnknownField { struct_name: s, .. }
        | Violation::UndeclaredStruct { struct_name: s }
        | Violation::StructNotFound { struct_name: s }
        | Violation::StaleOmission { struct_name: s, .. }
        | Violation::SchemaNotFound { struct_name: s, .. }
        | Violation::DuplicateStruct { struct_name: s }
        | Violation::UnsupportedShape { struct_name: s, .. } => s == struct_name,
        Violation::MalformedManifestEntry { .. } => false,
    })
}

// ─── Real-tree assertions ────────────────────────────────────────────────────

#[test]
fn commands_rs_has_no_plugin_format_field_parity_violations() {
    let src = repo_text("cli/src/commands.rs");
    let schema = plugin_format_schema_as_components();
    let manifest = repo_json("cli/plugin-format-mirrors.json");
    let scoped = manifest_for_file(&manifest, "cli/src/commands.rs");

    let vs = violations(&src, &schema, &scoped);
    assert!(
        vs.is_empty(),
        "cli/src/commands.rs has drifted from packages/plugin-format/schema/plugin-format.schema.json. \
         Each entry below is fixed by either adding the field to the struct or declaring the \
         omission (with a justification) in cli/plugin-format-mirrors.json:\n{vs:#?}"
    );
}

#[test]
fn spec_rs_has_no_plugin_format_field_parity_violations() {
    let src = repo_text("cli/src/spec.rs");
    let schema = plugin_format_schema_as_components();
    let manifest = repo_json("cli/plugin-format-mirrors.json");
    let scoped = manifest_for_file(&manifest, "cli/src/spec.rs");

    let vs = violations(&src, &schema, &scoped);
    assert!(
        vs.is_empty(),
        "cli/src/spec.rs has drifted from packages/plugin-format/schema/plugin-format.schema.json. \
         Each entry below is fixed by either adding the field to the struct or declaring the \
         omission (with a justification) in cli/plugin-format-mirrors.json:\n{vs:#?}"
    );
}

#[test]
fn connector_build_rs_has_no_plugin_format_field_parity_violations() {
    // The third mirror site (ADR 0113). Without this per-file test the gate's
    // `UndeclaredStruct` check never walks `cli/src/connector_build.rs` at all,
    // and a new `Deserialize` mirror of a frozen connector shape could land
    // undeclared.
    //
    // Since the schema export carries the connector and lock models (#1128),
    // these structs are declared as `mirrors`, so this test also compares
    // their fields against the exported `$defs`.
    let src = repo_text("cli/src/connector_build.rs");
    let schema = plugin_format_schema_as_components();
    let manifest = repo_json("cli/plugin-format-mirrors.json");
    let scoped = manifest_for_file(&manifest, "cli/src/connector_build.rs");

    let vs = violations(&src, &schema, &scoped);
    assert!(
        vs.is_empty(),
        "cli/src/connector_build.rs has drifted from packages/plugin-format/schema/plugin-format.schema.json. \
         Each entry below is fixed by either adding the field to the struct or declaring the \
         omission (with a justification) in cli/plugin-format-mirrors.json:\n{vs:#?}"
    );
}

#[test]
fn every_connector_mirror_struct_is_declared_in_the_manifest() {
    // The scenario the per-file test above is checked against, stated as its
    // own assertion so a manifest mistake elsewhere cannot hide it: each of the
    // connector declaration and lock mirrors must appear in exactly one of
    // `mirrors` / `non_mirrors` for its file. Adding a scratch `Deserialize`
    // struct to `cli/src/connector_build.rs` and leaving it undeclared must
    // fail, which is how `curie dev field-parity` is proved to see this module.
    let src = repo_text("cli/src/connector_build.rs");
    let schema = plugin_format_schema_as_components();
    let manifest = repo_json("cli/plugin-format-mirrors.json");
    let scoped = manifest_for_file(&manifest, "cli/src/connector_build.rs");
    let vs = violations(&src, &schema, &scoped);

    for name in [
        "ConnectorsFileDecl",
        "ConnectorSpecDecl",
        "ConnectorBuildDecl",
        "ConnectorLockFileDecl",
        "ConnectorLockEntryDecl",
    ] {
        assert!(
            !has_undeclared_struct(&vs, name),
            "{name} is a Deserialize mirror in cli/src/connector_build.rs with no \
             mirrors/non_mirrors entry in cli/plugin-format-mirrors.json:\n{vs:#?}"
        );
        assert!(
            src.contains(&format!("struct {name}")),
            "{name} is declared in cli/plugin-format-mirrors.json but does not exist in \
             cli/src/connector_build.rs -- a stale manifest entry hides a real drift"
        );
    }
}

#[test]
fn every_connector_mirror_struct_denies_unknown_fields() {
    // The runtime half of the tolerance gap review finding 9 named. The field
    // vector catches a Python field that never reached the Rust mirror; this
    // catches a real `connectors.yaml` or lock in the wild carrying a field the
    // Rust side does not model, which without the attribute is silently dropped
    // and the operator believes they declared something they did not.
    let src = repo_text("cli/src/connector_build.rs");
    for name in [
        "ConnectorsFileDecl",
        "ConnectorSpecDecl",
        "ConnectorBuildDecl",
        "ConnectorLockFileDecl",
        "ConnectorLockEntryDecl",
    ] {
        let at = src
            .find(&format!("struct {name}"))
            .unwrap_or_else(|| panic!("{name} is defined in cli/src/connector_build.rs"));
        let attrs = &src[at.saturating_sub(400)..at];
        assert!(
            attrs.contains("deny_unknown_fields"),
            "{name} must carry #[serde(deny_unknown_fields)] so an unmodelled key is refused \
             rather than dropped"
        );
    }
}

#[test]
fn approval_gate_and_policy_mirrors_stay_fully_covered() {
    // The issue's named drift class hinges on ApprovalGate/ApprovalPolicy
    // never silently losing coverage across BOTH mirror sites (commands.rs's
    // runtime read path and spec.rs's authoring-write path). Checked
    // independently of the whole-file sweep so a manifest mistake elsewhere
    // cannot hide a regression here.
    let schema = plugin_format_schema_as_components();
    let manifest = repo_json("cli/plugin-format-mirrors.json");

    for (file, structs) in [
        (
            "cli/src/commands.rs",
            vec!["ApprovalGateDecl", "ApprovalPolicyDecl"],
        ),
        (
            "cli/src/spec.rs",
            vec!["ApprovalGateSpec", "ApprovalPolicySpec"],
        ),
    ] {
        let src = repo_text(file);
        let scoped = manifest_for_file(&manifest, file);
        let vs = violations(&src, &schema, &scoped);
        for name in structs {
            assert!(
                !mentions_struct(&vs, name),
                "{name} in {file} does not fully mirror its ApprovalGate/ApprovalPolicy schema \
                 (expected zero omissions, full field coverage):\n{vs:#?}"
            );
        }
    }
}

// ─── Guard-rejects-a-deliberately-introduced-drift demonstration ────────────

#[test]
fn gate_rejects_a_deliberately_introduced_drift() {
    // Proves the gate's OWN wiring (schema wrapping + per-file manifest
    // filtering), not just the shared comparator (already exhaustively
    // fixture-tested by cli/tests/api_field_parity.rs). The real frozen schema
    // is reused unmodified; only the fixture Rust source + manifest drift.
    let src = fixture_text("drifted-mirrors.rs");
    let manifest = fixture_json("drifted-mirrors.json");
    let schema = plugin_format_schema_as_components();

    let vs = violations(&src, &schema, &manifest);
    assert!(
        has_missing_field(&vs, "MissingFieldGate", "route"),
        "expected MissingField for `route`:\n{vs:#?}"
    );
    assert!(
        has_undeclared_struct(&vs, "UndeclaredMirror"),
        "expected UndeclaredStruct for a struct in neither list:\n{vs:#?}"
    );
    assert!(
        has_unknown_field(&vs, "PhantomFieldGate", "ghost_field"),
        "expected UnknownField for a wire field with no schema property behind it:\n{vs:#?}"
    );
}
