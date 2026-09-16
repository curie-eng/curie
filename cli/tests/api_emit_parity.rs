// Emit-hop field-parity gate (issue #699): a `CliOutput::to_json` that
// hand-projects a `cli/src/api.rs` mirror struct into a `serde_json::json!`
// literal must carry every wire field of that struct (or the omission must be
// declared and justified in `cli/api-mirrors.json`'s `emits` array, mirroring
// the `mirrors` array's omissions). One hop downstream of the struct-level
// gate (#691, `cli/tests/api_field_parity.rs`) -- see the module doc on
// `emit_parity::violations` for exactly what this narrower gate does and does
// not catch, and why.

// This binary reuses only `field_parity::walk_structs`/`CollectedStruct` (the
// struct inventory); its own `violations`/`Violation` public surface (that
// binary's entry point) is unused here, hence the blanket allow rather than
// picking through the module's exports one by one.
#[path = "support/emit_parity.rs"]
mod emit_parity;
#[path = "support/field_parity.rs"]
#[allow(dead_code)]
mod field_parity;

use emit_parity::{violations, EmitViolation};
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

/// Every `cli/src/*.rs` file (flat directory, no submodules today), read as
/// (label, contents) pairs -- the corpus the emit-hop gate scans for
/// `CliOutput` impls and the free functions they may delegate a projection to.
/// `cli/src/api.rs` is included too (harmless: it defines no `CliOutput` impl).
fn cli_src_files() -> Vec<(String, String)> {
    let dir = format!("{}/src", env!("CARGO_MANIFEST_DIR"));
    let mut paths = Vec::new();
    collect_rs_paths(std::path::Path::new(&dir), &mut paths);
    paths.sort();
    let mut out = Vec::new();
    for path in paths {
        let label = path.display().to_string();
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {label}: {e}"));
        out.push((label, text));
    }
    assert!(
        !out.is_empty(),
        "expected to find .rs files under {dir}; the glob is misconfigured"
    );
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

// ─── Payload matchers (variant + payload, never message strings) ─────────────

fn has_missing_field(vs: &[EmitViolation], output: &str, field: &str) -> bool {
    vs.iter().any(|v| {
        matches!(v, EmitViolation::MissingField { output: o, field: f, .. }
            if o == output && f == field)
    })
}

fn has_stale_omission(vs: &[EmitViolation], output: &str, field: &str) -> bool {
    vs.iter().any(|v| {
        matches!(v, EmitViolation::StaleOmission { output: o, field: f, .. }
            if o == output && f == field)
    })
}

fn has_output_not_found(vs: &[EmitViolation], output: &str) -> bool {
    vs.iter()
        .any(|v| matches!(v, EmitViolation::OutputNotFound { output: o } if o == output))
}

fn has_struct_not_found(vs: &[EmitViolation], output: &str, struct_name: &str) -> bool {
    vs.iter().any(|v| {
        matches!(v, EmitViolation::StructNotFound { output: o, struct_name: s }
            if o == output && s == struct_name)
    })
}

fn has_malformed_manifest_entry(vs: &[EmitViolation], needle: &str) -> bool {
    vs.iter().any(
        |v| matches!(v, EmitViolation::MalformedManifestEntry { detail } if detail.contains(needle)),
    )
}

// ─── Real-tree assertion ──────────────────────────────────────────────────────

#[test]
fn real_tree_has_no_emit_parity_violations() {
    let api_src = repo_text("cli/src/api.rs");
    let cli_srcs = cli_src_files();
    let cli_srcs_ref: Vec<(&str, &str)> = cli_srcs
        .iter()
        .map(|(l, s)| (l.as_str(), s.as_str()))
        .collect();
    let manifest = repo_json("cli/api-mirrors.json");

    let vs = violations(&api_src, &cli_srcs_ref, &manifest);
    assert!(
        vs.is_empty(),
        "a CliOutput::to_json has drifted from the mirror struct its `emits` entry \
         declares. Each entry below is fixed by either emitting the field or declaring \
         the omission (with a justification) in cli/api-mirrors.json's `emits` array:\n{vs:#?}"
    );
}

// ─── Guard-rejects-a-violating-input demonstrations ───────────────────────────

/// A synthetic `CliOutput` impl that drops `beta` from its `json!` literal, the
/// exact shape of the proof case (`VersionsOutput` dropping `Version::id`).
const DRIFTED_OUTPUT_SRC: &str = r#"
pub enum DriftedOutput {
    Done { record: crate::api::DriftedThing },
}

impl crate::ui::CliOutput for DriftedOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            DriftedOutput::Done { record } => serde_json::json!({
                "alpha": record.alpha,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {}
}
"#;

const DRIFTED_STRUCT_SRC: &str = r#"
#[derive(Debug, Clone, Deserialize)]
pub struct DriftedThing {
    pub alpha: String,
    pub beta: String,
}
"#;

fn drifted_manifest(omissions: Value) -> Value {
    serde_json::json!({
        "emits": [
            { "output": "DriftedOutput", "struct": "DriftedThing", "omissions": omissions }
        ]
    })
}

#[test]
fn rejects_a_dropped_field_the_deliberate_reintroduction_case() {
    // Reintroduces the exact drift class the issue names: a struct field
    // (`beta`) present on the mirror struct but absent from its `to_json`
    // projection, with no declared omission.
    let manifest = drifted_manifest(serde_json::json!([]));
    let vs = violations(
        DRIFTED_STRUCT_SRC,
        &[("drifted_output.rs", DRIFTED_OUTPUT_SRC)],
        &manifest,
    );
    assert!(has_missing_field(&vs, "DriftedOutput", "beta"), "{vs:#?}");
    // The field that IS emitted must not also be flagged.
    assert!(!has_missing_field(&vs, "DriftedOutput", "alpha"), "{vs:#?}");
}

#[test]
fn passes_once_the_drop_is_declared_and_justified() {
    let manifest =
        drifted_manifest(serde_json::json!([{"field": "beta", "why": "not read by any CLI verb"}]));
    let vs = violations(
        DRIFTED_STRUCT_SRC,
        &[("drifted_output.rs", DRIFTED_OUTPUT_SRC)],
        &manifest,
    );
    assert!(vs.is_empty(), "{vs:#?}");
}

#[test]
fn rejects_a_blank_omission_justification() {
    let manifest = drifted_manifest(serde_json::json!([{"field": "beta", "why": ""}]));
    let vs = violations(
        DRIFTED_STRUCT_SRC,
        &[("drifted_output.rs", DRIFTED_OUTPUT_SRC)],
        &manifest,
    );
    assert!(has_stale_omission(&vs, "DriftedOutput", "beta"), "{vs:#?}");
}

#[test]
fn rejects_an_omission_for_a_field_the_struct_does_not_carry() {
    let manifest =
        drifted_manifest(serde_json::json!([{"field": "gamma", "why": "typo'd field name"}]));
    let vs = violations(
        DRIFTED_STRUCT_SRC,
        &[("drifted_output.rs", DRIFTED_OUTPUT_SRC)],
        &manifest,
    );
    assert!(has_stale_omission(&vs, "DriftedOutput", "gamma"), "{vs:#?}");
    // `beta` is still genuinely missing -- the bogus omission for `gamma` must
    // not accidentally suppress the real one.
    assert!(has_missing_field(&vs, "DriftedOutput", "beta"), "{vs:#?}");
}

#[test]
fn rejects_a_stale_omission_for_a_field_that_is_actually_emitted() {
    // `alpha` IS emitted; declaring it omitted is stale regardless of `why`.
    let manifest = drifted_manifest(serde_json::json!([{"field": "alpha", "why": "not read"}]));
    let vs = violations(
        DRIFTED_STRUCT_SRC,
        &[("drifted_output.rs", DRIFTED_OUTPUT_SRC)],
        &manifest,
    );
    assert!(has_stale_omission(&vs, "DriftedOutput", "alpha"), "{vs:#?}");
}

#[test]
fn rejects_an_emits_entry_naming_an_output_not_found() {
    let manifest = serde_json::json!({
        "emits": [{ "output": "GhostOutput", "struct": "DriftedThing", "omissions": [] }]
    });
    let vs = violations(
        DRIFTED_STRUCT_SRC,
        &[("drifted_output.rs", DRIFTED_OUTPUT_SRC)],
        &manifest,
    );
    assert!(has_output_not_found(&vs, "GhostOutput"), "{vs:#?}");
}

#[test]
fn rejects_an_emits_entry_naming_a_struct_not_found() {
    let manifest = serde_json::json!({
        "emits": [{ "output": "DriftedOutput", "struct": "NoSuchStruct", "omissions": [] }]
    });
    let vs = violations(
        DRIFTED_STRUCT_SRC,
        &[("drifted_output.rs", DRIFTED_OUTPUT_SRC)],
        &manifest,
    );
    assert!(
        has_struct_not_found(&vs, "DriftedOutput", "NoSuchStruct"),
        "{vs:#?}"
    );
}

#[test]
fn rejects_an_emits_entry_missing_a_required_key() {
    let manifest = serde_json::json!({
        "emits": [{ "output": "DriftedOutput", "omissions": [] }]
    });
    let vs = violations(
        DRIFTED_STRUCT_SRC,
        &[("drifted_output.rs", DRIFTED_OUTPUT_SRC)],
        &manifest,
    );
    assert!(
        has_malformed_manifest_entry(&vs, "DriftedOutput"),
        "{vs:#?}"
    );
}

#[test]
fn follows_a_projection_delegated_to_a_named_free_function() {
    // Mirrors the real `approval_record_json` pattern: `to_json` never writes
    // the `json!` literal itself, it maps a named free fn over the collection.
    // Reachability must follow that fn to find its `json!` literal.
    const DELEGATING_OUTPUT_SRC: &str = r#"
pub enum DriftedOutput {
    Done { records: Vec<crate::api::DriftedThing> },
}

fn drifted_thing_json(r: &crate::api::DriftedThing) -> serde_json::Value {
    serde_json::json!({
        "alpha": r.alpha,
        "beta": r.beta,
    })
}

impl crate::ui::CliOutput for DriftedOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            DriftedOutput::Done { records } => serde_json::json!({
                "records": records.iter().map(drifted_thing_json).collect::<Vec<_>>(),
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {}
}
"#;
    let manifest = drifted_manifest(serde_json::json!([]));
    let vs = violations(
        DRIFTED_STRUCT_SRC,
        &[("delegating_output.rs", DELEGATING_OUTPUT_SRC)],
        &manifest,
    );
    assert!(vs.is_empty(), "{vs:#?}");
}
